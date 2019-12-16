"""
AOU - De-identification Engine
Steve L. Nyemba <steve.l.nyemba@vanderbilt.edu>

This engine will run de-identificataion rules againsts a given table, certain
rules are applied to all tables (if possible).  We have divided rules and application
of the rules, in order to have granular visibility into what is happening.

DESIGN:

    - The solution:
    The Application of most of the rules are handled in the SQL projection, this
    allows for simpler jobs with no risk of limitations around joins imposed by big-query.
    By dissecting operations in this manner it is possible to log the nature of an
    operation on a given attribute as well as pull some sample data to illustrate it.

    We defined a vocabulary of rule specifications:
        -fields         Attributes upon which a given rule can be applied
        -values         conditional values that determine an out-come of a rule (can be followed by an operation like REGEXP)
                            If followed by "apply": "REGEXP" the the values are assumed to be applied using regular expressions
                            If NOT followed by anything the values are assumed to be integral values and and the IN operator in used instead
        -into           outcome related to a rule
        -key_field      attribute to be used as a filter that can be specified by value_field or values
        -value_field    value associated with a key_field
        -on             suggests a meta table and will have filter condition when generalization or a field name for row based suppression
        -copy_to        defined for generalizations.  uses the values associated with one field
                        to update another field.  to the user, it acts like a "copy new value to this field too" rule
        -dataset        defined for generalizations using aggregating rules.  helps develop the sub-query used
                        by the aggregate.  ':idataset' is default.  uses the dataset name
        -alias          defined for generalizations using aggregating rules.  allows a sub-query to
                        construct a temporary table named by this field
        -key_row        defined for generlizations using aggregating rules.  allows a sub-query to
                        define which field to use for joins


    Overall there are rules that suggest what needs to happen on values, and there
    is a fine specifying how to apply the rule on a given table.

    - The constraints:

        1. Bigquery is designed to be used as a warehouse not an RDBMS.That being said
            a. it lends itself to uncontrollable information redundancies and proliferation.
            b. There is not referential integrity support in bigquery.
            As a result thre is no mechanism that guarantees data-integrity.


        2. We have a method "simulate" that acts as a sampler to provide some visibility
        into what this engine has done given an attribute and the value space of the data.
        This potentially adds to data redundancies.  It must remain internal.

    LIMITATIONS:
        - The engine is not able to validate the rules without having to submit the job.
        - The engine can not simulate complex cases.  It's intent is to help in
        providing information about basic scenarios,
        - The engine does not resolve issues of consistency with data for instance:
        if a record has M, F on two fields for gender ... this issue is out of the scope of deid.
        Because it relates to data-integrity.

    USAGE:

        python aou.py --rules <path.json> --idataset <name> --private_key <file> --table <table.json> --action [submit, simulate|debug] [--cluster] [--log <path>]
        --rule  will point to the JSON file contianing rules
        --idataset  name of the input dataset (an output dataset with suffix _deid will be generated)
        --table     path of that specify how rules are to be applied on a table
        --private_key   service account file location
        --pipeline      specifies operations and the order in which operations are to be undertaken.
                        Operations should be comma separated
                        By default the pipeline is generalize, suppress, shift, compute
        --age-limit     This parameter is optional and sets the age limit by default it will apply 89 years
        --action        what to do:
                        simulate    will generate simulation without creating an output table
                        submit      will create an output table
                        debug       will just print output without simulation or submit (runs alone)
"""
# Python imports
import json
import logging
import os
import time
from copy import copy
from datetime import datetime

# Third party imports
import numpy as np
import pandas as pd
from google.cloud import bigquery as bq
from google.oauth2 import service_account

# Project imports
import bq_utils
import constants.bq_utils as bq_consts
from deid.parser import parse_args
from deid.press import Press
from resources import DEID_PATH

LOGGER = logging.getLogger(__name__)

def milliseconds_since_epoch():
    """
    Helper method to get the number of milliseconds from the epoch to now

    :return:  an integer number of milliseconds
    """
    return int((datetime.utcnow() - datetime(1970, 1, 1)).total_seconds() * 1000)


def create_participant_mapping_table(table, map_tablename, lower_bound, max_day_shift, credentials):
    # create the deid_map table.  set upper and lower bounds of the research_id array
    records = table.shape[0]
    upper_bound = lower_bound + (10 * records)
    map_table = pd.DataFrame({"person_id": table['person_id'].tolist()})

    # generate random research_ids
    research_id_array = np.random.choice(np.arange(lower_bound, upper_bound), records, replace=False)

    # throw in some extra, non-deterministic shuffling
    for _ in range(milliseconds_since_epoch() % 5):
        np.random.shuffle(research_id_array)
    map_table['research_id'] = research_id_array

    # generate date shift values
    shift_array = np.random.choice(np.arange(1, max_day_shift), records)

    # throw in some extra, non-deterministic shuffling
    for _ in range(milliseconds_since_epoch() % 5):
        np.random.shuffle(shift_array)
    map_table['shift'] = shift_array

    # write this to bigquery.
    map_table.to_gbq(map_tablename, credentials=credentials, if_exists='fail')
    LOGGER.info('created new patient mapping table')


def create_person_id_src_hpo_map(input_dataset, credentials):
    """
    Create a table containing person_ids and src_hpo_ids

    :param input_dataset:  the input dataset to deid
    :param credentidals:  the credentials needed to create a new table.
    """
    map_tablename = "_mapping_person_src_hpos"
    sql = ("select person_id, src_hpo_id "
           "from {input_dataset}._mapping_{table} "
           "join {input_dataset}.{table} "
           "using ({table}_id) "
           "where src_hpo_id not like 'rdr'"
          )

    # list dataset contents
    dataset_tables = bq_utils.list_dataset_contents(input_dataset)
    mapping_tables = []
    mapped_tables = []
    for table in dataset_tables:
        if table.startswith('_mapping_'):
            mapping_tables.append(table)
            mapped_tables.append(table[9:])

    # make sure mapped tables all exist
    check_tables = []
    for table in mapped_tables:
        if table in dataset_tables:
            check_tables.append(table)

    # make sure check_tables contain person_id fields
    person_id_tables = []
    for table in check_tables:
        info = bq_utils.get_table_info(table, dataset_id=input_dataset)
        schema = info.get('schema', {})
        for field_info in schema.get('fields', []):
            if 'person_id' in field_info.get('name'):
                person_id_tables.append(table)

    # revamp mapping tables to contain only mapping tables for tables
    # with person_id fields
    mapping_tables = ['_mapping_' + table for table in person_id_tables]

    sql_statement = []
    for table in person_id_tables:
        sql_statement.append(sql.format(table=table, input_dataset=input_dataset))

    final_query = ' UNION ALL '.join(sql_statement)

    # create the mapping table
    if map_tablename not in dataset_tables:
        fields = [
            {
                "type": "integer",
                "name": "person_id",
                "mode": "required",
                "description": "the person_id of someone with an ehr record"
            },
            {
                "type": "string",
                "name": "src_hpo_id",
                "mode": "required",
                "description": "the src_hpo_id of an ehr record"
            }
        ]
        bq_utils.create_table(map_tablename,
                              fields,
                              dataset_id=input_dataset)

    bq_utils.query(final_query,
                   destination_table_id=map_tablename,
                   destination_dataset_id=input_dataset,
                   write_disposition=bq_consts.WRITE_TRUNCATE)
    LOGGER.info("Created mapping table:\t%s.%s", input_dataset, map_tablename)


def create_allowed_states_table(input_dataset, credentials):
    """
    Create a mapping table of src_hpos to states they are located in.
    """

    map_tablename = input_dataset + "._mapping_src_hpos_to_allowed_states"
    data = pd.read_csv(
        os.path.join(DEID_PATH, 'config', 'internal_tables', 'src_hpos_to_allowed_states.csv')
    )

    # write this to bigquery.
    data.to_gbq(map_tablename, credentials=credentials, if_exists='replace')


def create_questionnaire_mapping_table(table, map_tablename, lower_bound, credentials, exists_flag=None):
    """
    Create a random mapping table for questionnaire response ids.

    :param lower_bound:  The smallest number that may be used as an identifier.
    """
    if not exists_flag:
        exists_flag = 'fail'

    # create the deid table.  set upper and lower bounds of the research_id array
    records = table.shape[0]
    upper_bound = lower_bound + (10 * records)
    qr_id_list = table['questionnaire_response_id'].tolist()
    map_table = pd.DataFrame({"questionnaire_response_id": qr_id_list})

    # generate random research_questionnaire_response_ids
    research_id_array = np.random.choice(np.arange(lower_bound, upper_bound), records, replace=False)

    # throw in some extra, non-deterministic shuffling
    for _ in range(milliseconds_since_epoch() % 5):
        np.random.shuffle(research_id_array)
    map_table['research_response_id'] = research_id_array

    # write this to bigquery.
    map_table.to_gbq(map_tablename, credentials=credentials, if_exists=exists_flag)
    LOGGER.info('created new questionnaire response mapping table')


class AOU(Press):
    def __init__(self, **args):
        args['store'] = 'bigquery'
        Press.__init__(self, **args)
        self.private_key = args.get('private_key', '')
        self.credentials = service_account.Credentials.from_service_account_file(self.private_key)
        self.odataset = self.idataset + '_deid'
        self.partition = args.get('cluster', False)
        self.priority = args.get('interactive', 'BATCH')

        if 'shift' in self.deid_rules:
            #
            # Minor updates that are the result of a limitation as to how rules are specified.
            # @TODO: Improve the rule specification language
            shift_days = ('SELECT shift from {idataset}._deid_map '
                          'WHERE _deid_map.person_id = {tablename}.person_id'
                          .format(idataset=self.idataset, tablename=self.tablename))
            self.deid_rules['shift'] = json.loads(json.dumps(self.deid_rules['shift']).replace(":SHIFT", shift_days))

    def initialize(self, **args):
        Press.initialize(self, **args)
        LOGGER.info('BEGINNING de-identification on table:\t%s', self.tablename)

        age_limit = args['age_limit']
        max_day_shift = args['max_day_shift']
        million = 1000000
        map_tablename = self.idataset + "._deid_map"

        sql = ("SELECT DISTINCT person_id, EXTRACT(YEAR FROM CURRENT_DATE()) - year_of_birth as age "
               "FROM person ORDER BY 2")
        job_config = {
            'query': {
                'defaultDataset': {
                    'datasetId': self.idataset
                }
            }
        }
        person_table = self.get_dataframe(sql=sql, query_config=job_config)
        LOGGER.info('patient count is:\t%s', person_table.shape[0])
        map_table = pd.DataFrame()

        # only need to create these tables deidentifying the observation table
        if 'observation' in self.get_tablename().lower().split('.'):
            create_allowed_states_table(self.idataset, self.credentials)
            self.map_questionnaire_response_ids(million)
            create_person_id_src_hpo_map(self.idataset, self.credentials)

        if person_table.shape[0] > 0:
            person_table = person_table[person_table.age < age_limit]
            map_sql = 'SELECT * FROM ' + map_tablename
            job_config = {
                'query': {
                    'defaultDataset': {
                        'datasetId': self.idataset
                    }
                }
            }
            map_table = self.get_dataframe(sql=map_sql, query_config=job_config)
            if map_table.shape[0] > 0:
                # Make sure the mapping table is mapping the expected data
                map_table_set = set(map_table.loc[:, 'person_id'].tolist())
                person_set = set(person_table.loc[:, 'person_id'].tolist())
                if map_table_set == person_set:
                    LOGGER.info('participant mapping table contains '
                                'all person ids.  continuing...')
                else:
                    LOGGER.info("creating new participant mapping table because the current mapping table doesn't match")
                    create_participant_mapping_table(
                        person_table, map_tablename, million, max_day_shift, self.credentials
                    )
            else:
                LOGGER.info("creating new participant mapping table because one doesn't exist")
                create_participant_mapping_table(
                    person_table, map_tablename, million, max_day_shift, self.credentials
                )
        else:
            LOGGER.error("Unable to initialize Deid.  Check "
                         "configuration files, parameters, and credentials.")

        LOGGER.info('map table contains %d participants.', map_table.shape[0])
        return person_table.shape[0] > 0 or map_table.shape[0] > 0

    def map_questionnaire_response_ids(self, lower_bound):
        """
        Create a random mapping table for questionnaire response ids.

        :param lower_bound:  The smallest number that may be used as an identifier.
        """
        map_tablename = self.idataset + "._deid_questionnaire_response_map"
        sql = ("SELECT DISTINCT o.questionnaire_response_id "
               "FROM observation as o "
               "WHERE o.questionnaire_response_id IS NOT NULL "
               "ORDER BY 1")
        job_config = {
            'query': {
                'defaultDataset': {
                    'datasetId': self.idataset
                }
            }
        }
        observation_table = self.get_dataframe(sql=sql, query_config=job_config)
        LOGGER.info('total of distinct questionnaire_response_ids:\t%d',
                    observation_table.shape[0])
        map_table = pd.DataFrame()

        if observation_table.shape[0] > 0:
            map_sql = 'SELECT * FROM ' + map_tablename
            job_config = {
                'query': {
                    'defaultDataset': {
                        'datasetId': self.idataset
                    }
                }
            }
            map_table = self.get_dataframe(sql=map_sql, query_config=job_config)
            if map_table.shape[0] > 0:
                # Make sure the mapping table is mapping the expected data
                map_table_set = set(map_table.loc[:, 'questionnaire_response_id'].tolist())
                observation_set = set(observation_table.loc[:, 'questionnaire_response_id'].tolist())
                if map_table_set == observation_set:
                    LOGGER.info('questionnaire response mapping table contains '
                                'all questionnaire response ids')
                else:
                    LOGGER.warning('creating new questionnaire response mapping '
                                   'table because the existing table doesn\'t match')
                    # The tables do NOT contain the same mapped elements
                    create_questionnaire_mapping_table(
                        observation_table, map_tablename, lower_bound, self.credentials, 'replace'
                    )
            else:
                LOGGER.info('creating a new questionnaire response mapping '
                            'table because it doesn\'t exist')
                # create the deid table.  set upper and lower bounds of the research_id array
                create_questionnaire_mapping_table(
                    observation_table, map_tablename, lower_bound, self.credentials
                )
        else:
            LOGGER.error("No questionnaire_response_ids found.")

        LOGGER.info('questionnaire response mapping table contains %d records',
                    map_table.shape[0])

    def get_dataframe(self, sql=None, limit=None, query_config=None):
        """
        This function will execute a query to a data-frame (for easy handling)
        """
        if sql is None:
            sql = ("SELECT * FROM {idataset}.{tablename}"
                   .format(idataset=self.idataset, tablename=self.tablename))

        if limit:
            sql = sql + " LIMIT " + str(limit)

        try:
            if query_config:
                df = pd.read_gbq(sql, credentials=self.credentials, dialect='standard', configuration=query_config)
            else:
                df = pd.read_gbq(sql, credentials=self.credentials, dialect='standard')

            return df
        except Exception:
            LOGGER.exception("Unable to execute the query:\t%s", sql)

        return pd.DataFrame()

    def _add_suppression_rules(self, columns):
        """
        Adding suppression rules that should always exist.

        :param columns: a list of column names for the given table
        """
        if 'suppress' not in self.table_info:
            self.table_info['suppress'] = []
        #
        # Relational attributes that require suppression are inventoried this allows automatic suppression
        # It's done to make the engine more usable (by minimizing the needs for configuration)
        #
        rem_cols = True
        for rule in self.table_info['suppress']:
            if 'rules' in rule and rule['rules'].endswith('DEMOGRAPHICS-COLUMNS'):
                rem_cols = False
                rule['table'] = self.get_tablename()
                rule['fields'] = columns

        if rem_cols:
            self.table_info['suppress'].append(
                {
                    "rules": "@suppress.DEMOGRAPHICS-COLUMNS",
                    "table": self.get_tablename(),
                    "fields": columns
                }
            )
            LOGGER.info('added demographics-columns suppression rules')

    def _add_temporal_shifting_rules(self, columns):
        """
        Add default date shifting rules, if not specified in the config file.

        :param columns: a list of column names for the given table
        """
        date_columns = []
        for name in columns:
            for temporal in ['date', 'time', 'datetime']:
                if temporal in name.split('_'):
                    date_columns.append(name)
        #
        # shifting date attributes can be automatically done for relational fields
        # by looking at the field name of the data-type
        # @TODO: consider looking at the field name, changes might happen all of a sudden
        #
        if date_columns:
            date = {}
            datetime_field = {}

            for name in date_columns:
                # check covers 'datetime' fields by default,
                # they already have 'time' in the name
                if 'time' in name:
                    if 'fields' not in datetime_field:
                        datetime_field['fields'] = []
                    datetime_field['fields'].append(name)
                    datetime_field['rules'] = '@shift.datetime'
                elif 'date' in name:
                    if 'fields' not in date:
                        date['fields'] = []

                    date['fields'].append(name)
                    date['rules'] = '@shift.date'
            _toshift = []

            if date and datetime_field:
                _toshift = [date, datetime_field]
            elif date or datetime_field:
                _toshift = [date] if date else [datetime_field]

            if 'shift' not in self.table_info:
                self.table_info['shift'] = []

            if _toshift:
                LOGGER.info('shifting fields:\t%s', _toshift)
                self.table_info['shift'] += _toshift

    def _add_compute_rules(self, columns):
        """
        Add default value mapping rules, if not specified in the config file.

        :param columns: a list of column names for the given table
        """
        # check if person_id mapping has already been configured
        needs_compute_id = True
        if 'compute' not in self.table_info:
            self.table_info['compute'] = []
        else:
            for rule_dict in self.table_info.get('compute', {}):
                if '@compute.id' in rule_dict.get('rules'):
                    needs_compute_id = False

        # if a person_id column exists and a mapping has not been configured, add one
        if needs_compute_id and 'person_id' in columns:
            self.table_info['compute'].append(
                {
                    "rules": "@compute.id",
                    "fields": ["person_id"],
                    "table": ":idataset._deid_map as map_user",
                    "key_field": "map_user.person_id",
                    "value_field": self.tablename + ".person_id"
                }
            )

    def _add_dml_statements_rules(self, columns):
        """
        Add default duplicate records dropping configuration, if not specified in the config file.
        """
        if 'generalize' in self.table_info:
            gen_rules = {}
            # pull the where clause information for generalization rules
            for item in self.table_info['generalize']:
                name = item.get('rules').split('.')[1]
                gen_rules[name] = item.get('on')

            # pull the rules that may have duplicates
            drop_duplicates_rules = {}
            for key, rule_list in self.deid_rules.get('generalize').items():
                rule_generalizations = []
                if isinstance(rule_list, list):
                    for rule in rule_list:
                        if rule.get('drop_duplicates', 'no').lower() in ['true', 't', 'yes', 'y']:
                            rule_generalizations.append(rule.get('into'))
                            drop_duplicates_rules[key] = rule_generalizations

            # create lists for the generalized values and where clauses
            values_to_drop_on = []
            generalized_multiple_values = []
            for drop_rule, drop_values in drop_duplicates_rules.items():
                on_dict = gen_rules.get(drop_rule)
                on_values = on_dict.get('values')
                values_to_drop_on.extend(on_values)
                generalized_multiple_values.extend(drop_values)

            # set a dml statement to execute with the generalized info
            self.table_info['dml_statements'] = [
                {
                    'rules': '@dml_statements.' + self.tablename,
                    'tablename': self.tablename,
                    'generalized_values': generalized_multiple_values,
                    'key_values': values_to_drop_on
                }
            ]

            self.pipeline.append('dml_statements')


    def update_rules(self):
        """
        This will add rules that are to be applied by default to the current table
        @TODO: Make sure there's a way to specify these in the configuration
        """
        columns = self.get_table_columns(self.tablename)

        self._add_suppression_rules(columns)
        self._add_temporal_shifting_rules(columns)
        self._add_compute_rules(columns)
        self._add_dml_statements_rules(columns)

    def submit(self, sql, create, dml=None):
        """
        Submit the sql query to create a de-identified table.

        :param sql:  The sql to send.
        :param create: a flag to identify if this query should create a new
            table or append to an existing table.
        :param dml:  boolean flag identifying if a statement is a dml statement
        """
        dml = False if dml is None else dml
        table_name = self.get_tablename()
        client = bq.Client.from_service_account_json(self.private_key)
        #
        # Let's make sure the out dataset exists
        datasets = list(client.list_datasets())
        found = np.sum([1  for dataset in datasets if dataset.dataset_id == self.odataset])
        if not found:
            dataset = bq.Dataset(client.dataset(self.odataset))
            client.create_dataset(dataset)

        # create the output table
        if create:
            LOGGER.info('creating new table:\t%s', self.tablename)
            bq_utils.create_standard_table(
                self.tablename, self.tablename, drop_existing=True, dataset_id=self.odataset
            )
            write_disposition = bq_consts.WRITE_EMPTY
        else:
            write_disposition = bq_consts.WRITE_APPEND
            LOGGER.info('appending results to table:\t%s', self.tablename)

        job = bq.QueryJobConfig()
        job.priority = self.priority
        job.dry_run = True

        dml_job = None
        if not dml:
            job.destination = client.dataset(self.odataset).table(self.tablename)
            job.use_query_cache = True
            job.allow_large_results = True
            job.write_disposition = write_disposition
            if self.partition:
                job._properties['timePartitioning'] = {'type': 'DAY'}
                job._properties['clustering'] = {'field': 'person_id'}
        else:
            # create a copy of the job config to use if the dry-run passes
            dml_job = copy(job)

        LOGGER.info('submitting a dry-run for:\t%s\t\tpriority:\t%s\t\tpartition:\t%s',
                    self.get_tablename(),
                    self.priority,
                    self.partition)

        logpath = os.path.join(self.logpath, self.idataset)
        try:
            os.makedirs(logpath)
        except OSError:
            # log path already exists and we don't care
            pass

        try:
            response = client.query(sql, location='US', job_config=job)
        except Exception:
            LOGGER.exception('dry run query failed for:\t%s\n'
                             '\t\tSQL:\t%s\n'
                             '\t\tjob config:\t%s',
                             self.get_tablename(),
                             sql,
                             job)
        else:

            if response.state == 'DONE':
                if dml_job:
                    job = dml_job

                job.dry_run = False

                LOGGER.info('dry-run passed.  submitting query for execution.')

                response = client.query(sql, location='US', job_config=job)
                LOGGER.info('submitted a %s job for table:\t%s\t\tstatus:\t%s\t\tvalue:\t%s',
                            'bigquery',
                            table_name,
                            'pending',
                            response.job_id)
                self.wait(client, response.job_id)

    def wait(self, client, job_id):
        """
        Wait for the query to finish executing.

        :param client:  The BigQuery client object.
        :param job_id:  job_id to verify finishes.
        """
        LOGGER.info('sleeping for table:\t%s\t\tjob_id:\t%s', self.get_tablename(), job_id)
        status = 'NONE'

        while True:
            status = client.get_job(job_id).state

            if status == 'DONE':
                break
            else:
                time.sleep(5)

        LOGGER.info('awake.  status is:\t%s', status)


def main(raw_args=None):
    """
    Run the de-identifying software.

    Entry point for de-identification.  Setting the main this way allows the
    module to run as a stand alone script or as part of the pipeline.
    """
    sys_args = parse_args(raw_args)

    handle = AOU(**sys_args)

    if handle.initialize(age_limit=sys_args.get('age-limit'), max_day_shift=365):
        handle.do()
    else:
        print ("Unable to initialize process ")
        print ("\tEnsure that the parameters are correct")


if __name__ == '__main__':
    main()
