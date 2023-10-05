"""
This class applies rules and meta data to yield a certain output
"""
# Python imports
import codecs
import json
import logging
import os
from abc import ABC, abstractmethod
from datetime import datetime

# Third party imports
import pandas as pd
import numpy as np

# Project imports
from gcloud.bq import BigQueryClient
import app_identity
from resources import fields_for
from deid.rules import Deid, create_on_string
from utils import auth
from common import CDR_SCOPES

LOGGER = logging.getLogger(__name__)


def set_up_logging(log_path, idataset):
    """
    Set up python logging, if not previously set up.

    If not previously set up, creates a python logger.  If python logging exists,
    adds a file handler for deid.

    :param log_path: desired string path of the log file.
    :param idataset:  input dataset string name.  addded to the log_path to create
        the output log file location.
    """
    output_log_path = os.path.join(log_path, idataset)
    try:
        os.makedirs(output_log_path)
    except OSError:
        # directory already exists.  move on.
        pass

    name = datetime.now().strftime('logs/deid-%Y-%m-%d.log')
    filename = os.path.join(log_path, name)
    file_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

    root_logger = logging.getLogger('')

    if root_logger.handlers:
        # Add file handler if logging is configured
        file_handler = logging.FileHandler(name)
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(logging.Formatter(file_format))
        root_logger.addHandler(file_handler)
    else:
        # Create basic config if logging isn't configured
        logging.basicConfig(filename=filename,
                            level=logging.INFO,
                            format=file_format)


class Press(ABC):

    def __init__(self, **args):
        """
        :rules_path  to the rule configuration file
        :info_path   path to the configuration of how the rules get applied
        :pipeline   operations and associated sequence in which they should be performed
        """
        self.idataset = args.get('idataset', '')
        self.odataset = args.get('odataset', '')
        self.tablepath = args.get('table')
        self.run_as_email = args.get('run_as_email', '')
        self.credentials = auth.get_impersonation_credentials(
            self.run_as_email, CDR_SCOPES)
        self.tablename = os.path.basename(
            self.tablepath).split('.json')[0].strip()
        self.project_id = app_identity.get_application_id()
        self.bq_client = BigQueryClient(project_id=self.project_id,
                                        credentials=self.credentials)

        self.logpath = args.get('logs', 'logs')
        set_up_logging(self.logpath, self.idataset)

        with codecs.open(args.get('rules'), 'r') as config:
            self.deid_rules = json.loads(config.read())

        self.pipeline = args.get('pipeline',
                                 ['generalize', 'suppress', 'shift', 'compute'])
        try:
            with codecs.open(self.tablepath, 'r') as config:
                self.table_info = json.loads(config.read())
        except (OSError, TypeError, ValueError):
            # In case a table name is not provided, we will apply default rules on he table
            #   I.e physical field suppression and row filter
            #   Date Shifting
            self.table_info = {}

        if isinstance(self.deid_rules, list):
            cache = {}
            for row in self.deid_rules:
                _id = row['_id']
                cache[_id] = row
            self.deid_rules = cache

        self.store = args.get('store', 'sqlite')

        if 'suppress' not in self.deid_rules:
            self.deid_rules['suppress'] = {'FILTERS': []}

        if 'FILTERS' not in self.deid_rules['suppress']:
            self.deid_rules['suppress']['FILTERS'] = []

        self.action = [term.strip() for term in args['action'].split(',')
                      ] if 'action' in args else ['submit']

    def initialize(self, **args):
        #
        # Let us update and see if the default filters apply at all
        dfilters = []
        columns = self.get_table_columns(self.tablename)

        for row in self.deid_rules['suppress']['FILTERS']:
            if set(columns) & set(row['filter'].split(' ')):
                dfilters.append(row)
        self.deid_rules['suppress']['FILTERS'] = dfilters

    def get_table_columns(self, tablename):
        """
        Return a list of columns for the given table name.
        """
        table_obj = self.bq_client.get_table(f'{self.idataset}.{tablename}')

        field_names = []
        for field in table_obj.schema:
            field_names.append(field.name)

        return field_names

    @abstractmethod
    def get_dataframe(self, sql=None, limit=None):
        """
        This function will execute an SQL statement and return the meta data for a given table
        """
        pass

    @abstractmethod
    def submit(self, sql, create, dml=None):
        """
        Should be overridden by subclasses.
        """
        pass

    @abstractmethod
    def update_rules(self):
        """
        Should be overridden by subclasses.
        """
        pass

    def do(self):
        """
        This function actually runs deid and using both rule specifications and application of the rules
        """
        self.update_rules()
        d = Deid(pipeline=self.pipeline, rules=self.deid_rules, parent=self)

        p = d.apply(self.table_info, self.store, self.get_tablename())

        is_meta = np.sum([1 * ('on' in _item) for _item in p]) != 0
        LOGGER.info(
            f"table:\t{self.get_tablename()}\t\tis a meta table:\t{is_meta}")
        if not is_meta:
            sql = [self.to_sql(p)]
            _rsql = None
            dml_sql = []
        else:
            #
            # Processing meta tables
            sql = []
            relational_cols = [col for col in p if 'on' not in col]
            meta_cols = [col for col in p if 'on' in col]

            _map = {}
            for col in meta_cols:
                on_string, _ = create_on_string(col['on'])
                if on_string not in _map:
                    _map[on_string] = {'specification': [], 'on': {}}
                _map[on_string]['specification'] += [col]
                _map[on_string]['on'] = col['on']

            fillter = []
            for filter_id in _map:
                item = _map.get(filter_id, {}).get('specification', [])
                item_filter = _map.get(filter_id, {}).get('on', {})
                fillter.append(item_filter)

                _sql = self.to_sql(item + relational_cols) + ' AND ' + filter_id

                sql.append(_sql)

            _rsql = self.gather_unfiltered_records(relational_cols, fillter)

            sql.append(_rsql)

            # create additional SQL cleaning statements
            dml_sql = self.gather_dml_queries(p)

            for index, segment in enumerate(sql):
                formatted = segment.replace(':idataset', self.idataset)
                sql[index] = formatted.replace(':join_tablename',
                                               self.tablename)

        if 'debug' in self.action:
            self.debug(p)
        else:
            # write SQL to file
            sql_filepath = os.path.join(self.logpath, self.idataset,
                                        self.tablename + '.sql')
            with open(sql_filepath, 'w') as sql_file:
                final_sql = "\n\nAppend these results to previous results\n\n".join(
                    sql)
                sql_file.write(final_sql)

                if dml_sql:
                    sql_file.write(
                        '\n\nDML SQL statements to execute on de-identified table data\n\n'
                    )
                    final_sql = '\n\n  ----------------------------------\n\n'.join(
                        dml_sql)
                    sql_file.write(final_sql)

            if 'submit' in self.action:
                for index, statement in enumerate(sql):
                    self.submit(statement, not index)

                for statement in dml_sql:
                    self.submit(statement, False, dml=True)

            if 'simulate' in self.action:
                #
                # Make this threaded if there is a submit action that is associated with it
                self.simulate(p)

        LOGGER.info(f"FINISHED de-identification on table:\t{self.tablename}")

    def get_tablename(self):
        return f'{self.idataset}.{self.tablename}' if self.idataset else self.tablename

    def debug(self, info):
        for row in info:
            print()
            print(row['label'], not row['apply'])
            print()

    def simulate(self, info):
        """
        This function will attempt to log the various transformations on every field.

        This will simulate and provide output on possible transformations.
        :info   payload of that has all the transformations applied to a given table as follows
                [{apply, label, name}] where
                    - apply is the SQL to be applied
                    - label is the flag for the operation (generalize, suppress, compute, shift)
                    - name  is the attribute name on which the rule gets applied
        """
        table_name = f'{self.idataset}.{self.tablename}'
        suppression_filters = self.deid_rules['suppress']['FILTERS']
        out = pd.DataFrame()
        counts = {}
        dirty_date = False
        filters = []

        for item in info:
            labels = item['label'].split('.')

            if not (set(labels) & set(self.pipeline)):
                LOGGER.info(
                    f"Skipping simulation for table:\t{table_name}\t\tvalues:\t{labels}"
                )
                continue

            if labels[0] not in counts:
                counts[labels[0].strip()] = 0

            counts[labels[0].strip()] += 1

            if 'suppress' in labels or item['name'] == 'person_id' or dirty_date:
                continue

            field = item['name']
            alias = f'original_{field}'
            sql_list = [
                "SELECT DISTINCT ", field, 'AS ', alias, ",", item['apply'],
                " FROM ", table_name
            ]

            if suppression_filters:
                sql_list.append('WHERE')

                for row in suppression_filters:
                    sql_list.append(row['filter'])

                    if suppression_filters.index(
                            row) < len(suppression_filters) - 1:
                        sql_list.append('AND')

            if 'on' in item:
                # This applies to meta tables
                filters.append(item['on'])
                if suppression_filters:
                    sql_list.extend(['AND', item['on']])
                else:
                    sql_list.extend(['WHERE ', item['on']])

            if 'shift' in labels:
                data_frame = self.get_dataframe(sql=" ".join(sql_list).replace(
                    ':idataset', self.idataset),
                                                limit=5)
            else:
                data_frame = self.get_dataframe(
                    sql=" ".join(sql_list).replace(':idataset', self.idataset))

            if data_frame.shape[0] == 0:
                LOGGER.info(
                    f"no data-found for simulation of table:\t{table_name}\t\t"
                    f"field:\t{field}\t\ttype:\t{item['label']}")
                continue

            data_frame.columns = ['original', 'transformed']
            data_frame['attribute'] = field
            data_frame['task'] = item['label'].upper().replace('.', ' ')
            out = out.append(data_frame)
        #-- Let's evaluate row suppression here
        #
        out.index = range(out.shape[0])
        rdf = pd.DataFrame()
        if suppression_filters:
            filters += [
                item['filter']
                for item in suppression_filters
                if 'filter' in item
            ]
            original_sql = ' (SELECT COUNT(*) as original FROM :table) AS ORIGINAL_TABLE ,'
            original_sql = original_sql.replace(':table', table_name)
            transformed_sql = '(SELECT COUNT(*) AS transformed FROM :table WHERE :filter) AS TRANSF_TABLE'
            transformed_sql = transformed_sql.replace(':table', table_name)
            transformed_sql = transformed_sql.replace(':filter',
                                                      " OR ".join(filters))
            sql_list = ['SELECT * FROM ', original_sql, transformed_sql]

            r = self.get_dataframe(
                sql=" ".join(sql_list).replace(":idataset", self.idataset))
            table_name = f'{self.idataset}.{self.tablename}'

            rdf = pd.DataFrame({
                "operation": ["row-suppression"],
                "count": r.transformed.tolist()
            })

        now = datetime.now()
        flag = "-".join(
            np.array([now.year, now.month, now.day,
                      now.hour]).astype(str).tolist())

        root = os.path.join(self.logpath, self.idataset, flag)
        try:
            os.makedirs(root)
        except OSError:
            # directory already exists.  move on.
            pass

        stats = pd.DataFrame({
            "operation": counts.keys(),
            "count": counts.values()
        })
        stats = stats.append(rdf)
        stats.index = range(stats.shape[0])
        stats.reset_index()

        _map = {
            os.path.join(root, f'samples-{self.tablename}.csv'): out,
            os.path.join(root, f'stats-{self.tablename}.csv'): stats
        }
        for path in _map:
            _data_frame = _map[path]
            _data_frame.to_csv(path)

        LOGGER.info(
            f"simulation completed for table:\t{table_name}\t\tvalue:\t{root}")

    def to_sql(self, info):
        """
        Create an SQL query from table information and config rules.

        :info   payload with information of the process to be performed

        :return: An SQL query
        """
        table_name = self.get_tablename()
        fields = self.get_table_columns(self.tablename)
        columns = list(fields)
        sql_list = []
        LOGGER.info(
            f"generating-sql for table:\t{table_name}\t\tfields:\t{fields}")

        for rule_id in self.pipeline:
            for row in info:
                name = row['name']

                if rule_id not in row['label'] or name not in fields:
                    continue

                index = fields.index(name)
                fields[index] = row['apply']
                LOGGER.info(
                    f"creating SQL for field:\t{name}\t\twith:\t{row['apply']}")

        sql_list = ['SELECT', ",".join(fields), 'FROM ', table_name]

        if 'suppress' in self.deid_rules and 'FILTERS' in self.deid_rules[
                'suppress']:
            suppression_filters = self.deid_rules['suppress']['FILTERS']

            if suppression_filters:
                sql_list.append('WHERE')
            for row in suppression_filters:
                if not (set(columns) & set(row['filter'].split(' '))):
                    continue

                sql_list.append(row['filter'])
                # add conjunction if not the last item in the filter list
                if row != suppression_filters[-1]:
                    sql_list.append('AND')

        return ' '.join(sql_list).replace(":idataset", self.idataset)

    def gather_unfiltered_records(self, relational_cols, filter_list):
        """
        Gather all the records that are not generalized or suppressed.

        Generalization only gathers those records a generalization applies to.
        Any record that is unaltered, needs to be gathered via SQL statements
        and added to the output table.  This uses the conditional clauses of
        the rules to determine which records need to be sent to output without
        generalizing or suppressing.  The mapped, shifted, and computed fields
        are still mapped, shifted, and computed as in the other queries used to
        create this table.

        :param relational_cols: a list of columns that are always changed
        :param filter_list: a list of filters applied to generalized columns
        """
        sql = self.to_sql(relational_cols)
        sql += ' AND '

        filter_list = self.create_filter_list(filter_list)
        filter_string = ' AND '.join(filter_list)
        filter_string = filter_string.replace(' in ', ' NOT IN ')
        filter_string = filter_string.replace(' exists ', ' NOT EXISTS ')
        filter_string = filter_string.replace(' not NOT ', ' NOT ')
        return sql + filter_string

    def create_filter_list(self, filter_list):
        """
        Create a list of filter expressions.

        :param filter_list: a list of dictionaries representing the filter
            expression to be used as part of an SQL WHERE clause

        :return: a list of strings representing the expressions.
        """
        field_definitions = {}
        for field_def in fields_for(self.tablename):
            field_definitions[field_def.get('name')] = field_def

        string_list = []
        for item in filter_list:
            string, field = create_on_string(item)

            field_definition = field_definitions.get(field)
            field_mode = field_definition.get('mode').lower()

            # if based on a nullable field, make sure to use the exists function
            if field_mode.lower() == 'nullable':
                item['qualifier'] = item['qualifier'].upper()
                string, _ = create_on_string(item)
                nullable_str = (
                    ' exists (SELECT * FROM `:idataset.observation` AS record2 '
                    'WHERE :join_tablename.observation_id = record2.observation_id '
                    'AND {conditional})')
                string = nullable_str.format(conditional=string)

            string_list.append(string)

        return string_list

    def gather_dml_queries(self, info):
        """
        Gather DML statements to run on the output table.

        Allows additional cleaning to happen on the table.  DML statements use
        a different job config, so the statements must be separate from the
        selection statements.

        :param info:  A list of dictionaries containing queries, or query build
            instructions

        :return:  A list of SQL statements to execute.
        """
        sql_list = []

        # add dml sql rules:  update, delete, insert, etc.
        for rule in info:
            if rule.get('dml_statement', False):
                query = rule.get('apply')
                LOGGER.info(f"adding dml statement:  {rule.get('name')}")
                sql_list.append(query)

        return sql_list
