"""
Ensures there is no data past the deactivation date for deactivated participants.

Original Issue: DC-686

The intent is to sandbox and drop records dated after the date of deactivation for participants
who have deactivated from the Program.
"""

# Python imports
import argparse
import logging

# Third-Party imports
import google.cloud.bigquery as gbq
import pandas as pd

# Project imports
import common
import constants.bq_utils as bq_consts
import constants.cdr_cleaner.clean_cdr as cdr_consts
import constants.retraction.retract_deactivated_pids as consts
import utils.participant_summary_requests as psr
import resources
import retraction.retract_deactivated_pids as rdp
import retraction.retract_utils as ru
from constants.retraction.retract_deactivated_pids import DEACTIVATED_PARTICIPANTS
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule
from utils import bq

LOGGER = logging.getLogger(__name__)

DEACTIVATED_PARTICIPANTS_COLUMNS = [
    'participantId', 'suspensionStatus', 'suspensionTime'
]

ISSUE_NUMBERS = ["DC-686", "DC-1184", "DC-1791", "DC-1896"]

TABLE_INFORMATION_SCHEMA = common.JINJA_ENV.from_string(  # language=JINJA2
    """
SELECT * except(is_generated, generation_expression, is_stored, is_hidden,
is_updatable, is_system_defined, clustering_ordinal_position)
FROM `{{project}}.{{dataset}}.INFORMATION_SCHEMA.COLUMNS`
""")

# Queries to create tables in associated sandbox with rows that will be removed per cleaning rule
SANDBOX_QUERY = common.JINJA_ENV.from_string(  # language=JINJA2
    """
CREATE OR REPLACE TABLE `{{sandbox_ref.project}}.{{sandbox_ref.dataset_id}}.{{sandbox_ref.table_id}}` AS (
SELECT t.*
FROM `{{table_ref.project}}.{{table_ref.dataset_id}}.{{table_ref.table_id}}` t

{% if is_deid %}
JOIN `{{pid_rid_table.project}}.{{pid_rid_table.dataset_id}}.{{pid_rid_table.table_id}}` p
ON t.person_id = p.research_id
JOIN `{{deact_pids_table.project}}.{{deact_pids_table.dataset_id}}.{{deact_pids_table.table_id}}` d
ON p.person_id = d.person_id
{% else %}
JOIN `{{deact_pids_table.project}}.{{deact_pids_table.dataset_id}}.{{deact_pids_table.table_id}}` d
USING (person_id)
{% endif %}

{% if has_start_date %}
WHERE (COALESCE({{end_date}}, EXTRACT(DATE FROM {{end_datetime}}),
    {{start_date}}, EXTRACT(DATE FROM {{start_datetime}})) >= d.deactivated_date
{% if table_ref.table_id == 'drug_exposure' %}
OR verbatim_end_date >= d.deactivated_date)
{% else %} )
{% endif %}
{% elif table_ref.table_id == 'death' %}
WHERE COALESCE(death_date, EXTRACT(DATE FROM death_datetime)) >= d.deactivated_date
{% elif table_ref.table_id in ['activity_summary', 'heart_rate_summary'] %}
WHERE date >= d.deactivated_date
{% elif table_ref.table_id in ['heart_rate_minute_level', 'steps_intraday']  %}
WHERE datetime >= PARSE_DATETIME('%F', CAST(d.deactivated_date as STRING))
{% elif table_ref.table_id in ['drug_era', 'condition_era', 'dose_era', 'payer_plan_period']  %}
WHERE COALESCE({{table_ref.table_id + '_end_date'}}, {{table_ref.table_id + '_start_date'}}) >= d.deactivated_date
{% else %}
WHERE COALESCE({{date}}, EXTRACT(DATE FROM {{datetime}})) >= d.deactivated_date
{% endif %})
""")

# Queries to truncate existing tables to remove deactivated EHR PIDS, two different queries for
# tables with standard entry dates vs. tables with start and end dates
CLEAN_QUERY = common.JINJA_ENV.from_string(  # language=JINJA2
    """
SELECT *
FROM `{{table_ref.project}}.{{table_ref.dataset_id}}.{{table_ref.table_id}}`
EXCEPT DISTINCT
SELECT *
FROM `{{sandbox_ref.project}}.{{sandbox_ref.dataset_id}}.{{sandbox_ref.table_id}}`
""")


def get_date_cols_dict(date_cols_list):
    """
    Converts list of date/datetime columns into dictionary mappings

    Assumes each date column has a corresponding datetime column due to OMOP specifications
    If a date column does not have a corresponding datetime column, skips it
    Used for determining available dates based on order of precedence stated in the SANDBOX_QUERY
    end_date > end_datetime > start_date > start_datetime. Non-conforming dates are factored into
    the query separately, e.g. verbatim_end_date in drug_exposure
    :param date_cols_list: list of date/datetime columns
    :return: dictionary with mappings for START_DATE, START_DATETIME, END_DATE, END_DATETIME
        or DATE, DATETIME
    """
    date_cols_dict = {}
    for field in date_cols_list:
        if field.endswith(consts.START_DATETIME):
            date_cols_dict[consts.START_DATETIME] = field
        elif field.endswith(consts.END_DATETIME):
            date_cols_dict[consts.END_DATETIME] = field
        elif field.endswith(consts.DATETIME):
            date_cols_dict[consts.DATETIME] = field
    for field in date_cols_list:
        if field.endswith(consts.START_DATE):
            if date_cols_dict.get(consts.START_DATETIME, '').startswith(field):
                date_cols_dict[consts.START_DATE] = field
        elif field.endswith(consts.END_DATE):
            if date_cols_dict.get(consts.END_DATETIME, '').startswith(field):
                date_cols_dict[consts.END_DATE] = field
        elif field.endswith(consts.DATE):
            if date_cols_dict.get(consts.DATETIME, '').startswith(field):
                date_cols_dict[consts.DATE] = field
    return date_cols_dict


def get_table_dates_info(table_cols_df):
    """
    Returns a dict with tables containing pids and date columns

    :param table_cols_df: dataframe of columns from INFORMATION_SCHEMA
    :return: dataframe with key table and date columns as values
    """
    pids_tables = table_cols_df[table_cols_df['column_name'] ==
                                'person_id']['table_name']
    date_tables_df = table_cols_df[table_cols_df['column_name'].str.contains(
        "date")]

    dates_info = {}
    for table in pids_tables:
        date_cols = date_tables_df[date_tables_df['table_name'] ==
                                   table]['column_name']
        # exclude person since it does not contain EHR data
        if date_cols.any() and table != 'person':
            dates_info[table] = date_cols.to_list()

    return dates_info


class RemoveParticipantDataPastDeactivationDate(BaseCleaningRule):
    """
    Ensures there is no data past the deactivation date for deactivated participants.
    """

    def __init__(self,
                 project_id,
                 dataset_id,
                 sandbox_dataset_id,
                 table_namer=None,
                 api_project_id=None):
        """
        Initialize the class with proper information.

        Set the issue numbers, description and affected datasets. As other tickets
        may affect this SQL, append them to the list of Jira Issues.

        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """
        desc = (
            'Sandbox and drop records dated after the date of deactivation for participants'
            'who have deactivated from the Program.')

        if not api_project_id:
            raise TypeError("`api_project_id` cannot be empty")

        super().__init__(issue_numbers=ISSUE_NUMBERS,
                         description=desc,
                         affected_datasets=[cdr_consts.COMBINED],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         affected_tables=common.CDM_TABLES +
                         common.FITBIT_TABLES,
                         table_namer=table_namer)
        self.api_project_id = api_project_id
        self.destination_table = (f'{self.project_id}.{self.sandbox_dataset_id}'
                                  f'.{DEACTIVATED_PARTICIPANTS}')

        # initialized to None so that if setup_rule is skipped, it will not
        # query live datasets for table information
        self.client = None
        self.retract_dataset_candidates = [] 

    def get_table_cols_df(self):
        """
        Returns a df of dataset's INFORMATION_SCHEMA.COLUMNS

        :return: dataframe of columns from INFORMATION_SCHEMA
        """
        table_cols_df = pd.DataFrame()
        if self.client:
            LOGGER.info(
                f"Getting column information from live dataset: `{self.dataset_id}`")
            # if possible, read live table schemas
            table_cols_query = TABLE_INFORMATION_SCHEMA.render(project=self.project_id,
                                                               dataset=self.dataset_id)
            table_cols_df = self.client.query(table_cols_query).to_dataframe()
        else:
            # if None is passed to the client, read the table data from JSON schemas
            # generate a dataframe from schema files
            LOGGER.info("Getting column information from schema files")
            table_dict_list = []
            for table in self.affected_tables:
                table_fields = resources.fields_for(table)
                for field in table_fields:
                    field['table_name'] = table
                table_dict_list.extend(table_fields)

            table_cols_df = pd.DataFrame(table_dict_list)
            table_cols_df = table_cols_df.rename(columns={"name": "column_name"})

        return table_cols_df


    def generate_queries(self,
                         deact_pids_table_ref,
                         pid_rid_table_ref=None,
                         data_stage_id=None):
        """
        Creates queries for sandboxing and deleting records

        :param deact_pids_table_ref: BigQuery table reference to dataset containing deactivated participants
        :param pid_rid_table_ref: BigQuery table reference to dataset containing pid-rid mappings
        :param data_stage_id: unique identifier to prepend to sandbox table names
        :return: List of query dicts
        :raises:
            RuntimeError: 1. If retracting from deid dataset, pid_rid table must be specified
                          2. If mapping or ext table does not exist, EHR data cannot be identified
        """
        table_cols_df = self.get_table_cols_df()
        table_dates_info = get_table_dates_info(table_cols_df)
        tables = table_cols_df['table_name'].to_list()
        is_deid = ru.is_deid_label_or_id(self.client, self.project_id, self.dataset_id)
        if is_deid and pid_rid_table_ref is None:
            raise RuntimeError(
                f"PID-RID mapping table must be specified for deid dataset {self.dataset_id}"
            )
        sandbox_queries = []
        clean_queries = []
        for table in table_dates_info:
            table_ref = gbq.TableReference.from_string(
                f"{self.project_id}.{self.dataset_id}.{table}")
            sandbox_table = self.sandbox_table_for(table)
            sandbox_ref = gbq.TableReference.from_string(
                f"{self.project_id}.{self.sandbox_dataset_id}.{sandbox_table}")
            date_cols = get_date_cols_dict(table_dates_info[table])
            has_start_date = consts.START_DATE in date_cols
            sandbox_queries.append({
                cdr_consts.QUERY:
                    SANDBOX_QUERY.render(table_ref=table_ref,
                                         table_id=f'{table}_id',
                                         sandbox_ref=sandbox_ref,
                                         pid_rid_table=pid_rid_table_ref,
                                         deact_pids_table=deact_pids_table_ref,
                                         is_deid=is_deid,
                                         has_start_date=has_start_date,
                                         **date_cols)
            })

            clean_queries.append({
                cdr_consts.QUERY:
                    CLEAN_QUERY.render(table_ref=table_ref,
                                       sandbox_ref=sandbox_ref),
                cdr_consts.DESTINATION_TABLE:
                    table,
                cdr_consts.DESTINATION_DATASET:
                    self.dataset_id,
                cdr_consts.DISPOSITION:
                    bq_consts.WRITE_TRUNCATE
            })
        return sandbox_queries + clean_queries


    def get_query_specs(self):
        """
        This function generates a list of query dicts.

        These queries should sandbox and remove all data past the
        participant's deactivation date.

        :return: a list of query dicts
        """
        deact_table_ref = gbq.TableReference.from_string(self.destination_table)
        # creates sandbox and truncate queries to run for deactivated participant data drops
        # setup_rule must be run before this to ensure the client is properly
        # configured.
        queries = self.generate_queries(deact_table_ref,
                                       data_stage_id=self.table_namer)

        return queries

    def setup_rule(self, client):
        """
        Responsible for grabbing and storing deactivated participant data.

        :param client: client object passed to store the data
        """
        LOGGER.info("Querying RDR API for deactivated participant data")
        # gets the deactivated participant dataset to ensure it's up-to-date
        df = psr.get_deactivated_participants(self.api_project_id,
                                              DEACTIVATED_PARTICIPANTS_COLUMNS)

        LOGGER.info(f"Found '{len(df)}' deactivated participants via RDR API")

        # To store dataframe in a BQ dataset table named _deactivated_participants
        psr.store_participant_data(df, self.project_id, self.destination_table)

        LOGGER.info(f"Finished storing participant records in: "
                    f"`{self.destination_table}`")

        LOGGER.debug("instantiating class client object")
        self.client = client

        # reinitializing self.affected_tables
        LOGGER.debug(
            "reinitializing self.affected_tables to actual tables available")
        tables_list = self.client.list_tables(self.dataset_id)
        self.affected_tables = [
            table_item.table_id for table_item in tables_list
        ]

        # if running setup, can get the list of retraction datasets here
        LOGGER.debug("getting list of live datasets that can be retracted from.") 
        self.retract_dataset_candidates = ru.get_datasets_list

    def get_sandbox_tablenames(self):
        """
        Return a list table names created to backup deleted data.
        """
        LOGGER.info("Generating list of possible sandbox table names "
                    "from self.affected_tables")

        return [
            self.sandbox_table_for(table)
            for table in self.affected_tables
        ]

    def setup_validation(self, client):
        """
        Run required steps for validation setup

        This abstract method was added to the base class after this rule was authored.
        This rule needs to implement logic to setup validation on cleaning rules that
        will be updating or deleting the values.
        Until done no issue exists for this yet.
        """
        raise NotImplementedError("Please fix me.")

    def validate_rule(self, client):
        """
        Validates the cleaning rule which deletes or updates the data from the tables

        This abstract method was added to the base class after this rule was authored.
        This rule needs to implement logic to run validation on cleaning rules that will
        be updating or deleting the values.
        Until done no issue exists for this yet.
        """
        raise NotImplementedError("Please fix me.")


def fq_table_name_verification(fq_table_name):
    """
    Ensures fq_table_name is of the format 'project.dataset.table'

    :param fq_table_name: fully qualified BQ table name
    :return: fq_table_name if valid
    :raises: ArgumentTypeError if invalid
    """
    if fq_table_name.count('.') == 2:
        return fq_table_name
    message = f"{fq_table_name} should be of the form 'project.dataset.table'"
    raise argparse.ArgumentTypeError(message)


def fq_deactivated_table_verification(fq_table_name):
    """
    Ensures fq_table_name is of the format 'project.dataset.table'

    :param fq_table_name: fully qualified BQ table name
    :return: fq_table_name if valid
    :raises: ArgumentTypeError if invalid
    """
    fq_table_name = fq_table_name_verification(fq_table_name)
    if fq_table_name.split('.')[-1] == consts.DEACTIVATED_PARTICIPANTS:
        return fq_table_name
    message = f"{fq_table_name} should be of the form 'project.dataset.{consts.DEACTIVATED_PARTICIPANTS}'"
    raise argparse.ArgumentTypeError(message)


def get_parser(parser, raw_args=None):
    parser.add_argument('-d',
                        '--dataset_ids',
                        action='store',
                        nargs='+',
                        dest='dataset_ids',
                        help='Identifies datasets to target. Set to '
                        '"all_datasets" to target all datasets in project '
                        'or specific datasets as -d dataset_1 dataset_2 etc.',
                        required=True)
    parser.add_argument('-a',
                        '--fq_deact_table',
                        action='store',
                        dest='fq_deact_table',
                        type=fq_table_name_verification,
                        help='Specify fully qualified deactivated table '
                        'as "project.dataset.table"',
                        required=True)
    parser.add_argument('-r',
                        '--fq_pid_rid_table',
                        action='store',
                        dest='fq_pid_rid_table',
                        type=fq_table_name_verification,
                        help='Specify fully qualified pid-rid mapping table '
                        'as "project.dataset.table"')
    parser.add_argument(
        '-q',
        '--api_project_id',
        action='store',
        dest='api_project_id',
        help='Identifies the RDR project for participant summary API',
        required=True)
    return parser.parse_args(raw_args)


if __name__ == '__main__':
    import cdr_cleaner.clean_cdr_engine as clean_engine
    import cdr_cleaner.args_parser as parser
    from utils import pipeline_logging

    pipeline_logging.configure(logging.DEBUG, add_console_handler=True)

    ext_parser = parser.get_argument_parser()
    ARGS = ext_parser.parse_args(ext_parser)

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(
            ARGS.project_id,
            ARGS.dataset_id,
            ARGS.sandbox_dataset_id,
            [(RemoveParticipantDataPastDeactivationDate,)],
            api_project_id=ARGS.api_project_id,
            table_namer='manual')
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(
            ARGS.project_id,
            ARGS.dataset_id,
            ARGS.sandbox_dataset_id,
            [(RemoveParticipantDataPastDeactivationDate,)],
            api_project_id=ARGS.api_project_id,
            table_namer='manual')
