"""
Ensures there is no data past the deactivation date for deactivated participants.

Original Issue: DC-686

The intent is to sandbox and drop records dated after the date of deactivation for participants
who have deactivated from the Program.

Subsequent issues: DC-686, DC-1184, DC-1799, DC-1791, DC-1896, DC-2129, DC-3164
"""

# Python imports
import logging

# Third-party imports
from pandas import DataFrame

# Project imports
from common import AOU_DEATH, FITBIT_TABLES, JINJA_ENV, DEVICE
import constants.cdr_cleaner.clean_cdr as cdr_consts
import utils.participant_summary_requests as psr
from constants.bq_utils import WRITE_TRUNCATE
from resources import fields_for, CDM_TABLES
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule

# Third-Party imports
import google.cloud.bigquery as gbq

LOGGER = logging.getLogger(__name__)

DEACTIVATED_PARTICIPANTS_COLUMNS = [
    'participantId', 'suspensionStatus', 'suspensionTime'
]

# For reference
DEACTIVATION_ISSUE_NUMBERS = ['DC686', 'DC1184', 'DC1799']
ISSUE_NUMBERS = ['DC1791', 'DC1896', 'DC2129', 'DC2631', 'DC3164']

TABLE_INFORMATION_SCHEMA = JINJA_ENV.from_string(  # language=JINJA2
    """
SELECT * except(is_generated, generation_expression, is_stored, is_hidden,
is_updatable, is_system_defined, clustering_ordinal_position)
FROM `{{project}}.{{dataset}}.INFORMATION_SCHEMA.COLUMNS`
""")

# Queries to create tables in associated sandbox with rows that will be removed per cleaning rule
SANDBOX_QUERY = JINJA_ENV.from_string(  # language=JINJA2
    """
CREATE OR REPLACE TABLE `{{sandbox_ref.project}}.{{sandbox_ref.dataset_id}}.{{sandbox_ref.table_id}}` AS (
SELECT t.*
FROM `{{table_ref.project}}.{{table_ref.dataset_id}}.{{table_ref.table_id}}` t

JOIN `{{deact_pids_table.project}}.{{deact_pids_table.dataset_id}}.{{deact_pids_table.table_id}}` d
USING (person_id)

{% if has_start_date %}
WHERE (({{end_datetime}} IS NOT NULL AND {{end_datetime}} >= d.deactivated_datetime)
OR ({{end_datetime}} IS NULL AND {{end_date}} IS NOT NULL AND {{end_date}} >= DATE(d.deactivated_datetime))
OR ({{end_datetime}} IS NULL AND {{end_date}} IS NULL AND {{start_datetime}} IS NOT NULL AND {{start_datetime}} >= d.deactivated_datetime)
OR ({{end_datetime}} IS NULL AND {{end_date}} IS NULL AND {{start_datetime}} IS NULL AND {{start_date}} IS NOT NULL AND {{start_date}} >= DATE(d.deactivated_datetime))
{% if table_ref.table_id == 'drug_exposure' %}
OR verbatim_end_date >= DATE(d.deactivated_datetime))
{% else %} )
{% endif %}
{% elif table_ref.table_id in ['death', 'aou_death'] %}
WHERE (death_datetime IS NOT NULL AND death_datetime >= d.deactivated_datetime)
OR (death_datetime IS NULL AND death_date >= DATE(d.deactivated_datetime))
{% elif table_ref.table_id in ['activity_summary', 'heart_rate_summary'] %}
WHERE date >= DATE(d.deactivated_datetime)
{% elif table_ref.table_id in ['heart_rate_intraday', 'steps_intraday']  %}
WHERE datetime >= DATETIME(d.deactivated_datetime)
{% elif table_ref.table_id in ['payer_plan_period', 'observation_period']  %}
WHERE COALESCE({{table_ref.table_id + '_end_date'}},
{{table_ref.table_id + '_start_date'}}) >= DATE(d.deactivated_datetime)
{% elif table_ref.table_id in ['drug_era', 'condition_era', 'dose_era']  %}
WHERE COALESCE({{table_ref.table_id + '_end_date'}},
{{table_ref.table_id + '_start_date'}}) >= d.deactivated_datetime
{% elif table_ref.table_id == 'sleep_level' %}
WHERE (start_datetime IS NOT NULL AND TIMESTAMP(start_datetime) >= d.deactivated_datetime)
OR (sleep_date IS NOT NULL AND sleep_date >= DATE(d.deactivated_datetime))
{% elif table_ref.table_id == 'sleep_daily_summary' %}
WHERE (sleep_date IS NOT NULL AND sleep_date >= DATE(d.deactivated_datetime))
{% elif table_ref.table_id == 'device' %}
WHERE (device_date IS NOT NULL AND device_date >= DATE(d.deactivated_datetime))
OR (last_sync_time IS NOT NULL AND last_sync_time >= DATETIME(d.deactivated_datetime))
OR (last_sync_time IS NULL AND device_date IS NULL)
{% else %}
WHERE ({{datetime}} IS NOT NULL AND {{datetime}} >= d.deactivated_datetime)
OR ({{datetime}} IS NULL AND {{date}} >= DATE(d.deactivated_datetime))
{% endif %})
""")

# Queries to truncate existing tables to remove deactivated EHR PIDS, two different queries for
# tables with standard entry dates vs. tables with start and end dates
CLEAN_QUERY = JINJA_ENV.from_string(  # language=JINJA2
    """
SELECT *
FROM `{{table_ref.project}}.{{table_ref.dataset_id}}.{{table_ref.table_id}}`
EXCEPT DISTINCT
SELECT *
FROM `{{sandbox_ref.project}}.{{sandbox_ref.dataset_id}}.{{sandbox_ref.table_id}}`
""")

DATETIME = 'datetime'
START_DATETIME = f'start_{DATETIME}'
END_DATETIME = f'end_{DATETIME}'

DATE = 'date'
START_DATE = f'start_{DATE}'
END_DATE = f'end_{DATE}'

DEACTIVATED_PARTICIPANTS = '_deactivated_participants'


class RemoveParticipantDataPastDeactivationDate(BaseCleaningRule):
    """
    Ensures there is no data past the deactivation date for deactivated participants.
    """

    def __init__(self,
                 project_id,
                 dataset_id,
                 sandbox_dataset_id,
                 table_namer=None,
                 api_project_id=None,
                 key_path=None):
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

        super().__init__(
            issue_numbers=ISSUE_NUMBERS,
            description=desc,
            affected_datasets=[cdr_consts.COMBINED],
            project_id=project_id,
            dataset_id=dataset_id,
            sandbox_dataset_id=sandbox_dataset_id,
            #  affected_tables=CDM_TABLES + FITBIT_TABLES + [AOU_DEATH],
            affected_tables=[DEVICE],
            table_namer=table_namer)
        self.api_project_id = api_project_id
        self.destination_table = (f'{self.project_id}.{self.sandbox_dataset_id}'
                                  f'.{DEACTIVATED_PARTICIPANTS}')
        self.deact_table_ref = gbq.TableReference.from_string(
            self.destination_table)
        self.table_cols_df = self.get_table_cols_df(None, self.project_id,
                                                    self.dataset_id)
        self.table_dates_info = self.get_table_dates_info(self.table_cols_df)
        self.key_path = key_path

    def get_date_cols_dict(self, date_cols_list):
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
            if field.endswith(START_DATETIME):
                date_cols_dict[START_DATETIME] = field
            elif field.endswith(END_DATETIME):
                date_cols_dict[END_DATETIME] = field
            elif field.endswith(DATETIME):
                date_cols_dict[DATETIME] = field
        for field in date_cols_list:
            if field.endswith(START_DATE):
                if date_cols_dict.get(START_DATETIME, '').startswith(field):
                    date_cols_dict[START_DATE] = field
            elif field.endswith(END_DATE):
                if date_cols_dict.get(END_DATETIME, '').startswith(field):
                    date_cols_dict[END_DATE] = field
            elif field.endswith(DATE):
                if date_cols_dict.get(DATETIME, '').startswith(field):
                    date_cols_dict[DATE] = field
        return date_cols_dict

    def get_table_cols_df(self, client, project_id, dataset_id):
        """
        Returns a df of dataset's INFORMATION_SCHEMA.COLUMNS

        :param project_id: bq name of project_id
        :param dataset_id: ba name of dataset_id
        :param client: bq client object
        :return: dataframe of columns from INFORMATION_SCHEMA
        """
        table_cols_df = DataFrame()
        if client:
            LOGGER.info(
                f"Getting column information from live dataset: `{self.dataset_id}`"
            )
            # if possible, read live table schemas
            table_cols_query = TABLE_INFORMATION_SCHEMA.render(
                project=project_id, dataset=dataset_id)
            table_cols_df = client.query(table_cols_query).to_dataframe()
        else:
            # if None is passed to the client, read the table data from JSON schemas
            # generate a dataframe from schema files
            LOGGER.info("Getting column information from schema files")
            table_dict_list = []
            for table in self.affected_tables:
                table_fields = fields_for(table)
                for field in table_fields:
                    field['table_name'] = table
                table_dict_list.extend(table_fields)

            table_cols_df = DataFrame(table_dict_list)
            table_cols_df = table_cols_df.rename(
                columns={"name": "column_name"})

        return table_cols_df

    def get_table_dates_info(self, table_cols_df):
        """
        Returns a dict with tables containing pids and date columns

        :param table_cols_df: dataframe of columns from INFORMATION_SCHEMA
        :return: dict with key table and date columns as values
        """
        pids_tables = table_cols_df[table_cols_df['column_name'] ==
                                    'person_id']['table_name']
        date_tables_df = table_cols_df[
            table_cols_df['column_name'].str.contains("date")]

        dates_info = {}
        for table in pids_tables:
            date_cols = date_tables_df[date_tables_df['table_name'] ==
                                       table]['column_name']
            # exclude person since it does not contain EHR data
            if date_cols.any() and table != 'person':
                dates_info[table] = date_cols.to_list()

        return dates_info

    def get_query_specs(self):
        """
        This function generates a list of query dicts.

        These queries should sandbox and remove all data past the
        participant's deactivation date.

        :return: a list of query dicts
        """
        sandbox_queries = []
        clean_queries = []
        for table in self.table_dates_info:
            table_ref = gbq.TableReference.from_string(
                f"{self.project_id}.{self.dataset_id}.{table}")
            sandbox_table = self.sandbox_table_for(table)
            sandbox_ref = gbq.TableReference.from_string(
                f"{self.project_id}.{self.sandbox_dataset_id}.{sandbox_table}")
            date_cols = self.get_date_cols_dict(self.table_dates_info[table])
            has_start_date = START_DATE in date_cols
            sandbox_queries.append({
                cdr_consts.QUERY:
                    SANDBOX_QUERY.render(table_ref=table_ref,
                                         table_id=f'{table}_id',
                                         sandbox_ref=sandbox_ref,
                                         deact_pids_table=self.deact_table_ref,
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
                    WRITE_TRUNCATE
            })
        return sandbox_queries + clean_queries

    def setup_rule(self, client):
        """
        Responsible for grabbing and storing deactivated participant data.

        :param client: a BiQueryClient passed to store the data
        """
        LOGGER.info("Querying RDR API for deactivated participant data")
        import os
        if self.key_path:
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = self.key_path

        # gets the deactivated participant dataset to ensure it's up-to-date
        df = psr.get_deactivated_participants(client, self.api_project_id,
                                              DEACTIVATED_PARTICIPANTS_COLUMNS)

        LOGGER.info(f"Found '{len(df)}' deactivated participants via RDR API")

        # To store dataframe in a BQ dataset table named _deactivated_participants
        psr.store_participant_data(df, client, self.destination_table)

        LOGGER.info(f"Finished storing participant records in: "
                    f"`{self.destination_table}`")
        if self.key_path:
            del os.environ["GOOGLE_APPLICATION_CREDENTIALS"]

        LOGGER.info(f"Finished storing participant records in: "
                    f"`{self.destination_table}`")

        # reinitializing self.affected_tables
        LOGGER.debug("Narrow down to actual tables with dates and pids")
        table_cols_df = self.get_table_cols_df(client, self.project_id,
                                               self.dataset_id)
        self.table_dates_info = self.get_table_dates_info(table_cols_df)
        self.affected_tables = table_cols_df['table_name'].to_list()

    def get_sandbox_tablenames(self):
        """
        Return a list table names created to backup deleted data.
        """
        return [self.sandbox_table_for(table) for table in self.affected_tables]

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


if __name__ == '__main__':
    import cdr_cleaner.clean_cdr_engine as clean_engine
    import cdr_cleaner.args_parser as parser

    ext_parser = parser.get_argument_parser()
    ext_parser.add_argument(
        '-q',
        '--api_project_id',
        action='store',
        dest='api_project_id',
        help='Identifies the RDR project for participant summary API',
        required=True)
    ext_parser.add_argument('-kp',
                            '--key_path',
                            action='store',
                            dest='key_path',
                            help='Path to service account key file',
                            required=True)
    ARGS = ext_parser.parse_args()

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(
            ARGS.project_id,
            ARGS.dataset_id,
            ARGS.sandbox_dataset_id,
            [(RemoveParticipantDataPastDeactivationDate,)],
            api_project_id=ARGS.api_project_id,
            key_path=ARGS.key_path,
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
            key_path=ARGS.key_path,
            table_namer='manual')
