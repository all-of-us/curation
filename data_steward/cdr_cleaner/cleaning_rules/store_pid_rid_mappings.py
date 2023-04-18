"""
Background

The Genomics program requires stable research IDs (RIDs). This is a script that will
add only pid/rid mappings for participants that don't currently exist in the
primary_pid_rid_mapping table.

These records will be appended to the pipeline_tables.primary_pid_rid_mapping table in BigQuery.
Duplicate mappings are not allowed.
"""
# Python imports
import logging
from datetime import datetime

# Third party imports
from google.cloud import bigquery

# Project imports
import constants.cdr_cleaner.clean_cdr as cdr_consts
from common import (JINJA_ENV, MAX_DEID_DATE_SHIFT, PID_RID_MAPPING,
                    PIPELINE_TABLES, PRIMARY_PID_RID_MAPPING)
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule
from resources import fields_for, validate_date_string

LOGGER = logging.getLogger(__name__)

CREATE_SANDBOX_PID_RID = JINJA_ENV.from_string("""
CREATE TABLE IF NOT EXISTS `{{rdr_sandbox.project}}.{{rdr_sandbox.dataset_id}}.{{rdr_sandbox.table_id}}`
-- create table with schema to match primary pid/rid table schema --
(
{{field_definitions}}
)
PARTITION BY
import_date
""")

GENERATE_NEW_MAPPINGS = JINJA_ENV.from_string("""
INSERT INTO  `{{rdr_sandbox.project}}.{{rdr_sandbox.dataset_id}}.{{rdr_sandbox.table_id}}`
(import_date, person_id, research_id, shift)
SELECT
  date('{{export_date}}')
  , person_id
  , research_id
-- generates random shifts between 1 and max_shift inclusive --
  , CAST(FLOOR({{max_shift}} * RAND() + 1) AS INT64) as shift
FROM `{{rdr_table.project}}.{{rdr_table.dataset_id}}.{{rdr_table.table_id}}`
WHERE person_id not in (
  SELECT person_id
  FROM `{{primary.project}}.{{primary.dataset_id}}.{{primary.table_id}}`)
-- This is just to make sure we don't duplicate either person_id OR research_id --
AND research_id not in (
  SELECT research_id
  FROM `{{primary.project}}.{{primary.dataset_id}}.{{primary.table_id}}`)
""")

STORE_NEW_MAPPINGS = JINJA_ENV.from_string("""
INSERT INTO  `{{primary.project}}.{{primary.dataset_id}}.{{primary.table_id}}`
(import_date, person_id, research_id, shift)
SELECT
  import_date
  , person_id
  , research_id
  , shift
FROM `{{rdr_sandbox.project}}.{{rdr_sandbox.dataset_id}}.{{rdr_sandbox.table_id}}`
""")


class StoreNewPidRidMappings(BaseCleaningRule):
    """
    Store only new occurrences of PID/RID mappings and new date shift components.
    """

    def __init__(self,
                 project_id,
                 dataset_id,
                 sandbox_dataset_id,
                 export_date=None,
                 namer=None):
        desc = (f'All new pid/rid mappings will be identified via SQL and '
                f'stored, along with a shift integer, in a sandbox table.  '
                f'The table will be read to load into the primary pipeline '
                f'table, pipeline_tables.primary_pid_rid_mapping.')

        namer = dataset_id if not namer else namer

        super().__init__(issue_numbers=['DC1543'],
                         description=desc,
                         affected_datasets=[cdr_consts.RDR],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         table_namer=namer)

        # primary table ref
        dataset_ref = bigquery.DatasetReference(project_id, PIPELINE_TABLES)
        self.primary_mapping_table = bigquery.TableReference(
            dataset_ref, PRIMARY_PID_RID_MAPPING)

        # rdr table ref
        dataset_ref = bigquery.DatasetReference(project_id, dataset_id)
        self.rdr_table = bigquery.TableReference(dataset_ref, PID_RID_MAPPING)

        # rdr sandbox table ref
        dataset_ref = bigquery.DatasetReference(project_id, sandbox_dataset_id)
        self.rdr_sandbox_table = bigquery.TableReference(
            dataset_ref, self.sandbox_table_for(PID_RID_MAPPING))

        # store fields as json object
        self.fields = fields_for(PRIMARY_PID_RID_MAPPING,
                                 sub_path='pipeline_tables')

        # set export date
        try:
            self.export_date = validate_date_string(export_date)
            LOGGER.info(f'Using provided export_date: `{export_date}`')
        except (TypeError, ValueError):
            # otherwise, default to using today's date
            LOGGER.warning(
                f"Failed to validate the export_date:  '{export_date}'")
            self.export_date = datetime.now().strftime('%Y-%m-%d')
            LOGGER.warning(f"Setting export_date to now: '{self.export_date}'")

    def get_query_specs(self):
        """
        Store the provided mappings and create required date shifts.

        Curation must maintain a stable pid/rid mapping for participants, as well
        as a date shift integer.  Curation gets the pid/rid mapping table from the
        RDR team as part of their ETL process.  Curation must identify new pid/rid
        mapping pairs, create random date shifts for each pair, and store the three
        tuple to the pipeline_tables.primary_pid_rid_mapping table.

        The script assumes the newly provided mapping table exists in the same
        project as the primary mapping table.

        :return: a list of SQL strings to run
        """
        LOGGER.info(f'RDR mapping info: '
                    f'project -> {self.rdr_table.project}\t'
                    f'dataset -> {self.rdr_table.dataset_id}\t'
                    f'table -> {self.rdr_table.table_id}')
        LOGGER.info(f'Primary mapping info: '
                    f'project -> {self.primary_mapping_table.project}\t'
                    f'dataset -> {self.primary_mapping_table.dataset_id}\t'
                    f'table -> {self.primary_mapping_table.table_id}')

        fields_str = self.get_bq_fields_sql(self.fields)

        create_sandbox_table = CREATE_SANDBOX_PID_RID.render(
            rdr_sandbox=self.rdr_sandbox_table, field_definitions=fields_str)

        sandbox_query = GENERATE_NEW_MAPPINGS.render(
            rdr_table=self.rdr_table,
            rdr_sandbox=self.rdr_sandbox_table,
            primary=self.primary_mapping_table,
            export_date=self.export_date,
            max_shift=MAX_DEID_DATE_SHIFT)

        insert_query = STORE_NEW_MAPPINGS.render(
            rdr_sandbox=self.rdr_sandbox_table,
            primary=self.primary_mapping_table)

        LOGGER.info(f'Preparing queries:'
                    f'\n{create_sandbox_table}'
                    f'\n{sandbox_query}'
                    f'\n{insert_query}')

        create_sandbox_table_dict = {cdr_consts.QUERY: create_sandbox_table}
        sandbox_query_dict = {cdr_consts.QUERY: sandbox_query}
        insert_query_dict = {cdr_consts.QUERY: insert_query}

        return [
            create_sandbox_table_dict, sandbox_query_dict, insert_query_dict
        ]

    def get_sandbox_tablenames(self):
        return [self.rdr_sandbox_table.table_id]

    def setup_rule(self, export_date):
        pass

    def setup_validation(self):
        pass

    def validate_rule(self):
        pass


if __name__ == '__main__':
    from utils import pipeline_logging

    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ext_parser = parser.get_argument_parser()
    ext_parser.add_argument(
        '-e',
        '--export_date',
        action='store',
        dest='export_date',
        help=('Date of the RDR export. Should adhere to '
              'YYYY-MM-DD format'),
        type=validate_date_string,
    )

    ARGS = ext_parser.parse_args()
    pipeline_logging.configure(level=logging.DEBUG, add_console_handler=True)

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(ARGS.project_id,
                                                 ARGS.dataset_id,
                                                 ARGS.sandbox_dataset_id,
                                                 [(StoreNewPidRidMappings,)],
                                                 ARGS.export_date)
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(StoreNewPidRidMappings,)],
                                   ARGS.export_date)
