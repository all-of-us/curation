"""
Cleaning rule to generalize conflicting HPO states
"""
import logging
import os

from google.cloud import bigquery
from google.api_core.exceptions import Conflict

from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule, query_spec_list
from constants.cdr_cleaner import clean_cdr as cdr_consts
from common import JINJA_ENV, CATI_TABLES, CARE_SITE, FACT_RELATIONSHIP, \
    LOCATION, PROVIDER, OBSERVATION
from resources import DEID_PATH
import constants.bq_utils as bq_consts

LOGGER = logging.getLogger(__name__)

JIRA_ISSUE_NUMBERS = ['DC512', 'DC834']
JIRA_ISSUE_URL = [
    'https://precisionmedicineinitiative.atlassian.net/browse/DC-512',
    'https://precisionmedicineinitiative.atlassian.net/browse/DC-834'
]
CLEANING_RULE_NAME = 'conflicting_hpo_state_generalization'

AFFECTED_TABLES = list(
    set(CATI_TABLES) - {CARE_SITE, FACT_RELATIONSHIP, LOCATION, PROVIDER})

MAP_TABLE_NAME = "_mapping_person_src_hpos"
MAP_HPO_ALLOWED_STATES = "_mapping_src_hpos_to_allowed_states"
UNIT_MAPPING_TABLE_DISPOSITION = "WRITE_TRUNCATE"

HPO_ID_NOT_RDR_QUERY = JINJA_ENV.from_string("""
  SELECT
  DISTINCT person_id, src_hpo_id
  FROM
    `{{input_dataset}}._mapping_{{table}}`
  JOIN
    `{{input_dataset}}.{{table}}`
  USING
    `{{table}}_id`
  WHERE
    src_hpo_id NOT LIKE 'rdr'
""")

LIST_PERSON_ID_TABLES = JINJA_ENV.from_string("""
  SELECT
  table_name
  from `{{project_id}}.{{dataset_id}}.INFORMATION_SCHEMA.COLUMNS`
  where lower(column_name) = 'person_id'
""")


class ConflictingHpoStateGeneralize(BaseCleaningRule):
    """Generalize conflicting hpo states."""

    def __init__(self,
                 project_id,
                 dataset_id,
                 sandbox_dataset_id,
                 table_namer=None):
        desc = (
            "Set all participant home states to a generalized value if they have "
            "EHR records from a different state.")

        self.final_hpo_id_not_rdr_query = None

        super().__init__(issue_numbers=JIRA_ISSUE_NUMBERS,
                         issue_urls=JIRA_ISSUE_URL,
                         description=desc,
                         affected_datasets=[cdr_consts.EHR],
                         affected_tables=AFFECTED_TABLES,
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         table_namer=table_namer)

    def setup_rule(self, client, *args, **keyword_args):
        """
        Run required steps for setup rule

        Create a mapping table of src_hpos to states they are located in.
        Create a table containing person_ids and src_hpo_ids
        """
        if OBSERVATION not in self.dataset_id:
            return

        # Create a mapping table of src_hpos to states they are located in.
        map_tablename = f"{self.dataset_id}.{MAP_HPO_ALLOWED_STATES}"
        client.create_table(map_tablename, exists_ok=True)

        data_path = os.path.join(DEID_PATH, 'config', 'internal_tables',
                                 'src_hpos_to_allowed_states.csv')

        # write this to bigquery.
        try:
            _ = client.upload_csv_data_to_bq_table(
                self.dataset_id, MAP_HPO_ALLOWED_STATES, data_path,
                UNIT_MAPPING_TABLE_DISPOSITION)

            LOGGER.info(
                f"Created {self.dataset_id}.{MAP_HPO_ALLOWED_STATES} and "
                f"loaded data from {data_path}")

        except Conflict as c:
            LOGGER.info(
                f"{self.dataset_id}.{MAP_HPO_ALLOWED_STATES} Data load encountered conflict: "
                f"{c.errors}")

        # Create a table containing person_ids and src_hpo_ids
        # List Dataset Content
        dataset_tables = client.list_tables(self.dataset_id)
        dataset_table_ids = [table.table_id for table in dataset_tables]

        mapped_tables = [
            table[9:]
            for table in dataset_table_ids
            if table.startswith('_mapping_')
        ]

        # Make sure all mapped_tables exists
        check_tables = [
            table for table in mapped_tables if table in dataset_table_ids
        ]

        # Make sure check_tables contains person_id field
        person_id_query = LIST_PERSON_ID_TABLES.render(
            project_id=self.project_id, dataset_id=self.dataset_id)
        result = client.query(person_id_query).result()

        person_id_tables = []
        for row in result:
            table = row.get('table_name')
            if table in check_tables:
                person_id_tables.append(table)

        # mapping_tables corresponding to person_id_tables
        # mapping_tables = [f"_mapping_{table}" for table in person_id_tables]

        # Run UNION DISTINCT query to join person_id_tables
        sql_statements = []
        for table in person_id_tables:
            sql_statements.append(
                HPO_ID_NOT_RDR_QUERY.render(input_dataset=self.dataset_id,
                                            table=table))
        self.final_hpo_id_not_rdr_query = ' UNION ALL '.join(sql_statements)

        # Create the mapping table in Sandbox if it does not exist in dataset_table_ids
        if MAP_TABLE_NAME not in dataset_table_ids:
            schema = [{
                "type": "integer",
                "name": "person_id",
                "mode": "required",
                "description": "the person_id of someone with an ehr record"
            }, {
                "type": "string",
                "name": "src_hpo_id",
                "mode": "required",
                "description": "the src_hpo_id of an ehr record"
            }]
            table_name = f"{self.project_id}.{self.dataset_id}.{MAP_TABLE_NAME}"
            table = bigquery.Table(table_name, schema=schema)
            client.create_table(table, exists_ok=True)

    def get_query_specs(self, *args, **keyword_args) -> query_spec_list:
        """
        Interface to return a list of query dictionaries.
        """
        conflicting_hpo_state_generalize_query = {
            cdr_consts.QUERY: self.final_hpo_id_not_rdr_query
        }
        return [conflicting_hpo_state_generalize_query]

    def get_sandbox_tablenames(self):
        """
        Generate SandBox Table Names
        """
        return [self.sandbox_table_for(OBSERVATION)]

    def validate_rule(self, client, *args, **keyword_args):
        """
        Validates the cleaning rule which deletes or updates the data from the tables
        """
        raise NotImplementedError("Please fix me.")

    def setup_validation(self, client, *args, **keyword_args):
        """
        Run required steps for validation setup
        """
        raise NotImplementedError("Please fix me.")


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.get_argument_parser().parse_args()

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(
            ARGS.project_id, ARGS.dataset_id, ARGS.sandbox_dataset_id,
            [(ConflictingHpoStateGeneralize,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(ConflictingHpoStateGeneralize,)])
