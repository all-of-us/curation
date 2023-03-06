"""
Cleaning rule to generalize conflicting HPO states
"""
import logging
import os

from google.cloud import bigquery
from google.api_core.exceptions import Conflict

from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule, query_spec_list
from constants.cdr_cleaner import clean_cdr as cdr_consts
from common import JINJA_ENV, OBSERVATION
from resources import DEID_PATH

LOGGER = logging.getLogger(__name__)

JIRA_ISSUE_NUMBERS = ['DC512', 'DC834']
JIRA_ISSUE_URL = [
    'https://precisionmedicineinitiative.atlassian.net/browse/DC-512',
    'https://precisionmedicineinitiative.atlassian.net/browse/DC-834'
]

MAP_TABLE_NAME = "_mapping_person_src_hpos"

SCHEMA_MAP_TABLE = [{
    "type": "integer",
    "name": "person_id",
    "mode": "required",
    "description": "the person_id of someone with an ehr record"
}, {
    "type": "string",
    "name": "src_id",
    "mode": "required",
    "description": "the src_id of an ehr record"
}]

HPO_ID_NOT_RDR_QUERY = JINJA_ENV.from_string("""
  SELECT
  DISTINCT person_id, src_id
  FROM
    `{{project_id}}.{{dataset_id}}.{{table}}_ext`
  JOIN
    `{{project_id}}.{{dataset_id}}.{{table}}`
  USING
    ({{table}}_id)
  WHERE
    src_id NOT LIKE 'PPI/PM'
""")

LIST_PERSON_ID_TABLES = JINJA_ENV.from_string("""
  SELECT
  DISTINCT table_name
  FROM `{{project_id}}.{{dataset_id}}.INFORMATION_SCHEMA.COLUMNS`
  WHERE lower(column_name) = 'person_id'
  and lower(table_name) || "_ext" in (
    select distinct lower(table_id) 
    from `{{project_id}}.{{dataset_id}}.__TABLES__` 
    where REGEXP_CONTAINS(table_id, r'(?i)_ext$')
  )
""")

INSERT_TO_MAP_TABLE_NAME = JINJA_ENV.from_string("""
  INSERT INTO `{{project_id}}.{{sandbox_dataset_id}}.{{table_name}}`
  (person_id,
   src_id)
  {{select_query}}
""")

SANDBOX_QUERY_TO_FIND_RECORDS = JINJA_ENV.from_string("""
  CREATE OR REPLACE TABLE
  `{{project_id}}.{{sandbox_dataset_id}}.{{target_table}}` AS (
  SELECT observation_id FROM (
    SELECT DISTINCT src_id, obs.person_id, value_source_concept_id, observation_id
    FROM `{{project_id}}.{{sandbox_dataset_id}}.{{map_table_name}}` AS person_hpos
    JOIN `{{project_id}}.{{dataset_id}}.{{target_table}}` AS obs
    USING (person_id)
    LEFT JOIN `{{project_id}}.pipeline_tables.site_maskings`
    USING (src_id, value_source_concept_id)
    WHERE observation_source_concept_id = 1585249  AND state IS NULL))
""")

GENERALIZE_STATE_QUERY = JINJA_ENV.from_string("""
  UPDATE
  `{{project_id}}.{{dataset_id}}.{{updated_table}}` AS D_OBS
  SET D_OBS.value_source_concept_id = 2000000011,
  D_OBS.value_as_concept_id = 2000000011
  FROM `{{project_id}}.{{sandbox_dataset_id}}.{{target_table}}` AS SB_OBS
  WHERE D_OBS.observation_id = SB_OBS.observation_id
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

        super().__init__(issue_numbers=JIRA_ISSUE_NUMBERS,
                         issue_urls=JIRA_ISSUE_URL,
                         description=desc,
                         affected_datasets=[cdr_consts.REGISTERED_TIER_DEID],
                         affected_tables=[OBSERVATION],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         table_namer=table_namer)

    def setup_rule(self, client, *args, **keyword_args):
        """
        Run required steps for setup rule

        1. Identify the states where EHR data is originating from.
        2. Identify the states participants claim to live in
           (This is value_source_concept_id where observation_source_concept_id = 1585249.)
        3. If you find any EHR records for a participant that originates from a state other than the state
           where the participant claims to live, then that participants original response should be sandboxed
           and their data should be generalized such that value_source_concept_id = 2000000011 and
           value_as_concept_id = 2000000011  when observation_source_concept_id = 1585249.
        """
        # Currently this rule is only run for 'OBSERVATION' table.
        # To include more tables in the future, update 'affected_tables' in __init__ args.

        # Get all the tables that has extension tables and has person_id column
        person_id_tables_query = LIST_PERSON_ID_TABLES.render(
            project_id=self.project_id, dataset_id=self.dataset_id)
        result = client.query(person_id_tables_query).result()
        person_id_tables = [row.get('table_name') for row in result]

        # Run UNION DISTINCT query to join person_id_tables
        sql_statements = []
        for table in person_id_tables:
            sql_statements.append(
                HPO_ID_NOT_RDR_QUERY.render(project_id=self.project_id,
                                            dataset_id=self.dataset_id,
                                            table=table))
        final_hpo_id_not_rdr_query = ' UNION ALL '.join(sql_statements)

        # Create the mapping table in Sandbox if it does not exist
        if MAP_TABLE_NAME not in client.list_tables(self.sandbox_dataset_id):
            table_name = f"{self.project_id}.{self.sandbox_dataset_id}.{MAP_TABLE_NAME}"
            table = bigquery.Table(table_name, schema=SCHEMA_MAP_TABLE)
            client.create_table(table, exists_ok=True)

        # 1. Create 'person_id' and 'src_hpo_id' lookup table (MAP_TABLE_NAME table) in sandbox
        # for mapping person data to source hpo_site.
        client.query(
            INSERT_TO_MAP_TABLE_NAME.render(
                project_id=self.project_id,
                sandbox_dataset_id=self.sandbox_dataset_id,
                table_name=MAP_TABLE_NAME,
                select_query=final_hpo_id_not_rdr_query))
        LOGGER.info(
            f"Created mapping table:\t{self.sandbox_dataset_id}.{MAP_TABLE_NAME}"
        )
        # 2. State of hpo_site: site_maskings table.
        # 3. State of Person: Observation table
        #    (value_source_concept_id if observation_source_concept_id = 1585249)

    def get_query_specs(self, *args, **keyword_args) -> query_spec_list:
        """
        Interface to return a list of query dictionaries.
        """
        identify_conflicting_hpo_state_data_query = {}
        conflicting_hpo_state_generalize_query = {}

        for target_table in self.affected_tables:
            identify_conflicting_hpo_state_data_query = {
                cdr_consts.QUERY:
                    SANDBOX_QUERY_TO_FIND_RECORDS.render(
                        project_id=self.project_id,
                        dataset_id=self.dataset_id,
                        sandbox_dataset_id=self.sandbox_dataset_id,
                        target_table=target_table,
                        map_table_name=MAP_TABLE_NAME)
            }

            conflicting_hpo_state_generalize_query = {
                cdr_consts.QUERY:
                    GENERALIZE_STATE_QUERY.render(
                        project_id=self.project_id,
                        dataset_id=self.dataset_id,
                        sandbox_dataset_id=self.sandbox_dataset_id,
                        updated_table=target_table,
                        target_table=target_table)
            }

        return [
            identify_conflicting_hpo_state_data_query,
            conflicting_hpo_state_generalize_query
        ]

    def get_sandbox_tablenames(self):
        """
        Generate SandBox Table Names
        """
        return [self.sandbox_table_for(table) for table in self.affected_tables]

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
