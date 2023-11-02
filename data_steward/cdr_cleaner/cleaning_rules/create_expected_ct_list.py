"""
Background

The Genomics program requires predicted research IDs (RIDs) as soon as possible after
the data cut off date.  The actual list takes some time to generate, and this is a prediction
of what the list will be.

The list here should match the participants in the CT dataset once the dataset has been created.
"""
# Python imports
import logging

# Third party imports

# Project imports
import constants.cdr_cleaner.clean_cdr as cdr_consts
from common import (JINJA_ENV, PIPELINE_TABLES, PRIMARY_PID_RID_MAPPING,
                    AIAN_LIST)
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule
from cdr_cleaner.cleaning_rules.clean_mapping import CleanMappingExtTables
from cdr_cleaner.cleaning_rules.create_aian_lookup import CreateAIANLookup

LOGGER = logging.getLogger(__name__)
EXPECTED_CT_LIST = 'expected_ct_list'

CREATE_EXPECTED_CT_LIST = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project_id}}.{{sandbox_dataset_id}}.{{storage_table_name}}` AS (
-- get the list of all participants at the end of the RDR cleaning stage --
WITH
  rdr_persons AS (SELECT DISTINCT person_id
  FROM `{{project_id}}.{{dataset_id}}.person`)

-- get list of all participants who have completed The Basics survey --
, has_the_basics AS (
    SELECT DISTINCT person_id
  FROM `{{project_id}}.{{dataset_id}}.concept_ancestor`
  INNER JOIN `{{project_id}}.{{dataset_id}}.observation` o ON observation_concept_id = descendant_concept_id
  INNER JOIN `{{project_id}}.{{dataset_id}}.concept` d ON d.concept_id = descendant_concept_id
  WHERE ancestor_concept_id = 1586134

  UNION DISTINCT

  SELECT DISTINCT person_id
  FROM `{{project_id}}.{{dataset_id}}.concept`
  JOIN `{{project_id}}.{{dataset_id}}.concept_ancestor`
    ON (concept_id = ancestor_concept_id)
  JOIN `{{project_id}}.{{dataset_id}}.observation`
    ON (descendant_concept_id = observation_concept_id)
  WHERE concept_class_id = 'Module'
    AND concept_name IN ('The Basics')
    AND questionnaire_response_id IS NOT NULL
)
-- get list of all participants who have not completed The Basics survey --
-- can be used during debugging to determine who doesn't have the basics --
, missing_the_basics AS (
  SELECT DISTINCT person_id
  FROM rdr_persons
  WHERE person_id NOT IN (SELECT DISTINCT person_id FROM has_the_basics)
)
 -- get list of all participants that could be dropped due to bad birth date records --
, bad_birthdate_records AS (
    SELECT person_id
    FROM `{{project_id}}.{{dataset_id}}.person` p
    WHERE p.year_of_birth < 1800
    OR p.year_of_birth > (EXTRACT(YEAR FROM CURRENT_DATE()) - 17)
)

-- store the research_id, person_id, and their AIAN status of all participants --
SELECT m.research_id, m.person_id AS participant_id,
  CASE
    WHEN
      a.person_id = m.person_id THEN 'yes' ELSE 'no'
    END AS is_aian
  FROM rdr_persons r
  LEFT JOIN `{{project_id}}.{{pipeline_lookup_tables}}.primary_pid_rid_mapping` m
    USING(person_id)
  LEFT JOIN `{{project_id}}.{{sandbox_dataset_id}}.aian_list` a
    USING(person_id)
  WHERE person_id IN (SELECT person_id FROM has_the_basics)
  AND person_id NOT IN (SELECT person_id FROM bad_birthdate_records)
);""")


class StoreExpectedCTList(BaseCleaningRule):
    """
    Store the expected CT participant list.
    """

    def __init__(self,
                 project_id,
                 dataset_id,
                 sandbox_dataset_id,
                 table_namer=None):
        desc = (
            f'Creates a sandbox table with expected participant research_ids '
            f'that will be found in the controlled tier dataset.  May be used '
            f'to help validate a particular pipeline run.  May be extended to '
            f'update which service accounts can read the data in the table.')

        super().__init__(
            issue_numbers=[
                'DC2595',
                'DC3404',
            ],
            description=desc,
            affected_datasets=[],  # has no side effects
            project_id=project_id,
            dataset_id=dataset_id,
            sandbox_dataset_id=sandbox_dataset_id,
            depends_on=[
                CleanMappingExtTables,
                CreateAIANLookup,
            ],
            table_namer=table_namer,
        )

    def get_query_specs(self):
        """
        Store the predicted CT participant list.

        :return: a list of SQL strings to run
        """
        create_sandbox_table = CREATE_EXPECTED_CT_LIST.render(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            sandbox_dataset_id=self.sandbox_dataset_id,
            mapping_table=PRIMARY_PID_RID_MAPPING,
            storage_table_name=self.sandbox_table_for(EXPECTED_CT_LIST),
            pipeline_lookup_tables=PIPELINE_TABLES,
            aian_list=AIAN_LIST,
        )

        create_sandbox_table_dict = {cdr_consts.QUERY: create_sandbox_table}

        return [create_sandbox_table_dict]

    def get_sandbox_tablenames(self):
        return [self.sandbox_table_for(EXPECTED_CT_LIST)]

    def setup_rule(self, client):
        pass

    def setup_validation(self):
        pass

    def validate_rule(self):
        pass


if __name__ == '__main__':
    from utils import pipeline_logging

    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.parse_args()
    pipeline_logging.configure(level=logging.DEBUG, add_console_handler=True)

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(ARGS.project_id,
                                                 ARGS.dataset_id,
                                                 ARGS.sandbox_dataset_id,
                                                 [(StoreExpectedCTList,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(StoreExpectedCTList,)])
