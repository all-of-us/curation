"""
Original Issues: DC-1012, DC-1514

Background
In order to avoid further changes to the standard OMOP person table, five non-standard fields will be housed in a
person_ext table.

Cleaning rule script to run AFTER deid. This needs to happen in deid_base. It depends on the cleaning rules applied in 
deid to be correctly de-identified.
This cleaning rule will populate the person_ext table
The following fields will need to be copied from the observation table:
src_id (from observation_ext, should all be “PPI/PM”)
state_of_residence_concept_id: the value_source_concept_id field in the OBSERVATION table row where
observation_source_concept_id  = 1585249 (StreetAddress_PIIState)
state_of_residence_source_value: the concept_name from the concept table for the state_of_residence_concept_id
person_id (as research_id) can be pulled from the person table
sex_at_birth_concept_id: value_as_concept_id in observation where observation_source_concept_id = 1585845
sex_at_birth_source_concept_id: value_source_concept_id in observation where observation_source_concept_id = 1585845
sex_at_birth_source_value: concept_code in the concept table where joining from observation where 
observation_source_concept_id = 1585845
"""
import logging

# Project imports
import constants.cdr_cleaner.clean_cdr as cdr_consts
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule
from common import JINJA_ENV

LOGGER = logging.getLogger(__name__)

# Query to create person_ext table
PERSON_EXT_TABLE_QUERY = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE
  `{{project}}.{{dataset}}.person_ext` AS (
  SELECT
    p.person_id,
    e.src_id,
    o.value_source_concept_id AS state_of_residence_concept_id,
    c.concept_name AS state_of_residence_source_value,
    coalesce(os.value_as_concept_id,
      0) AS sex_at_birth_concept_id,
    coalesce(os.value_source_concept_id,
      0) AS sex_at_birth_source_concept_id,
    coalesce(sc.concept_code,
      'No matching concept') AS sex_at_birth_source_value
  FROM
    `{{project}}.{{dataset}}.person` p
  LEFT JOIN
    `{{project}}.{{dataset}}.observation` o
  ON
    p.person_id = o.person_id
    AND o.observation_source_concept_id = 1585249
  LEFT JOIN
    `{{project}}.{{dataset}}.concept` c
  ON
    o.value_source_concept_id = c.concept_id
    AND o.observation_source_concept_id = 1585249
  LEFT JOIN
    `{{project}}.{{dataset}}.observation_ext` e
  ON
    o.observation_id = e.observation_id
    AND o.observation_source_concept_id = 1585249
  LEFT JOIN
    `{{project}}.{{dataset}}.observation` os
  ON
    p.person_id = os.person_id
    AND os.observation_source_concept_id = 1585845
  LEFT JOIN
    `{{project}}.{{dataset}}.concept` sc
  ON
    os.value_source_concept_id = sc.concept_id
    AND os.observation_source_concept_id = 1585845)
""")

tables = ['person_ext']


class CreatePersonExtTable(BaseCleaningRule):
    """
    Create person_ext table after DEID, adds three non-standard fields:
    state_of_residence_concept_id, state_of_residence_source_value
    """

    def __init__(self,
                 project_id,
                 dataset_id,
                 sandbox_dataset_id,
                 table_namer=''):
        """
        Initialize the class with proper information.

        Set the issue numbers, description and affected datasets. As other tickets may affect
        this SQL, append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """
        desc = ('Create person_ext table')
        super().__init__(issue_numbers=['DC1012', 'DC1514'],
                         description=desc,
                         affected_datasets=[
                             cdr_consts.DEID_BASE,
                             cdr_consts.CONTROLLED_TIER_DEID_BASE
                         ],
                         affected_tables=tables,
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         table_namer=table_namer)

    def get_query_specs(self):
        """
        Return a list of dictionary query specifications.

        :return:  A list of dictionaries. Each dictionary contains a single query
            and a specification for how to execute that query. The specifications
            are optional but the query is required.
        """

        person_ext_table_query = {
            cdr_consts.QUERY:
                PERSON_EXT_TABLE_QUERY.render(project=self.project_id,
                                              dataset=self.dataset_id)
        }
        return [person_ext_table_query]

    def setup_rule(self, client):
        """
        Function to run any data upload options before executing a query.
        """
        pass

    def setup_validation(self, client):
        """
        Run required steps for validation setup
        """
        raise NotImplementedError("Please fix me.")

    def validate_rule(self, client):
        """
        Validates the cleaning rule which deletes or updates the data from the tables
        """
        raise NotImplementedError("Please fix me.")

    def get_sandbox_tablenames(self):

        return []


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.parse_args()

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(ARGS.project_id,
                                                 ARGS.dataset_id,
                                                 ARGS.sandbox_dataset_id,
                                                 [(CreatePersonExtTable,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(CreatePersonExtTable,)])
