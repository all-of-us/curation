"""
Generalizes sex/gender concepts

For the following conditions ONLY, generalizes gender as described below:
1. Male sex at birth AND female gender
    a. Male sex: observation_source_concept_id = 1585845 with value_source_concept_id = 1585846
    b. Female gender: observation_source_concept_id = 1585838 with value_source_concept_id = 1585840
    c. In these conditions, sets value_source_concept_id for gender (observation_source_concept_id = 1585838  ) to 2000000002
2. Female sex at birth AND male gender
    a. Female sex: observation_source_concept_id = 1585845 with value_source_concept_id = 1585847
    b. Male gender: observation_source_concept_id = 1585838 with value_source_concept_id = 1585839
    c. In these conditions, set value_source_concept_id for gender (observation_source_concept_id = 1585838  ) to 2000000002

Original Issues: DC-526, DC-838
"""

#Python imports
import logging

# Third-party imports
from google.cloud import bigquery

# Project imports
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule
import constants.cdr_cleaner.clean_cdr as cdr_consts
from common import JINJA_ENV, OBSERVATION

LOGGER = logging.getLogger(__name__)

ISSUE_NUMBERS = ['DC526', 'DC838']

GENERALIZE_GENDER_CONCEPT_ID = 2000000002
WOMAN_CONCEPT_ID = 1585840
MAN_CONCEPT_ID = 1585839
SEX_AT_BIRTH_MALE_CONCEPT_ID = 1585846
SEX_AT_BIRTH_FEMALE_CONCEPT_ID = 1585847

SANDBOX_CONCEPT_ID_QUERY_TEMPLATE = JINJA_ENV.from_string("""
-- Using INSERT query because table creation is handled by rule_setup function --
INSERT INTO
  `{{project_id}}.{{sandbox_dataset}}.{{sandbox_table}}`
(
  SELECT
    *
  FROM
    `{{project_id}}.{{dataset_id}}.observation`
  WHERE
    observation_source_concept_id = 1585838
  AND 
      value_source_concept_id = {{gender_value_source_concept_id}}
  AND person_id IN 
  (
    SELECT
      person_id
    FROM
      `{{project_id}}.{{dataset_id}}.observation`
    WHERE
      observation_source_concept_id = 1585845
    AND 
        value_source_concept_id = {{biological_sex_birth_concept_id}}
  )
)
""")

GENERALIZED_CONCEPT_ID_QUERY_TEMPLATE = JINJA_ENV.from_string("""
UPDATE
  `{{project_id}}.{{dataset_id}}.observation`
SET
  value_as_concept_id = {{generalized_gender_concept_id}},
  value_source_concept_id = {{generalized_gender_concept_id}}
WHERE
  observation_source_concept_id = 1585838
  AND value_source_concept_id = {{gender_value_source_concept_id}}
  AND person_id IN 
  (
    SELECT
      person_id
    FROM
      `{{project_id}}.{{dataset_id}}.observation`
    WHERE
      observation_source_concept_id = 1585845
        AND value_source_concept_id = {{biological_sex_birth_concept_id}}
  )
""")


class GeneralizeSexGenderConcepts(BaseCleaningRule):

    def __init__(self,
                 project_id,
                 dataset_id,
                 sandbox_dataset_id,
                 table_namer=None):
        """
        Initialize the class with proper information.

        Set the issue numbers, description and affected datasets. As other tickets may affect
        this SQL, append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """
        desc = ('Rule to add generalizations for gender/sex mismatch')
        super().__init__(issue_numbers=ISSUE_NUMBERS,
                         description=desc,
                         affected_datasets=[cdr_consts.REGISTERED_TIER_DEID],
                         affected_tables=[OBSERVATION],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         table_namer=table_namer)

    def setup_rule(self, client):
        """
        Function to run any data upload options before executing a query.
        """

        #Create sandbox table
        sandbox_table_name = f'{self.project_id}.{self.sandbox_dataset_id}.{self.get_sandbox_tablenames()[0]}'
        schema = client.get_table_schema(OBSERVATION)
        table = bigquery.Table(sandbox_table_name, schema=schema)
        client.create_table(table, exists_ok=True)

    def get_query_specs(self, *args, **keyword_args):
        """
        Return a list of dictionary query specifications.

        :return: A list of dictionaries. Each dictionary contains a single query
             and a specification for how to execute that query. The specifications
             are optional but the query is required.
        """

        sandbox_woman_to_generalized_concept_id = {
            cdr_consts.QUERY:
                SANDBOX_CONCEPT_ID_QUERY_TEMPLATE.render(
                    project_id=self.project_id,
                    dataset_id=self.dataset_id,
                    sandbox_dataset=self.sandbox_dataset_id,
                    sandbox_table=self.get_sandbox_tablenames()[0],
                    gender_value_source_concept_id=WOMAN_CONCEPT_ID,
                    biological_sex_birth_concept_id=SEX_AT_BIRTH_MALE_CONCEPT_ID
                )
        }

        sandbox_man_to_generalized_concept_id = {
            cdr_consts.QUERY:
                SANDBOX_CONCEPT_ID_QUERY_TEMPLATE.render(
                    project_id=self.project_id,
                    dataset_id=self.dataset_id,
                    sandbox_dataset=self.sandbox_dataset_id,
                    sandbox_table=self.get_sandbox_tablenames()[0],
                    gender_value_source_concept_id=MAN_CONCEPT_ID,
                    biological_sex_birth_concept_id=
                    SEX_AT_BIRTH_FEMALE_CONCEPT_ID)
        }

        # An update query to update the gender to the generalized gender concept_id for the cases
        # where the biological sex is reported as male and gender is reported as woman.
        updating_woman_to_generalized_concept_id = {
            cdr_consts.QUERY:
                GENERALIZED_CONCEPT_ID_QUERY_TEMPLATE.render(
                    project_id=self.project_id,
                    dataset_id=self.dataset_id,
                    gender_value_source_concept_id=WOMAN_CONCEPT_ID,
                    biological_sex_birth_concept_id=SEX_AT_BIRTH_MALE_CONCEPT_ID,
                    generalized_gender_concept_id=GENERALIZE_GENDER_CONCEPT_ID)
        }

        # An update query to update the gender to the generalized gender concept_id for the cases
        # where the biological sex is reported as female and gender is reported as man.
        updating_man_to_generalized_concept_id = {
            cdr_consts.QUERY:
                GENERALIZED_CONCEPT_ID_QUERY_TEMPLATE.render(
                    project_id=self.project_id,
                    dataset_id=self.dataset_id,
                    gender_value_source_concept_id=MAN_CONCEPT_ID,
                    biological_sex_birth_concept_id=
                    SEX_AT_BIRTH_FEMALE_CONCEPT_ID,
                    generalized_gender_concept_id=GENERALIZE_GENDER_CONCEPT_ID)
        }

        return [
            sandbox_woman_to_generalized_concept_id,
            sandbox_man_to_generalized_concept_id,
            updating_woman_to_generalized_concept_id,
            updating_man_to_generalized_concept_id
        ]

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
        """
        generates sandbox table names
        """
        sandbox_table = self.sandbox_table_for(OBSERVATION)
        return [sandbox_table]


if __name__ == '__main__':
    import cdr_cleaner.clean_cdr_engine as clean_engine
    import cdr_cleaner.args_parser as parser

    ARGS = parser.parse_args()

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(
            ARGS.project_id, ARGS.dataset_id, ARGS.sandbox_dataset_id,
            [(GeneralizeSexGenderConcepts,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(GeneralizeSexGenderConcepts,)])
