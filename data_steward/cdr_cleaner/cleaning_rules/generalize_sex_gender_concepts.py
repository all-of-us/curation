#Python imports
import logging

# Third-party imports
from google.cloud import bigquery

# Project imports
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule
import constants.cdr_cleaner.clean_cdr as cdr_consts
from common import JINJA_ENV, OBSERVATION

LOGGER = logging.getLogger(__name__)

ISSUE_NUMBERS = ['DC838']

GENERALIZE_GENDER_CONCEPT_ID = 2000000002
WOMAN_CONCEPT_ID = 1585840
MAN_CONCEPT_ID = 1585839
SEX_AT_BIRTH_MALE_CONCEPT_ID = 1585846
SEX_AT_BIRTH_FEMALE_CONCEPT_ID = 1585847

SANDBOX_CONCEPT_ID_QUERY_TEMPLATE = JINJA_ENV.from_string("""
INSERT INTO
  `{{project_id}}.{{sandbox_dataset}}.{{sandbox_table}}` AS
(
  SELECT
    *
  FROM
    `{{project_id}}.{{dataset_id}}.observation`
  WHERE
    observation_source_concept_id = 1585838 -- the concept for gender identity
  AND 
      value_source_concept_id = {{gender_value_source_concept_id}}
  AND person_id IN 
  (
    SELECT
      person_id
    FROM
      `{{project_id}}.{{dataset_id}}.observation`
    WHERE
      observation_source_concept_id = 1585845 -- the concept for biological sex at birth
    AND 
        value_source_concept_id = {{biological_sex_birth_concept_id}}
  )
)
""")

NEW_GENERALIZED_CONCEPT_ID_QUERY_TEMPLATE = JINJA_ENV.from_string("""
UPDATE
  `{{project_id}}.{{dataset_id}}.observation`
SET
  value_as_concept_id = {{generalized_gender_concept_id}},
  value_source_concept_id = {{generalized_gender_concept_id}}
WHERE
  observation_source_concept_id = 1585838 -- the concept for gender identity
  AND value_source_concept_id = {{gender_value_source_concept_id}}
  AND person_id IN 
  (
    SELECT
      person_id
    FROM
      `{{project_id}}.{{dataset_id}}.observation`
    WHERE
      observation_source_concept_id = 1585845 -- the concept for biological sex at birth
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
                         affected_datasets=[cdr_consts.FITBIT],
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
                NEW_GENERALIZED_CONCEPT_ID_QUERY_TEMPLATE.render(
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
                NEW_GENERALIZED_CONCEPT_ID_QUERY_TEMPLATE.render(
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
                NEW_GENERALIZED_CONCEPT_ID_QUERY_TEMPLATE.render(
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
                NEW_GENERALIZED_CONCEPT_ID_QUERY_TEMPLATE.render(
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


# Will be removed after integration test has been implemented
GENERALIZED_CONCEPT_ID_QUERY_TEMPLATE = '''
UPDATE
  `{project_id}.{dataset_id}.observation`
SET
  value_as_concept_id = {generalized_gender_concept_id},
  value_source_concept_id = {generalized_gender_concept_id}
WHERE
  observation_source_concept_id = 1585838 -- the concept for gender identity
  AND value_source_concept_id = {gender_value_source_concept_id}
  AND person_id IN (
  SELECT
    person_id
  FROM
    `{project_id}.{dataset_id}.observation`
  WHERE
    observation_source_concept_id = 1585845 -- the concept for biological sex at birth
      AND value_source_concept_id = {biological_sex_birth_concept_id})
'''


# Will be removed after integration test has been implemented
def parse_query_for_updating_woman_to_generalized_concept_id(
    project_id, dataset_id):
    """
    This function returns an update query to update the gender to the generalized gender concept_id for the cases
    where the biological sex is reported as male and gender is reported as woman.
    :param project_id: the project id
    :param dataset_id: the dataset id
    :return:an update query to update the gender from woman to the generalized concept id
    """
    return GENERALIZED_CONCEPT_ID_QUERY_TEMPLATE.format(
        project_id=project_id,
        dataset_id=dataset_id,
        gender_value_source_concept_id=WOMAN_CONCEPT_ID,
        biological_sex_birth_concept_id=SEX_AT_BIRTH_MALE_CONCEPT_ID,
        generalized_gender_concept_id=GENERALIZE_GENDER_CONCEPT_ID)


# Will be removed after integration test has been implemented
def parse_query_for_updating_man_to_generalized_concept_id(
    project_id, dataset_id):
    """
    This function returns an update query to update the gender to the generalized gender concept_id for the cases
    where the biological sex is reported as female and gender is reported as man.
    :param project_id: the project id
    :param dataset_id: the dataset id
    :return:an update query to update the gender from man to the generalized concept id
    """
    return GENERALIZED_CONCEPT_ID_QUERY_TEMPLATE.format(
        project_id=project_id,
        dataset_id=dataset_id,
        gender_value_source_concept_id=MAN_CONCEPT_ID,
        biological_sex_birth_concept_id=SEX_AT_BIRTH_FEMALE_CONCEPT_ID,
        generalized_gender_concept_id=GENERALIZE_GENDER_CONCEPT_ID)


# Will be removed after integration test has been implemented
def get_generalized_concept_id_queries(project_id,
                                       dataset_id,
                                       sandbox_dataset_id=None):
    """
    This function generates a list of query dicts for updating the records for which we need to the generalize gender
    in both of value_as_concept_id and value_source_concept_id
    :param project_id: the project_id in which the query is run
    :param dataset_id: the dataset_id in which the query is run
    :param sandbox_dataset_id: Identifies the sandbox dataset to store rows 
    #TODO use sandbox_dataset_id for CR
    :return: a list of query dicts for updating the gender concept ids
    """

    queries = []

    query = dict()
    query[cdr_consts.
          QUERY] = parse_query_for_updating_woman_to_generalized_concept_id(
              project_id, dataset_id)
    query[cdr_consts.BATCH] = True
    queries.append(query)

    query = dict()
    query[cdr_consts.
          QUERY] = parse_query_for_updating_man_to_generalized_concept_id(
              project_id, dataset_id)
    query[cdr_consts.BATCH] = True
    queries.append(query)

    return queries


if __name__ == '__main__':
    import cdr_cleaner.clean_cdr_engine as clean_engine
    import cdr_cleaner.args_parser as parser

    ARGS = parser.parse_args()

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(
            ARGS.project_id, ARGS.dataset_id, ARGS.sandbox_dataset_id,
            [(get_generalized_concept_id_queries,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(get_generalized_concept_id_queries,)])
