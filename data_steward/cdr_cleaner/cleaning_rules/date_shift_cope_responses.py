"""
Reverse Date Shift for COPE Responses.

The COPE survey is longitudinal (administered monthly), so researchers will need a way to identify with which survey
monthly iteration data is associated. Although there is a long-term plan to allow for survey versioning,
this is anticipated to provide a short-term solution to support inclusion of COPE data in the CDR.

The Date shift is applied on the cope survey responses by following steps:

1)lookup table with all the COPE survey module descendants were generated using the concept_relationship table.

2) Created a CTE with filtering out the observation table with only observation_concept_ids that were cope_concept_ids
 using the `pipeline_tables.cope_concepts` and then apply the date_shit to those records in the CTE.

3) Then Left joined this CTE temp table to the observation table post_deid using the observation_id.

4) Using the Left join used coalesce function to get the date_shifted observation_date and observation_datetime for the
 cope survey responses.

Original Issue: DC-938
"""

# Python imports
import logging

# Third party imports
from jinja2 import Environment

# Project Imports
import constants.bq_utils as bq_consts
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule
from constants.cdr_cleaner import clean_cdr as cdr_consts

LOGGER = logging.getLogger(__name__)
jinja_env = Environment(
    # help protect against cross-site scripting vulnerabilities
    autoescape=True,
    # block tags on their own lines
    # will not cause extra white space
    trim_blocks=True,
    lstrip_blocks=True,
    # syntax highlighting should be better
    # with these comment delimiters
    comment_start_string='--',
    comment_end_string=' --')

PIPELINE_DATASET = 'pipeline_tables'
COPE_CONCEPTS_TABLE = 'cope_concepts'
OBSERVATION = 'observation'

SANDBOX_COPE_SURVEY_QUERY = jinja_env.from_string("""
CREATE OR REPLACE TABLE
  `{{project_id}}.{{sandbox_dataset}}.{{intermediary_table}}` AS
SELECT
  *
FROM
  `{{project_id}}.{{dataset_id}}.{{observation_table}}`
WHERE
  observation_source_value IN (
  SELECT
    concept_code
  FROM
    `{{project_id}}.{{pipeline_tables_dataset}}.{{cope_concepts_table}}`)
    """)

DATE_SHIFT_QUERY = jinja_env.from_string("""
WITH
  cope_shift AS (
  SELECT
    observation_id,
    observation_date,
    observation_datetime,
  FROM
    `{{project_id}}.{{pre_deid_dataset}}.{{observation_table}}`
   WHERE
    observation_source_value IN (
    SELECT
      concept_code
    FROM
      `{{project_id}}.{{pipeline_tables_dataset}}.{{cope_concepts_table}}`))
SELECT
  ob.observation_id,
  ob.person_id,
  ob.observation_concept_id,
  coalesce(cs.observation_date,
    ob.observation_date) AS observation_date,
  coalesce(cs.observation_datetime,
    ob.observation_datetime) AS observation_datetime,
  ob.observation_type_concept_id,
  ob.value_as_number,
  ob.value_as_string,
  ob.value_as_concept_id,
  ob.qualifier_concept_id,
  ob.unit_concept_id,
  ob.provider_id,
  ob.visit_occurrence_id,
  ob.observation_source_value,
  ob.observation_source_concept_id,
  ob.unit_source_value,
  ob.qualifier_source_value,
  ob.value_source_concept_id,
  ob.value_source_value,
  ob.questionnaire_response_id
FROM
  `{{project_id}}.{{dataset_id}}.{{observation_table}}` AS ob
LEFT JOIN
  cope_shift AS cs
USING
  (observation_id)""")


class DateShiftCopeResponses(BaseCleaningRule):
    """
    Reverse Date Shift for COPE Responses...
    """

    def __init__(self, project_id, dataset_id, sandbox_dataset_id):
        """
        Initialize the class with proper information.

        Set the issue numbers, description and affected datasets. As other tickets may affect
        this SQL, append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """
        desc = 'Reverse Date Shift for COPE Responses'
        super().__init__(issue_numbers=['DC938', 'DC982', 'DC970'],
                         description=desc,
                         affected_datasets=[cdr_consts.DEID_BASE],
                         affected_tables=[OBSERVATION],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id)

    def get_combined_dataset_from_deid_dataset(self, dataset_name):
        """
        Returns a combined dataset name from a de_identified dataset name
        :param dataset_name: name of a de_identified_dataset_name
        :return: combined dataset name as a string
        """

        return f'{dataset_name[1:9].lower()}_combined'

    def setup_rule(self, client=None):
        """
        Load required resources prior to executing cleaning rule queries.

        Method to run data upload options before executing the first cleaning
        rule of a class.  For example, if your class requires loading a static
        table, that load operation should be defined here.  It SHOULD NOT BE
        defined as part of get_query_specs().
        :param client:
        :return:
        """
        pass

    def get_query_specs(self):
        """
        :return:
        """
        sandbox_query = dict()
        sandbox_query[cdr_consts.QUERY] = SANDBOX_COPE_SURVEY_QUERY.render(
            project_id=self.project_id,
            sandbox_dataset=self.sandbox_dataset_id,
            intermediary_table=self.get_sandbox_tablenames()[0],
            dataset_id=self.dataset_id,
            pipeline_tables_dataset=PIPELINE_DATASET,
            cope_concepts_table=COPE_CONCEPTS_TABLE,
            observation_table=OBSERVATION)

        update_query = dict()
        update_query[cdr_consts.QUERY] = DATE_SHIFT_QUERY.render(
            project_id=self.project_id,
            pre_deid_dataset=self.get_combined_dataset_from_deid_dataset(
                self.dataset_id),
            dataset_id=self.dataset_id,
            pipeline_tables_dataset=PIPELINE_DATASET,
            cope_concepts_table=COPE_CONCEPTS_TABLE,
            observation_table=OBSERVATION)
        update_query[cdr_consts.DESTINATION_TABLE] = OBSERVATION
        update_query[cdr_consts.DISPOSITION] = bq_consts.WRITE_TRUNCATE
        update_query[cdr_consts.DESTINATION_DATASET] = self.dataset_id
        return [sandbox_query, update_query]

    def setup_validation(self, client):
        """
        Run required steps for validation setup
        """
        pass

    def validate_rule(self, client):
        """
        Validates the cleaning rule which deletes or updates the data from the tables
        """
        pass

    def get_sandbox_table_name(self):
        return f'{self._issue_numbers[0].lower()}_{OBSERVATION}'

    def get_sandbox_tablenames(self):
        return [self.get_sandbox_table_name()]


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.parse_args()

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(ARGS.project_id,
                                                 ARGS.dataset_id,
                                                 [(DateShiftCopeResponses,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   [(DateShiftCopeResponses,)])
