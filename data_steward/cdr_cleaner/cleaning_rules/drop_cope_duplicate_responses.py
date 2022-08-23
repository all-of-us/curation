"""
Removes the duplicate sets of COPE responses to the same questions in the same survey version.

In PPI(COPE) surveys, the purpose of questionnaire_response_id is to group all responses from the same survey together.
Some COPE questions allowed participants to provide multiple answers, which be will connected via the same
questionnaire_response_id. However, a participant may submit the responses multiple times for the same questions,
therefore creating duplicates. for the COPE surveys there are multiple versions of the survey where the questions can be
reused in multiple versions. we need to keep the same question answer pairs from different versions.
We need to use the combination of person_id, observation_source_concept_id,
observation_source_value, and questionnaire_response_id and cope_month to identify multiple sets of responses.
We only want to keep the most recent set of responses and remove previous sets of responses per each cope_month version.
cope_survey_semantic_version_map in the rdr dataset can be used to get the cope_month version.

In short the query should achieve
Step 1:
 Identify most recent questionnaire_response_id for same person, question, cope_month combination.
Step 2:
 Prioritize responses with same person, question, cope_month combination with the most recent questionnaire_response_id.
Step 3:
 Keep only records associated with most questionnaire_response_id, person, question, answer per each cope_month version.
"""
import logging

from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule
# Project imports
from common import JINJA_ENV
from constants.cdr_cleaner import clean_cdr as cdr_consts
from utils import pipeline_logging

LOGGER = logging.getLogger(__name__)

COPE_SURVEY_VERSION_MAP_TABLE = 'cope_survey_semantic_version_map'

ISSUE_NUMBERS = ['DC1146', 'DC1135']
OBSERVATION = 'observation'

SANDBOX_DUPLICATE_COPE_RESPONSES = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE
  `{{project}}.{{sandbox_dataset}}.{{intermediary_table}}` AS
SELECT
  o.* EXCEPT (cope_month,
              rank_order,
              is_pmi_skip,
              max_observation_datetime)
FROM (
  SELECT
    *,
    DENSE_RANK() OVER(
    PARTITION BY person_id,
      observation_source_concept_id,
      observation_source_value,
      value_source_value,
      cope_month 
      ORDER BY 
        is_pmi_skip ASC,
        max_observation_datetime DESC,
        questionnaire_response_id DESC,
        observation_id DESC) AS rank_order
  FROM (
    SELECT
      obs.*,
    IF
      (value_source_value = 'PMI_Skip',
        1,
        0) AS is_pmi_skip,
      MAX(observation_datetime) OVER(
      PARTITION BY person_id,
          observation_source_concept_id, 
          observation_source_value,
          cope.cope_month,
          obs.questionnaire_response_id) AS max_observation_datetime,
      cope.cope_month /* will handle case if obs table has valid cope_month or not */
    FROM
      `{{project}}.{{dataset}}.observation` obs
    JOIN
      `{{project}}.{{dataset}}.{{cope_survey_version_table}}` cope
    ON
      obs.questionnaire_response_id = cope.questionnaire_response_id 
      AND obs.person_id = cope.participant_id)
    ) o
WHERE
  o.rank_order != 1
""")

REMOVE_DUPLICATE_COPE_RESPONSES = JINJA_ENV.from_string("""
DELETE
FROM
  `{{project}}.{{dataset}}.observation`
WHERE
  observation_id IN (
  SELECT
    observation_id
  FROM
    `{{project}}.{{sandbox_dataset}}.{{intermediary_table}}` )
""")


class DropCopeDuplicateResponses(BaseCleaningRule):
    """
    Removes the duplicate sets of responses to the same questions excluding COPE survey.
    """

    def __init__(self, project_id, dataset_id, sandbox_dataset_id):
        """
        Initialize the class with proper information.

        Set the issue numbers, description and affected datasets. As other tickets may affect
        this SQL, append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """
        desc = 'Removes the duplicate sets of COPE responses to the same questions from the same survey version.'
        super().__init__(issue_numbers=ISSUE_NUMBERS,
                         description=desc,
                         affected_datasets=[cdr_consts.RDR],
                         affected_tables=[OBSERVATION],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         run_for_synthetic=True)

    def get_query_specs(self):
        """
        Return a list of dictionary query specifications.

        :return:  A list of dictionaries. Each dictionary contains a single query
            and a specification for how to execute that query. The specifications
            are optional but the query is required.
        """

        sandbox_duplicate_rows = {
            cdr_consts.QUERY:
                SANDBOX_DUPLICATE_COPE_RESPONSES.render(
                    project=self.project_id,
                    dataset=self.dataset_id,
                    sandbox_dataset=self.sandbox_dataset_id,
                    cope_survey_version_table=COPE_SURVEY_VERSION_MAP_TABLE,
                    intermediary_table=self.get_sandbox_tablenames()[0])
        }

        delete_duplicate_rows = {
            cdr_consts.QUERY:
                REMOVE_DUPLICATE_COPE_RESPONSES.render(
                    project=self.project_id,
                    dataset=self.dataset_id,
                    sandbox_dataset=self.sandbox_dataset_id,
                    intermediary_table=self.get_sandbox_tablenames()[0])
        }

        return [sandbox_duplicate_rows, delete_duplicate_rows]

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
        sandbox_table = f'{self._issue_numbers[0].lower()}_{self.affected_tables[0]}'
        return [sandbox_table]


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    pipeline_logging.configure()
    ARGS = parser.parse_args()

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(
            ARGS.project_id, ARGS.dataset_id, ARGS.sandbox_dataset_id,
            [(DropCopeDuplicateResponses,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(DropCopeDuplicateResponses,)])
