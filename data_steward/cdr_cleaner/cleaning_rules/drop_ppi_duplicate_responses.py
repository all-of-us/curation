"""
Removes the duplicate sets of responses to the same questions excluding COPE survey responses.

Original Issues: DC-1051, DC-532

In PPI surveys, the purpose of questionnaire_response_id is to group all responses from the same survey together.
Some PPI questions allowed participants to provide multiple answers, which be will connected via the same
questionnaire_response_id. However, a participant may submit the responses multiple times for the same questions,
therefore creating duplicates. We need to use the combination of person_id, observation_source_concept_id,
observation_source_value, and questionnaire_response_id to identify multiple sets of responses. We only want to keep
the most recent set of responses and remove previous sets of responses. When identifying the most recent responses,
we can't use questionnaire_response_id alone because a larger questionnaire_response_id doesn't mean it's created at
a later time therefore we need to create ranks (lowest rank = most recent responses) based on observation_date_time.
However, we also need to add questionnaire_response_id for assigning unique ranks to different sets of responses
because there are cases where the multiple sets of responses for the same question were submitted at exactly the same
timestamp but with different answers. In addition, we also need to check whether one of the duplicate responses is
PMI_Skip because there are cases where the most recent response is a skip and the legitimate response was submitted
earlier, we want to keep the actual response instead of PMI_Skip regardless of the timestamps of those responses.
"""

# Python imports
import logging

# Project imports
from common import JINJA_ENV
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule
from constants.cdr_cleaner import clean_cdr as cdr_consts

LOGGER = logging.getLogger(__name__)

COPE_SURVEY_VERSION_MAP_TABLE = 'cope_survey_semantic_version_map'

SELECT_DUPLICATE_TEMPLATE = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE
    `{{project}}.{{sandbox_dataset}}.{{intermediary_table}}` AS 
SELECT
  *
FROM
  `{{project}}.{{dataset}}.observation`
WHERE
  observation_id IN 
""")

REMOVE_DUPLICATE_TEMPLATE = JINJA_ENV.from_string("""
DELETE
FROM
  `{{project}}.{{dataset}}.observation`
WHERE
  observation_id IN 
""")

IDENTIFY_DUPLICATE_ID_TEMPLATE = JINJA_ENV.from_string("""
    ( SELECT
      observation_id
    FROM (
      SELECT
        *,
        DENSE_RANK() OVER(
              PARTITION BY person_id, 
              observation_source_concept_id, 
              observation_source_value 
              ORDER BY is_pmi_skip ASC, max_observation_datetime DESC, questionnaire_response_id DESC) AS rank_order
      FROM (
        SELECT
          observation_id,
          person_id,
          observation_source_concept_id,
          observation_source_value,
          questionnaire_response_id,
          IF(value_source_value = \'PMI_Skip\', 1, 0) AS is_pmi_skip,
          MAX(observation_datetime) OVER(
              PARTITION BY person_id, 
              observation_source_concept_id, 
              observation_source_value, 
              questionnaire_response_id) AS max_observation_datetime
        FROM `{{project}}.{{dataset}}.observation` 
        WHERE observation_source_concept_id != 1586099  /* exclude EHRConsentPII_ConsentPermission */
        AND observation_id NOT IN (
            SELECT
                observation_id
            FROM
                `{{project}}.{{dataset}}.observation` ob
            JOIN
                `{{project}}.{{dataset}}.{{cope_survey_version_table}}` svm
            ON
                person_id = participant_id
            AND ob.questionnaire_response_id = svm.questionnaire_response_id
            /* exclude COPE & MINUTE Survey Responses */
        )
      ) o
    ) o
    WHERE o.rank_order != 1 )
""")


def get_select_statement(project_id, dataset_id, sandbox_dataset_id,
                         sandbox_table):
    duplicate_id_query = IDENTIFY_DUPLICATE_ID_TEMPLATE.render(
        project=project_id,
        dataset=dataset_id,
        cope_survey_version_table=COPE_SURVEY_VERSION_MAP_TABLE)

    select_duplicates_query = SELECT_DUPLICATE_TEMPLATE.render(
        project=project_id,
        dataset=dataset_id,
        sandbox_dataset=sandbox_dataset_id,
        intermediary_table=sandbox_table)
    return select_duplicates_query + duplicate_id_query


def get_delete_statement(project_id, dataset_id):
    duplicate_id_query = IDENTIFY_DUPLICATE_ID_TEMPLATE.render(
        project=project_id,
        dataset=dataset_id,
        cope_survey_version_table=COPE_SURVEY_VERSION_MAP_TABLE)
    delete_duplicates_template = REMOVE_DUPLICATE_TEMPLATE.render(
        project=project_id, dataset=dataset_id)
    return delete_duplicates_template + duplicate_id_query


class DropPpiDuplicateResponses(BaseCleaningRule):
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
        desc = 'Removes the duplicate sets of responses to the same questions excluding COPE survey responses.'
        super().__init__(issue_numbers=['DC1051', 'DC532'],
                         description=desc,
                         affected_datasets=[cdr_consts.RDR],
                         affected_tables=['observation'],
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

        save_duplicate_rows = {
            cdr_consts.QUERY:
                get_select_statement(self.project_id, self.dataset_id,
                                     self.sandbox_dataset_id,
                                     self.get_sandbox_tablenames()[0])
        }

        delete_duplicate_rows = {
            cdr_consts.QUERY:
                get_delete_statement(self.project_id, self.dataset_id)
        }

        return [save_duplicate_rows, delete_duplicate_rows]

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

    def get_sandbox_table_name(self):
        return f'{self._issue_numbers[0].lower()}_{self.affected_tables[0]}'

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
                                                 ARGS.sandbox_dataset_id,
                                                 [(DropPpiDuplicateResponses,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(DropPpiDuplicateResponses,)])
