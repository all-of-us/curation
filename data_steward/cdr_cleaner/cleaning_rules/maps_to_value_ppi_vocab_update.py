"""
Due to a bug in the Odysseus ETL, we need to manually fix the VALUE AS CONCEPT ID mapping.
This cleaning rule corrects all VALUE AS CONCEPT ID to be the ANSWER concept ID which may
not be from PPI vocab.

Original Issues: DC-418, DC-2590
"""

import logging

# Project imports
import constants.cdr_cleaner.clean_cdr as cdr_consts
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule
from cdr_cleaner.cleaning_rules.set_unmapped_question_answer_survey_concepts import (
    SetConceptIdsForSurveyQuestionsAnswers)
from common import JINJA_ENV, OBSERVATION

LOGGER = logging.getLogger(__name__)

JIRA_ISSUE_NUMBERS = ['DC418', 'DC2590']

SANDBOX_PPI_QUERY = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project}}.{{sandbox_dataset}}.{{sandbox_table}}` AS (
    SELECT a.observation_id, a.value_as_concept_id 
    FROM `{{project}}.{{dataset}}.observation` AS a 
    JOIN (
        SELECT *
        FROM (
            SELECT
                c2.concept_name,
                c2.concept_id,
                o.*,
                RANK() OVER (
                PARTITION BY o.observation_id, o.value_source_concept_id ORDER BY c2.concept_id ASC
                ) AS rank
            FROM `{{project}}.{{dataset}}.observation` o
            JOIN `{{project}}.{{dataset}}.concept` c
            ON o.value_source_concept_id = c.concept_id
            JOIN `{{project}}.{{dataset}}.concept_relationship` cr
            ON c.concept_id = cr.concept_id_1
            AND cr.relationship_id = 'Maps to value'
            JOIN `{{project}}.{{dataset}}.concept` c2
            ON c2.concept_id = cr.concept_id_2
            WHERE o.observation_concept_id = o.value_as_concept_id
            AND o.observation_concept_id != 0
        )
        WHERE rank=1
    ) AS b
    ON a.observation_id = b.observation_id
    WHERE a.value_as_concept_id != b.concept_id
)
""")

UPDATE_PPI_QUERY = JINJA_ENV.from_string("""
UPDATE `{{project}}.{{dataset}}.observation` a
SET a.value_as_concept_id = b.concept_id
FROM (
    SELECT *
    FROM (
        SELECT
            c2.concept_name,
            c2.concept_id,
            o.*,
            RANK() OVER (
              PARTITION BY o.observation_id, o.value_source_concept_id ORDER BY c2.concept_id ASC
            ) AS rank
        FROM `{{project}}.{{dataset}}.observation` o
        JOIN `{{project}}.{{dataset}}.concept` c
        ON o.value_source_concept_id = c.concept_id
        JOIN `{{project}}.{{dataset}}.concept_relationship` cr
        ON c.concept_id = cr.concept_id_1
        AND cr.relationship_id = 'Maps to value'
        JOIN `{{project}}.{{dataset}}.concept` c2
        ON c2.concept_id = cr.concept_id_2
        WHERE o.observation_concept_id = o.value_as_concept_id
        AND o.observation_concept_id != 0
    )
    WHERE rank=1
) AS b
WHERE a.observation_id = b.observation_id
""")


class MapsToValuePpiVocabUpdate(BaseCleaningRule):

    def __init__(self,
                 project_id,
                 dataset_id,
                 sandbox_dataset_id=None,
                 table_namer=None):
        """
        Initialize the class with proper information.

        Set the issue numbers, description and affected datasets. As other tickets may affect
        this SQL, append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """
        desc = ('Corrects VALUE AS CONCEPT ID to be the ANSWER concept ID')

        super().__init__(issue_numbers=JIRA_ISSUE_NUMBERS,
                         description=desc,
                         affected_datasets=[cdr_consts.RDR],
                         affected_tables=[OBSERVATION],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         depends_on=[SetConceptIdsForSurveyQuestionsAnswers],
                         table_namer=table_namer,
                         run_for_synthetic=True)

    def get_query_specs(self):
        """
        Return a list of dictionary query specifications.

        :return:  A list of dictionaries. Each dictionary contains a single query
            and a specification for how to execute that query. The specifications
            are optional but the query is required.
        """
        queries_list = []

        sandbox_query, update_query = dict(), dict()

        sandbox_query[cdr_consts.QUERY] = SANDBOX_PPI_QUERY.render(
            sandbox_dataset=self.sandbox_dataset_id,
            sandbox_table=self.sandbox_table_for(OBSERVATION),
            project=self.project_id,
            dataset=self.dataset_id)
        queries_list.append(sandbox_query)

        update_query[cdr_consts.QUERY] = UPDATE_PPI_QUERY.render(
            project=self.project_id, dataset=self.dataset_id)
        queries_list.append(update_query)

        return queries_list

    def setup_rule(self, client, *args, **keyword_args):
        pass

    def setup_validation(self, client):
        raise NotImplementedError("Please fix me.")

    def validate_rule(self, client):
        raise NotImplementedError("Please fix me.")

    def get_sandbox_tablenames(self):
        return [self.sandbox_table_for(table) for table in self.affected_tables]


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.parse_args()

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(ARGS.project_id,
                                                 ARGS.dataset_id,
                                                 ARGS.sandbox_dataset_id,
                                                 [(MapsToValuePpiVocabUpdate,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(MapsToValuePpiVocabUpdate,)])
