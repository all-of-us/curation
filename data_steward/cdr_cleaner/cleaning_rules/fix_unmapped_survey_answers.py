"""Original Issues: DC-1043, DC-1053
PPI records may have value_as_concept_id set to 0 by the RDR ETL as a result of changes to the
vocabulary. This rule sets value_as_concept_id to an appropriate value for these records. Before
they are updated rows are stored in a sandbox table with the same columns as observation and
an additional column new_value_as_concept_id.

The RDR ETL populates value_as_concept_id with a concept in the concept_relationship table
having the 'Maps to value' relationship with value_source_concept_id, however concept_relationship
may not have such a mapping for all PPI answer concepts and in these cases the associated
value_as_concept_id will get a default value of 0. For these rows this rule uses concepts related
to value_source_concept_id by 'Maps to' instead. In order to prevent usage of erroneous mappings
which may be present in the vocabulary, the target concept is also constrained to a limited set
of concept classes ('Answer', 'Context-dependent', 'Clinical Finding', 'Unit').
PPI answers are mapped to standard answer concepts in concept_relationship through 'Maps to
value' (it has been this case historically and it is still the case now), however, there are a
bunch of PPI answer concepts missing such relationships in concept_relationship. Interestingly,
the corresponding standard PPI answer concepts could be found through 'Maps to'.  This might have
been a bug in the vocabulary and they probably should’ve used 'Maps to value' for mapping Answer
concepts.

There are survey answers (value_source_concept_ids) in observation that are mapped to a '0'
value_as_concept_id. Those unmapped survey answers could be standard concepts, non-standard
concepts that could be mapped to a standard concept through Maps to in concept_relationship,
or deprecated concepts that do not map to anything.

For the standard answers, we could just use it as-is for populating value_as_concept_id. However,
among the non-standard concepts, not all of the mapped standard concepts are classified as
'Answer' and they could belong to other concept_classes. Below is a list of the concept_class_id
of the mapped concepts:

Context-dependent
Answer
Clinical Finding
Question
Unit
Module

Question or Module concept classes don't make sense so will get excluded. In conclusion,

1. For standard concepts --> Set value_as_concept_id to the value_source_concept_id.

2. For deprecated concepts --> Set value_as_concept_id to 0.
Actually we don't need to do anything for this case because value_as_concept_id is already 0.

3, For non-standard concepts --> Set value_as_concept_id to standard concept ids mapped through
'Maps to' for the concept classes ( 'Answer', 'Context-dependent', 'Clinical Finding',
'Unit') only. """

import logging

# Project imports
import constants.cdr_cleaner.clean_cdr as cdr_consts
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule, query_spec_list
from cdr_cleaner.cleaning_rules.set_unmapped_question_answer_survey_concepts import (
    SetConceptIdsForSurveyQuestionsAnswers)
from constants.bq_utils import WRITE_TRUNCATE
from common import OBSERVATION, JINJA_ENV

LOGGER = logging.getLogger(__name__)

JIRA_ISSUE_NUMBERS = ['DC1043', 'DC1053']

# Query to create tables in sandbox with the rows that will be updated per cleaning rule
SANDBOX_FIX_UNMAPPED_SURVEY_ANSWERS_QUERY = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project}}.{{sandbox_dataset}}.{{sandbox_table}}` AS
SELECT
  o.*,
  COALESCE(cr.concept_id_2, 0) AS new_value_as_concept_id
FROM
  `{{project}}.{{dataset}}.observation` AS o
JOIN
  `{{project}}.{{dataset}}.concept_relationship` AS cr
ON
  o.value_source_concept_id = cr.concept_id_1 AND cr.relationship_id = 'Maps to'
JOIN
  `{{project}}.{{dataset}}.concept` AS c
ON
  c.concept_id = cr.concept_id_2
    AND c.concept_class_id IN ('Answer', 'Context-dependent', 'Clinical Finding', 'Unit')
WHERE
    o.observation_type_concept_id = 45905771 --Observation Recorded from a Survey --
        AND o.value_source_concept_id <> 0 AND o.value_as_concept_id = 0
""")

UPDATE_FIX_UNMAPPED_SURVEY_ANSWERS_QUERY = JINJA_ENV.from_string("""
SELECT
  o.observation_id,
  o.person_id,
  o.observation_concept_id,
  o.observation_date,
  o.observation_datetime,
  o.observation_type_concept_id,
  o.value_as_number,
  o.value_as_string,
  COALESCE(o2.new_value_as_concept_id, o.value_as_concept_id, 0) AS value_as_concept_id,
  o.qualifier_concept_id,
  o.unit_concept_id,
  o.provider_id,
  o.visit_occurrence_id,
  o.visit_detail_id,
  o.observation_source_value,
  o.observation_source_concept_id,
  o.unit_source_value,
  o.qualifier_source_value,
  o.value_source_concept_id,
  o.value_source_value,
  o.questionnaire_response_id
FROM `{{project}}.{{dataset}}.observation` AS o
LEFT JOIN `{{project}}.{{sandbox_dataset}}.{{sandbox_table}}` AS o2
  ON o.observation_id = o2.observation_id
""")


class FixUnmappedSurveyAnswers(BaseCleaningRule):

    def __init__(self, project_id, dataset_id, sandbox_dataset_id):
        """
        Initialize the class with proper information.

        Set the issue numbers, description and affected datasets. As other tickets may affect
        this SQL, append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """
        desc = (
            'Update the survey answers that are not standard in value_as_concept_id using the '
            'Maps to relationship in concept_relationship ')

        super().__init__(issue_numbers=JIRA_ISSUE_NUMBERS,
                         description=desc,
                         affected_datasets=[cdr_consts.RDR],
                         affected_tables=[OBSERVATION],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         depends_on=[SetConceptIdsForSurveyQuestionsAnswers],
                         run_for_synthetic=True)

    def get_query_specs(self, *args, **keyword_args) -> query_spec_list:
        """
        Return a list of dictionary query specifications.

        :return:  A list of dictionaries. Each dictionary contains a single query
            and a specification for how to execute that query. The specifications
            are optional but the query is required.
        """
        sandbox_query_dict = {
            cdr_consts.QUERY:
                SANDBOX_FIX_UNMAPPED_SURVEY_ANSWERS_QUERY.render(
                    project=self.project_id,
                    sandbox_dataset=self.sandbox_dataset_id,
                    sandbox_table=self.sandbox_table_for(OBSERVATION),
                    dataset=self.dataset_id)
        }

        update_query_dict = {
            cdr_consts.QUERY:
                UPDATE_FIX_UNMAPPED_SURVEY_ANSWERS_QUERY.render(
                    project=self.project_id,
                    sandbox_dataset=self.sandbox_dataset_id,
                    sandbox_table=self.sandbox_table_for(OBSERVATION),
                    dataset=self.dataset_id),
            cdr_consts.DESTINATION_TABLE:
                OBSERVATION,
            cdr_consts.DESTINATION_DATASET:
                self.dataset_id,
            cdr_consts.DISPOSITION:
                WRITE_TRUNCATE
        }

        return [sandbox_query_dict, update_query_dict]

    def setup_rule(self, client, *args, **keyword_args):
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
        return [
            self.sandbox_table_for(affected_table)
            for affected_table in self._affected_tables
        ]


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.parse_args()

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(ARGS.project_id,
                                                 ARGS.dataset_id,
                                                 ARGS.sandbox_dataset_id,
                                                 [(FixUnmappedSurveyAnswers,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(FixUnmappedSurveyAnswers,)])
