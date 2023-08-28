"""
There are a set of answer codes that are still being mapped to “OMOP invalidated” concepts.

There are 15 value_source_concept_ids that need to be updated, representing 2 cases:

1. OMOP vocabulary has concepts with different capitalization of “prediabetes”, affecting 3 answer concept_ids.
2. PTSC is sending some codes with trailing spaces; OMOP vocabulary has made new concepts without them and invalidated
the old ones. This affects 12 concept_ids.
In addition, a set of three question codes are also affected by #1 above:
3. OMOP vocabulary has concepts with different capitalization of “prediabetes”, affecting 3 question concept_ids.

These concept_ids are actually duplicated by the RDR->CDR export, so the invalid rows need to be dropped in the clean dataset.

Original Issues: DC-539

Intent is to drop the duplicated concepts for questions and answers created because of casing.
"""

# Python imports
import logging

# Project imports
import constants.cdr_cleaner.clean_cdr as cdr_consts
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule
from constants.bq_utils import WRITE_TRUNCATE
from common import JINJA_ENV

LOGGER = logging.getLogger(__name__)

OBSERVATION = 'observation'
ppi_modules = ['ppi_answers', 'ppi_questions']

# Query to create tables in sandbox with the rows that will be removed per cleaning rule
SANDBOX_PPI_ANSWERS = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project}}.{{sandbox_dataset}}.{{ans_table}}` as(
SELECT value_source_concept_id, concept_id as new_value_source_concept_id, concept_id_2 as new_value_as_concept_id
FROM `{{project}}.{{dataset}}.concept` c
right join
(SELECT value_source_concept_id, value_source_value, count( *)
FROM `{{project}}.{{dataset}}.{{clinical_table_name}}`
 join `{{project}}.{{dataset}}.concept` on (concept_id=value_source_concept_id)
WHERE  invalid_reason is not null and questionnaire_response_id is not null
group by 1,2
) on (trim(lower(value_source_value))=lower(concept_code))
join `{{project}}.{{dataset}}.concept_relationship` on (concept_id_1=concept_id)
where vocabulary_id='PPI' and c.invalid_reason is null
and relationship_id = 'Maps to value')
""")

SANDBOX_PPI_QUESTIONS = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project}}.{{sandbox_dataset}}.{{ques_table}}` as(
SELECT observation_source_concept_id, concept_id as new_observation_source_concept_id,
 concept_id_2 as new_observation_concept_id
FROM `{{project}}.{{dataset}}.concept` c
right join
(SELECT observation_source_concept_id, observation_source_value, count( *)
FROM `{{project}}.{{dataset}}.{{clinical_table_name}}`
 join `{{project}}.{{dataset}}.concept` on (concept_id=observation_source_concept_id)
WHERE  invalid_reason is not null and questionnaire_response_id is not null
group by 1,2
) on (trim(lower(observation_source_value))=lower(concept_code))
join `{{project}}.{{dataset}}.concept_relationship` on (concept_id_1=concept_id)
where vocabulary_id='PPI' and c.invalid_reason is null
and relationship_id = 'Maps to')
""")

DELETE_DUPLICATE_ANSWERS = JINJA_ENV.from_string("""
select * 
from `{{project}}.{{dataset}}.{{clinical_table_name}}` o
where value_source_concept_id not in (select value_source_concept_id 
from `{{project}}.{{sandbox_dataset}}.{{ans_table}}`)
""")

DELETE_DUPLICATE_QUESTIONS = JINJA_ENV.from_string("""
select * 
from `{{project}}.{{dataset}}.{{clinical_table_name}}` o
where value_source_concept_id not in (select value_source_concept_id 
from `{{project}}.{{sandbox_dataset}}.{{ques_table}}`)
""")


class DropDuplicatePpiQuestionsAndAnswers(BaseCleaningRule):
    """
    Apply value ranges to ensure that values are reasonable and to minimize the likelihood
    of sensitive information (like phone numbers) within the free text fields.
    """

    def __init__(self, project_id, dataset_id, sandbox_dataset_id):
        """
        Initialize the class with proper information.

        Set the issue numbers, description and affected datasets. As other tickets may affect
        this SQL, append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """
        desc = (
            'Drops the duplicated PPI questions and answers created because of casing in vocabulary'
        )
        super().__init__(issue_numbers=['DC539', 'DC704'],
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
        sandbox_answers = {
            cdr_consts.QUERY:
                SANDBOX_PPI_ANSWERS.render(
                    project=self.project_id,
                    sandbox_dataset=self.sandbox_dataset_id,
                    dataset=self.dataset_id,
                    ans_table=self.get_sandbox_tablenames()[0],
                    clinical_table_name=OBSERVATION),
        }

        sandbox_questions = {
            cdr_consts.QUERY:
                SANDBOX_PPI_QUESTIONS.render(
                    project=self.project_id,
                    sandbox_dataset=self.sandbox_dataset_id,
                    dataset=self.dataset_id,
                    ques_table=self.get_sandbox_tablenames()[1],
                    clinical_table_name=OBSERVATION),
        }

        delete_ppi_duplicate_answers = {
            cdr_consts.QUERY:
                DELETE_DUPLICATE_ANSWERS.render(
                    project=self.project_id,
                    dataset=self.dataset_id,
                    clinical_table_name=OBSERVATION,
                    sandbox_dataset=self.sandbox_dataset_id,
                    ans_table=self.get_sandbox_tablenames()[0]),
            cdr_consts.DESTINATION_TABLE:
                'observation',
            cdr_consts.DESTINATION_DATASET:
                self.dataset_id,
            cdr_consts.DISPOSITION:
                WRITE_TRUNCATE
        }

        delete_ppi_duplicate_questions = {
            cdr_consts.QUERY:
                DELETE_DUPLICATE_QUESTIONS.render(
                    project=self.project_id,
                    dataset=self.dataset_id,
                    clinical_table_name=OBSERVATION,
                    sandbox_dataset=self.sandbox_dataset_id,
                    ques_table=self.get_sandbox_tablenames()[1]),
            cdr_consts.DESTINATION_TABLE:
                'observation',
            cdr_consts.DESTINATION_DATASET:
                self.dataset_id,
            cdr_consts.DISPOSITION:
                WRITE_TRUNCATE
        }

        return [
            sandbox_answers, sandbox_questions, delete_ppi_duplicate_answers,
            delete_ppi_duplicate_questions
        ]

    def setup_rule(self, client):
        """
        Function to run any data upload options before executing a query.
        """
        pass

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

    def get_sandbox_tablenames(self):
        sandbox_table_names = list()
        for i in range(0, len(ppi_modules)):
            sandbox_table_names.append(self._issue_numbers[i].lower() + '_' +
                                       self._affected_tables[0] + '_' +
                                       ppi_modules[i])
        return sandbox_table_names


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.parse_args()

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(
            ARGS.project_id, ARGS.dataset_id, ARGS.sandbox_dataset_id,
            [(DropDuplicatePpiQuestionsAndAnswers,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(DropDuplicatePpiQuestionsAndAnswers,)])
