"""
Unit test for drop_duplicate_ppi_questions_and_answers module

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
import unittest

from cdr_cleaner.cleaning_rules.drop_duplicate_ppi_questions_and_answers import DropDuplicatePpiQuestionsAndAnswers, \
    SANDBOX_PPI_QUESTIONS, SANDBOX_PPI_ANSWERS, DELETE_DUPLICATE_ANSWERS, DELETE_DUPLICATE_QUESTIONS, OBSERVATION
# Project imports
from constants.bq_utils import WRITE_TRUNCATE
from constants.cdr_cleaner import clean_cdr as clean_consts


class DropDuplicatePpiQuestionsAndAnswersTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = 'foo_project'
        self.dataset_id = 'bar_dataset'
        self.sandbox_id = 'baz_sandbox'
        self.client = None

        self.rule_instance = DropDuplicatePpiQuestionsAndAnswers(
            self.project_id, self.dataset_id, self.sandbox_id)

        self.assertEquals(self.rule_instance.project_id, self.project_id)
        self.assertEquals(self.rule_instance.dataset_id, self.dataset_id)
        self.assertEquals(self.rule_instance.sandbox_dataset_id,
                          self.sandbox_id)

    def test_setup_rule(self):
        # Test
        self.rule_instance.setup_rule(self.client)

        # No errors are raised, nothing will happen

    def test_get_query_specs(self):
        # Pre conditions
        self.assertEqual(self.rule_instance.affected_datasets,
                         [clean_consts.RDR])

        # Test
        results_list = self.rule_instance.get_query_specs()

        # Post conditions
        expected_list = [{
            clean_consts.QUERY:
                SANDBOX_PPI_ANSWERS.render(
                    project=self.project_id,
                    dataset=self.dataset_id,
                    clinical_table_name=OBSERVATION,
                    sandbox_dataset=self.sandbox_id,
                    ans_table=self.rule_instance.get_sandbox_tablenames()[0])
        }, {
            clean_consts.QUERY:
                SANDBOX_PPI_QUESTIONS.render(
                    project=self.project_id,
                    dataset=self.dataset_id,
                    clinical_table_name=OBSERVATION,
                    sandbox_dataset=self.sandbox_id,
                    ques_table=self.rule_instance.get_sandbox_tablenames()[1])
        }, {
            clean_consts.QUERY:
                DELETE_DUPLICATE_ANSWERS.render(
                    project=self.project_id,
                    dataset=self.dataset_id,
                    clinical_table_name=OBSERVATION,
                    sandbox_dataset=self.sandbox_id,
                    ans_table=self.rule_instance.get_sandbox_tablenames()[0]),
            clean_consts.DESTINATION_TABLE:
                'observation',
            clean_consts.DESTINATION_DATASET:
                self.dataset_id,
            clean_consts.DISPOSITION:
                WRITE_TRUNCATE
        }, {
            clean_consts.QUERY:
                DELETE_DUPLICATE_QUESTIONS.render(
                    project=self.project_id,
                    dataset=self.dataset_id,
                    clinical_table_name=OBSERVATION,
                    sandbox_dataset=self.sandbox_id,
                    ques_table=self.rule_instance.get_sandbox_tablenames()[1]),
            clean_consts.DESTINATION_TABLE:
                'observation',
            clean_consts.DESTINATION_DATASET:
                self.dataset_id,
            clean_consts.DISPOSITION:
                WRITE_TRUNCATE
        }]

        self.assertEqual(results_list, expected_list)

    def test_log_queries(self):
        # Pre conditions
        self.assertEqual(self.rule_instance.affected_datasets,
                         [clean_consts.RDR])

        sandbox_answers = SANDBOX_PPI_ANSWERS.render(
            project=self.project_id,
            dataset=self.dataset_id,
            clinical_table_name=OBSERVATION,
            sandbox_dataset=self.sandbox_id,
            ans_table=self.rule_instance.get_sandbox_tablenames()[0])

        sandbox_questions = SANDBOX_PPI_QUESTIONS.render(
            project=self.project_id,
            dataset=self.dataset_id,
            clinical_table_name=OBSERVATION,
            sandbox_dataset=self.sandbox_id,
            ques_table=self.rule_instance.get_sandbox_tablenames()[1])

        delete_ppi_duplicate_answers = DELETE_DUPLICATE_ANSWERS.render(
            project=self.project_id,
            dataset=self.dataset_id,
            clinical_table_name=OBSERVATION,
            sandbox_dataset=self.sandbox_id,
            ans_table=self.rule_instance.get_sandbox_tablenames()[0])

        delete_ppi_duplicate_questions = DELETE_DUPLICATE_QUESTIONS.render(
            project=self.project_id,
            dataset=self.dataset_id,
            clinical_table_name=OBSERVATION,
            sandbox_dataset=self.sandbox_id,
            ques_table=self.rule_instance.get_sandbox_tablenames()[1])

        # Test
        with self.assertLogs(level='INFO') as cm:
            self.rule_instance.log_queries()

            expected = [
                'INFO:cdr_cleaner.cleaning_rules.base_cleaning_rule:Generated SQL Query:\n'
                + sandbox_answers,
                'INFO:cdr_cleaner.cleaning_rules.base_cleaning_rule:Generated SQL Query:\n'
                + sandbox_questions,
                'INFO:cdr_cleaner.cleaning_rules.base_cleaning_rule:Generated SQL Query:\n'
                + delete_ppi_duplicate_answers,
                'INFO:cdr_cleaner.cleaning_rules.base_cleaning_rule:Generated SQL Query:\n'
                + delete_ppi_duplicate_questions
            ]

            # Post condition
            self.assertEqual(cm.output, expected)
