import os
from datetime import date

from common import JINJA_ENV, ACTIVITY_SUMMARY, STEPS_INTRADAY, PERSON
from app_identity import PROJECT_ID
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest
from cdr_cleaner.cleaning_rules.remove_non_existing_pids import RemoveNonExistingPids

STEPS_TEMPLATE = JINJA_ENV.from_string("""
INSERT INTO `{{project_id}}.{{dataset_id}}.{{table_id}}`
 (person_id, datetime, steps)
VALUES
(1, NULL, 10),
(2, NULL, 20),
(3, DATETIME('2021-01-01'), 30),
(4, NULL, 40)
""")

SUMMARY_TEMPLATE = JINJA_ENV.from_string("""
INSERT INTO `{{project_id}}.{{dataset_id}}.{{table_id}}`
 (person_id, date, activity_calories)
VALUES
(1, '2021-02-01', 100),
(2, '2021-03-01', 200),
(3, '2021-04-01', 300),
(4, '2021-05-01', 400)
""")

PERSON_TEMPLATE = JINJA_ENV.from_string("""
INSERT INTO `{{project_id}}.{{dataset_id}}.{{table_id}}`
 (person_id, gender_concept_id, year_of_birth, race_concept_id, ethnicity_concept_id)
VALUES
(1, 10, 1970, 5, 2),
(2, 20, 1980, 6, 3),
(4, 10, 1991, 7, 4)
""")


class RemoveNonExistingPidsTest(BaseTest.CleaningRulesTestBase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        super().initialize_class_vars()

        # Set the test project identifier
        cls.project_id = os.environ.get(PROJECT_ID)

        # Set the expected test datasets
        cls.dataset_id = os.environ.get('COMBINED_DATASET_ID')
        cls.sandbox_id = cls.dataset_id + '_sandbox'

        cls.reference_dataset_id = os.environ.get('UNIONED_DATASET_ID')
        cls.kwargs.update({'reference_dataset_id': cls.reference_dataset_id})

        cls.rule_instance = RemoveNonExistingPids(
            cls.project_id,
            cls.dataset_id,
            cls.sandbox_id,
            reference_dataset_id=cls.reference_dataset_id)

        # Generates list of fully qualified table names and their corresponding sandbox table names
        for table in [STEPS_INTRADAY, ACTIVITY_SUMMARY]:
            cls.fq_table_names.append(
                f'{cls.project_id}.{cls.dataset_id}.{table}')
            cls.fq_sandbox_table_names.append(
                f'{cls.project_id}.{cls.sandbox_id}.{cls.rule_instance.sandbox_table_for(table)}'
            )
        cls.fq_table_names.append(
            f'{cls.project_id}.{cls.reference_dataset_id}.{PERSON}')

        # call super to set up the client, create datasets
        cls.up_class = super().setUpClass()

    def setUp(self):
        """
        Create empty tables for the rule to run on
        """
        super().setUp()

        person_query = PERSON_TEMPLATE.render(
            project_id=self.project_id,
            dataset_id=self.reference_dataset_id,
            table_id=PERSON)
        steps_query = STEPS_TEMPLATE.render(project_id=self.project_id,
                                            dataset_id=self.dataset_id,
                                            table_id=STEPS_INTRADAY)
        summary_query = SUMMARY_TEMPLATE.render(project_id=self.project_id,
                                                dataset_id=self.dataset_id,
                                                table_id=ACTIVITY_SUMMARY)

        # Load test data
        self.load_test_data([person_query, steps_query, summary_query])

    def test_remove_non_existing_pids(self):
        """
        person_ids 1, 2, 4 exist in the reference dataset so 3 should be dropped
        """

        # Expected results list
        tables_and_counts = [{
            'fq_table_name': self.fq_table_names[0],
            'fq_sandbox_table_name': self.fq_sandbox_table_names[0],
            'loaded_ids': [1, 2, 3, 4],
            'sandboxed_ids': [3],
            'fields': ['person_id', 'datetime', 'steps'],
            'cleaned_values': [(1, None, 10), (2, None, 20), (4, None, 40)]
        }, {
            'fq_table_name':
                self.fq_table_names[1],
            'fq_sandbox_table_name':
                self.fq_sandbox_table_names[1],
            'loaded_ids': [1, 2, 3, 4],
            'sandboxed_ids': [3],
            'fields': ['person_id', 'date', 'activity_calories'],
            'cleaned_values': [(1, date.fromisoformat('2021-02-01'), 100),
                               (2, date.fromisoformat('2021-03-01'), 200),
                               (4, date.fromisoformat('2021-05-01'), 400)]
        }]

        self.default_test(tables_and_counts)
