"""Integration test for RemoveMultipleRaceEthnicityAnswersQueries
"""
# Python imports
import os

# Third party imports
from dateutil import parser

# Project imports
from app_identity import PROJECT_ID
from common import JINJA_ENV, OBSERVATION
from cdr_cleaner.cleaning_rules.remove_multiple_race_ethnicity_answers import RemoveMultipleRaceEthnicityAnswersQueries
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest

INSERT_STATEMENT = JINJA_ENV.from_string("""
    INSERT INTO `{{fq_table_name}}`
        (observation_id, person_id, observation_concept_id, observation_date, observation_type_concept_id,
         observation_source_concept_id, value_source_concept_id)
    VALUES
        (1, 101, 9999, date('2019-03-03'), 999, 9999999, 9999999), 
        (2, 102, 1586140, date('2019-03-03'), 45905771, 1586140, 1586148),
        (3, 103, 1586140, date('2019-03-03'), 45905771, 1586140, 1586142),
        (4, 104, 1586140, date('2019-03-03'), 45905771, 1586140, 1586148),
        (5, 104, 1586140, date('2019-03-03'), 45905771, 1586140, 1586142),
        (6, 104, 1586140, date('2019-03-03'), 45905771, 1586140, 1586141),
        (7, 104, 9999, date('2019-03-03'), 999, 9999999, 9999999)
""")


class RemoveMultipleRaceEthnicityAnswersQueriesTest(
        BaseTest.CleaningRulesTestBase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        super().initialize_class_vars()

        # set the test project identifier
        project_id = os.environ.get(PROJECT_ID)
        cls.project_id = project_id

        # set the expected test datasets
        dataset_id = os.environ.get('RDR_DATASET_ID')
        cls.dataset_id = dataset_id
        sandbox_id = dataset_id + '_sandbox'
        cls.sandbox_id = sandbox_id

        cls.rule_instance = RemoveMultipleRaceEthnicityAnswersQueries(
            project_id, dataset_id, sandbox_id)

        sb_table_name = cls.rule_instance.get_sandbox_table_name()

        cls.fq_sandbox_table_names.append(
            f'{cls.project_id}.{cls.sandbox_id}.{sb_table_name}')

        cls.fq_table_names = [f'{project_id}.{dataset_id}.{OBSERVATION}']

        super().setUpClass()

    def setUp(self):
        """
        Create common information for tests.

        Creates common expected parameter types from cleaned tables and a common fully qualified (fq)
        dataset name string used to load the data.
        """
        fq_dataset_name = self.fq_table_names[0].split('.')
        self.fq_dataset_name = '.'.join(fq_dataset_name[:-1])
        self.date = parser.parse('2019-03-03').date()

        super().setUp()

    def test_remove_multiple_race_ethnicity_answers(self):
        """
        Validates pre-conditions, test execution and post conditions based on the tables_and_counts variable.
        """
        drop_records_query_tmpl = INSERT_STATEMENT.render(
            fq_table_name=self.fq_table_names[0])

        self.load_test_data([drop_records_query_tmpl])

        tables_and_counts = [{
            'fq_table_name':
                '.'.join([self.fq_dataset_name, 'observation']),
            'fq_sandbox_table_name':
                self.fq_sandbox_table_names[0],
            'fields': [
                'observation_id', 'person_id', 'observation_concept_id',
                'observation_date', 'observation_type_concept_id',
                'observation_source_concept_id', 'value_source_concept_id'
            ],
            'loaded_ids': [1, 2, 3, 4, 5, 6, 7],
            'sandboxed_ids': [5, 6],
            'cleaned_values': [
                (1, 101, 9999, self.date, 999, 9999999, 9999999),
                (2, 102, 1586140, self.date, 45905771, 1586140, 1586148),
                (3, 103, 1586140, self.date, 45905771, 1586140, 1586142),
                (4, 104, 1586140, self.date, 45905771, 1586140, 1586148),
                (7, 104, 9999, self.date, 999, 9999999, 9999999),
            ]
        }]

        self.default_test(tables_and_counts)
