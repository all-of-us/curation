# Python imports
import os
import datetime

# Third party imports
import mock

# Project imports
from common import DEVICE, WEARABLES_DEVICE_ID_MASKING
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.generate_research_device_ids import GenerateResearchDeviceIds
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest


class GenerateResearchDeviceIdsTest(BaseTest.CleaningRulesTestBase):

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
        # intended to be run on the rdr dataset
        dataset_id = os.environ.get('RDR_DATASET_ID')
        cls.dataset_id = dataset_id
        sandbox_id = dataset_id + '_sandbox'
        cls.sandbox_id = sandbox_id

        cls.rule_instance = GenerateResearchDeviceIds(cls.project_id,
                                                      cls.dataset_id,
                                                      cls.sandbox_id)

        cls.affected_tables = [WEARABLES_DEVICE_ID_MASKING, DEVICE]

        # Generates list of fully qualified table names and their corresponding sandbox table names
        cls.fq_table_names = [
            f"{cls.project_id}.{cls.dataset_id}.{table}"
            for table in cls.affected_tables
        ]

        cls.fq_sandbox_table_names = [
            f'{cls.project_id}.{cls.sandbox_id}.{cls.rule_instance.sandbox_table_for(table)}'
            for table in cls.affected_tables
        ]

        # call super to set up the client, create datasets
        cls.up_class = super().setUpClass()

    def setUp(self):
        # create tables
        super().setUp()

        device_query = self.jinja_env.from_string("""
            INSERT INTO `{{project_id}}.{{dataset_id}}.device`
                (person_id, device_id, battery)
            VALUES 
                (1, 'AAA', 'high'),
                (1, 'AAA', 'low'),
                (1, 'BBB', 'high'),
                (2, 'BBB', 'high'),
                (2, 'CCC', 'high')
            """).render(project_id=self.project_id, dataset_id=self.dataset_id)

        wearables_device_id_masking_query = self.jinja_env.from_string("""
            INSERT INTO `{{project_id}}.{{dataset_id}}.wearables_device_id_masking` (
                    person_id, device_id, research_device_id, wearable_type, import_date)
            VALUES
                    (2, 'CCC', 'UUID_HERE', 'fitbit', '2022-01-01')
            """).render(project_id=self.project_id, dataset_id=self.dataset_id)

        # load the test data
        self.load_test_data([device_query, wearables_device_id_masking_query])

    def test_queries(self):
        """
        Validates pre-conditions, test execution and post conditions based on the tables_and_counts variable.
        """
        tables_and_counts = [{
            'name':
                WEARABLES_DEVICE_ID_MASKING,
            'fq_table_name':
                self.fq_table_names[0],
            'fields': [
                'person_id', 'device_id', 'research_device_id', 'wearable_type',
                'import_date'
            ],
            'loaded_ids': [2],
            'sandboxed_ids': [],
            'cleaned_values': [
                (1, 'AAA', 'UUID_HERE', 'fitbit',
                 datetime.datetime.now().date()),
                (1, 'BBB', 'UUID_HERE', 'fitbit',
                 datetime.datetime.now().date()),
                (2, 'CCC', 'UUID_HERE', 'fitbit',
                 datetime.datetime.strptime('2022-01-01', '%Y-%m-%d').date()),
                (2, 'BBB', 'UUID_HERE', 'fitbit',
                 datetime.datetime.now().date())
            ]
        }]

        self.maxDiff = None

        # mock the PIPELINE_TABLES variable
        with mock.patch(
                'cdr_cleaner.cleaning_rules.generate_research_device_ids.PIPELINE_TABLES',
                self.dataset_id):
            self.default_test(tables_and_counts)
