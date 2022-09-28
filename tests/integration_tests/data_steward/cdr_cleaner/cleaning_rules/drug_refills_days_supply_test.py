"""
Integration test for DrugRefillsDaysSupply module
Original Issues: DC-403, DC-815
"""
# Python Imports
import os

# Project imports
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.drug_refills_days_supply import DrugRefillsDaysSupply
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest
from common import DRUG_EXPOSURE

# Third party imports


class DrugRefillsDaysSupplyTest(BaseTest.CleaningRulesTestBase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        super().initialize_class_vars()

        cls.project_id = os.environ.get(PROJECT_ID)
        cls.dataset_id = os.environ.get('COMBINED_DATASET_ID')
        cls.sandbox_id = f'{cls.dataset_id}_sandbox'

        cls.rule_instance = DrugRefillsDaysSupply(cls.project_id,
                                                  cls.dataset_id,
                                                  cls.sandbox_id)

        cls.fq_table_names = [
            f'{cls.project_id}.{cls.dataset_id}.{DRUG_EXPOSURE}'
        ]

        cls.fq_sandbox_table_names = []
        for table in cls.rule_instance.get_sandbox_tablenames():
            cls.fq_sandbox_table_names.append(
                f'{cls.project_id}.{cls.sandbox_id}.{table}')

        super().setUpClass()

    def test_drug_refills_days_supply(self):
        """
        Tests that the specifications perform as designed.
        """

        INSERT_DRUG_EXPOSURE_QUERY = self.jinja_env.from_string("""
            INSERT INTO `{{project_id}}.{{dataset_id}}.drug_exposure`
                (drug_exposure_id, person_id, drug_concept_id,
                 drug_exposure_start_date, drug_exposure_start_datetime,
                 drug_type_concept_id, refills, days_supply)
            VALUES
                (11, 1, 0, date('2022-09-01'), timestamp('2022-09-01 00:00:00'), 0, 10, 180),
                (12, 2, 0, date('2022-09-01'), timestamp('2022-09-01 00:00:00'), 0, 11, 180),
                (13, 3, 0, date('2022-09-01'), timestamp('2022-09-01 00:00:00'), 0, 10, 181)
        """).render(project_id=self.project_id, dataset_id=self.dataset_id)

        queries = [INSERT_DRUG_EXPOSURE_QUERY]

        self.load_test_data(queries)

        tables_and_counts = [{
            'fq_table_name':
                '.'.join([self.project_id, self.dataset_id, DRUG_EXPOSURE]),
            'fq_sandbox_table_name':
                self.fq_sandbox_table_names[0],
            'loaded_ids': [11, 12, 13],
            'sandboxed_ids': [12, 13],
            'fields': ['drug_exposure_id'],
            'cleaned_values': [(11,)]
        }]

        self.default_test(tables_and_counts)
