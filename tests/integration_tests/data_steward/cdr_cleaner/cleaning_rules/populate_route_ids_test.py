"""
Integration test for PopulateRouteIds module.

Original Issues: DC-405, DC-817
"""
# Python Imports
import os

# Project imports
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.populate_route_ids import (
    DOSE_FORM_ROUTE_MAPPING_TABLE, DRUG_ROUTE_MAPPING_TABLE, PopulateRouteIds)
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest
from common import DRUG_EXPOSURE, VOCABULARY_TABLES


class PopulateRouteIdsTest(BaseTest.CleaningRulesTestBase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        super().initialize_class_vars()

        cls.project_id = os.environ.get(PROJECT_ID)
        cls.dataset_id = os.environ.get('COMBINED_DATASET_ID')
        cls.route_mapping_dataset_id = os.environ.get('COMBINED_DATASET_ID')
        cls.vocabulary_id = os.environ.get('VOCABULARY_DATASET')
        cls.sandbox_id = f'{cls.dataset_id}_sandbox'

        cls.rule_instance = PopulateRouteIds(cls.project_id, cls.dataset_id,
                                             cls.sandbox_id)

        cls.fq_table_names = [
            f'{cls.project_id}.{cls.dataset_id}.{DRUG_EXPOSURE}'
        ]

        for table in VOCABULARY_TABLES:
            cls.fq_table_names.append(
                f'{cls.project_id}.{cls.dataset_id}.{table}')

        cls.fq_sandbox_table_names = []
        for table in cls.rule_instance.get_sandbox_tablenames():
            cls.fq_sandbox_table_names.append(
                f'{cls.project_id}.{cls.sandbox_id}.{table}')

        cls.fq_sandbox_table_names.append(
            f'{cls.project_id}.{cls.sandbox_id}.{DOSE_FORM_ROUTE_MAPPING_TABLE}'
        )
        cls.fq_sandbox_table_names.append(
            f'{cls.project_id}.{cls.sandbox_id}.{DRUG_ROUTE_MAPPING_TABLE}')

        super().setUpClass()

    def setUp(self):

        super().setUp()

        self.copy_vocab_tables(self.vocabulary_id)

    def test_populate_route_ids(self):
        """
        Tests that the specifications perform as designed.
        """

        INSERT_DRUG_EXPOSURE_QUERY = self.jinja_env.from_string("""
            INSERT INTO `{{fq_table_name}}`
                (drug_exposure_id, person_id, drug_concept_id,
                 drug_exposure_start_date, drug_exposure_start_datetime,
                 drug_type_concept_id, route_concept_id)
            VALUES
                (11, 1, 99999999, date('2022-09-01'), timestamp('2022-09-01 00:00:00'), 0, 99999999),
                (12, 2, 43012486, date('2022-09-01'), timestamp('2022-09-01 00:00:00'), 0, NULL),
                (13, 3, 43012486, date('2022-09-01'), timestamp('2022-09-01 00:00:00'), 0, 99999999)
        """).render(fq_table_name=self.fq_table_names[0])

        queries = [INSERT_DRUG_EXPOSURE_QUERY]

        self.load_test_data(queries)

        tables_and_counts = [{
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{DRUG_EXPOSURE}',
            'fq_sandbox_table_name':
                self.fq_sandbox_table_names[0],
            'loaded_ids': [11, 12, 13],
            'sandboxed_ids': [12, 13],
            'fields': ['drug_exposure_id', 'route_concept_id'],
            'cleaned_values': [(11, 99999999), (12, 4132161), (13, 4132161)]
        }]

        self.default_test(tables_and_counts)
