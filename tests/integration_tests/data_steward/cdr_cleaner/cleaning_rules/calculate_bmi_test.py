"""
Integration test for CalculateBmi
"""
# Python Imports
import os

# Project imports
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.calculate_bmi import CalculateBmi
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest
from common import MAPPING_PREFIX, MEASUREMENT


class CalculateBmiTest(BaseTest.CleaningRulesTestBase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        super().initialize_class_vars()

        cls.project_id = os.environ.get(PROJECT_ID)
        cls.dataset_id = os.environ.get('BIGQUERY_DATASET_ID')
        cls.sandbox_id = f"{cls.dataset_id}_sandbox"

        cls.rule_instance = CalculateBmi(cls.project_id, cls.dataset_id,
                                         cls.sandbox_id)

        cls.fq_table_names = [
            f'{cls.project_id}.{cls.dataset_id}.{MEASUREMENT}'
        ]

        # NOTE _mapping_measurement is not in cls.fq_table_names because its columns are different from the ones
        # defined in the resource_files folder. It has the columns defined in `create_rdr_snapshot.py` instead.
        cls.fq_mapping_table_name = f'{cls.project_id}.{cls.dataset_id}.{MAPPING_PREFIX}{MEASUREMENT}'

        cls.fq_sandbox_table_names = [
            f'{cls.project_id}.{cls.sandbox_id}.{cls.rule_instance.sandbox_table_for(MEASUREMENT)}'
        ]

        super().setUpClass()

    def setUp(self):
        super().setUp()

        insert_meas = self.jinja_env.from_string("""
        INSERT INTO `{{project}}.{{dataset}}.measurement`
            (measurement_id, person_id, measurement_concept_id, measurement_date, measurement_type_concept_id, 
             visit_occurrence_id, value_as_number, unit_concept_id, unit_source_value, value_source_value)
        VALUES
            -- these are in-person PM --
            (11, 1, 3036277, '2020-01-01', 44818701, NULL, 165, 8582, 'cm', '165 cm'),
            (12, 1, 3025315, '2020-01-01', 44818701, NULL, 60, 9529, 'kg', '60 kg'),
            (21, 2, 3036277, '2020-01-01', 44818701, NULL, 165, 8582, 'cm', '165 cm'),
            (22, 2, 3025315, '2020-01-01', 44818701, NULL, 60, 9529, 'kg', '60 kg'),
            -- these are self-report PM --
            (23, 2, 3036277, '2020-01-01', 32865, 201, 180, 8582, 'cm', '180 cm'),
            (24, 2, 3025315, '2020-01-01', 32865, 201, 60, 9529, 'kg', '60 kg'),
            (25, 2, 3036277, '2020-01-01', 32865, 202, 170, 8582, 'cm', '170 cm'),
            (26, 2, 3025315, '2020-01-01', 32865, 202, 85, 9529, 'kg', '85 kg'),
            (27, 2, 3036277, '2020-01-01', 32865, 203, 200, 8582, 'cm', '200 cm'),
            (28, 2, 3025315, '2020-01-01', 32865, 203, 50, 9529, 'kg', '50 kg'),
            (31, 3, 3036277, '2020-01-01', 32865, 301, 180, 8582, 'cm', '180 cm'),
            (32, 3, 3025315, '2020-01-01', 32865, 301, 60, 9529, 'kg', '60 kg'),
            (33, 3, 3036277, '2020-01-01', 32865, 302, 170, 8582, 'cm', '170 cm'),
            (34, 3, 3025315, '2020-01-01', 32865, 302, 85, 9529, 'kg', '85 kg'),
            (39, 3, 3036277, '2020-01-01', 32865, 309, 170, 8582, 'cm', '170 cm'), -- missing counterpart weight --
            (41, 4, 3036277, '2020-01-01', 32865, 401, 180, 8582, 'cm', '180 cm'),
            (42, 4, 3025315, '2020-01-01', 32865, 401, 60, 9529, 'kg', '60 kg'),
            (43, 4, 3036277, '2020-01-01', 32865, 402, 170, 8582, 'cm', '170 cm'),
            (44, 4, 3025315, '2020-01-01', 32865, 402, 85, 9529, 'kg', '85 kg'),
            (45, 4, 3036277, '2020-01-01', 32865, 403, 170, 8582, 'cm', '170 cm'), -- pair exists but unmatching measurement_date --
            (46, 4, 3025315, '2020-01-02', 32865, 403, 85, 9529, 'kg', '85 kg'),  -- pair exists but unmatching measurement_date --
            (49, 4, 3025315, '2020-01-01', 32865, 409, 85, 9529, 'kg', '85 kg')   -- missing counterpart height --
        """).render(project=self.project_id, dataset=self.dataset_id)

        insert_meas_ext = self.jinja_env.from_string("""
        CREATE OR REPLACE TABLE `{{project}}.{{dataset}}._mapping_measurement`
        (measurement_id INT64, src_id STRING)
        ;
        INSERT INTO `{{project}}.{{dataset}}._mapping_measurement`
            (measurement_id, src_id)
        VALUES
            (11, 'healthpro'),
            (12, 'healthpro'),
            (21, 'healthpro'),
            (22, 'healthpro'),
            (23, 'vibrent'),
            (24, 'vibrent'),
            (25, 'ce'),
            (26, 'ce'),
            (27, 'vibrent'),
            (28, 'vibrent'),
            (31, 'vibrent'),
            (32, 'vibrent'),
            (33, 'ce'),
            (34, 'ce'),
            (39, 'vibrent'),
            (41, 'vibrent'),
            (42, 'vibrent'),
            (43, 'ce'),
            (44, 'ce'),
            (45, 'ce'),
            (46, 'ce'),
            (49, 'vibrent')
        """).render(project=self.project_id, dataset=self.dataset_id)

        self.load_test_data([insert_meas, insert_meas_ext])

    def test_calculate_bmi(self):
        """
        Test cases for BMI calculation:
        person_id = 1: 
            No self-reported height/weight data. Nothing happens.
        person_id = 2: 
            3 self-reported height/weight pairs. BMIs are calculated for each pair.
        person_id = 3: 
            2 self-reported height/weight pairs. BMIs are calculated for each pair. 
            1 height is missing its counterpart weight. No BMI for that.
        person_id = 4: 
            2 self-reported height/weight pairs. BMIs are calculated for each pair. 
            1 weight is missing its counterpart weight and 1 pair not matching measurement_date. No BMI for those.
        """
        self.maxDiff = None

        tables_and_counts = [{
            'fq_table_name':
                self.fq_table_names[0],
            'loaded_ids': [
                11, 12, 21, 22, 23, 24, 25, 26, 27, 28, 31, 32, 33, 34, 39, 41,
                42, 43, 44, 45, 46, 49
            ],
            'fields': [
                'measurement_id', 'measurement_concept_id',
                'measurement_type_concept_id', 'value_as_number',
                'visit_occurrence_id', 'unit_concept_id', 'unit_source_value',
                'value_source_value'
            ],
            'cleaned_values': [
                (11, 3036277, 44818701, 165, None, 8582, 'cm', '165 cm'),
                (12, 3025315, 44818701, 60, None, 9529, 'kg', '60 kg'),
                (21, 3036277, 44818701, 165, None, 8582, 'cm', '165 cm'),
                (22, 3025315, 44818701, 60, None, 9529, 'kg', '60 kg'),
                (23, 3036277, 32865, 180, 201, 8582, 'cm', '180 cm'),
                (24, 3025315, 32865, 60, 201, 9529, 'kg', '60 kg'),
                (25, 3036277, 32865, 170, 202, 8582, 'cm', '170 cm'),
                (26, 3025315, 32865, 85, 202, 9529, 'kg', '85 kg'),
                (27, 3036277, 32865, 200, 203, 8582, 'cm', '200 cm'),
                (28, 3025315, 32865, 50, 203, 9529, 'kg', '50 kg'),
                (31, 3036277, 32865, 180, 301, 8582, 'cm', '180 cm'),
                (32, 3025315, 32865, 60, 301, 9529, 'kg', '60 kg'),
                (33, 3036277, 32865, 170, 302, 8582, 'cm', '170 cm'),
                (34, 3025315, 32865, 85, 302, 9529, 'kg', '85 kg'),
                (39, 3036277, 32865, 170, 309, 8582, 'cm', '170 cm'),
                (41, 3036277, 32865, 180, 401, 8582, 'cm', '180 cm'),
                (42, 3025315, 32865, 60, 401, 9529, 'kg', '60 kg'),
                (43, 3036277, 32865, 170, 402, 8582, 'cm', '170 cm'),
                (44, 3025315, 32865, 85, 402, 9529, 'kg', '85 kg'),
                (45, 3036277, 32865, 170, 403, 8582, 'cm', '170 cm'),
                (46, 3025315, 32865, 85, 403, 9529, 'kg', '85 kg'),
                (49, 3025315, 32865, 85, 409, 9529, 'kg', '85 kg'),
                (50, 3038553, 32865, 18.51851851851852, 201, 9531, 'kg/m2',
                 '18.5 kg/m2'),
                (51, 3038553, 32865, 29.411764705882355, 202, 9531, 'kg/m2',
                 '29.4 kg/m2'),
                (52, 3038553, 32865, 12.5, 203, 9531, 'kg/m2', '12.5 kg/m2'),
                (53, 3038553, 32865, 18.51851851851852, 301, 9531, 'kg/m2',
                 '18.5 kg/m2'),
                (54, 3038553, 32865, 29.411764705882355, 302, 9531, 'kg/m2',
                 '29.4 kg/m2'),
                (55, 3038553, 32865, 18.51851851851852, 401, 9531, 'kg/m2',
                 '18.5 kg/m2'),
                (56, 3038553, 32865, 29.411764705882355, 402, 9531, 'kg/m2',
                 '29.4 kg/m2'),
            ]
        }, {
            'fq_table_name':
                self.fq_mapping_table_name,
            'loaded_ids': [
                11, 12, 21, 22, 23, 24, 25, 26, 27, 28, 31, 32, 33, 34, 39, 41,
                42, 43, 44, 45, 46, 49
            ],
            'fields': ['measurement_id', 'src_id'],
            'cleaned_values': [
                (11, 'healthpro'),
                (12, 'healthpro'),
                (21, 'healthpro'),
                (22, 'healthpro'),
                (23, 'vibrent'),
                (24, 'vibrent'),
                (25, 'ce'),
                (26, 'ce'),
                (27, 'vibrent'),
                (28, 'vibrent'),
                (31, 'vibrent'),
                (32, 'vibrent'),
                (33, 'ce'),
                (34, 'ce'),
                (39, 'vibrent'),
                (41, 'vibrent'),
                (42, 'vibrent'),
                (43, 'ce'),
                (44, 'ce'),
                (45, 'ce'),
                (46, 'ce'),
                (49, 'vibrent'),
                (50, 'vibrent'),
                (51, 'ce'),
                (52, 'vibrent'),
                (53, 'vibrent'),
                (54, 'ce'),
                (55, 'vibrent'),
                (56, 'ce'),
            ]
        }]

        self.default_test(tables_and_counts)

    def tearDown(self):
        self.client.delete_table(self.fq_mapping_table_name, not_found_ok=True)
        super().tearDown()