"""
Integration test for CalculateBmi
"""
# Python Imports
import os

# Project imports
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.calculate_bmi import CalculateBmi
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest
from common import MEASUREMENT, EXT_SUFFIX


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
            f'{cls.project_id}.{cls.dataset_id}.{MEASUREMENT}',
            f'{cls.project_id}.{cls.dataset_id}.{MEASUREMENT}{EXT_SUFFIX}'
        ]

        cls.fq_sandbox_table_names = [
            f'{cls.project_id}.{cls.sandbox_id}.{cls.rule_instance.sandbox_table_for(MEASUREMENT)}'
        ]

        super().setUpClass()

    def setUp(self):
        super().setUp()

        insert_meas = self.jinja_env.from_string("""
        INSERT INTO `{{project}}.{{dataset}}.measurement`
            (measurement_id, person_id, measurement_concept_id, measurement_date, measurement_type_concept_id, 
             visit_occurrence_id, value_as_number, unit_concept_id, unit_source_value)
        VALUES
            (11, 1, 3036277, '2020-01-01', 32836, NULL, 165, 8582, 'cm'), -- height from EHR --
            (12, 1, 3025315, '2020-01-01', 32836, NULL, 60, 9529, 'kg'),  -- weight from EHR --
            (21, 2, 3036277, '2020-01-01', 32836, NULL, 165, 8582, 'cm'), -- height from EHR --
            (22, 2, 3025315, '2020-01-01', 32836, NULL, 60, 9529, 'kg'),  -- weight from EHR --
            (23, 2, 3036277, '2020-01-01', 32836, 201, 180, 8582, 'cm'),  -- self-reported height --
            (24, 2, 3025315, '2020-01-01', 32836, 201, 60, 9529, 'kg'),   -- self-reported weight --
            (25, 2, 3036277, '2020-01-01', 32836, 202, 170, 8582, 'cm'),  -- self-reported height --
            (26, 2, 3025315, '2020-01-01', 32836, 202, 85, 9529, 'kg'),   -- self-reported weight --
            (27, 2, 3036277, '2020-01-01', 32836, 203, 200, 8582, 'cm'),  -- self-reported height --
            (28, 2, 3025315, '2020-01-01', 32836, 203, 50, 9529, 'kg'),   -- self-reported weight --
            (31, 3, 3036277, '2020-01-01', 32836, 301, 180, 8582, 'cm'),  -- self-reported height --
            (32, 3, 3025315, '2020-01-01', 32836, 301, 60, 9529, 'kg'),   -- self-reported weight --
            (33, 3, 3036277, '2020-01-01', 32836, 302, 170, 8582, 'cm'),  -- self-reported height --
            (34, 3, 3025315, '2020-01-01', 32836, 302, 85, 9529, 'kg'),   -- self-reported weight --
            (39, 3, 3036277, '2020-01-01', 32836, 309, 170, 8582, 'cm'),  -- missing counterpart weight --
            (41, 4, 3036277, '2020-01-01', 32836, 401, 180, 8582, 'cm'),  -- self-reported height --
            (42, 4, 3025315, '2020-01-01', 32836, 401, 60, 9529, 'kg'),   -- self-reported weight --
            (43, 4, 3036277, '2020-01-01', 32836, 402, 170, 8582, 'cm'),  -- self-reported height --
            (44, 4, 3025315, '2020-01-01', 32836, 402, 85, 9529, 'kg'),   -- self-reported weight --
            (45, 4, 3036277, '2020-01-01', 32836, 403, 170, 8582, 'cm'),  -- pair exists but unmatching measurement_date --
            (46, 4, 3025315, '2020-01-02', 32836, 403, 85, 9529, 'kg'),   -- pair exists but unmatching measurement_date --
            (49, 4, 3025315, '2020-01-01', 32836, 409, 85, 9529, 'kg')    -- missing counterpart height --
        """).render(project=self.project_id, dataset=self.dataset_id)

        insert_meas_ext = self.jinja_env.from_string("""
        INSERT INTO `{{project}}.{{dataset}}.measurement_ext`
            (measurement_id, src_id)
        VALUES
            (11, 'HPO dummy'),
            (12, 'HPO dummy'),
            (21, 'HPO dummy'),
            (22, 'HPO dummy'),
            (23, 'Participant Portal: PTSC'),
            (24, 'Participant Portal: PTSC'),
            (25, 'Participant Portal: TPC'),
            (26, 'Participant Portal: TPC'),
            (27, 'Participant Portal: PTSC'),
            (28, 'Participant Portal: PTSC'),
            (31, 'Participant Portal: PTSC'),
            (32, 'Participant Portal: PTSC'),
            (33, 'Participant Portal: TPC'),
            (34, 'Participant Portal: TPC'),
            (39, 'Participant Portal: PTSC'),
            (41, 'Participant Portal: PTSC'),
            (42, 'Participant Portal: PTSC'),
            (43, 'Participant Portal: TPC'),
            (44, 'Participant Portal: TPC'),
            (45, 'Participant Portal: TPC'),
            (46, 'Participant Portal: TPC'),
            (49, 'Participant Portal: PTSC')
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
                'visit_occurrence_id', 'unit_concept_id', 'unit_source_value'
            ],
            'cleaned_values': [
                (11, 3036277, 32836, 165, None, 8582, 'cm'),
                (12, 3025315, 32836, 60, None, 9529, 'kg'),
                (21, 3036277, 32836, 165, None, 8582, 'cm'),
                (22, 3025315, 32836, 60, None, 9529, 'kg'),
                (23, 3036277, 32836, 180, 201, 8582, 'cm'),
                (24, 3025315, 32836, 60, 201, 9529, 'kg'),
                (25, 3036277, 32836, 170, 202, 8582, 'cm'),
                (26, 3025315, 32836, 85, 202, 9529, 'kg'),
                (27, 3036277, 32836, 200, 203, 8582, 'cm'),
                (28, 3025315, 32836, 50, 203, 9529, 'kg'),
                (31, 3036277, 32836, 180, 301, 8582, 'cm'),
                (32, 3025315, 32836, 60, 301, 9529, 'kg'),
                (33, 3036277, 32836, 170, 302, 8582, 'cm'),
                (34, 3025315, 32836, 85, 302, 9529, 'kg'),
                (39, 3036277, 32836, 170, 309, 8582, 'cm'),
                (41, 3036277, 32836, 180, 401, 8582, 'cm'),
                (42, 3025315, 32836, 60, 401, 9529, 'kg'),
                (43, 3036277, 32836, 170, 402, 8582, 'cm'),
                (44, 3025315, 32836, 85, 402, 9529, 'kg'),
                (45, 3036277, 32836, 170, 403, 8582, 'cm'),
                (46, 3025315, 32836, 85, 403, 9529, 'kg'),
                (49, 3025315, 32836, 85, 409, 9529, 'kg'),
                (50, 3038553, 32865, 18.51851851851852, 201, 9531, 'kg/m2'),
                (51, 3038553, 32865, 29.411764705882355, 202, 9531, 'kg/m2'),
                (52, 3038553, 32865, 12.5, 203, 9531, 'kg/m2'),
                (53, 3038553, 32865, 18.51851851851852, 301, 9531, 'kg/m2'),
                (54, 3038553, 32865, 29.411764705882355, 302, 9531, 'kg/m2'),
                (55, 3038553, 32865, 18.51851851851852, 401, 9531, 'kg/m2'),
                (56, 3038553, 32865, 29.411764705882355, 402, 9531, 'kg/m2'),
            ]
        }, {
            'fq_table_name':
                self.fq_table_names[1],
            'loaded_ids': [
                11, 12, 21, 22, 23, 24, 25, 26, 27, 28, 31, 32, 33, 34, 39, 41,
                42, 43, 44, 45, 46, 49
            ],
            'fields': ['measurement_id', 'src_id'],
            'cleaned_values': [
                (11, 'HPO dummy'),
                (12, 'HPO dummy'),
                (21, 'HPO dummy'),
                (22, 'HPO dummy'),
                (23, 'Participant Portal: PTSC'),
                (24, 'Participant Portal: PTSC'),
                (25, 'Participant Portal: TPC'),
                (26, 'Participant Portal: TPC'),
                (27, 'Participant Portal: PTSC'),
                (28, 'Participant Portal: PTSC'),
                (31, 'Participant Portal: PTSC'),
                (32, 'Participant Portal: PTSC'),
                (33, 'Participant Portal: TPC'),
                (34, 'Participant Portal: TPC'),
                (39, 'Participant Portal: PTSC'),
                (41, 'Participant Portal: PTSC'),
                (42, 'Participant Portal: PTSC'),
                (43, 'Participant Portal: TPC'),
                (44, 'Participant Portal: TPC'),
                (45, 'Participant Portal: TPC'),
                (46, 'Participant Portal: TPC'),
                (49, 'Participant Portal: PTSC'),
                (50, 'Participant Portal: PTSC'),
                (51, 'Participant Portal: TPC'),
                (52, 'Participant Portal: PTSC'),
                (53, 'Participant Portal: PTSC'),
                (54, 'Participant Portal: TPC'),
                (55, 'Participant Portal: PTSC'),
                (56, 'Participant Portal: TPC'),
            ]
        }]

        self.default_test(tables_and_counts)
