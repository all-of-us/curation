"""
Integration test for drop_extreme_measurements module

DC-1211
"""

# Python Imports
import os

# Third party imports

# Project Imports
from app_identity import PROJECT_ID
from common import JINJA_ENV, MEASUREMENT, PERSON
from cdr_cleaner.cleaning_rules.drop_extreme_measurements import DropExtremeMeasurements
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest

EXTREME_MEASUREMENTS_TEMPLATE = JINJA_ENV.from_string("""
INSERT INTO `{{project_id}}.{{dataset_id}}.measurement`
(measurement_id, person_id, measurement_concept_id, measurement_date, measurement_datetime,
measurement_type_concept_id, value_as_number, measurement_source_value, measurement_source_concept_id)
VALUES
    -- Extreme height/weight/BMI. Dropped. --
    (101, 1, 3036277, '2023-01-01', '2023-01-01 01:00:00', 44818701, 89, 'height', 903133),
    (102, 2, 3036277, '2023-01-01', '2023-01-01 02:00:00', 32865, 229, 'height', 903133),
    (103, 3, 3025315, '2023-01-01', '2023-01-01 03:00:00', 44818701, 29, 'weight', 903121),
    (104, 4, 3025315, '2023-01-01', '2023-01-01 04:00:00', 32865, 251, 'weight', 903121),
    (105, 5, 3038553, '2023-01-01', '2023-01-01 05:00:00', 44818701, 9, 'bmi', 903124),
    (106, 6, 3038553, '2023-01-01', '2023-01-01 06:00:00', 32865, 126, 'bmi', 903124),
    -- Non-extreme height/weight/BMI. Not dropped. --
    (201, 7, 3036277, '2023-01-01', '2023-01-02 00:00:00', 44818701, 90, 'height', 903133),
    (202, 8, 3025315, '2023-01-01', '2023-01-02 00:00:00', 32865, 30, 'weight', 903121),
    (203, 9, 3038553, '2023-01-01', '2023-01-02 00:00:00', 44818701, 10, 'bmi', 903124),
    -- Non-extreme height/weight associated with extreme BMI. Dropped. --
    (301, 5, 3036277, '2023-01-01', '2023-01-01 05:00:00', 44818701, 180, 'height', 903133),
    (302, 6, 3025315, '2023-01-01', '2023-01-01 06:00:00', 32865, 200, 'weight', 903121),
    -- Non-extreme BMI associated with extreme height or weight. Dropped. --
    (303, 1, 3038553, '2023-01-01', '2023-01-01 01:00:00', 44818701, 20, 'bmi', 903124),
    (304, 4, 3038553, '2023-01-01', '2023-01-01 04:00:00', 32865, 20, 'bmi', 903124),
    -- Non-extreme height associated with extreme weight and vice-versa.  Not dropped. --
    (401, 3, 3036277, '2023-01-01', '2023-01-01 03:00:00', 44818701, 90, 'height', 903133),
    (402, 2, 3025315, '2023-01-01', '2023-01-01 02:00:00', 32865, 30, 'weight', 903121),
    -- In-person vs self-report PM so deemed non-associated though the same datetime. Not dropped. --
    (501, 5, 3036277, '2023-01-01', '2023-01-01 05:00:00', 32865, 180, 'height', 903133),
    (502, 6, 3025315, '2023-01-01', '2023-01-01 06:00:00', 44818701, 200, 'weight', 903121),
    (503, 1, 3038553, '2023-01-01', '2023-01-01 01:00:00', 32865, 20, 'bmi', 903124),
    (504, 4, 3038553, '2023-01-01', '2023-01-01 04:00:00', 44818701, 20, 'bmi', 903124),
    -- Irrelevant concept. Not dropped --
    (999, 1, 903135, '2023-01-01', '2023-01-01 01:00:00', 44818701, 250, 'waist-circumference-mean', 903135),
    -- Pediatric values outside adult ranges are not dropped. --
    (1000, 10, 3036277, '2023-01-01', '2023-01-01 01:00:00', 44818701, 50, 'height', 903133),
    (1001, 10, 3025315, '2023-01-01', '2023-01-01 01:00:00', 44818701, 20, 'weight', 903121)
""")

PERSON_TEMPLATE = JINJA_ENV.from_string("""
INSERT INTO `{{project_id}}.{{dataset_id}}.person`
(person_id, birth_datetime, year_of_birth, gender_concept_id, race_concept_id, ethnicity_concept_id)
VALUES
    (1, '1980-01-01', 1980, 0, 0, 0),
    (2, '1980-01-01', 1980, 0, 0, 0),
    (3, '1980-01-01', 1980, 0, 0, 0),
    (4, '1980-01-01', 1980, 0, 0, 0),
    (5, '1980-01-01', 1980, 0, 0, 0),
    (6, '1980-01-01', 1980, 0, 0, 0),
    (7, '1980-01-01', 1980, 0, 0, 0),
    (8, '1980-01-01', 1980, 0, 0, 0),
    (9, '1980-01-01', 1980, 0, 0, 0),
    (10, '2015-01-01', 2015, 0, 0, 0)
""")


class DropExtremeMeasurementsTest(BaseTest.CleaningRulesTestBase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        super().initialize_class_vars()

        cls.project_id = os.environ.get(PROJECT_ID)
        cls.dataset_id = os.environ.get('COMBINED_DATASET_ID')
        cls.sandbox_id = f'{cls.dataset_id}_sandbox'

        cls.rule_instance = DropExtremeMeasurements(cls.project_id,
                                                    cls.dataset_id,
                                                    cls.sandbox_id)

        sandbox_table_names = cls.rule_instance.get_sandbox_tablenames()
        for table_name in sandbox_table_names:
            cls.fq_sandbox_table_names.append(
                f'{cls.project_id}.{cls.sandbox_id}.{table_name}')

        measurement_table_name = f'{cls.project_id}.{cls.dataset_id}.{MEASUREMENT}'
        person_table_name = f'{cls.project_id}.{cls.dataset_id}.{PERSON}'
        cls.fq_table_names = [measurement_table_name, person_table_name]

        # call super to set up the client, create datasets, and create
        # empty test tables
        # NOTE:  does not create empty sandbox tables.
        super().setUpClass()

    def setUp(self):
        """
        Create test table for the rule to run on
        """
        super().setUp()

        extreme_measurement_template = EXTREME_MEASUREMENTS_TEMPLATE.render(
            project_id=self.project_id, dataset_id=self.dataset_id)
        person_template = PERSON_TEMPLATE.render(project_id=self.project_id,
                                                 dataset_id=self.dataset_id)

        self.load_test_data([person_template, extreme_measurement_template])

    def test_field_cleaning(self):
        """
        Test the CR works as designed. 
        See the inline comment in EXTREME_MEASUREMENTS_TEMPLATE for the expected results.
        """
        tables_and_counts = [{
            'fq_table_name':
                self.fq_table_names[0],
            'fq_sandbox_table_name':
                self.fq_sandbox_table_names[0],
            'loaded_ids': [
                101, 102, 103, 104, 105, 106, 201, 202, 203, 301, 302, 303, 304,
                401, 402, 501, 502, 503, 504, 999, 1000, 1001
            ],
            'sandboxed_ids': [101, 102, 103, 104, 105, 106, 301, 302, 303, 304],
            'fields': ['measurement_id'],
            'cleaned_values': [(201,), (202,), (203,), (401,), (402,), (501,),
                               (502,), (503,), (504,), (999,), (1000,), (1001,)]
        }]

        self.default_test(tables_and_counts)
