"""
Integration Test for the replace_pediatric_measurement_codes module.

Original Issues: DL-2480

Asserts that each of the five pediatric growth-measurement placeholder codes is
replaced by its specified LOINC code in measurement_source_value and by that
code's concept id in both measurement_source_concept_id and
measurement_concept_id, that a row arriving under the PMI code with unresolved
concept ids is replaced the same way, and that neither a correctly annotated
row nor a measurement outside the replacement map is touched.
"""
# Python imports
import os

# Project imports
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.replace_pediatric_measurement_codes import (
    ReplacePediatricMeasurementCodes)
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest


class ReplacePediatricMeasurementCodesTest(BaseTest.CleaningRulesTestBase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        super().initialize_class_vars()

        # set the test project identifier
        project_id = os.environ.get(PROJECT_ID)
        cls.project_id = project_id

        # the rule runs at the RDR stage; the combined dataset environment
        # variable is the one guaranteed to exist for integration tests
        dataset_id = os.environ.get('COMBINED_DATASET_ID')
        cls.dataset_id = dataset_id
        sandbox_id = dataset_id + '_sandbox'
        cls.sandbox_id = sandbox_id

        cls.rule_instance = ReplacePediatricMeasurementCodes(
            project_id, dataset_id, sandbox_id)

        sb_table_names = cls.rule_instance.get_sandbox_tablenames()
        for table_name in sb_table_names:
            cls.fq_sandbox_table_names.append(
                f'{project_id}.{sandbox_id}.{table_name}')

        cls.fq_table_names = [f"{project_id}.{dataset_id}.measurement"]

        # call super to set up the client, create datasets, and create
        # empty test tables
        # NOTE:  does not create empty sandbox tables.
        super().setUpClass()

    def test_replace_pediatric_measurement_codes(self):
        """
        Nine rows: the five placeholders, one PMI-coded row on each side of the
        unresolved guard, and two measurements outside the replacement map.
        """
        insert_fake_measurements = self.jinja_env.from_string("""
        INSERT INTO `{{project}}.{{dataset}}.measurement`
        (measurement_id, person_id, measurement_concept_id, measurement_date, measurement_datetime,
        measurement_time, measurement_type_concept_id, operator_concept_id, value_as_number, value_as_concept_id,
        unit_concept_id, range_low, range_high, provider_id, visit_occurrence_id, visit_detail_id,
        measurement_source_value, measurement_source_concept_id, unit_source_value, value_source_value)
        VALUES
        -- placeholder, weight for age -> 8336-0 / 3013131 --
          (801, 1, 0, '2026-05-01', "2026-05-01 05:30:00+00", NULL, 0, 0, 55, 0, 0, NULL, NULL, NULL, NULL, NULL, "22222-0", 0, "", ""),
        -- placeholder, height for age -> 8303-0 / 3036798 --
          (802, 2, 0, '2026-05-01', "2026-05-01 05:30:00+00", NULL, 0, 0, 40, 0, 0, NULL, NULL, NULL, NULL, NULL, "33333-0", 0, "", ""),
        -- placeholder, weight for length -> 77606-2 / 46236327 --
          (803, 3, 0, '2026-05-01', "2026-05-01 05:30:00+00", NULL, 0, 0, 62, 0, 0, NULL, NULL, NULL, NULL, NULL, "44444-0", 0, "", ""),
        -- placeholder, head circumference for age -> 8289-1 / 3035763 --
          (804, 4, 0, '2026-05-01', "2026-05-01 05:30:00+00", NULL, 0, 0, 71, 0, 0, NULL, NULL, NULL, NULL, NULL, "55555-0", 0, "", ""),
        -- placeholder, BMI for age -> 59576-9 / 40762638 --
          (805, 5, 0, '2026-05-01', "2026-05-01 05:30:00+00", NULL, 0, 0, 48, 0, 0, NULL, NULL, NULL, NULL, NULL, "66666-0", 0, "", ""),
        -- non-placeholder PMI code, already resolved, must not be modified --
          (806, 6, 3036277, '2026-05-01', "2026-05-01 05:30:00+00", NULL, 0, 0, 110, 0, 0, NULL, NULL, NULL, NULL, NULL, "height", 903133, "cm", ""),
        -- LOINC code outside the replacement map, must not be modified --
          (807, 7, 3013762, '2026-05-01', "2026-05-01 05:30:00+00", NULL, 0, 0, 18, 0, 0, NULL, NULL, NULL, NULL, NULL, "8302-2", 0, "", ""),
        -- PMI code for a growth percentile, unresolved, must be replaced --
          (808, 8, 0, '2026-05-01', "2026-05-01 05:30:00+00", NULL, 0, 0, 55, 0, 0, NULL, NULL, NULL, NULL, NULL, "growth-percentile-weight-for-age", 0, "", ""),
        -- PMI code for a growth percentile, already resolved, must not be modified --
          (809, 9, 40762638, '2026-05-01', "2026-05-01 05:30:00+00", NULL, 0, 0, 48, 0, 0, NULL, NULL, NULL, NULL, NULL, "growth-percentile-bmi-for-age", 903124, "", "")
        """).render(project=self.project_id, dataset=self.dataset_id)

        self.load_test_data([insert_fake_measurements])

        tables_and_counts = [{
            'name':
                self.fq_table_names[0].split('.')[-1],
            'fq_table_name':
                self.fq_table_names[0],
            'fq_sandbox_table_name':
                self.fq_sandbox_table_names[0],
            'fields': [
                'measurement_id', 'measurement_source_value',
                'measurement_source_concept_id', 'measurement_concept_id'
            ],
            'loaded_ids': [801, 802, 803, 804, 805, 806, 807, 808, 809],
            'sandboxed_ids': [801, 802, 803, 804, 805, 808],
            'cleaned_values': [
                (801, '8336-0', 3013131, 3013131),
                (802, '8303-0', 3036798, 3036798),
                (803, '77606-2', 46236327, 46236327),
                (804, '8289-1', 3035763, 3035763),
                (805, '59576-9', 40762638, 40762638),
                (806, 'height', 903133, 3036277),
                (807, '8302-2', 0, 3013762),
                (808, '8336-0', 3013131, 3013131),
                (809, 'growth-percentile-bmi-for-age', 903124, 40762638),
            ]
        }]

        self.default_test(tables_and_counts)
