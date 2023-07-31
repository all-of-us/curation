"""
Integration Test for the dedup_measurement_value_as_concept_id  module.

"""
# Python imports
import os

# Third party imports

# Project imports
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.dedup_measurement_value_as_concept_id import (
    DedupMeasurementValueAsConceptId)
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest
from common import IDENTICAL_LABS_LOOKUP_TABLE


class DedupMeasurementValueAsConceptIdTest(BaseTest.CleaningRulesTestBase):

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
        # intended to be run on the deid_base dataset.  The combined dataset
        # environment variable should be guaranteed to exist
        dataset_id = os.environ.get('COMBINED_DATASET_ID')
        cls.dataset_id = dataset_id
        sandbox_id = dataset_id + '_sandbox'
        cls.sandbox_id = sandbox_id

        cls.rule_instance = DedupMeasurementValueAsConceptId(
            project_id, dataset_id, sandbox_id)

        sb_table_names = cls.rule_instance.get_sandbox_tablenames()
        for table_name in sb_table_names:
            cls.fq_sandbox_table_names.append(
                f'{project_id}.{sandbox_id}.{table_name}')

        cls.fq_table_names = [f"{project_id}.{dataset_id}.measurement"]
        cls.fq_table_names.append(
            f'{cls.project_id}.pipeline_tables.{IDENTICAL_LABS_LOOKUP_TABLE}')

        cls.dataset_id = dataset_id
        # call super to set up the client, create datasets, and create
        # empty test tables
        # NOTE:  does not create empty sandbox tables.
        super().setUpClass()

    def test_dedup_measurement_value_as_concept_id(self):
        """
        Use the default drop rows test function.

        Validates pre-conditions, test execution and post conditions based on
        the load statements and the tables_and_counts variable.
        """
        insert_fake_measurements = self.jinja_env.from_string("""
        INSERT INTO `{{project}}.{{dataset}}.measurement`
        (measurement_id, person_id, measurement_concept_id, measurement_date, measurement_datetime,
        measurement_time, measurement_type_concept_id, operator_concept_id, value_as_number, value_as_concept_id, 
        unit_concept_id, range_low, range_high, provider_id, visit_occurrence_id, visit_detail_id, 
        measurement_source_value, measurement_source_concept_id, unit_source_value, value_source_value)
        VALUES
        -- Concept_name 0 -> 45880618 LOINC, should not be modified --
          (801, 1, 0, '2016-05-01', "2016-05-01 05:30:00+00", NULL, 0, 0, 0, 45880618, 0, 0, 0, 0, 0, 0, "", 0, "", ""),
        -- Concept_name 0 -> 4121196 SNOMED, should be updated to LOINC code for 0 - 45880618 --
          (802, 2, 0, '2016-05-01', "2016-05-01 05:30:00+00", NULL, 0, 0, 0, 4121196, 0, 0, 0, 0, 0, 0, "", 0, "", ""),
        -- Concept_name Abnormal -> 45878745 LOINC, should not be modified --
          (803, 3, 0, '2016-05-01', "2016-05-01 05:30:00+00", NULL, 0, 0, 0, 45878745, 0, 0, 0, 0, 0, 0, "", 0, "", ""),
        -- Concept_name Abnormal -> 4135493 SNOMED, should be updated to LOINC code for Abnormal - 45878745 --
          (804, 4, 0, '2016-05-01', "2016-05-01 05:30:00+00", NULL, 0, 0, 0, 4135493, 0, 0, 0, 0, 0, 0, "", 0, "", ""),
        -- Concept_name 0 -> 35919331 NAACCR, should not be modified --
          (805, 5, 0, '2016-05-01', "2016-05-01 05:30:00+00", NULL, 0, 0, 0, 35919331, 0, 0, 0, 0, 0, 0, "", 0, "", "")
        """).render(project=self.project_id, dataset=self.dataset_id)

        insert_fake_lookup = self.jinja_env.from_string("""
        INSERT INTO `{{project_id}}.pipeline_tables.{{lookup_table}}`
        (value_as_concept_id,vac_name,vac_vocab,aou_standard_vac,
               date_added)
        VALUES
        --  id= 801  --
            (45880618, '0', 'LOINC', 45880618, '2000-01-01'),
        --  id= 802  --
            (4121196, '0', 'SNOMED', 45880618, '2000-01-01'),
        --  id= 803  --
            (45878745, 'Abnormal', 'LOINC', 45878745, '2000-01-01'),
        --  id= 804  --
            (4135493, 'Abnormal', 'SNOMED', 45878745, '2000-01-01'),
        --  id= 805  --
            (35919331, '0', 'NAACCR', 35919331, '2000-01-01')
        """).render(project_id=self.project_id,
                    lookup_table=IDENTICAL_LABS_LOOKUP_TABLE)

        self.load_test_data([insert_fake_measurements, insert_fake_lookup])

        tables_and_counts = [{
            'name':
                self.fq_table_names[0].split('.')[-1],
            'fq_table_name':
                self.fq_table_names[0],
            'fq_sandbox_table_name':
                self.fq_sandbox_table_names[0],
            'fields': ['measurement_id', 'value_as_concept_id'],
            'loaded_ids': [801, 802, 803, 804, 805],
            'sandboxed_ids': [802, 804],
            'cleaned_values': [(801, 45880618), (802, 45880618),
                               (803, 45878745), (804, 45878745),
                               (805, 35919331)]
        }]

        # Ensures the lookup table copy, created by the CR is also deleted.
        self.fq_sandbox_table_names.append(
            f'{self.project_id}.{self.sandbox_id}.{IDENTICAL_LABS_LOOKUP_TABLE}'
        )

        self.default_test(tables_and_counts)
