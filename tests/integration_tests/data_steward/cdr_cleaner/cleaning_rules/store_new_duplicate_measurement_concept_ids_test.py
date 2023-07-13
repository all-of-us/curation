"""
Integration test for store_new_duplicate_measurement_concept_ids.py
"""

# Python imports
import os
from datetime import datetime, timezone

# Third party imports
import mock

# Project imports
from common import JINJA_ENV, IDENTICAL_LABS_LOOKUP_TABLE, MEASUREMENT, CONCEPT
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.store_new_duplicate_measurement_concept_ids import StoreNewDuplicateMeasurementConceptIds
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest


class StoreNewDuplicateMeasurementConceptIdsTest(BaseTest.CleaningRulesTestBase):

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
        dataset_id = os.environ.get('COMBINED_DATASET_ID')
        cls.dataset_id = dataset_id
        sandbox_id = dataset_id + '_sandbox'
        cls.sandbox_id = sandbox_id

        cls.rule_instance = StoreNewDuplicateMeasurementConceptIds(
            cls.project_id, cls.dataset_id, cls.sandbox_id)

        sb_table_names = cls.rule_instance.get_sandbox_tablenames()
        for table_name in sb_table_names:
            cls.fq_sandbox_table_names.append(
                f'{cls.project_id}.{cls.sandbox_id}.{table_name}')

        # Store affected table names
        affected_tables = [IDENTICAL_LABS_LOOKUP_TABLE, MEASUREMENT, CONCEPT]
        for table_name in affected_tables:
            cls.fq_table_names.append(
                f'{cls.project_id}.{cls.dataset_id}.{table_name}')

        # call super to set up the client, create datasets, and create
        # empty test tables
        # NOTE:  does not create empty sandbox tables.
        super().setUpClass()

    def test_store_new_duplicate_measurement_concept_ids(self):
        """
        Tests unit_normalization for the loaded test data
        """

        MEASUREMENT_TEMPLATE = self.jinja_env.from_string("""
        INSERT INTO `{{project_id}}.{{dataset_id}}.measurement`
        (measurement_id, person_id, measurement_concept_id, measurement_date,
         measurement_type_concept_id, value_as_number, value_as_concept_id)
        VALUES
        -- Test function on concept_name duplicates --
          (801, 111, 100001, '2019-01-01', 45905771, null, 100001),
          (802, 222, 100002, '2019-01-01', 45905771, null, 100002),
        -- Test function on concept_name duplicates and casing --  
          (803, 333, 100003, '2019-01-01', 45905771, null, 100003),
        -- Test function on concept_class_ids --
          (804, 444, 100004, '2019-01-01', 45905771, null, 100004),
          (805, 555, 100005, '2019-01-01', 45905771, null, 100005),
        -- Test for duplicates within one vocabulary -- 
          (806, 666, 100006, '2019-01-01', 45905771, null, 100006),
          (807, 777, 100007, '2019-01-01', 45905771, null, 100007),
          (808, 888, 100008, '2019-01-01', 45905771, null, 100008),
        -- Test function without duplciates --
          (809, 999, 100009, '2019-01-01', 45905771, null, 100009),
        -- Test no overwrite or duplicate creation in lookup table -- 
          (811, 121, 100011, '2019-01-01', 45905771, null, 100011),
          (812, 122, 100012, '2019-01-01', 45905771, null, 100012),
          (813, 123, 100013, '2019-01-01', 45905771, null, 100013),
        -- Test function of multiple measurements per duplicate type --
          (814, 124, 100014, '2019-01-01', 45905771, null, 100014)
            """).render(project_id=self.project_id,
                        dataset_id=self.dataset_id)

        CONCEPT_TEMPLATE = JINJA_ENV.from_string("""
        INSERT INTO `{{project_id}}.{{dataset_id}}.concept`
        (concept_id, concept_name, vocabulary_id, domain_id, concept_class_id, concept_code, valid_start_date, valid_end_date)
        VALUES
        -- Test function on concept_name duplicates --
          (100001, 'Negative', 'LOINC',  'Measurement', 'Lab test', '1-1','2000-01-01', '2099-01-01'),
          (100002, 'Negative', 'NAACCR',  'Measurement', 'Lab test', '1-2','2000-01-01', '2099-01-01'),
        -- Test function on concept_name duplicates and casing --
          (100003, 'negative', 'LOINC',  'Measurement', 'Lab test', '1-3','2000-01-01', '2099-01-01'),
        -- Test function on concept_class_ids --
          (100004, 'Positive', 'LOINC',  'Measurement', 'Lab test', '1-4','2000-01-01', '2099-01-01'),
          (100005, 'Positive', 'NAACCR', 'Measurement', 'Clinical Observation', '1-5','2000-01-01', '2099-01-01'),
        -- Test for duplicates within one vocabulary --
          (100006, 'Decreased', 'LOINC',  'Measurement', 'Lab test', '1-4','2000-01-01', '2099-01-01'),
          (100007, 'Decreased', 'NAACCR', 'Measurement', 'Clinical Observation', '1-7','2000-01-01', '2099-01-01'),
          (100008, 'Decreased', 'NAACCR', 'Measurement', 'Clinical Observation', '1-8','2000-01-01', '2099-01-01'),
        -- Test function without duplciates --
          (100009, 'One Thousand mg', 'LOINC', 'Measurement', 'Lab test', '1-9','2000-01-01', '2099-01-01'),
        -- Test no overwrite or duplicate creation in lookup table --
          (1000011, 'Increased', 'LOINC',  'Measurement', 'Lab test', '1-11','2000-01-01', '2099-01-01'),
          (1000012, 'Increased', 'SNOMED', 'Measurement', 'Lab test', '1-12','2000-01-01', '2099-01-01'),
          (1000013, 'Increased', 'NAACCR', 'Measurement', 'Lab test', '1-13','2000-01-01', '2099-01-01')
        """).render(project_id=self.project_id, dataset_id=self.dataset_id)

        P_LOOKUP_TEMPLATE = JINJA_ENV.from_string("""
        INSERT INTO `{{project_id}}.{{dataset_id}}.{{lookup_table}}`
        (value_as_concept_id,vac_name,vac_vocab,aou_standard_vac,
                n_measurement_concept_id,n_measurement,date_added)
        VALUES
        -- Test for existing records --
            (1000011, 'Increased', 'LOINC', 100001, 6,5000, '2022-01-01'),
            (1000012, 'Increased', 'SNOMED',1000011, 6,100, '2022-01-01'),
            (1000013, 'Increased', 'NAACCR',1000011, 6,100, '2022-01-01')
        """).render(project_id=self.project_id, dataset_id=self.dataset_id,lookup_table=IDENTICAL_LABS_LOOKUP_TABLE)

        self.load_test_data([MEASUREMENT_TEMPLATE, CONCEPT_TEMPLATE, P_LOOKUP_TEMPLATE])

        tables_and_counts = [{
            'fq_table_name':
                self.fq_table_names[0],
            'fq_sandbox_table_name':
                self.fq_sandbox_table_names[0],
            'fields': [
                'value_as_concept_id','vac_name','vac_vocab','aou_standard_vac',
                'n_measurement_concept_id','n_measurement','date_added'],
            'cleaned_values': [
                (1, 'Negative', 'SNOMED',100001, 2,2, datetime.now(timezone.utc).date())
            ]
        }]

        # mock the PIPELINE_TABLES variable
        with mock.patch(
                'cdr_cleaner.cleaning_rules.store_new_duplicate_measurement_concept_ids.PIPELINE_TABLES',
                self.dataset_id):
            self.default_test(tables_and_counts)
