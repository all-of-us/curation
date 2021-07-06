"""
Integration test for unit_normalization cleaning rule.

Original Issue: DC-414
"""
# Python imports
import os

# Third party imports
from google.cloud import bigquery

# Project Imports
import tests.test_util as test_util
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.unit_normalization import UnitNormalization, UNIT_MAPPING_TABLE
from common import JINJA_ENV, MEASUREMENT
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import \
    BaseTest

test_query = JINJA_ENV.from_string("""select * from `{{intermediary_table}}`""")

INSERT_UNITS_RAW_DATA = JINJA_ENV.from_string("""
  INSERT INTO `{{project_id}}.{{dataset_id}}.measurement` (
      measurement_id,
      person_id,
      measurement_concept_id,
      value_as_number,
      unit_concept_id,
      range_low,
      range_high,
      measurement_date,
      measurement_datetime,
      measurement_type_concept_id,
      operator_concept_id,
      value_as_concept_id,
      provider_id,
      visit_occurrence_id,
      measurement_source_value,
      measurement_source_concept_id,
      unit_source_value,
      value_source_value
      )
    VALUES
    -- 3020509 Albumin/Globulin [Mass Ratio] in Serum or Plasma https://athena.ohdsi.org/search-terms/terms/3020509 --
    -- 3020891 Body temperature https://athena.ohdsi.org/search-terms/terms/3020891 --
    -- 3016293 Bicarbonate [Moles/volume] in Serum or Plasma https://athena.ohdsi.org/search-terms/terms/3016293 --
    -- 3027970 Globulin [Mass/volume] in Serum by calculation https://athena.ohdsi.org/search-terms/terms/3027970 --
    -- 3000963 Hemoglobin [Mass/volume] in Blood https://athena.ohdsi.org/search-terms/terms/3000963 --
    -- 000905 Leukocytes [#/volume] in Blood by Automated count https://athena.ohdsi.org/search-terms/terms/3000905 --
    -- 3020630 Protein [Mass/volume] in Serum or Plasma https://athena.ohdsi.org/search-terms/terms/3020630 --
    -- 3020416 Erythrocytes [#/volume] in Blood by Automated count https://athena.ohdsi.org/search-terms/terms/3020416 --
      (1,1,3020509,0.4,8523,1.0,2.4,'2020-01-01', '2020-01-01 01:01:01' ,0,0,0,0,0,null,0,null,null),
      (2,1,3020891,97.7,9289,0.0,150.0,'2020-01-01', '2020-01-01 01:01:01' ,0,0,0,0,0,null,0,null,null),
      (3,1,3016293,25.0,8753,21.0,31.0,'2020-01-01', '2020-01-01 01:01:01' ,0,0,0,0,0,null,0,null,null),
      (4,1,3027970,2.3,8713,1.5,4.5,'2020-01-01', '2020-01-01 01:01:01' ,0,0,0,0,0,null,0,null,null),
      (5,1,3027970,2.6,4121395,1.9,3.7,'2020-01-01', '2020-01-01 01:01:01' ,0,0,0,0,0,null,0,null,null),
      (6,1,3000963,15.8,4121395,13.2,17.1,'2020-01-01', '2020-01-01 01:01:01' ,0,0,0,0,0,null,0,null,null),
      (7,1,3000905,10.1,8647,4.5,11.0,'2020-01-01', '2020-01-01 01:01:01' ,0,0,0,0,0,null,0,null,null),
      (8,1,3020630,6.2,8840,6.4,8.2,'2020-01-01', '2020-01-01 01:01:01' ,0,0,0,0,0,null,0,null,null),
      (9,1,3020416,5.06,8816,4.2,5.8,'2020-01-01', '2020-01-01 01:01:01' ,0,0,0,0,0,null,0,null,null),
      (10,1,3000963,3.4,8554,0.5,5.0,'2020-01-01', '2020-01-01 01:01:01' ,0,0,0,0,0,null,0,null,null)
""")


class UnitNormalizationTest(BaseTest.CleaningRulesTestBase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        super().initialize_class_vars()

        # Set the test project identifier
        cls.project_id = os.environ.get(PROJECT_ID)

        # Set the expected test datasets
        cls.dataset_id = os.environ.get('COMBINED_DATASET_ID')
        cls.sandbox_id = cls.dataset_id + '_sandbox'
        cls.rule_instance = UnitNormalization(cls.project_id, cls.dataset_id,
                                              cls.sandbox_id)
        # Generates list of fully qualified table names and their corresponding sandbox table names
        cls.fq_table_names.append(
            f'{cls.project_id}.{cls.dataset_id}.{MEASUREMENT}')

        sandbox_table_names = cls.rule_instance.get_sandbox_tablenames()
        for table in sandbox_table_names:
            cls.fq_sandbox_table_names.append(
                f'{cls.project_id}.{cls.sandbox_id}.{table}')

        # Appending UNIT_MAPPING_TABLE table to fq_sandbox_table_names to delete after the test
        cls.fq_sandbox_table_names.append(
            f'{cls.project_id}.{cls.sandbox_id}.{UNIT_MAPPING_TABLE}')

        # call super to set up the client, create datasets
        cls.up_class = super().setUpClass()

    def setUp(self):
        # Set the test project identifier
        super().setUp()
        raw_units_load_query = INSERT_UNITS_RAW_DATA.render(
            project_id=self.project_id, dataset_id=self.dataset_id)

        # Load test data
        self.load_test_data([f'{raw_units_load_query}'])

    def test_setup_rule(self):

        # test if intermediary table exists before running the cleaning rule
        intermediary_table = f'{self.project_id}.{self.sandbox_id}.{UNIT_MAPPING_TABLE}'

        # run setup_rule and see if the table is created
        self.rule_instance.setup_rule(self.client)

        actual_table = self.client.get_table(intermediary_table)
        self.assertIsNotNone(actual_table.created)

        # test if exception is raised if table already exists
        with self.assertRaises(RuntimeError) as c:
            self.rule_instance.setup_rule(self.client)

        self.assertEqual(str(c.exception),
                         f"Unable to create tables: ['{intermediary_table}']")

        query = test_query.render(intermediary_table=intermediary_table)
        query_job_config = bigquery.job.QueryJobConfig(use_query_cache=False)
        result = self.client.query(query,
                                   job_config=query_job_config).to_dataframe()
        self.assertEqual(result.empty, False)

    def test_unit_normalization(self):
        """
        Tests unit_normalization for the loaded test data
        """
        # Expected results list
        tables_and_counts = [{
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{MEASUREMENT}',
            'loaded_ids': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            'sandboxed_ids': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            'fields': [
                'measurement_id', 'person_id', 'measurement_concept_id',
                'value_as_number', 'unit_concept_id', 'range_low', 'range_high'
            ],
            'cleaned_values': [(1, 1, 3020509, 0.4, 8523, 1.0, 2.4),
                               (2, 1, 3020891, 36.5, 8653, -17.77777777777778,
                                65.55555555555556),
                               (3, 1, 3016293, 25.0, 8753, 21.0, 31.0),
                               (4, 1, 3027970, 23.0, 8636, 15.0, 45.0),
                               (5, 1, 3027970, 26.0, 8636, 19.0, 37.0),
                               (6, 1, 3000963, 158.0, 8636, 132.0, 171.0),
                               (7, 1, 3000905, 0.0101, 8848,
                                0.0045000000000000005, 0.011),
                               (8, 1, 3020630, 0.006200000000000001, 8713,
                                0.0064, 0.008199999999999999),
                               (9, 1, 3020416, 5060.0, 8815, 4200.0, 5800.0),
                               (10, 1, 3000963, 0.34, 8636, 0.05, 0.5)]
        }]

        self.default_test(tables_and_counts)
