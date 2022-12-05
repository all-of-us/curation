"""
Integration test for truncate_era_tables module

Original Issue: DC-2786
"""

# Python Imports
import os

# Project Imports
from app_identity import PROJECT_ID
from common import JINJA_ENV
from cdr_cleaner.cleaning_rules.truncate_era_tables import TruncateEraTables
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest

DOSE_ERA_TEMPLATE = JINJA_ENV.from_string("""
INSERT INTO `{{project_id}}.{{dataset_id}}.dose_era` (
    dose_era_id,
    person_id,
    drug_concept_id,
    unit_concept_id,
    dose_value,
    dose_era_start_date,
    dose_era_end_date
)

VALUES
    (1, 10, 100, 1000, 50, TIMESTAMP('2009-04-01'), TIMESTAMP('2009-04-30')),
    (2, 20, 200, 2000, 60, TIMESTAMP('2010-05-01'), TIMESTAMP('2010-05-30')),
    (3, 30, 300, 3000, 70, TIMESTAMP('2011-06-01'), TIMESTAMP('2011-06-30')),
    (4, 40, 400, 4000, 80, TIMESTAMP('2012-07-01'), TIMESTAMP('2012-07-30')),
    (5, 50, 500, 5000, 90, TIMESTAMP('2013-08-01'), TIMESTAMP('2013-05-30'))
""")

DRUG_ERA_TEMPLATE = JINJA_ENV.from_string("""
INSERT INTO `{{project_id}}.{{dataset_id}}.drug_era` (
    drug_era_id,
    person_id,
    drug_concept_id,
    drug_era_start_date,
    drug_era_end_date,
    drug_exposure_count,
    gap_days
)

VALUES
    (1, 10, 100, TIMESTAMP('2009-04-01'), TIMESTAMP('2009-04-30'), 10, 20),
    (2, 20, 200, TIMESTAMP('2010-05-01'), TIMESTAMP('2010-05-30'), 15, 20),
    (3, 30, 300, TIMESTAMP('2011-06-01'), TIMESTAMP('2011-06-30'), 14, 20),
    (4, 40, 400, TIMESTAMP('2012-07-01'), TIMESTAMP('2012-07-30'), 29, 20),
    (5, 50, 500, TIMESTAMP('2013-08-01'), TIMESTAMP('2013-05-30'), 33, 20)
""")

CONDITION_ERA_TEMPLATE = JINJA_ENV.from_string("""
INSERT INTO `{{project_id}}.{{dataset_id}}.condition_era` (
    condition_era_id,
    person_id,
    condition_concept_id,
    condition_era_start_date,
    condition_era_end_date,
    condition_occurrence_count
)

VALUES
    (100, 1000, 50, TIMESTAMP('2009-04-01'), TIMESTAMP('2009-04-30'), 7),
    (200, 2000, 60, TIMESTAMP('2010-05-01'), TIMESTAMP('2010-05-30'), 20),
    (300, 3000, 70, TIMESTAMP('2011-06-01'), TIMESTAMP('2011-06-30'), 16),
    (400, 4000, 80, TIMESTAMP('2012-07-01'), TIMESTAMP('2012-07-30'), 10),
    (500, 5000, 90, TIMESTAMP('2013-08-01'), TIMESTAMP('2013-05-30'), 24)
""")


class TruncateEraTablesTest(BaseTest.CleaningRulesTestBase):

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
        cls.sandbox_id = f'{cls.dataset_id}_sandbox'

        # Instantiate class
        cls.rule_instance = TruncateEraTables(cls.project_id, cls.dataset_id,
                                              cls.sandbox_id)

        # Generate sandbox table names
        sandbox_table_names = cls.rule_instance.get_sandbox_tablenames()
        for table_name in sandbox_table_names:
            cls.fq_sandbox_table_names.append(
                f'{cls.project_id}.{cls.sandbox_id}.{table_name}')

        # Store affected table names
        affected_tables = cls.rule_instance.affected_tables
        for table_name in affected_tables:
            cls.fq_table_names.append(
                f'{cls.project_id}.{cls.dataset_id}.{table_name}')

        super().setUpClass()

    def setUp(self):
        """
        Create empty tables for the rule to run on
        """
        super().setUp()

        test_queries = []
        templates = [
            DOSE_ERA_TEMPLATE, DRUG_ERA_TEMPLATE, CONDITION_ERA_TEMPLATE
        ]

        # Queries to insert test records into _era tables
        for template in templates:
            test_queries.append(
                template.render(project_id=self.project_id,
                                dataset_id=self.dataset_id))

        self.load_test_data(test_queries)

    def test_truncate_data(self):
        """
        All the data from condition_era, drug_era, and dose_era tables is sandboxed and dropped
        """

        # Expected results list
        tables_and_counts = [{
            'fq_table_name': self.fq_table_names[0],
            'fq_sandbox_table_name': self.fq_sandbox_table_names[0],
            'loaded_ids': [1, 2, 3, 4, 5],
            'sandboxed_ids': [1, 2, 3, 4, 5],
            'fields': [
                'dose_era_id', 'person_id', 'drug_concept_id',
                'unit_concept_id', 'dose_value', 'dose_era_start_date',
                'dose_era_end_date'
            ],
            'cleaned_values': []
        }, {
            'fq_table_name': self.fq_table_names[1],
            'fq_sandbox_table_name': self.fq_sandbox_table_names[1],
            'loaded_ids': [1, 2, 3, 4, 5],
            'sandboxed_ids': [1, 2, 3, 4, 5],
            'fields': [
                'drug_era_id', 'person_id', 'drug_concept_id',
                'drug_era_start_date', 'drug_era_end_date',
                'drug_exposure_count', 'gap_days'
            ],
            'cleaned_values': []
        }, {
            'fq_table_name': self.fq_table_names[2],
            'fq_sandbox_table_name': self.fq_sandbox_table_names[2],
            'loaded_ids': [100, 200, 300, 400, 500],
            'sandboxed_ids': [100, 200, 300, 400, 500],
            'fields': [
                'condition_era_id', 'person_id', 'condition_concept_id',
                'condition_era_start_date', 'condition_era_end_date',
                'condition_occurrence_count'
            ],
            'cleaned_values': []
        }]

        self.default_test(tables_and_counts)