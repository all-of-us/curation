"""
Integration test to ensure the identifying fields are accurately suppressed in the
identifying_field_suppression.py module

Original Issue: DC-1372

To ensure participant privacy, curation will null the data in identifying fields.  These fields will be
nulled/de-identified regardless of the table the column exists in.

For all OMOP common data model tables, null or otherwise de-identify the following fields, if they exist in the table.

Fields:
month_of_birth
day_of_birth
location_id
provider_id
care_site_id

person_source_value, value_source_value, and value_as_string: these fields will be caught by the rule implemented for
DC-1369

For NULLABLE fields, use the NULL value.
For REQUIRED fields:
if numeric, use zero, 0.
if varchar/character/string, use empty string , ''.
If using a DML statement, sandboxing is not required.

Should be added to list of CONTROLLED_TIER_DEID_CLEANING_CLASSES in data_steward/cdr_cleaner/clean_cdr.py
Should occur after data remapping rules.

Should not be applied to mapping tables or other non-OMOP tables.
"""

# Python imports
import os

# Project imports
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.identifying_field_suppression import IDFieldSuppression
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest


class IDFieldSuppressionTest(BaseTest.CleaningRulesTestBase):

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
        dataset_id = os.environ.get('COMBINED_DATASET_ID')
        cls.dataset_id = dataset_id
        sandbox_id = dataset_id + '_sandbox'
        cls.sandbox_id = sandbox_id

        cls.rule_instance = IDFieldSuppression(project_id, dataset_id,
                                               sandbox_id)

        # Generates list of fully qualified table names and their corresponding sandbox table names
        for table_name in cls.rule_instance.affected_tables:
            cls.fq_table_names.append(
                f'{cls.project_id}.{cls.dataset_id}.{table_name}')

        # call super to set up the client, create datasets, and create
        # empty test tables
        # NOTE:  does not create empty sandbox tables.
        super().setUpClass()

    def setUp(self):
        """
        Create common information for tests.

        Creates common expected parameter types from cleaned tables and a common fully qualified (fq)
        dataset name string used to load the data.

        Creates tables with test data for the rule to run on
        """
        fq_dataset_name = self.fq_table_names[0].split('.')
        self.fq_dataset_name = '.'.join(fq_dataset_name[:-1])

        super().setUp()

        # test data for measurement table, identifying fields: provider_id
        measurement_data_query = self.jinja_env.from_string("""
            INSERT INTO `{{project_id}}.{{dataset_id}}.measurement` (
                measurement_id,
                person_id,
                measurement_concept_id,
                measurement_type_concept_id,
                provider_id,
                measurement_date)
            VALUES
              (321, 12345, 111, 444, 789, '2020-01-01'),
              (123, 6789, 222, 555, 1011, '2020-01-01')
            """).render(project_id=self.project_id, dataset_id=self.dataset_id)

        # test data for person table, identifying fields:
        # month_of_birth, day_of_birth, location_id, provider_id, care_site_id
        person_data_query = self.jinja_env.from_string("""
            INSERT INTO
              `{{project_id}}.{{dataset_id}}.person` (person_id,
                gender_concept_id,
                year_of_birth,
                month_of_birth,
                day_of_birth,
                location_id,
                provider_id,
                care_site_id,
                race_concept_id,
                ethnicity_concept_id)
            VALUES
              (12345, 1, 1990, 12, 29, 22, 33, 44, 0, 0),
              (6789, 2, 1980, 11, 20, 40, 50, 60, 0, 0)
            """).render(project_id=self.project_id, dataset_id=self.dataset_id)

        # test data for fact_relationship table, no identifying fields contained in table
        fact_relationship_data_query = self.jinja_env.from_string("""
            INSERT INTO
              `{{project_id}}.{{dataset_id}}.fact_relationship` (domain_concept_id_1,
                fact_id_1,
                domain_concept_id_2,
                fact_id_2,
                relationship_concept_id)
            VALUES
              (12345, 1111, 9101112, 131415, 161718),
              (6789, 2222, 891011, 121314, 151617)
            """).render(project_id=self.project_id, dataset_id=self.dataset_id)

        self.load_test_data([
            measurement_data_query, person_data_query,
            fact_relationship_data_query
        ])

    def test_identifying_field_suppression(self):
        """
        Tests that the specifications for SANDBOX_RECORDS_QUERY and DROP_RECORDS_QUERY perform as designed.

        Validates pre conditions, tests execution, and post conditions based on the load
        statements and the tables_and_counts variable.
        """

        tables_and_counts = [
            {
                'fq_table_name':
                    '.'.join([self.fq_dataset_name, 'measurement']),
                'fields': [
                    'measurement_id', 'person_id', 'measurement_concept_id',
                    'measurement_type_concept_id', 'provider_id'
                ],
                'loaded_ids': [321, 123],
                'cleaned_values': [(321, 12345, 111, 444, None),
                                   (123, 6789, 222, 555, None)]
            },
            {
                'fq_table_name':
                    '.'.join([self.fq_dataset_name, 'person']),
                'fields': [
                    'person_id', 'gender_concept_id', 'year_of_birth',
                    'month_of_birth', 'day_of_birth', 'location_id',
                    'provider_id', 'care_site_id'
                ],
                'loaded_ids': [12345, 6789],
                'cleaned_values': [
                    (12345, 1, 1990, None, None, None, None, None),
                    (6789, 2, 1980, None, None, None, None, None)
                ]
            },
            # Should remain the same since death table does not contain any identifying fields
            {
                'fq_table_name':
                    '.'.join([self.fq_dataset_name, 'fact_relationship']),
                'fields': [
                    'domain_concept_id_1', 'fact_id_1', 'domain_concept_id_2',
                    'fact_id_2', 'relationship_concept_id'
                ],
                'loaded_ids': [12345, 6789],
                'cleaned_values': [(12345, 1111, 9101112, 131415, 161718),
                                   (6789, 2222, 891011, 121314, 151617)]
            }
        ]

        self.default_test(tables_and_counts)
