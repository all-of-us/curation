"""
Integration test for null_person_birthdate module

In the person table, the fields month_of_birth, day_of_birth, and birth_datetime should be nulled.
The year_of_birth field should remain unchanged.

Original Issue: DC-1356
"""

# Python Imports
import os

#Project imports
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.null_person_birthdate import NullPersonBirthdate
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest


class NullPersonBirthdateTest(BaseTest.CleaningRulesTestBase):

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

        cls.rule_instance = NullPersonBirthdate(project_id, dataset_id,
                                                sandbox_id)

        cls.fq_table_names = [f'{project_id}.{dataset_id}.person']

        # call super to set up the client, create datasets, and create
        # empty test tables
        # NOTE:  does not create empty sandbox tables.
        super().setUpClass()

    def setUp(self):
        fq_dataset_name = self.fq_table_names[0].split('.')
        self.fq_dataset_name = '.'.join(fq_dataset_name[:-1])

        super().setUp()

    def test_null_person_birthdate_cleaning(self):
        """
        Tests that the sepcifications for NULL_DATE_QUERY perform as designed.

        Validates pre conditions, tests execution, and post conditions based on the load
        statements and the tables_and_counts variable.        
        """
        queries = []

        birthdate_field_values_tmpl = self.jinja_env.from_string("""
            INSERT INTO `{{fq_dataset_name}}.person`
            (person_id, gender_concept_id, year_of_birth, month_of_birth, day_of_birth,
             birth_datetime, race_concept_id, ethnicity_concept_id)
            VALUES
                (1001, 1001, 2020, 8, 30, timestamp('2020-08-30 11:00:00'), 1001,1001),
                (1002, 1002, 2020, 8, 30, timestamp('2020-08-30 11:00:00'), 1002,1002),
                (1003, 1003, 2020, 8, 30, NULL,  1003,1003),
                (1004, 1004, 2020, 8, 30, timestamp('2020-08-30 11:00:00'), 1004,1004),
                (1005, 1005, 2020, 8, NULL, timestamp('2020-08-30 11:00:00'), 1005,1005),
                (1006, 1006, 2020, 8, 30, timestamp('2020-08-30 11:00:00'),1006,1006),
                (1007, 1007, 2020, NULL, 30, timestamp('2020-08-30 11:00:00'), 1007,1007),
                (1008, 1008, 2020, 8, 30, timestamp('2020-08-30 11:00:00'), 1008,1008),
                (1009, 1009, 2020, 8, 30, timestamp('2020-08-30 11:00:00'), 1009,1009)
            """).render(fq_dataset_name=self.fq_dataset_name)
        queries.append(birthdate_field_values_tmpl)

        self.load_test_data(queries)

        tables_and_counts = [{
            'fq_table_name':
                '.'.join([self.fq_dataset_name, 'person']),
            'fq_sandbox_table_name':
                '',
            'loaded_ids': [
                1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009
            ],
            'sandboxed_ids': [],
            'fields': [
                'person_id', 'gender_concept_id', 'year_of_birth',
                'month_of_birth', 'day_of_birth', 'birth_datetime',
                'race_concept_id', 'ethnicity_concept_id'
            ],
            'cleaned_values': [(1001, 1001, 2020, None, None, None, 1001, 1001),
                               (1002, 1002, 2020, None, None, None, 1002, 1002),
                               (1003, 1003, 2020, None, None, None, 1003, 1003),
                               (1004, 1004, 2020, None, None, None, 1004, 1004),
                               (1005, 1005, 2020, None, None, None, 1005, 1005),
                               (1006, 1006, 2020, None, None, None, 1006, 1006),
                               (1007, 1007, 2020, None, None, None, 1007, 1007),
                               (1008, 1008, 2020, None, None, None, 1008, 1008),
                               (1009, 1009, 2020, None, None, None, 1009, 1009)]
        }]

        self.default_test(tables_and_counts)
