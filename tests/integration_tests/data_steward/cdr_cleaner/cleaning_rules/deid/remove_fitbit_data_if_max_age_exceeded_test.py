"""
Integration test for remove_fitbit_data_if_max_age_exceeded module

Original Issues: DC-1001, DC-1037, DC-2429, DC-2135, DC-3165

The intent is to ensure there is no data for participants over the age of 89 in
Activity Summary, Heart Rate Minute Level, Heart Rate Summary, Steps Intraday,
Sleep Daily Summary, Sleep Level, and Device tables by sandboxing the applicable records
and then dropping them.
"""

# Python Imports
import os

# Project Imports
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.deid.remove_fitbit_data_if_max_age_exceeded import RemoveFitbitDataIfMaxAgeExceeded
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest
from common import FITBIT_TABLES


class RemoveFitbitDataIfMaxAgeExceededTest(BaseTest.CleaningRulesTestBase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        super().initialize_class_vars()

        # Set the test project identifier
        project_id = os.environ.get(PROJECT_ID)
        cls.project_id = project_id

        # Set the expected test datasets
        dataset_id = os.environ.get('UNIONED_DATASET_ID')
        cls.dataset_id = dataset_id
        sandbox_id = dataset_id + '_sandbox'
        cls.sandbox_id = sandbox_id
        combined_dataset_id = os.environ.get('COMBINED_DATASET_ID')
        cls.combined_dataset_id = combined_dataset_id

        cls.kwargs.update({'combined_dataset_id': combined_dataset_id})

        cls.rule_instance = RemoveFitbitDataIfMaxAgeExceeded(
            project_id,
            dataset_id,
            sandbox_id,
            combined_dataset_id=combined_dataset_id,
        )

        # template for data that will be inserted into the FitBit tables
        cls.insert_fake_fitbit_data_tmpls = [
            cls.jinja_env.from_string("""
            INSERT INTO `{{fq_table_name}}`
            (person_id)
            VALUES
            (111), (222), (333), (444), (555), (666)
            """)
        ]

        # Generates list of fully qualified sandbox table names
        sb_table_names = cls.rule_instance.get_sandbox_tablenames()
        for table_name in sb_table_names:
            cls.fq_sandbox_table_names.append(
                f'{project_id}.{sandbox_id}.{table_name}')

        # Generates list of fully qualified table names
        cls.fq_table_names.append(f'{project_id}.{combined_dataset_id}.person')
        for table in FITBIT_TABLES:
            cls.fq_table_names.append(f'{project_id}.{dataset_id}.{table}')

        # call super to set up the client, create datasets, and create
        # empty test tables
        # NOTE:  does not create empty sandbox tables.
        super().setUpClass()

    def setUp(self):
        """
        Add data to the tables for the rule to run on
        """

        self.load_statements = []

        # Create the string(s) to load the data
        for tmpl in self.insert_fake_fitbit_data_tmpls:
            for i in range(1, len(FITBIT_TABLES) + 1):
                query = tmpl.render(fq_table_name=self.fq_table_names[i])
                self.load_statements.append(query)

        super().setUp()

    def test_max_age(self):
        """
        Tests that the specifications for SAVE_ROWS_TO_BE_DROPPED_QUERY and
        DROP_MAX_AGE_EXCEEDED_ROWS_QUERY perform as designed.

        Validates pre conditions, tests execution, and post conditions based on the load
        statements and the tables_and_counts variable.
        """

        tmpl = self.jinja_env.from_string("""
        INSERT INTO `{{fq_dataset_name}}.person`
        (person_id, gender_concept_id, year_of_birth, birth_datetime, race_concept_id, ethnicity_concept_id)
        VALUES
        (111, 0, 1900, '1900-01-01 00:00:00', 0, 0),
        (222, 0, 1910, '1910-01-01 00:00:00', 0, 0),
        (333, 0, 1920, '1920-01-01 00:00:00', 0, 0),
        (444, 0, 1951, '1951-01-01 00:00:00', 0, 0),
        (555, 0, 1951, '1951-01-01 00:00:00', 0, 0),
        (666, 0, 1931, '1931-01-01 00:00:00', 0, 0)
        """)

        query = tmpl.render(
            fq_dataset_name=f'{self.project_id}.{self.combined_dataset_id}')

        # Load person table data before the rest of the data is loaded
        # so all required columns are included
        self.load_test_data([query])

        # Iterate through and load data for all tables except for the person table
        self.load_test_data(self.load_statements)

        tables_and_counts_list = []
        for i in range(0, len(FITBIT_TABLES)):
            tables_and_counts = {
                'fq_table_name': self.fq_table_names[i + 1],
                'fq_sandbox_table_name': self.fq_sandbox_table_names[i],
                'fields': ['person_id'],
                'loaded_ids': [111, 222, 333, 444, 555, 666],
                'sandboxed_ids': [111, 222, 333, 666],
                'cleaned_values': [(444,), (555,)]
            }
            tables_and_counts_list.append(tables_and_counts)

        self.default_test(tables_and_counts_list)
