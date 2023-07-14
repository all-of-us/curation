# Python imports
import os

# Project imports
import common
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.person_id_validator import PersonIdValidation
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest


class PersonIdValidationTest(BaseTest.CleaningRulesTestBase):

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
        cls.rule_instance = PersonIdValidation(cls.project_id, cls.dataset_id,
                                               cls.sandbox_id)

        cls.affected_tables = [common.PERSON, common.OBSERVATION]
        supporting_tables = ['observation_ext']
        # Generates list of fully qualified table names and their corresponding sandbox table names
        cls.fq_table_names = [
            f"{cls.project_id}.{cls.dataset_id}.{table}"
            for table in cls.affected_tables + supporting_tables
        ]

        cls.fq_sandbox_table_names = [
            f'{cls.project_id}.{cls.sandbox_id}.{table}'
            for table in cls.rule_instance.get_sandbox_tablenames()
        ]

        # call super to set up the client, create datasets
        cls.up_class = super().setUpClass()

    def setUp(self):
        # create tables
        super().setUp()

        # Person 1:  has EHR consent and exists in Person table
        # Person 2: does not have EHR consent and exists in Person table
        # Person 3: does not exist in the Person table
        person_tmpl = self.jinja_env.from_string("""
            INSERT INTO `{{project_id}}.{{dataset_id}}.person`
                (person_id, gender_concept_id, year_of_birth, race_concept_id, ethnicity_concept_id)
            VALUES 
                (1, 0, 2000, 0, 0),
                (2, 0, 2001, 0, 0)
            """)
        observation_tmpl = self.jinja_env.from_string("""
            INSERT INTO `{{project_id}}.{{dataset_id}}.observation`
                (observation_id, person_id, observation_concept_id, observation_date,
                observation_type_concept_id, observation_source_value, value_source_concept_id)
            VALUES
                (100, 1, 0, '2021-01-01', 0, 'EHRConsentPII_ConsentPermission', 1586100),
                (101, 1, 0, '2020-01-01', 0, 'EHRConsentPII_ConsentPermission', 1586102),
                (102, 1, 0, '2022-01-01', 0, 'foo', 9999),
                (201, 2, 0, '2021-01-01', 0, 'EHRConsentPII_ConsentPermission', 1586103),
                (202, 2, 0, '2020-01-01', 0, 'EHRConsentPII_ConsentPermission', 1586100),
                (203, 2, 0, '2021-01-01', 0, 'foo', 9999), -- this record gets dropped --
                -- all person 3's records get dropped --
                (301, 3, 0, '2021-01-01', 0, 'foo', 9999),
                (302, 3, 0, '2021-01-01', 0, 'EHRConsentPII_ConsentPermission', 1586100)
            """)
        mapping_observation_tmpl = self.jinja_env.from_string("""
            INSERT INTO `{{project_id}}.{{dataset_id}}.observation_ext`
                (observation_id, src_id, survey_version_concept_id)
            VALUES
                (100, 'Portal 1', null),
                (101, 'Portal 2', null),
                (102, 'site bar', null),
                (201, 'Portal 3', null),
                (202, 'Portal 4', null),
                (203, 'site baz', null),
                (301, 'site raz', null),
                (302, 'Portal 5', null)
            """)

        observation_query = observation_tmpl.render(project_id=self.project_id,
                                                    dataset_id=self.dataset_id)
        mapping_observation_query = mapping_observation_tmpl.render(
            project_id=self.project_id, dataset_id=self.dataset_id)
        person_query = person_tmpl.render(project_id=self.project_id,
                                          dataset_id=self.dataset_id)

        # load the test data
        self.load_test_data(
            [observation_query, mapping_observation_query, person_query])

        # run rule setup function now that table data is loaded
        self.rule_instance.setup_rule(self.client)

    def test_queries(self):
        """
        Validates pre-conditions, test execution and post conditions based on the tables_and_counts variable.
        """
        tables_and_counts = [{
            'name': common.PERSON,
            'fq_table_name': self.fq_table_names[0],
            'fields': ['person_id'],
            'loaded_ids': [1, 2],
            'cleaned_values': [(2,), (1,)]
        }, {
            'name': common.OBSERVATION,
            'fq_table_name': self.fq_table_names[1],
            'fields': ['observation_id', 'person_id'],
            'loaded_ids': [100, 101, 102, 201, 202, 203, 301, 302],
            'cleaned_values': [(100, 1), (101, 1), (102, 1), (201, 2), (202, 2)]
        }]

        self.default_test(tables_and_counts)
