"""
Original Issues: DC-1012, DC-1514

Background
In order to avoid further changes to the standard OMOP person table, five non-standard fields will be housed in a
person_ext table.

Cleaning rule script to run AFTER deid. This needs to happen in deid_base. It depends on the cleaning rules applied in 
deid to be correctly de-identified.
This cleaning rule will populate the person_ext table
The following fields will need to be copied from the observation table:
src_id (from observation_ext, should all be “PPI/PM”)
state_of_residence_concept_id: the value_source_concept_id field in the OBSERVATION table row where
observation_source_concept_id  = 1585249 (StreetAddress_PIIState)
state_of_residence_source_value: the concept_name from the concept table for the state_of_residence_concept_id
person_id (as research_id) can be pulled from the person table
sex_at_birth_concept_id: value_as_concept_id in observation where observation_source_concept_id = 1585845
sex_at_birth_source_concept_id: value_source_concept_id in observation where observation_source_concept_id = 1585845
sex_at_birth_source_value: concept_code in the concept table where joining from observation where 
observation_source_concept_id = 1585845
"""

# Python imports
import os

# Project imports
from app_identity import get_application_id
from cdr_cleaner.cleaning_rules.create_person_ext_table import CreatePersonExtTable
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest


class CreatePersonExtTableTest(BaseTest.CleaningRulesTestBase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        super().initialize_class_vars()

        # set the test project identifier
        project_id = get_application_id()
        cls.project_id = project_id

        # set the expected test datasets
        dataset_id = os.environ.get('COMBINED_DATASET_ID')
        cls.dataset_id = dataset_id
        sandbox_id = dataset_id + '_sandbox'
        cls.sandbox_id = sandbox_id

        table_namer = 'test_table_namer'
        cls.table_namer = table_namer

        cls.rule_instance = CreatePersonExtTable(project_id, dataset_id,
                                                 sandbox_id, table_namer)

        # Generates list of fully qualified table names and their corresponding sandbox table names
        for table_name in cls.rule_instance.affected_tables:
            cls.fq_table_names.append(
                f'{cls.project_id}.{cls.dataset_id}.{table_name}')

        for table_name in [
                'observation', 'observation_ext', 'concept', 'person',
                'person_ext'
        ]:
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

        # test data for person table
        person_data_query = self.jinja_env.from_string("""
            INSERT INTO
              `{{project_id}}.{{dataset_id}}.person` 
            (person_id, gender_concept_id, year_of_birth, race_concept_id, ethnicity_concept_id)
            VALUES
              (123, 0, 1980, 0, 0),
              (345, 0, 1981, 0, 0),
              (678, 0, 1982, 0, 0),
              (910, 0, 1983, 0, 0),
              (1112, 0, 1984, 0, 0)
            """).render(project_id=self.project_id, dataset_id=self.dataset_id)

        person_ext_data_query = self.jinja_env.from_string("""
                    INSERT INTO
                      `{{project_id}}.{{dataset_id}}.person_ext` 
                    (person_id, src_id, state_of_residence_concept_id, state_of_residence_source_value, sex_at_birth_concept_id, sex_at_birth_source_concept_id, sex_at_birth_source_value)
                    VALUES
                      (123, 'PPI/PM', 0, '', 0, 0, ''),
                      (345, 'PPI/PM', 0, '', 0, 0, ''),
                      (678, 'PPI/PM', 0, '', 0, 0, ''),
                      (910, 'PPI/PM', 0, '', 0, 0, ''),
                      (1112, 'PPI/PM', 0, '', 0, 0, '')
                    """).render(project_id=self.project_id,
                                dataset_id=self.dataset_id)

        # test data for observation table
        observation_data_query = self.jinja_env.from_string("""
            INSERT INTO
              `{{project_id}}.{{dataset_id}}.observation` (observation_id,
                person_id,
                value_source_concept_id,
                value_as_concept_id,
                observation_source_concept_id,
                observation_concept_id, 
                observation_date,
                observation_type_concept_id)
            VALUES
              (111, 123, 1585266, 0, 1585249, 0, date('2021-01-01'), 0),
              (222, 345, 1585266, 0, 1585249, 0, date('2021-01-01'), 0),
              (333, 678, 1585266, 0, 1585249, 0, date('2021-01-01'), 0),
              (444, 910, 1585266, 0, 1585249, 0, date('2021-01-01'), 0),
              (1122, 123, 1585847, 45878463, 1585845, 0, date('2021-01-01'), 0),
              (3344, 345, 1585847, 45878463, 1585845, 0, date('2021-01-01'), 0),
              (5566, 678, 1585847, 45878463, 1585845, 0, date('2021-01-01'), 0),
              (7788, 910, 1585847, 45878463, 1585845, 0, date('2021-01-01'), 0),
              (9910, 1112, 1585266, 0, 1585249, 0, date('2021-01-01'), 0),
              (1112, 1112, 1585848, 45878463, 1585845, 0, date('2021-01-01'), 0)
            """).render(project_id=self.project_id, dataset_id=self.dataset_id)

        # test data for concept table
        concept_data_query = self.jinja_env.from_string("""
            INSERT INTO
              `{{project_id}}.{{dataset_id}}.concept` (
                concept_id,
                concept_code,
                concept_name,
                domain_id,
                vocabulary_id,
                concept_class_id,
                valid_start_date,
                valid_end_date)
            VALUES
              (1585266, 'PIIState_CA', 'PII State: CA', 'observation', 'PPI', 'answer', date('2017-04-24'), date('2099-12-31')),
              (1585847, 'SexAtBirth_Female', 'Female', 'observation', 'PPI', 'answer', date('2017-05-22'), date('2099-12-31'))
            """).render(project_id=self.project_id, dataset_id=self.dataset_id)

        # test data for observation_ext table
        observation_ext_data_query = self.jinja_env.from_string("""
            INSERT INTO
              `{{project_id}}.{{dataset_id}}.observation_ext` (
                observation_id,
                src_id
              )
            VALUES
              (111, 'PPI/PM'),
              (222, 'PPI/PM'),
              (333, 'PPI/PM'),
              (444, 'PPI/PM'),
              (9910, 'PPI/PM')
                    """).render(project_id=self.project_id,
                                dataset_id=self.dataset_id)

        self.load_test_data([
            person_data_query, person_ext_data_query, observation_data_query,
            concept_data_query, observation_ext_data_query
        ])

    def test_person_ext_creation(self):
        """
        Tests that the person_ext table is created as designed.

        Validates pre conditions, tests execution, and post conditions based on the load
        statements and the tables_and_counts variable.
        """

        tables_and_counts = [{
            'fq_table_name':
                '.'.join([self.fq_dataset_name, 'person_ext']),
            'fields': [
                'person_id', 'src_id', 'state_of_residence_concept_id',
                'state_of_residence_source_value', 'sex_at_birth_concept_id',
                'sex_at_birth_source_concept_id', 'sex_at_birth_source_value'
            ],
            'loaded_ids': [123, 345, 678, 910, 1112],
            'cleaned_values': [(123, 'PPI/PM', 1585266, 'PII State: CA',
                                45878463, 1585847, 'SexAtBirth_Female'),
                               (345, 'PPI/PM', 1585266, 'PII State: CA',
                                45878463, 1585847, 'SexAtBirth_Female'),
                               (678, 'PPI/PM', 1585266, 'PII State: CA',
                                45878463, 1585847, 'SexAtBirth_Female'),
                               (910, 'PPI/PM', 1585266, 'PII State: CA',
                                45878463, 1585847, 'SexAtBirth_Female'),
                               (1112, 'PPI/PM', 1585266, 'PII State: CA',
                                45878463, 1585848, 'No matching concept')]
        }]

        self.default_test(tables_and_counts)
