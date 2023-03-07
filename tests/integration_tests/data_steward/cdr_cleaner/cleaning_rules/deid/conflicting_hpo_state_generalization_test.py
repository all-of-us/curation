"""
 Integration test for generalization of conflicting HPO States.

 Original Issue: DC-512
 """
# Python imports
import os

# Third Party Imports
from dateutil.parser import parse

# Project Imports
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.deid.conflicting_hpo_state_generalization import ConflictingHpoStateGeneralize
from common import JINJA_ENV, OBSERVATION
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest

INSERT_RAW_DATA_OBS = JINJA_ENV.from_string("""
   INSERT INTO `{{project_id}}.{{dataset_id}}.observation` (
       observation_id,
       person_id,
       observation_concept_id,
       observation_date,
       observation_type_concept_id,
       value_as_number,
       value_as_string,
       value_as_concept_id,
       observation_source_concept_id,
       value_source_concept_id,
       value_source_value
    )
    VALUES
       (1,101,0,'2020-01-01',1,2,'',100,1585249,100,'Test Value'),
       (2,102,0,'2020-01-01',1,2,'',100,1585250,100,'Test Value'),
       (3,103,0,'2020-01-01',1,2,'',100,1585249,100,'Test Value'),
       (4,104,0,'2020-01-01',1,2,'',100,1585248,100,'Test Value')
 """)

INSERT_RAW_DATA_EXT = JINJA_ENV.from_string("""
   INSERT INTO `{{project_id}}.{{dataset_id}}.observation_ext`(
       observation_id,
       src_id
   )
   VALUES
       (1,'EHR site 119'),
       (2,'PPI/PM'),
       (3,'PPI/PM'),
       (4,'EHR site 131')
 """)


class ConflictingHpoStateGeneralizeTest(BaseTest.CleaningRulesTestBase):

    @classmethod
    def setUpClass(cls) -> None:
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        super().initialize_class_vars()

        # Set the test project identifier
        cls.project_id = os.environ.get(PROJECT_ID)

        # Set the expected test datasets
        cls.dataset_id = os.environ.get('COMBINED_DATASET_ID')
        cls.sandbox_id = f"{cls.dataset_id}_sandbox"

        cls.rule_instance = ConflictingHpoStateGeneralize(
            cls.project_id, cls.dataset_id, cls.sandbox_id)

        # Generates list of fully qualified table names and their corresponding sandbox table names
        for table in [OBSERVATION]:
            cls.fq_table_names.extend([
                f'{cls.project_id}.{cls.dataset_id}.{table}',
                f'{cls.project_id}.{cls.dataset_id}.{table}_ext'
            ])

        for table in cls.rule_instance.get_sandbox_tablenames():
            cls.fq_sandbox_table_names.append(
                f'{cls.project_id}.{cls.sandbox_id}.{table}')

        # call super to set up the client, create datasets
        super().setUpClass()

    def setUp(self):
        # Set the test project identifier
        super().setUp()

        raw_data_load_query_obs = INSERT_RAW_DATA_OBS.render(
            project_id=self.project_id, dataset_id=self.dataset_id)
        raw_data_load_query_mapping = INSERT_RAW_DATA_EXT. \
            render(project_id=self.project_id, dataset_id=self.dataset_id)

        self.date = parse('2020-01-01').date()

        # Load test data
        self.load_test_data([
            raw_data_load_query_obs,
            raw_data_load_query_mapping,
        ])

    def test_conflicting_hpo_id(self):
        """
        Tests hpo_ids except 'rdr' are updating.
        """
        # Expected results list
        tables_and_counts = [{
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{OBSERVATION}',
            'fq_sandbox_table_name':
                f'{self.project_id}.{self.sandbox_id}.{OBSERVATION}',
            'loaded_ids': [1, 2, 3, 4],
            'sandboxed_ids': [1],
            'fields': [
                'observation_id', 'person_id', 'observation_concept_id',
                'observation_date', 'observation_type_concept_id',
                'value_as_number', 'value_as_string', 'value_as_concept_id',
                'observation_source_concept_id', 'value_source_concept_id',
                'value_source_value'
            ],
            'cleaned_values': [(1, 101, 0, self.date, 1, 2, '', 2000000011,
                                1585249, 2000000011, 'Test Value'),
                               (2, 102, 0, self.date, 1, 2, '', 100, 1585250,
                                100, 'Test Value'),
                               (3, 103, 0, self.date, 1, 2, '', 100, 1585249,
                                100, 'Test Value'),
                               (4, 104, 0, self.date, 1, 2, '', 100, 1585248,
                                100, 'Test Value')]
        }]

        self.default_test(tables_and_counts)
