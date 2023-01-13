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
       value_as_concept_id,
       observation_source_concept_id,
       value_source_concept_id
       )
     VALUES
       (1,101,1001,2020-01-01,1,,1585249,),
       (2,102,1002,2020-01-01,1,100,1585250,),
       (3,103,1003,2020-01-01,1,100,1585249,),
       (4,104,1004,2020-01-01,1,100,1585248,)
 """)

INSERT_RAW_DATA_MAPPING = JINJA_ENV.from_string("""
   INSERT INTO `{{project_id}}.{{dataset_id}}._mapping_observation` (
       observation_id,
       src_hpo_id
       )
     VALUES
       (1,'hpo_100'),
       (2,'rdr'),
       (3,'rdr'),
       (4,'hpo_103')
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
                f'{cls.project_id}.{cls.dataset_id}._mapping_{table}'
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
        raw_data_load_query_mapping = INSERT_RAW_DATA_MAPPING. \
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
            'fq_table_name': f'{self.project_id}.{self.dataset_id}.{OBSERVATION}',
            'loaded_ids': [1, 2, 3, 4],
            'sandboxed_ids': [1],
            'fields': ['observation_id', 'person_id', 'observation_concept_id',
                       'observation_date', 'observation_type_concept_id',
                       'value_as_concept_id', 'observation_source_concept_id',
                       'value_source_concept_id'],
            'cleaned_values': [
                (1, 101, 1001, self.date, 1, 2000000011, 1585249, 2000000011),
                (2, 102, 1002, self.date, 1, 100, 1585249,),
                (3, 103, 1003, self.date, 1, 100, 1585249,),
                (4, 104, 1004, self.date, 1, 100, 1585248,),
            ]
        }]

        self.default_test(tables_and_counts)
