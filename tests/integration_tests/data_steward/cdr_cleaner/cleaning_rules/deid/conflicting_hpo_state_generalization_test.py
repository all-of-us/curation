"""
 Integration test for generalization of conflicting HPO States.

 Original Issue: DC-512
 """
# Python imports
import os

# Third Party Imports

# Project Imports
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.deid.conflicting_hpo_state_generalization import ConflictingHpoStateGeneralize
from common import JINJA_ENV, OBSERVATION, VOCABULARY_TABLES
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest

INSERT_RAW_DATA_OBS = JINJA_ENV.from_string("""
   INSERT INTO `{{project_id}}.{{dataset_id}}.observation` (
       observation_id,
       person_id,
       observation_concept_id,
       observation_date,
       observation_type_concept_id
       )
     VALUES
       (1,101,1001,date('2020-01-01'),1),
       (2,102,1002,date('2020-01-01'),1),
       (3,103,1003,date('2020-01-01'),1),
       (4,104,1004,date('2020-01-01'),1)
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
        cls.dataset_id = os.environ.get('UNIONED_DATASET_ID')
        cls.sandbox_id = f"{cls.dataset_id}_sandbox"
        cls.vocabulary_id = os.environ.get('VOCABULARY_DATASET')

        cls.rule_instance = ConflictingHpoStateGeneralize(
            cls.project_id, cls.dataset_id, cls.sandbox_id)

        # Generates list of fully qualified table names and their corresponding sandbox table names
        cls.fq_table_names.append(
            f'{cls.project_id}.{cls.dataset_id}.{OBSERVATION}')
        for table in VOCABULARY_TABLES:
            cls.fq_table_names.append(
                f'{cls.project_id}.{cls.dataset_id}.{table}')

        for table in cls.rule_instance.get_sandbox_tablenames():
            cls.fq_sandbox_table_names.append(
                f'{cls.project_id}.{cls.sandbox_id}.{table}')

        # call super to set up the client, create datasets
        super().setUpClass()

    def setUp(self):
        # Set the test project identifier
        super().setUp()
        self.copy_vocab_tables(self.vocabulary_id)
        raw_data_load_query_obs = INSERT_RAW_DATA_OBS.render(
            project_id=self.project_id, dataset_id=self.dataset_id)
        raw_data_load_query_mapping = INSERT_RAW_DATA_MAPPING. \
            render(project_id=self.project_id, dataset_id=self.dataset_id)

        # Load test data
        self.load_test_data([
            f'{raw_data_load_query_obs}',
            f'{raw_data_load_query_mapping}',
        ])

    def test_conflicting_hpo_id(self):
        """
        Tests hpo_ids except 'rdr' are updating.
        """
        # Expected results list
        tables_and_counts = [{
            'fq_table_name':
                '.'.join([
                    f'{self.project_id}.{self.dataset_id}.{OBSERVATION}',
                    f'{self.project_id}.{self.dataset_id}._mapping_{OBSERVATION}',
                ]),
            'loaded_ids': [1, 2, 3, 4],
            'sandboxed_ids': [1, 4],
            'fields': ['person_id', 'src_hpo_id'],
            'cleaned_values': [(101, 'hpo_100'), (104, 'hpo_103')]
        }]

        self.default_test(tables_and_counts)
