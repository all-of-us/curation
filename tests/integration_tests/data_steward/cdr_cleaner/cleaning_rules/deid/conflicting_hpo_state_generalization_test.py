"""
 Integration test for generalization of conflicting HPO States.

 Original Issue: DC-512, DC3268
 """
# Python imports
import mock
import os

# Third Party Imports
from dateutil.parser import parse

# Project Imports
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.deid.conflicting_hpo_state_generalization import (
    ConflictingHpoStateGeneralize, MAP_TABLE_NAME)
from common import EXT_SUFFIX, JINJA_ENV, OBSERVATION, SITE_MASKING_TABLE_ID
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
        value_source_concept_id,
        value_source_value
    )
    VALUES
    -- person_id 1 answered that she lives in Alabama. -- 
    -- And all the HPO records come from a HPO site in Alabama. --
    -- Nothing happens to person_id 1 --
        (101, 1, 0, '2020-01-01', 1, 999, 1585249, 1585261, 'PIIState_AL'),
        (102, 1, 0, '2020-01-01', 1, 999, 1500000, 9999999, 'Dummy'),
        (103, 1, 0, '2020-01-01', 1, 999, 1500000, 9999999, 'Dummy'),
    -- person_id 2 answered that she lives in Alabama -- 
    -- And all the HPO records come from HPO sites in Alabama --
    -- Nothing happens to person_id 2 --
        (201, 2, 0, '2020-01-01', 1, 999, 1585249, 1585261, 'PIIState_AL'),
        (202, 2, 0, '2020-01-01', 1, 999, 1500000, 9999999, 'Dummy'),
        (203, 2, 0, '2020-01-01', 1, 999, 1500000, 9999999, 'Dummy'),
    -- person_id 3 answered that she lives in Alabama. -- 
    -- But all the HPO records come from a HPO site in Arizona. --
    -- State info will be generalized for person_id 3 --
        (301, 3, 0, '2020-01-01', 1, 999, 1585249, 1585261, 'PIIState_AL'),
        (302, 3, 0, '2020-01-01', 1, 999, 1500000, 9999999, 'Dummy'),
        (303, 3, 0, '2020-01-01', 1, 999, 1500000, 9999999, 'Dummy'),
    -- person_id 4 answered that she lives in Alabama. -- 
    -- But one of the HPO records come from a HPO site in Arizona. --
    -- State info will be generalized for person_id 4 --
        (401, 4, 0, '2020-01-01', 1, 999, 1585249, 1585261, 'PIIState_AL'),
        (402, 4, 0, '2020-01-01', 1, 999, 1500000, 9999999, 'Dummy'),
        (403, 4, 0, '2020-01-01', 1, 999, 1500000, 9999999, 'Dummy')
 """)

INSERT_RAW_DATA_EXT = JINJA_ENV.from_string("""
    INSERT INTO `{{project_id}}.{{dataset_id}}.observation_ext`
        (observation_id, src_id)
    VALUES
        (101, 'Portal1'), (102, 'bar 001'), (103, 'bar 001'),
        (201, 'Portal2'), (202, 'bar 001'), (203, 'bar 002'),
        (301, 'Portal3'), (302, 'bar 003'), (303, 'bar 003'),
        (401, 'Portal4'), (402, 'bar 001'), (403, 'bar 003')
 """)

INSERT_TEMP_MASK_TABLE = JINJA_ENV.from_string("""
    INSERT INTO `{{project_id}}.{{dataset_id}}.site_maskings`
        (hpo_id, src_id, state, value_source_concept_id)
    VALUES
        ('hpo site in Alabama 1', 'bar 001', 'PIIState_AL', 1585261),
        ('hpo site in Alabama 2', 'bar 002', 'PIIState_AL', 1585261),
        ('hpo site in Arizona 3', 'bar 003', 'PIIState_AZ', 1585264)
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
        for table in [
                OBSERVATION, f'{OBSERVATION}{EXT_SUFFIX}', SITE_MASKING_TABLE_ID
        ]:
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

        raw_data_load_query_obs = INSERT_RAW_DATA_OBS.render(
            project_id=self.project_id, dataset_id=self.dataset_id)

        raw_data_load_query_mapping = INSERT_RAW_DATA_EXT.render(
            project_id=self.project_id, dataset_id=self.dataset_id)

        # The location of the table will be mocked in the test
        temp_mask_query = INSERT_TEMP_MASK_TABLE.render(
            project_id=self.project_id, dataset_id=self.dataset_id)

        self.date = parse('2020-01-01').date()

        # Load test data
        self.load_test_data([
            raw_data_load_query_obs, raw_data_load_query_mapping,
            temp_mask_query
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
                f'{self.project_id}.{self.sandbox_id}.{self.rule_instance.sandbox_table_for(OBSERVATION)}',
            # The following tables are created when `setup_rule` runs,
            # so this will break the sandboxing check that runs in 'default_test()'
            # We get around the check by declaring these tables are created before
            # the rule runs and this is expected.
            'tables_created_on_setup': [
                f'{self.project_id}.{self.sandbox_id}.{MAP_TABLE_NAME}'
            ],
            'loaded_ids': [
                101, 102, 103, 201, 202, 203, 301, 302, 303, 401, 402, 403
            ],
            'sandboxed_ids': [301, 401],
            'fields': [
                'observation_id', 'person_id', 'value_as_concept_id',
                'observation_source_concept_id', 'value_source_concept_id'
            ],
            'cleaned_values': [(101, 1, 999, 1585249, 1585261),
                               (102, 1, 999, 1500000, 9999999),
                               (103, 1, 999, 1500000, 9999999),
                               (201, 2, 999, 1585249, 1585261),
                               (202, 2, 999, 1500000, 9999999),
                               (203, 2, 999, 1500000, 9999999),
                               (301, 3, 2000000011, 1585249, 2000000011),
                               (302, 3, 999, 1500000, 9999999),
                               (303, 3, 999, 1500000, 9999999),
                               (401, 4, 2000000011, 1585249, 2000000011),
                               (402, 4, 999, 1500000, 9999999),
                               (403, 4, 999, 1500000, 9999999)]
        }]

        # mock the PIPELINE_TABLES variable so tests on different branches
        # don't overwrite each other.
        with mock.patch(
                'cdr_cleaner.cleaning_rules.deid.conflicting_hpo_state_generalization.PIPELINE_TABLES',
                self.dataset_id):
            self.default_test(tables_and_counts)

        self.assertTableValuesMatch(
            f'{self.project_id}.{self.sandbox_id}.{MAP_TABLE_NAME}',
            ['person_id', 'src_id'], [(1, 'bar 001'), (2, 'bar 001'),
                                      (2, 'bar 002'), (3, 'bar 003'),
                                      (4, 'bar 001'), (4, 'bar 003')])

        self.assertTableValuesMatch(
            f'{self.project_id}.{self.sandbox_id}.{self.rule_instance.sandbox_table_for(f"{OBSERVATION}_identifier")}',
            [
                'observation_id', 'person_id', 'src_id',
                'value_source_concept_id'
            ], [(301, 3, 'bar 003', 1585261), (401, 4, 'bar 003', 1585261)])
