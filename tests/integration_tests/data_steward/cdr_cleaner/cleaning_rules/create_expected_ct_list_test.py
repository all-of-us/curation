from datetime import datetime
import os

import mock
from dateutil import parser

from app_identity import get_application_id
from common import (OBSERVATION, PERSON, PRIMARY_PID_RID_MAPPING,
                    VOCABULARY_TABLES, AIAN_LIST)
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest
from cdr_cleaner.cleaning_rules.create_expected_ct_list import StoreExpectedCTList
from cdr_cleaner.cleaning_rules.create_expected_ct_list import EXPECTED_CT_LIST


class StoreExpectedCTListTest(BaseTest.CleaningRulesTestBase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        super().initialize_class_vars()

        # set the test project identifier
        cls.project_id = get_application_id()

        # set the expected test datasets
        cls.dataset_id = os.getenv('RDR_DATASET_ID')
        cls.sandbox_id = f'{cls.dataset_id}_sandbox'
        cls.vocabulary_dataset = os.getenv('VOCABULARY_DATASET')

        cls.rule_instance = StoreExpectedCTList(cls.project_id, cls.dataset_id,
                                                cls.sandbox_id)

        sb_table_names = cls.rule_instance.get_sandbox_tablenames()
        for table_name in sb_table_names:
            cls.fq_sandbox_table_names.append(
                f'{cls.project_id}.{cls.sandbox_id}.{table_name}')

        # This will normally be the PIPELINE_TABLES dataset, but is being
        # mocked for this test
        cls.stable_map_name = f'{cls.project_id}.{cls.dataset_id}.{PRIMARY_PID_RID_MAPPING}'
        cls.fq_table_names = [
            f'{cls.project_id}.{cls.dataset_id}.{table}'
            for table in [OBSERVATION, PERSON, PRIMARY_PID_RID_MAPPING] +
            VOCABULARY_TABLES
        ]

        cls.fq_table_names.extend(
            [f'{cls.project_id}.{cls.dataset_id}_sandbox.{AIAN_LIST}'])

        # call super to set up the client, create datasets, and create
        # empty test tables
        # NOTE:  does not create empty sandbox tables.

        super().setUpClass()

    def setUp(self):
        super().setUp()
        self.copy_vocab_tables(self.vocabulary_dataset)
        # pre-conditions
        person_tmpl = self.jinja_env.from_string("""
            INSERT INTO `{{project}}.{{dataset}}.{{table}}`
                (person_id, year_of_birth,
                gender_concept_id, race_concept_id, ethnicity_concept_id)
            VALUES
                -- should not be included in sandbox table because of bad birth year --
                (100, 1799, 5, 5, 5),
                -- should be included --
                (200, 1801, 5, 5, 5),
                -- should not be included in sandbox table because of bad birth year --
                (300, 2020, 5, 5, 5),
                -- should be included --
                (400, 2000, 5, 5, 5),
                -- should be included.  added for ai/an regression checks --
                (500, 1990, 5, 5, 5),
                (600, 1980, 5, 5, 5),
                -- should not be included in sandbox table for not having the basics --
                (700, 1985, 5, 5, 5),
                -- included for having the basics --
                (800, 1985, 5, 5, 5)
        """).render(project=self.project_id,
                    dataset=self.dataset_id,
                    table=PERSON)

        primary_map_tmpl = self.jinja_env.from_string("""
        INSERT INTO `{{project}}.{{dataset}}.{{table}}`
            (person_id, research_id, shift, import_date)
        VALUES
            (100, 90, 50, '2020-01-01'),
            (200, 80, 40, '2020-01-01'),
            (300, 70, 30, '2020-01-01'),
            (400, 60, 20, '2020-01-01'),
            (500, 50, 10, '2020-01-01'),
            (600, 40, 60, '2020-01-01'),
            (700, 30, 70, '2020-01-01'),
            (800, 20, 80, '2020-01-01')
        """).render(project=self.project_id,
                    dataset=self.dataset_id,
                    table=PRIMARY_PID_RID_MAPPING)

        observation_tmpl = self.jinja_env.from_string("""
        INSERT INTO `{{project}}.{{dataset}}.{{table}}`
          (observation_id, person_id, observation_source_concept_id, value_source_concept_id,
          observation_concept_id, observation_date, observation_type_concept_id)
        VALUES
          -- should be included in sandbox table.  ai/an regression check --
          (10, 500, 1586140, 1586141, 1586140, '1900-01-01', 0),
          (20, 500, 1586140, 1586147, 1586140, '1900-01-01', 0),
          -- include because not ai/an --
          (30, 600, 1586140, 1586145, 1586140, '1900-01-01', 0),
          -- should not be included in sandbox table.  does not have the basics --
          (40, 700, 1000, 0, 1000, '1900-01-01', 0),
          -- does have the basics --
          (50, 800, 1585838, 1585840, 1585838, '1900-01-01', 0),
          (60, 200, 1585838, 1585840, 1585838, '1900-01-01', 0),
          (70, 400, 1585838, 1585840, 1585838, '1900-01-01', 0)
        """).render(project=self.project_id,
                    dataset=self.dataset_id,
                    table=OBSERVATION)

        # a list of PID's that are aian
        aian_tmpl = self.jinja_env.from_string("""
        INSERT INTO `{{project}}.{{dataset}}.{{table}}`
          (person_id)
        VALUES
          (500)
        """).render(
            project=self.project_id,
            dataset=self.sandbox_id,
            #dataset=self.dataset_id,
            table=AIAN_LIST,
        )

        queries = [observation_tmpl, person_tmpl, primary_map_tmpl, aian_tmpl]
        self.load_test_data(queries)

    def test_store_expected_ct_list(self):
        self.maxDiff = None
        obs_date = parser.parse('1900-01-01').date()
        tables_and_counts = [
            {
                'fq_table_name':
                    f'{self.project_id}.{self.dataset_id}.{PERSON}',
                'fields': [
                    'person_id', 'year_of_birth', 'gender_concept_id',
                    'race_concept_id', 'ethnicity_concept_id'
                ],
                'loaded_ids': [100, 200, 300, 400, 500, 600, 700, 800],
                'cleaned_values': [
                    # No changes should be made
                    (100, 1799, 5, 5, 5),
                    (200, 1801, 5, 5, 5),
                    (300, 2020, 5, 5, 5),
                    (400, 2000, 5, 5, 5),
                    (500, 1990, 5, 5, 5),
                    (600, 1980, 5, 5, 5),
                    (700, 1985, 5, 5, 5),
                    (800, 1985, 5, 5, 5),
                ]
            },
            {
                'fq_table_name':
                    f'{self.project_id}.{self.sandbox_id}.{AIAN_LIST}',
                'fields': ['person_id'],
                'loaded_ids': [500,],
                'cleaned_values': [(500,),],
                #'sandbox_fields': ['research_id'],
                #'sandboxed_ids': [80, 60, 50, 40, 20]
            },
            {
                'fq_table_name':
                    f'{self.project_id}.{self.dataset_id}.{OBSERVATION}',
                'fields': [
                    'observation_id', 'person_id',
                    'observation_source_concept_id', 'value_source_concept_id',
                    'observation_concept_id', 'observation_date',
                    'observation_type_concept_id'
                ],
                'loaded_ids': [10, 20, 30, 40, 50, 60, 70],
                'cleaned_values': [
                    # No changes should be made
                    (10, 500, 1586140, 1586141, 1586140, obs_date, 0),
                    (20, 500, 1586140, 1586147, 1586140, obs_date, 0),
                    (30, 600, 1586140, 1586145, 1586140, obs_date, 0),
                    (40, 700, 1000, 0, 1000, obs_date, 0),
                    (50, 800, 1585838, 1585840, 1585838, obs_date, 0),
                    (60, 200, 1585838, 1585840, 1585838, obs_date, 0),
                    (70, 400, 1585838, 1585840, 1585838, obs_date, 0)
                ],  # verifying the correct fields and data are sandboxed here
                'fq_sandbox_table_name':
                    self.fq_sandbox_table_names[0],
                'sandbox_fields': ['research_id'],
                'sandboxed_ids': [80, 60, 50, 40, 20]
            }
        ]

        # mock the PIPELINE_TABLES variable so tests on different branches
        # don't overwrite each other.  Just changes the value of the string variable
        with mock.patch(
                'cdr_cleaner.cleaning_rules.create_expected_ct_list.PIPELINE_TABLES',
                self.dataset_id):
            self.default_test(tables_and_counts)
