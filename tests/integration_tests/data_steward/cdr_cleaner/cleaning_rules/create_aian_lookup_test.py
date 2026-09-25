"""
Integration test for CreateAIANLookup.
"""
import os

from app_identity import get_application_id
from common import OBSERVATION
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest
from cdr_cleaner.cleaning_rules.create_aian_lookup import CreateAIANLookup


class CreateAIANLookupTest(BaseTest.CleaningRulesTestBase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        super().initialize_class_vars()

        cls.project_id = get_application_id()
        cls.dataset_id = os.getenv('RDR_DATASET_ID')
        cls.sandbox_id = f'{cls.dataset_id}_sandbox'

        cls.rule_instance = CreateAIANLookup(cls.project_id, cls.dataset_id,
                                             cls.sandbox_id)

        sb_table_names = cls.rule_instance.get_sandbox_tablenames()
        for table_name in sb_table_names:
            cls.fq_sandbox_table_names.append(
                f'{cls.project_id}.{cls.sandbox_id}.{table_name}')

        cls.fq_table_names = [
            f'{cls.project_id}.{cls.dataset_id}.{OBSERVATION}'
        ]

        super().setUpClass()

    def setUp(self):
        super().setUp()

        observation_tmpl = self.jinja_env.from_string("""
        INSERT INTO `{{project}}.{{dataset}}.{{table}}`
          (observation_id, person_id, observation_source_concept_id, value_source_concept_id,
          observation_source_value, observation_concept_id, observation_date,
          observation_type_concept_id)
        VALUES
          -- Meets the AIAN criteria --
          (101, 11, 1586140, 1586141, NULL, 0, '2000-01-01', 0),
          (102, 12, 1586150, 0, NULL, 0, '2000-01-01', 0),
          (103, 13, 1585599, 0, NULL, 0, '2000-01-01', 0),
          (104, 14, 1586139, 0, NULL, 0, '2000-01-01', 0),
          (105, 15, 1585604, 0, NULL, 0, '2000-01-01', 0),
          -- Pediatric rows carry observation_source_concept_id 0. The mixed case --
          -- on 301 checks the match is case-insensitive. --
          -- Race question answered AIAN --
          (301, 31, 0, 1586141, 'Race_WhatRaceEthnicity_Ped', 0, '2000-01-01', 0),
          -- Follow-up question answered with a skip code --
          (302, 32, 0, 903096, 'aian_aianspecific_ped', 0, '2000-01-01', 0),
          -- Follow-up question answered with 0 --
          (303, 33, 0, 0, 'aian_tribe_ped', 0, '2000-01-01', 0),
          -- Matched by an adult and a pediatric branch, must appear once --
          (305, 35, 1586150, 0, NULL, 0, '2000-01-01', 0),
          (306, 35, 0, 903096, 'aiannoneofthesedescribeme_aianfreetext_ped', 0, '2000-01-01', 0),
          -- Not meet the AIAN criteria --
          (201, 21, 1586140, 0, NULL, 0, '2000-01-01', 0),
          (202, 22, 0, 1586141, NULL, 0, '2000-01-01', 0),
          (203, 23, 0, 0, NULL, 0, '2000-01-01', 0),
          -- Pediatric race question answered with a non-AIAN race --
          (304, 34, 0, 1586142, 'race_whatraceethnicity_ped', 0, '2000-01-01', 0)
        """).render(project=self.project_id,
                    dataset=self.dataset_id,
                    table=OBSERVATION)

        queries = [observation_tmpl]
        self.load_test_data(queries)

    def test_create_aian_list(self):
        self.default_test([])
        self.assertTableValuesMatch(self.fq_sandbox_table_names[0],
                                    ['person_id'], [(11,), (12,), (13,), (14,),
                                                    (15,), (31,), (32,), (33,),
                                                    (35,)])
