"""
Integration test for SandboxAndRemovePidsList module
"""
# Python imports
import os
from datetime import datetime

# Third party imports
from google.cloud.bigquery import Table

# Project Imports
from app_identity import PROJECT_ID
from common import JINJA_ENV, COMBINED_DATASET_ID, RDR_DATASET_ID, OBSERVATION, PERSON
from cdr_cleaner.cleaning_rules.sandbox_and_remove_pids_list import SandboxAndRemovePidsList, AOU_DEATH
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest

OBSERVATION_TABLE_TEMPLATE = JINJA_ENV.from_string("""
    INSERT INTO `{{project_id}}.{{dataset_id}}.observation` 
        (observation_id, person_id, observation_concept_id, observation_date, observation_type_concept_id)
    VALUES
        (10101, 101, 0, date('2022-01-01'), 0),
        (10102, 101, 0, date('2022-01-01'), 0),
        (10201, 102, 0, date('2022-01-02'), 0),
        (10202, 102, 0, date('2022-01-02'), 0),
        (10301, 103, 0, date('2022-01-03'), 0),
        (10302, 103, 0, date('2022-01-03'), 0),
        (10401, 104, 0, date('2022-01-04'), 0),
        (10402, 104, 0, date('2022-01-04'), 0),
        (20101, 201, 0, date('2022-01-01'), 0),
        (20102, 201, 0, date('2022-01-01'), 0),
        (20201, 202, 0, date('2022-01-02'), 0),
        (20202, 202, 0, date('2022-01-02'), 0),
        (20301, 203, 0, date('2022-01-03'), 0),
        (20302, 203, 0, date('2022-01-03'), 0),
        (20401, 204, 0, date('2022-01-04'), 0),
        (20402, 204, 0, date('2022-01-04'), 0),
        (30101, 301, 0, date('2022-01-01'), 0),
        (30102, 301, 0, date('2022-01-01'), 0),
        (30201, 302, 0, date('2022-01-02'), 0),
        (30202, 302, 0, date('2022-01-02'), 0),
        (30301, 303, 0, date('2022-01-03'), 0),
        (30302, 303, 0, date('2022-01-03'), 0),
        (30401, 304, 0, date('2022-01-04'), 0),
        (30402, 304, 0, date('2022-01-04'), 0),
        (40101, 401, 0, date('2022-01-01'), 0),
        (40102, 401, 0, date('2022-01-01'), 0),
        (40201, 402, 0, date('2022-01-02'), 0),
        (40202, 402, 0, date('2022-01-02'), 0),
        (40301, 403, 0, date('2022-01-03'), 0),
        (40302, 403, 0, date('2022-01-03'), 0),
        (40401, 404, 0, date('2022-01-04'), 0),
        (40402, 404, 0, date('2022-01-04'), 0)
""")

PERSON_DATA_TEMPLATE = JINJA_ENV.from_string("""
    INSERT INTO 
        `{{project_id}}.{{dataset_id}}.person` 
            (person_id, gender_concept_id, year_of_birth, race_concept_id, ethnicity_concept_id)
    VALUES
        (101, 0, 1991, 0, 0),
        (102, 0, 1992, 0, 0),
        (103, 0, 1993, 0, 0),
        (104, 0, 1994, 0, 0),
        (201, 0, 1991, 0, 0),
        (202, 0, 1992, 0, 0),
        (203, 0, 1993, 0, 0),
        (204, 0, 1994, 0, 0),
        (301, 0, 1991, 0, 0),
        (302, 0, 1992, 0, 0),
        (303, 0, 1993, 0, 0),
        (304, 0, 1994, 0, 0),
        (401, 0, 1991, 0, 0),
        (402, 0, 1992, 0, 0),
        (403, 0, 1993, 0, 0),
        (404, 0, 1994, 0, 0)
""")

AOU_DEATH_TEMPLATE = JINJA_ENV.from_string("""
    INSERT INTO 
        `{{project_id}}.{{dataset_id}}.aou_death`
            (aou_death_id, person_id, death_date, death_type_concept_id, cause_concept_id, cause_source_concept_id, src_id, primary_death_record)
        VALUES
            ('a10101', 101, date('2020-05-05'), 0, 0, 0, 'Staff Portal: HealthPro', False),
            ('a10202', 102, date('2020-05-05'), 0, 0, 0, 'Participant Portal 1', False),
            ('a10301', 103, date('2020-05-05'), 0, 0, 0, 'Staff Portal: HealthPro', False),
            ('a10402', 104, date('2020-05-05'), 0, 0, 0, 'Participant Portal 1', False),
            ('a20102', 201, date('2020-05-05'), 0, 0, 0, 'Participant Portal 2', False),
            ('a20202', 202, date('2020-05-05'), 0, 0, 0, 'Participant Portal 2', False),
            ('a20302', 203, date('2020-05-05'), 0, 0, 0, 'Participant Portal 2', False),
            ('a20401', 204, date('2020-05-05'), 0, 0, 0, 'Staff Portal: HealthPro', False),
            ('a30101', 301, date('2020-05-05'), 0, 0, 0, 'Staff Portal: HealthPro', False),
            ('a30202', 302, date('2020-05-05'), 0, 0, 0, 'Participant Portal 3', False),
            ('a30302', 303, date('2020-05-05'), 0, 0, 0, 'Participant Portal 3', False),
            ('a30401', 304, date('2020-05-05'), 0, 0, 0, 'Staff Portal: HealthPro', False),
            ('a40101', 401, date('2020-05-05'), 0, 0, 0, 'Staff Portal: HealthPro', False),
            ('a40202', 402, date('2020-05-05'), 0, 0, 0, 'Participant Portal 4', False),
            ('a40301', 403, date('2020-05-05'), 0, 0, 0, 'Staff Portal: HealthPro', False),
            ('a40401', 404, date('2020-05-05'), 0, 0, 0, 'Staff Portal: HealthPro', False)
""")

LOOKUP_TABLE_TEMPLATE = JINJA_ENV.from_string("""
    INSERT INTO `{{project_id}}.{{rdr_dataset_id}}.{{lookup_table}}` 
        (participant_id)
    VALUES
        (104),
        (202),
        (204),
        (301),
        (401),
        (403)
""")

LOOKUP_TABLE_SCHEMA = [{
    "type": "integer",
    "name": "participant_id",
    "mode": "nullable"
}, {
    "type": "integer",
    "name": "hpo_id",
    "mode": "nullable"
}, {
    "type": "string",
    "name": "src_id",
    "mode": "nullable"
}, {
    "type": "DATE",
    "name": "consent_for_study_enrollment_authored",
    "mode": "nullable"
}, {
    "type": "string",
    "name": "withdrawal_status",
    "mode": "nullable"
}]


class SandboxAndRemovePidsListTest(BaseTest.CleaningRulesTestBase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        super().initialize_class_vars()

        # Set the test project identifier
        cls.project_id = os.environ.get(PROJECT_ID)

        # Set the expected test datasets
        cls.dataset_id = COMBINED_DATASET_ID
        cls.rdr_dataset_id = RDR_DATASET_ID
        cls.lookup_table = 'lookup_table'
        cls.sandbox_id = f'{cls.dataset_id}_sandbox'

        cls.kwargs = {
            'rdr_dataset_id': cls.rdr_dataset_id,
            'lookup_table': cls.lookup_table
        }

        # Instantiate class
        cls.rule_instance = SandboxAndRemovePidsList(
            project_id=cls.project_id,
            dataset_id=cls.dataset_id,
            sandbox_dataset_id=cls.sandbox_id,
            rdr_dataset_id=RDR_DATASET_ID,
            lookup_table=cls.lookup_table)

        # Generates list of fully qualified table names
        affected_table_names = ['observation', 'person', 'aou_death']
        for table_name in affected_table_names:
            cls.fq_table_names.append(
                f'{cls.project_id}.{cls.dataset_id}.{table_name}')

        for table_name in affected_table_names:
            cls.fq_sandbox_table_names.append(
                f'{cls.project_id}.{cls.sandbox_id}.{cls.rule_instance.sandbox_table_for(table_name)}'
            )

        # call super to set up the client, create datasets
        cls.up_class = super().setUpClass()

    def setUp(self):
        """
        Create tables and test data
        """
        super().setUp()

        # Create a temp lookup_table in rdr_dataset for testing
        lookup_table_name = f'{self.project_id}.{self.rdr_dataset_id}.{self.lookup_table}'
        self.client.create_table(Table(lookup_table_name, LOOKUP_TABLE_SCHEMA),
                                 exists_ok=True)
        self.fq_table_names.append(lookup_table_name)

        # Build temp lookup table records query
        lookup_table_query = LOOKUP_TABLE_TEMPLATE.render(
            project_id=self.project_id,
            rdr_dataset_id=self.rdr_dataset_id,
            lookup_table=self.lookup_table)

        # Build test data queries
        observation_records_query = OBSERVATION_TABLE_TEMPLATE.render(
            project_id=self.project_id, dataset_id=self.dataset_id)
        person_records_query = PERSON_DATA_TEMPLATE.render(
            project_id=self.project_id, dataset_id=self.dataset_id)
        aou_death_records_query = AOU_DEATH_TEMPLATE.render(
            project_id=self.project_id, dataset_id=self.dataset_id)

        table_test_queries = [
            observation_records_query, person_records_query,
            aou_death_records_query
        ]

        # Load test data
        self.load_test_data([lookup_table_query] + table_test_queries)

    def test_sandbox_and_remove_pids_list(self):
        """
        Validates that the data for participants in the lookup table has been removed. 
        """
        tables_and_counts = [{
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{OBSERVATION}',
            'fq_sandbox_table_name':
                self.fq_sandbox_table_names[0],
            'fields': [
                'observation_id', 'person_id', 'observation_concept_id',
                'observation_date', 'observation_type_concept_id'
            ],
            'loaded_ids': [
                10101, 10102, 10201, 10202, 10301, 10302, 10401, 10402, 20101,
                20102, 20201, 20202, 20301, 20302, 20401, 20402, 30101, 30102,
                30201, 30202, 30301, 30302, 30401, 30402, 40101, 40102, 40201,
                40202, 40301, 40302, 40401, 40402
            ],
            'sandboxed_ids': [
                10401, 10402, 20201, 20202, 20401, 20402, 30101, 30102, 40101,
                40102, 40301, 40302
            ],
            'cleaned_values': [
                (10101, 101, 0, datetime.fromisoformat('2022-01-01').date(), 0),
                (10102, 101, 0, datetime.fromisoformat('2022-01-01').date(), 0),
                (10201, 102, 0, datetime.fromisoformat('2022-01-02').date(), 0),
                (10202, 102, 0, datetime.fromisoformat('2022-01-02').date(), 0),
                (10301, 103, 0, datetime.fromisoformat('2022-01-03').date(), 0),
                (10302, 103, 0, datetime.fromisoformat('2022-01-03').date(), 0),
                (20101, 201, 0, datetime.fromisoformat('2022-01-01').date(), 0),
                (20102, 201, 0, datetime.fromisoformat('2022-01-01').date(), 0),
                (20301, 203, 0, datetime.fromisoformat('2022-01-03').date(), 0),
                (20302, 203, 0, datetime.fromisoformat('2022-01-03').date(), 0),
                (30201, 302, 0, datetime.fromisoformat('2022-01-02').date(), 0),
                (30202, 302, 0, datetime.fromisoformat('2022-01-02').date(), 0),
                (30301, 303, 0, datetime.fromisoformat('2022-01-03').date(), 0),
                (30302, 303, 0, datetime.fromisoformat('2022-01-03').date(), 0),
                (30401, 304, 0, datetime.fromisoformat('2022-01-04').date(), 0),
                (30402, 304, 0, datetime.fromisoformat('2022-01-04').date(), 0),
                (40201, 402, 0, datetime.fromisoformat('2022-01-02').date(), 0),
                (40202, 402, 0, datetime.fromisoformat('2022-01-02').date(), 0),
                (40401, 404, 0, datetime.fromisoformat('2022-01-04').date(), 0),
                (40402, 404, 0, datetime.fromisoformat('2022-01-04').date(), 0)
            ]
        }, {
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{PERSON}',
            'fq_sandbox_table_name':
                self.fq_sandbox_table_names[1],
            'fields': [
                'person_id', 'gender_concept_id', 'year_of_birth',
                'race_concept_id', 'ethnicity_concept_id'
            ],
            'loaded_ids': [
                101, 102, 103, 104, 201, 202, 203, 204, 301, 302, 303, 304, 401,
                402, 403, 404
            ],
            'sandboxed_ids': [104, 202, 204, 301, 401, 403],
            'cleaned_values': [(101, 0, 1991, 0, 0), (102, 0, 1992, 0, 0),
                               (103, 0, 1993, 0, 0), (201, 0, 1991, 0, 0),
                               (203, 0, 1993, 0, 0), (302, 0, 1992, 0, 0),
                               (303, 0, 1993, 0, 0), (304, 0, 1994, 0, 0),
                               (402, 0, 1992, 0, 0), (404, 0, 1994, 0, 0)]
        }, {
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{AOU_DEATH}',
            'fq_sandbox_table_name':
                self.fq_sandbox_table_names[2],
            'fields': [
                'aou_death_id', 'person_id', 'death_date',
                'death_type_concept_id', 'cause_concept_id',
                'cause_source_concept_id', 'src_id', 'primary_death_record'
            ],
            'loaded_ids': [
                'a10101', 'a10202', 'a10301', 'a10402', 'a20102', 'a20202',
                'a20302', 'a20401', 'a30101', 'a30202', 'a30302', 'a30401',
                'a40101', 'a40202', 'a40301', 'a40401'
            ],
            'sandboxed_ids': [
                'a10402', 'a20202', 'a20401', 'a30101', 'a40101', 'a40301'
            ],
            'cleaned_values': [
                ('a10101', 101, datetime.fromisoformat('2020-05-05').date(), 0,
                 0, 0, 'Staff Portal: HealthPro', False),
                ('a10202', 102, datetime.fromisoformat('2020-05-05').date(), 0,
                 0, 0, 'Participant Portal 1', False),
                ('a10301', 103, datetime.fromisoformat('2020-05-05').date(), 0,
                 0, 0, 'Staff Portal: HealthPro', False),
                ('a20102', 201, datetime.fromisoformat('2020-05-05').date(), 0,
                 0, 0, 'Participant Portal 2', False),
                ('a20302', 203, datetime.fromisoformat('2020-05-05').date(), 0,
                 0, 0, 'Participant Portal 2', False),
                ('a30202', 302, datetime.fromisoformat('2020-05-05').date(), 0,
                 0, 0, 'Participant Portal 3', False),
                ('a30302', 303, datetime.fromisoformat('2020-05-05').date(), 0,
                 0, 0, 'Participant Portal 3', False),
                ('a30401', 304, datetime.fromisoformat('2020-05-05').date(), 0,
                 0, 0, 'Staff Portal: HealthPro', False),
                ('a40202', 402, datetime.fromisoformat('2020-05-05').date(), 0,
                 0, 0, 'Participant Portal 4', False),
                ('a40401', 404, datetime.fromisoformat('2020-05-05').date(), 0,
                 0, 0, 'Staff Portal: HealthPro', False),
            ]
        }]
        self.default_test(tables_and_counts)
