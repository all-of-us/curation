"""
Integration test for SandboxAndRemovePidsList module
"""
# Python imports
import os

# Third party imports
from google.cloud.bigquery import Table

# Project Imports
from app_identity import PROJECT_ID
from common import JINJA_ENV, COMBINED_DATASET_ID
from cdr_cleaner.cleaning_rules.sandbox_and_remove_pids_list import SandboxAndRemovePidsList
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

MEASUREMENT_DATA_TEMPLATE = JINJA_ENV.from_string("""
    INSERT INTO 
        `{{project_id}}.{{dataset_id}}.measurement` 
    (
        measurement_id,
        person_id,
        measurement_concept_id,
        measurement_date,
        measurement_datetime,
        measurement_type_concept_id,
        operator_concept_id,
        value_as_number,
        value_as_concept_id,
        unit_concept_id,
        range_low,
        range_high,
        provider_id,
        visit_occurrence_id,
        measurement_source_value,
        measurement_source_concept_id,
        unit_source_value,
        value_source_value,
        measurement_time,
        visit_detail_id
    )
    VALUES
        (1,101,3036277,"2017-03-25","2017-03-25 01:00:00 UTC",44818701,4172703,5.6,0,9330,null,null,null,1,"8302-2",3036277,"in","","2020-01-01",88),
        (2,102,3036277,"2017-10-18","2017-10-18 01:00:00 UTC",44818701,4172703,62.5,0,9330,null,null,null,2,"8302-2",3036277,"in","","2020-01-01",88),
        (3,103,3036277,"2017-02-07","2017-02-07 01:00:00 UTC",44818701,4172703,1.8,null,8582,null,null,null,3,"8302-2",3036277,"cm","","2020-01-01",88),
        (4,104,3036277,"2017-01-06","2017-01-06 01:00:00 UTC",44818701,4172703,193.0,0,8582,null,null,null,4,"8302-2",3036277,"cm","","2020-01-01",88),
        (5,201,3036277,"2018-01-11","2018-01-11 01:00:00 UTC",44818701,4172703,165.0,0,8582,null,null,null,5,"8302-2",3036277,"cm","","2020-01-01",88),
        (6,202,3036277,"2018-02-07","2018-02-07 01:00:00 UTC",44818701,4172703,165.0,0,8582,null,null,null,6,"8302-2",3036277,"cm","","2020-01-01",88),
        (7,203,3036277,"2018-11-14","2018-11-14 01:00:00 UTC",44818701,4172703,170.18,0,8582,null,null,null,7,"8302-2",3036277,"cm","","2020-01-01",88),
        (8,204,3036277,"2017-05-20","2017-05-20 01:00:00 UTC",44818701,4172703,210.0,0,8582,null,null,null,8,"8302-2",3036277,"cm","","2020-01-01",88),
        (9,301,3036277,"2019-08-20","2019-08-20 01:00:00 UTC",44818701,4172703,120,0,8582,null,null,null,9,"8302-2",3036277,"cm","","2020-01-01",88),
        (10,302,3036277,"2018-09-10","2018-09-10 01:00:00 UTC",44818701,4172703,125,0,8582,null,null,null,10,"8302-2",3036277,"cm","","2020-01-01",88),
        (11,303,3036277,"2017-01-11","2017-01-11 01:00:00 UTC",44818701,4172703,173.0,0,8582,null,null,null,11,"8302-2",3036277,"cm","","2020-01-01",88),
        (12,304,3036277,"2018-02-10","2018-02-10 01:00:00 UTC",44818701,4172703,189.0,0,8582,null,null,null,12,"8302-2",3036277,"cm","","2020-01-01",88),
        (14,401,3025315,"2018-06-11","2018-06-11 01:00:00 UTC",44818701,4172703,-85.729,0,9529,null,null,null,14,"29463-7",3025315,"kg","","2020-01-01",88),
        (15,402,3025315,"2017-03-17","2017-03-17 01:00:00 UTC",44818702,4172703,250.0,0,8739,null,null,25570,15,"29463-7",3025315,"LBS","","2020-01-01",88),
        (16,403,3025315,"2017-04-08","2017-04-08 01:00:00 UTC",44818702,4172703,225.0,null,8739,null,null,null,16,"29463-7",3025315,"LBS","","2020-01-01",88),
        (17,404,3025315,"2018-10-22","2018-10-22 01:00:00 UTC",44818702,4172703,298.0,null,null,null,null,50921,17,"29463-7",3025315,"LBS","","2020-01-01",88),
        (18,501,3025315,"2018-01-03","2018-01-03 01:00:00 UTC",44818702,4172703,222.0,null,null,null,null,50921,18,"29463-7",3025315,"LBS","","2020-01-01",88)
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
            ('a40401', 404, date('2020-05-05'), 0, 0, 0, 'Staff Portal: HealthPro', False),
""")

LOOKUP_TABLE_TEMPLATE = JINJA_ENV.from_string("""
    INSERT INTO `{{project_id}}.{{sandbox_dataset_id}}.lookup_table` 
        (participant_id)
    VALUES
        (104),
        (202),
        (204),
        (301),
        (401),
        (403),
""")


class SandboxAndRemovePidsListTest(BaseTest.CleaningRulesTestBase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        # Set the test project identifier
        cls.project_id = os.environ.get(PROJECT_ID)

        # Set the expected test datasets
        cls.dataset_id = COMBINED_DATASET_ID

        # Instantiate class
        cls.rule_instance = SandboxAndRemovePidsList(project_id='',
                                                     dataset_id='',
                                                     sandbox_dataset_id='')

        # Generates list of fully qualified table names
        affected_table_names = cls.rule_instance.affected_tables
        for table_name in affected_table_names:
            cls.fq_table_names.append(
                f'{cls.project_id}.{cls.dataset_id}.{table_name}')

        # call super to set up the client, create datasets
        cls.up_class = super().setUpClass()

    def setUp(self):
        """
        Create tables and test data
        """
        super().setUp()

        # Create a temp lookup_table for testing
        lookup_table = f'{self.project_id}.{self.sandbox_id}.lookup_table'
        schema = {
            "type": "integer",
            "name": "participant_id",
            "mode": "nullable",
            "description": ""
        }
        self.client.create_table(Table(lookup_table, schema), exists_ok=True)
        self.fq_sandbox_table_names.append(lookup_table)

        # Insert temp records into the temp lookup_table
        lookup_table_query = LOOKUP_TABLE_TEMPLATE.render(
            project_id=self.project_id, dataset_id=self.dataset_id)

        # Insert test records
        observation_records_query = OBSERVATION_TABLE_TEMPLATE.render(
            project_id=self.project_id, dataset_id=self.dataset_id)
        measurement_records_query = MEASUREMENT_DATA_TEMPLATE.render(
            project_id=self.project_id, dataset_id=self.dataset_id)
        aou_death_records_query = AOU_DEATH_TEMPLATE.render(
            project_id=self.project_id, dataset_id=self.dataset_id)
        table_test_queries = [
            observation_records_query, measurement_records_query,
            aou_death_records_query
        ]

        self.load_test_data([lookup_table_query] + table_test_queries)

    def test_sandbox_and_remove_pids_list(self):
        """
        """
        pass
