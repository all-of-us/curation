"""
Integration test for the create_update_drc_id_match_table module.

Ensures that a partitioned (by hour) drc validation table is created for the site properly and that the created table is
    populated with 'missing_rdr' if there does not exist data for that field in the `ps_values` table or the default
    value 'missing_ehr'

Original Issue: DC-1216

The intent of this module is to check that the drc validation table is created properly and the drc validation table is
 updated properly.
"""

# Python imports
import os
import mock
from unittest import TestCase

# Third party imports
from google.cloud import bigquery
from google.cloud.bigquery import DatasetReference, Table

# Project imports
import bq_utils
from constants.bq_utils import WRITE_EMPTY
from gcloud.bq import BigQueryClient
from tests import test_util
from app_identity import PROJECT_ID
from common import JINJA_ENV, PERSON
from validation.participants import create_update_drc_id_match_table as id_validation
from constants.validation.participants.identity_match import IDENTITY_MATCH_TABLE

CONTENT_QUERY = JINJA_ENV.from_string("""
SELECT *
FROM {{project_id}}.{{drc_dataset_id}}.{{id_match_table_id}}
""")

EXPECTED_CASE_STATEMENTS = """
'missing_ehr' AS first_name, 
'missing_ehr' AS middle_name, 
'missing_ehr' AS last_name, 
'missing_ehr' AS phone_number, 
'missing_ehr' AS email, 
'missing_ehr' AS address_1, 
'missing_ehr' AS address_2, 
'missing_ehr' AS city, 
'missing_ehr' AS state, 
'missing_ehr' AS zip, 
'missing_ehr' AS birth_date, 
'missing_ehr' AS sex"""


class CreateUpdateDrcIdMatchTableTest(TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = os.environ.get(PROJECT_ID)
        self.dataset_id = os.environ.get('COMBINED_DATASET_ID')
        self.dataset_ref = DatasetReference(self.project_id, self.dataset_id)
        self.bq_client = BigQueryClient(self.project_id)
        self.hpo_id = 'fake_site'
        self.id_match_table_id = f'{IDENTITY_MATCH_TABLE}_{self.hpo_id}'
        self.person_table = f'{self.hpo_id}_{PERSON}'
        self.fq_person_table = f'{self.project_id}.{self.dataset_id}.{self.person_table}'

    def test_get_case_statements(self):
        expected = EXPECTED_CASE_STATEMENTS
        actual = id_validation.get_case_statements()

        self.assertEqual(actual, expected)

    def test_create_drc_validation_table(self):

        # Test
        expected = id_validation.create_drc_validation_table(
            self.bq_client,
            self.id_match_table_id,
            drc_dataset_id=self.dataset_id)

        all_tables_obj = self.bq_client.list_tables(self.dataset_id)
        all_tables = [t.table_id for t in all_tables_obj]

        self.assertTrue(expected in all_tables)

    def test_validation_creation_and_population(self):
        # Preconditions
        schema = self.bq_client.get_table_schema(PERSON)
        p_table = Table(self.fq_person_table, schema=schema)
        self.bq_client.create_table(p_table)

        job_config = bigquery.LoadJobConfig()
        job_config.source_format = bigquery.SourceFormat.CSV
        job_config.skip_leading_rows = 1
        job_config.autodetect = False
        job_config.write_disposition = WRITE_EMPTY

        with open(test_util.FIVE_PERSONS_PERSON_CSV, "rb") as source_file:
            job = self.bq_client.load_table_from_file(source_file,
                                                      self.fq_person_table,
                                                      job_config=job_config)
        job.result()

        # Use data from test_util.FIVE_PERSONS_PERSON_CSV
        expected = [{
            'person_id': person_id,
            'first_name': 'missing_ehr',
            'middle_name': 'missing_ehr',
            'last_name': 'missing_ehr',
            'phone_number': 'missing_ehr',
            'email': 'missing_ehr',
            'address_1': 'missing_ehr',
            'address_2': 'missing_ehr',
            'city': 'missing_ehr',
            'state': 'missing_ehr',
            'zip': 'missing_ehr',
            'birth_date': 'missing_ehr',
            'sex': 'missing_ehr',
            'algorithm': ''
        } for person_id in [16, 17, 18, 19, 20]]

        # Creates validation table if it does not already exist
        # Will need to be created if this test is ran individually
        if not bq_utils.table_exists(self.id_match_table_id, self.dataset_id):
            id_validation.create_drc_validation_table(
                self.bq_client,
                self.id_match_table_id,
                drc_dataset_id=self.dataset_id)

        # Test validation table population
        id_validation.populate_validation_table(self.bq_client,
                                                self.id_match_table_id,
                                                self.hpo_id,
                                                ehr_dataset_id=self.dataset_id,
                                                drc_dataset_id=self.dataset_id)

        query_contents = CONTENT_QUERY.render(
            project_id=self.project_id,
            drc_dataset_id=self.dataset_id,
            id_match_table_id=self.id_match_table_id)

        content_job = self.bq_client.query(query_contents)
        contents = list(content_job.result())
        actual = [dict(row.items()) for row in contents]

        self.assertCountEqual(actual, expected)

    def tearDown(self):
        test_util.delete_all_tables(self.dataset_id)
