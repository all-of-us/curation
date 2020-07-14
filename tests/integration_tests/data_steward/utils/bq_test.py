# Python imports
import unittest

# Third party imports
from google.cloud import bigquery
import google.auth
import pandas as pd

# Project Imports
from utils.bq import create_dataset, define_dataset, delete_dataset
import app_identity
from constants.utils import bq as consts


class BQTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = app_identity.get_application_id()
        self.dataset_id = 'fake_dataset'
        self.description = 'Test dataset created for testing BQ'
        self.label_or_tag = {'test': 'bq'}
        # Remove dataset if it already exists
        delete_dataset(self.project_id, self.dataset_id)

    def test_create_dataset(self):
        dataset = create_dataset(self.project_id, self.dataset_id,
                                 self.description, self.label_or_tag)
        self.assertEqual(dataset.dataset_id, self.dataset_id)

        # Try to create same dataset, which now already exists
        self.assertRaises(RuntimeError, create_dataset, self.project_id,
                          self.dataset_id, self.description, self.label_or_tag)

        dataset = create_dataset(self.project_id,
                                 self.dataset_id,
                                 self.description,
                                 self.label_or_tag,
                                 overwrite_existing=True)
        self.assertEqual(dataset.dataset_id, self.dataset_id)

    def test_define_dataset(self):
        self.assertRaises(RuntimeError, define_dataset, None, self.dataset_id,
                          self.description, self.label_or_tag)
        self.assertRaises(RuntimeError, define_dataset, '', self.dataset_id,
                          self.description, self.label_or_tag)
        self.assertRaises(RuntimeError, define_dataset, self.project_id, False,
                          self.description, self.label_or_tag)
        self.assertRaises(RuntimeError, define_dataset, self.project_id,
                          self.dataset_id, ' ', self.label_or_tag)
        self.assertRaises(RuntimeError, define_dataset, self.project_id,
                          self.dataset_id, self.description, None)
        dataset = define_dataset(self.project_id, self.dataset_id,
                                 self.description, self.label_or_tag)
        self.assertEqual(dataset.dataset_id, self.dataset_id)

    def tearDown(self):
        # Remove dataset created in project
        delete_dataset(self.project_id, self.dataset_id)

    def test_query_sheet_linked_bq_table_compute_engine(self):
        # add Google Drive scope
        external_data_scopes = [
            "https://www.googleapis.com/auth/drive",
            "https://www.googleapis.com/auth/cloud-platform"
        ]
        credentials, _ = google.auth.default(scopes=external_data_scopes)
        client = bigquery.Client(credentials=credentials,
                                 project=self.project_id)

        # Configure the external data source and query job.
        external_config = bigquery.ExternalConfig("GOOGLE_SHEETS")

        # Grant viewing access to the test sheet to BigQuery test accounts
        sheet_url = (
            "https://docs.google.com/spreadsheets"
            "/d/1JI-KyigmwZU9I2J6TZqVTPNoEAWVqiFeF8Y549-dvzM/edit#gid=0")
        external_config.source_uris = [sheet_url]
        external_config.schema = [
            bigquery.SchemaField("site_name", "STRING"),
            bigquery.SchemaField("hpo_id", "STRING"),
            bigquery.SchemaField("site_point_of_contact", "STRING"),
        ]
        external_config.options.range = "Sheet2!A2:C4"
        external_config.options.skip_leading_rows = 1  # Optionally skip header row.

        table_id = "hpo_id_contact_list"
        job_config = bigquery.QueryJobConfig(
            table_definitions={table_id: external_config})

        sql = f'SELECT * FROM `{consts.LOOKUP_TABLES_DATASET_ID}.{table_id}`'

        query_job = client.query(sql, job_config=job_config)

        actual_df = query_job.to_dataframe()
        expected_dict = [{
            'site_name':
                'Fake Site Name 1',
            'hpo_id':
                'fake_1',
            'site_point_of_contact':
                'fake.email.1@site_1.fakedomain; fake.email.2@site_1.fakedomain'
        }, {
            'site_name': 'Fake Site Name 2',
            'hpo_id': 'fake_2',
            'site_point_of_contact': 'no data steward'
        }, {
            'site_name':
                'Fake Site Name 3',
            'hpo_id':
                'fake_3',
            'site_point_of_contact':
                'Fake.Email.1@site_3.fake_domain; Fake.Email.2@site_3.fake_domain'
        }, {
            'site_name':
                'Fake Site Name 4',
            'hpo_id':
                'fake_4',
            'site_point_of_contact':
                'FAKE.EMAIL.1@site4.fakedomain; FAKE.EMAIL.2@site4.fakedomain'
        }, {
            'site_name': 'Fake Site Name 5',
            'hpo_id': None,
            'site_point_of_contact': None
        }]
        expected_df = pd.DataFrame(
            expected_dict,
            columns=["site_name", "hpo_id", "site_point_of_contact"])
        pd.testing.assert_frame_equal(actual_df, expected_df)
