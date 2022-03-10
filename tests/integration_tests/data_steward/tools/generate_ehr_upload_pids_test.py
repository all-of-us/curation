import unittest
import os

import app_identity
from gcloud.bq import BigQueryClient
from constants.utils.bq import LOOKUP_TABLES_DATASET_ID, HPO_SITE_ID_MAPPINGS_TABLE_ID
from tools import generate_ehr_upload_pids as eup


class GenerateEhrUploadPids(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = app_identity.get_application_id()
        self.dataset_id = os.environ.get('UNIONED_DATASET_ID')
        self.bq_client = BigQueryClient(self.project_id)

    def test_generate_ehr_upload_pids_query(self):
        hpo_query = f"SELECT hpo_id FROM `{self.project_id}.{LOOKUP_TABLES_DATASET_ID}.{HPO_SITE_ID_MAPPINGS_TABLE_ID}`"
        hpo_query_job = self.bq_client.query(hpo_query)
        hpo_ids = hpo_query_job.result().to_dataframe()["hpo_id"].to_list()
        hpo_ids.sort()
        query = eup.generate_ehr_upload_pids_query(self.project_id,
                                                   self.dataset_id)
        queries = query.split("\nUNION ALL \n")
        for i, query in enumerate(sorted(queries)):
            self.assertIn(hpo_ids[i], query)
