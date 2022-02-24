"""
Test the Google Cloud Big Query Client and associated helper functions
"""
# Python stl imports
from unittest import TestCase

# Project imports
import app_identity
from gcloud.bq import BigQueryClient


class BqClientTest(TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = app_identity.get_application_id()
        self.bq_client = BigQueryClient(self.project_id)
