import unittest
from mock import patch
import mock
from google.api.monitored_resource_pb2 import MonitoredResource
from data_steward.curation_logging.curation_gae_handler import GCPStackDriverLogger

LOG_BUFFER_SIZE = 50


class GCPStackDriverLoggerTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.logging_resource = MonitoredResource(type='gae_app', labels=dict())
        self.mock_setup_logging_resource_patcher = patch(
            'data_steward.curation_logging.curation_gae_handler.setup_logging_resource')
        self.mock_setup_logging_resource = self.mock_setup_logging_resource_patcher.start()
        self.mock_setup_logging_resource.return_value = self.logging_resource
        self.mock_LoggingServiceV2Client_patcher = patch(
            'data_steward.curation_logging.curation_gae_handler.gcp_logging_v2.LoggingServiceV2Client')
        self.LoggingServiceV2Client_patcher = self.mock_LoggingServiceV2Client_patcher.start()
        self.gcp_stackdriver_logger = GCPStackDriverLogger(LOG_BUFFER_SIZE)

    @mock.patch('flask.request')
    def test_setup_from_request(self, mock_request):
        self.gcp_stackdriver_logger.setup_from_request(mock_request)
