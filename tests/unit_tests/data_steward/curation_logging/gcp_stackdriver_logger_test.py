import logging
import mock
import unittest
from datetime import datetime, timedelta
from logging import LogRecord
from mock import patch
from mock import MagicMock, PropertyMock

from google.api.monitored_resource_pb2 import MonitoredResource
from curation_logging.google.logging.v2.log_entry_pb2 import LogEntryOperation
from google.protobuf import json_format as gcp_json_format, any_pb2 as gcp_any_pb2
import pytz

from curation_logging import curation_gae_handler
from curation_logging.curation_gae_handler import GCPStackDriverLogger, LogCompletionStatusEnum
from curation_logging.curation_gae_handler import GAE_LOGGING_MODULE_ID, GAE_LOGGING_VERSION_ID

LOG_BUFFER_SIZE = 3
SEVERITY_DEBUG = 100  # 100 is the equivalence of logging.DEBUG
SEVERITY_INFO = 200  # 200 is the equivalence of logging.INFO
SEVERITY_ERROR = 300  # 300 is the equivalence of logging.ERROR


class GCPStackDriverLoggerTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = 'aou-res-curation-test'
        self.request_method = 'GET'
        self.request_full_path = '/admin/v1/RemoveExpiredServiceAccountKeys'
        self.request_user_agent = 'AppEngine-Google; (+http://code.google.com/appengine)'
        self.request_ip = '0.1.0.1'
        self.request_host_name = 'py3.aou-res-curation-test.appspot.com'
        self.request_log_id = 'fake_request_id'
        self.request_trace_id = 'fake_trace_id'
        self.request_trace = 'projects/{0}/traces/{1}'.format(
            self.project_id, self.request_trace_id)

        self.request_start_time = pytz.utc.localize(datetime(2020, 1, 1))
        self.request_end_time = pytz.utc.localize(datetime(
            2020, 1, 1)) + timedelta(minutes=1)
        self.request_log_entry_ts = self.request_start_time + timedelta(
            seconds=10)
        self.log_record_created = self.request_start_time - timedelta(
            seconds=10)

        self.mock_get_application_id_patcher = patch(
            'app_identity.get_application_id')
        self.mock_get_application_id = self.mock_get_application_id_patcher.start(
        )
        self.mock_get_application_id.return_value = self.project_id

        # Mock a flask request for testing
        self.request = MagicMock()
        type(self.request).method = PropertyMock(
            return_value=self.request_method)
        type(self.request).full_path = PropertyMock(
            return_value=self.request_full_path)
        type(self.request).user_agent = PropertyMock(
            return_value=self.request_user_agent)

        headers = dict()
        headers['X-Appengine-User-Ip'] = self.request_ip
        headers['X-Appengine-Default-Version-Hostname'] = self.request_host_name
        headers['X-Appengine-Request-Log-Id'] = self.request_log_id
        headers['X-Appengine-Taskname'] = None
        headers['X-Appengine-Queuename'] = None
        headers['X-Cloud-Trace-Context'] = self.request_trace_id

        type(self.request).headers = PropertyMock(return_value=headers)

        # Define the log records for testing
        self.file_path = 'data_steward/validation/main'
        self.file_name = 'main'
        self.info_log_record = self.create_log_record(
            'info', self.log_record_created, logging.INFO, self.file_name,
            self.file_path, 10, 'info message')
        self.debug_log_record = self.create_log_record(
            'debug', self.log_record_created, logging.DEBUG, self.file_name,
            self.file_path, 11, 'debug message')
        self.error_log_record = self.create_log_record(
            'error', self.log_record_created, logging.ERROR, self.file_name,
            self.file_path, 12, 'error message')

        self.info_log_line = {
            'logMessage': 'info message',
            'severity': SEVERITY_INFO,
            'time': self.log_record_created.isoformat(),
            'sourceLocation': {
                'file': self.file_path,
                'functionName': self.file_name,
                'line': 10
            }
        }

        self.debug_log_line = {
            'logMessage': 'debug message',
            'severity': SEVERITY_DEBUG,
            'time': self.log_record_created.isoformat(),
            'sourceLocation': {
                'file': self.file_path,
                'functionName': self.file_name,
                'line': 11
            }
        }

        self.error_log_line = {
            'logMessage': 'error message',
            'severity': SEVERITY_ERROR,
            'time': self.log_record_created.isoformat(),
            'sourceLocation': {
                'file': self.file_path,
                'functionName': self.file_name,
                'line': 12
            }
        }

        self.mock_logging_service_client_patcher = patch(
            'curation_logging.curation_gae_handler.gcp_logging_v2.LoggingServiceV2Client'
        )
        self.mock_logging_service_client = self.mock_logging_service_client_patcher.start(
        )

    def tearDown(self):
        self.mock_logging_service_client_patcher.stop()
        self.request.stop()

    @staticmethod
    def create_log_record(name, record_created, level_no, func_name, pathname,
                          lineno, msg):
        log_record = LogRecord(name=name,
                               levelno=level_no,
                               lineno=lineno,
                               func=func_name,
                               pathname=pathname,
                               msg=msg,
                               level=level_no,
                               args={},
                               exc_info={})
        log_record.created = record_created
        return log_record

    @mock.patch('curation_logging.curation_gae_handler.datetime')
    def test_gcp_stackdriver_logger(self, mock_datetime):
        mock_datetime.now.return_value.isoformat.return_value = self.request_start_time.isoformat(
        )
        mock_datetime.utcnow.return_value = self.request_start_time
        mock_datetime.utcfromtimestamp.return_value = self.log_record_created

        # Initialize GCPStackDriverLogger
        self.gcp_stackdriver_logger = GCPStackDriverLogger(LOG_BUFFER_SIZE)
        self.gcp_stackdriver_logger.setup_from_request(self.request)

        self.assertIsNone(self.gcp_stackdriver_logger._first_log_ts)
        self.assertEqual(self.gcp_stackdriver_logger._start_time,
                         self.request_start_time.isoformat())
        self.assertEqual(self.gcp_stackdriver_logger._request_method,
                         self.request_method)
        self.assertEqual(self.gcp_stackdriver_logger._request_resource,
                         self.request_full_path)
        self.assertEqual(self.gcp_stackdriver_logger._request_agent,
                         self.request_user_agent)
        self.assertEqual(self.gcp_stackdriver_logger._request_remote_addr,
                         self.request_ip)
        self.assertEqual(self.gcp_stackdriver_logger._request_host,
                         self.request_host_name)
        self.assertEqual(self.gcp_stackdriver_logger._request_log_id,
                         self.request_log_id)
        self.assertEqual(self.gcp_stackdriver_logger._trace, self.request_trace)

        self.gcp_stackdriver_logger.log_event(self.info_log_record)
        self.gcp_stackdriver_logger.log_event(self.debug_log_record)
        self.gcp_stackdriver_logger.log_event(self.error_log_record)

        self.assertEqual(
            len(self.gcp_stackdriver_logger._buffer), 0,
            'expected log buffer to flush itself after being filled')
        self.assertEqual(
            self.mock_logging_service_client.return_value.write_log_entries.
            call_count, 1)

        self.gcp_stackdriver_logger.finalize()
        self.assertIsNone(self.gcp_stackdriver_logger._first_log_ts)
        self.assertEqual(self.gcp_stackdriver_logger._start_time, None)
        self.assertEqual(self.gcp_stackdriver_logger._request_method, None)
        self.assertEqual(self.gcp_stackdriver_logger._request_resource, None)
        self.assertEqual(self.gcp_stackdriver_logger._request_agent, None)
        self.assertEqual(self.gcp_stackdriver_logger._request_remote_addr, None)
        self.assertEqual(self.gcp_stackdriver_logger._request_host, None)
        self.assertEqual(self.gcp_stackdriver_logger._request_log_id, None)
        self.assertEqual(self.gcp_stackdriver_logger._trace, None)

    @mock.patch('curation_logging.curation_gae_handler.get_gcp_logger')
    def test_initialize_logging(self, mock_get_gcp_logger):
        with patch.dict('os.environ', {'GAE_ENV': ''}):
            curation_gae_handler.initialize_logging(logging.DEBUG)
            logging.info(self.info_log_record)
            logging.debug(self.debug_log_line)
            logging.error(self.error_log_record)
            self.assertEqual(3, mock_get_gcp_logger.call_count)

    @mock.patch('requests.get')
    def test_setup_logging_zone(self, mock_requests_get):
        with patch.dict('os.environ', {'GAE_SERVICE': ''}):
            timezone = 'test time zone'
            mock_response = MagicMock()
            type(mock_response).status_code = PropertyMock(return_value=200)
            type(mock_response).text = timezone
            mock_requests_get.return_value = mock_response
            actual_time_zone = curation_gae_handler.setup_logging_zone()
            self.assertEqual(actual_time_zone, timezone)

            type(mock_response).status_code = PropertyMock(return_value=500)
            actual_time_zone = curation_gae_handler.setup_logging_zone()
            self.assertEqual(actual_time_zone, 'local-machine')

        with patch.dict('os.environ', dict()):
            actual_time_zone = curation_gae_handler.setup_logging_zone()
            self.assertEqual(actual_time_zone, 'local-machine')

    @mock.patch('curation_logging.curation_gae_handler.setup_logging_zone')
    def test_setup_logging_resource(self, mock_setup_logging_zone):
        timezone = 'test time zone'
        mock_setup_logging_zone.return_value = timezone
        actual_resource = curation_gae_handler.setup_logging_resource()
        expected_resource = MonitoredResource(
            type='gae_app',
            labels={
                'project_id': self.project_id,
                'module_id': GAE_LOGGING_MODULE_ID,
                'version_id': GAE_LOGGING_VERSION_ID,
                'zone': timezone
            })
        self.assertEqual(expected_resource, actual_resource)

    @mock.patch('curation_logging.curation_gae_handler.datetime')
    @mock.patch(
        'curation_logging.curation_gae_handler.gcp_logging._helpers._normalize_severity'
    )
    def test_setup_log_line(self, mock_normalize_severity, mock_datetime):
        mock_datetime.utcfromtimestamp.return_value = self.log_record_created
        mock_normalize_severity.side_effect = [
            SEVERITY_INFO, SEVERITY_DEBUG, SEVERITY_ERROR
        ]

        actual_info_log_line = curation_gae_handler.setup_log_line(
            self.info_log_record)
        self.assertDictEqual(self.info_log_line, actual_info_log_line)

        actual_debug_log_line = curation_gae_handler.setup_log_line(
            self.debug_log_record)
        self.assertDictEqual(self.debug_log_line, actual_debug_log_line)

        actual_error_log_line = curation_gae_handler.setup_log_line(
            self.error_log_record)
        self.assertDictEqual(self.error_log_line, actual_error_log_line)

    def test_get_highest_severity_level_from_lines(self):
        lines = [self.info_log_line, self.debug_log_line, self.error_log_line]
        actual_severity_level = curation_gae_handler.get_highest_severity_level_from_lines(
            lines)
        self.assertEqual(SEVERITY_ERROR, actual_severity_level)

    def test_setup_proto_payload(self):
        lines = [self.info_log_line, self.debug_log_line, self.error_log_line]

        proto_payload_args = {
            'startTime': self.request_start_time.isoformat(),
            'endTime': self.request_end_time.isoformat(),
            'method': self.request_method,
            'resource': self.request_full_path,
            'userAgent': self.request_user_agent,
            'host': self.request_host_name,
            'ip': self.request_ip,
            'responseSize': None
        }

        actual_proto_payload = curation_gae_handler.setup_proto_payload(
            lines, LogCompletionStatusEnum.PARTIAL_BEGIN, **proto_payload_args)

        expected_dict = dict(
            {
                '@type': curation_gae_handler.REQUEST_LOG_TYPE,
                'first': True,
                'finished': False,
                'line': lines
            }, **proto_payload_args)

        expected_proto_payload = gcp_json_format.ParseDict(
            expected_dict, gcp_any_pb2.Any())

        self.assertEqual(expected_proto_payload, actual_proto_payload)

    def test_update_long_operation(self):
        expected_operation = LogEntryOperation(
            id=self.request_log_id,
            producer='appengine.googleapis.com/request_id',
            first=True,
            last=True)

        actual_operation = curation_gae_handler.update_long_operation(
            self.request_log_id, LogCompletionStatusEnum.COMPLETE)
        self.assertEqual(expected_operation, actual_operation)

        expected_operation = LogEntryOperation(
            id=self.request_log_id,
            producer='appengine.googleapis.com/request_id',
            first=True,
            last=False)

        actual_operation = curation_gae_handler.update_long_operation(
            self.request_log_id, LogCompletionStatusEnum.PARTIAL_BEGIN)
        self.assertEqual(expected_operation, actual_operation)

        expected_operation = LogEntryOperation(
            id=self.request_log_id,
            producer='appengine.googleapis.com/request_id',
            first=False,
            last=False)

        actual_operation = curation_gae_handler.update_long_operation(
            self.request_log_id, LogCompletionStatusEnum.PARTIAL_MORE)
        self.assertEqual(expected_operation, actual_operation)
