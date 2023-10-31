"""Based closely on
https://github.com/all-of-us/raw-data-repository/blob/1.60.6/rdr_service/services/gcp_logging.py. This custom handler
groups all the log messages generated within the same http request into an operation, this grouping mechanism allows
us to quickly navigate to the relevant log message. """
import collections
import json
import logging
import os
import string
import threading
import app_identity
from datetime import datetime, timezone
from enum import IntEnum
import random

import requests
from flask import request, Response

from google.api.monitored_resource_pb2 import MonitoredResource
from google.cloud import logging as gcp_logging
from google.cloud import logging_v2 as gcp_logging_v2
from google.logging.type import http_request_pb2 as gcp_http_request_pb2
from google.protobuf import json_format as gcp_json_format, any_pb2 as gcp_any_pb2

# Do not remove this import.
# import curation_logging.gcp_request_log_pb2  # pylint: disable=unused-import

# https://pypi.org/project/google-cloud-logging/
# https://cloud.google.com/logging/docs/reference/v2/rpc/google.logging.v2
# https://developers.google.com/resources/api-libraries/documentation/logging/v2/python/latest/logging_v2.entries.html

# How many log lines should be batched before pushing them to StackDriver.
_LOG_BUFFER_SIZE = 100

GAE_LOGGING_MODULE_ID = 'app-' + os.environ.get('GAE_SERVICE', 'default')
GAE_LOGGING_VERSION_ID = os.environ.get('GAE_VERSION', 'devel')
LOG_NAME_TEMPLATE = 'projects/{project_id}/logs/appengine.googleapis.com%2Frequest_log'
REQUEST_LOG_TYPE = 'type.googleapis.com/google.appengine.logging.v1.RequestLog'

# This is where we save all data that is tied to a specific execution thread.
_thread_store = threading.local()


class LogCompletionStatusEnum(IntEnum):
    """
    Indicator for log entry completion status, which can span multiple log entries.
    """
    COMPLETE = 0
    PARTIAL_BEGIN = 1
    PARTIAL_MORE = 2
    PARTIAL_FINISHED = 3


def setup_logging_zone():
    """
    Attempt to get the project zone information.
    return: Zone pb2 structure.
    """
    zone = 'local-machine'
    if 'GAE_SERVICE' in os.environ:
        try:
            resp = requests.get(
                'http://metadata.google.internal/computeMetadata/v1/instance/zone',
                timeout=15.0)
            if resp.status_code == 200:
                zone = resp.text.strip()
        # pylint: disable=broad-except
        except Exception:
            zone = 'unknown'
    return zone


def setup_logging_resource():
    """
    Set the values for the Google Logging Resource object. Thread safe.
    :return: MonitoredResource pb2 structure.
    """
    labels = {
        "project_id": app_identity.get_application_id(),
        "module_id": GAE_LOGGING_MODULE_ID,
        "version_id": GAE_LOGGING_VERSION_ID,
        "zone": setup_logging_zone()
    }

    # https://cloud.google.com/logging/docs/reference/v2/rpc/google.api#google.api.MonitoredResource
    resource_pb2 = MonitoredResource(type='gae_app', labels=labels)
    return resource_pb2


# pylint: disable=unused-argument
def setup_log_line(record: logging.LogRecord, resource=None, method=None):
    """
    Prepare a log event for sending to GCP StackDriver.  Thread safe.
    :param record: Log event record.
    :param resource: request resource
    :param method: request method
    :return: LogLine proto buffer object
    """
    event_ts = datetime.utcfromtimestamp(record.created)
    event_ts = event_ts.replace(tzinfo=timezone.utc)
    event_ts = event_ts.isoformat()
    severity = gcp_logging._helpers._normalize_severity(record.levelno)
    message = str(record.msg) % record.args if record.args else str(record.msg)

    # Look for embedded traceback source location override information
    if '%%' in message:
        tmp_sl = message[message.find('%%'):message.rfind('%%') + 2]
        message = message.replace(tmp_sl, '')
        tmp_sl = tmp_sl.replace('%%', '')
        source_location = json.loads(tmp_sl)

    else:
        funcname = record.funcName if record.funcName else ''
        file = record.pathname if record.pathname else ''
        lineno = record.lineno if record.lineno else 0
        source_location = {
            "file": file,
            "functionName": funcname,
            "line": lineno
        }

    message = message.replace('$$method$$', method if method else '')
    message = message.replace('$$resource$$', resource if resource else '/')

    log_line = {
        "logMessage": message,
        "severity": severity,
        "sourceLocation": source_location,
        "time": event_ts
    }

    return log_line


def get_highest_severity_level_from_lines(lines):
    """
    Figure out the highest severity level in a given set of log records.
    :param lines: List of log records
    """
    if lines:

        s = sorted([line['severity'] for line in lines],
                   key=lambda severity: -severity)
        return s[0]
    else:
        return gcp_logging_v2.gapic.enums.LogSeverity(200)


def setup_proto_payload(lines: list, log_status: LogCompletionStatusEnum,
                        **kwargs):
    """
    Build the log protoPayload portion of the log entry. Thread safe.
    :param lines: List of LogMessage lines to add.
    :param log_status: Logging completion status value.
    :return: RequestLog pb2 object.
    """

    # Base set of values for proto_payload object.
    req_dict = {
        "@type": REQUEST_LOG_TYPE,
        "startTime": datetime.now(timezone.utc).isoformat(),
        "ip": "0.0.0.0",
        "first": True,
        "finished": True,
        "endTime": datetime.now(timezone.utc).isoformat(),
        "responseSize": 355,
        "line": [],
        # If we see these lines in the logs, we know something isn't working correctly.
        "userAgent": "Bad Mojo",
        "resource": "/Bad-Mojo-Teacups/Logging",
    }

    # Override any key values.
    for k, v in kwargs.items():
        req_dict[k] = v

    # Set completion statuses
    if log_status == LogCompletionStatusEnum.PARTIAL_BEGIN:
        req_dict['finished'] = False
    elif log_status == LogCompletionStatusEnum.PARTIAL_MORE:
        req_dict['first'] = False
        req_dict['finished'] = False
    elif log_status == LogCompletionStatusEnum.PARTIAL_FINISHED:
        req_dict['first'] = False

    if lines and len(lines) > 0:
        for x in range(len(lines)):
            req_dict["line"].append(lines[x])

    # Convert dict to Generic pb2 message object, requires gcp_request_log_pb2 import.
    request_log_pb2 = gcp_json_format.ParseDict(req_dict, gcp_any_pb2.Any())

    return request_log_pb2


def update_long_operation(request_log_id, op_status):
    """
    Handle long operations. Thread safe.
    :param request_log_id: request logging id.
    :param op_status: LogCompletionStatusEnum value.
    """
    if op_status == LogCompletionStatusEnum.COMPLETE:
        first = last = True
    else:
        first = True if op_status == LogCompletionStatusEnum.PARTIAL_BEGIN else False
        last = True if op_status == LogCompletionStatusEnum.PARTIAL_FINISHED else False

    # https://cloud.google.com/logging/docs/reference/v2/rpc/google.logging.v2#google.logging.v2.LogEntryOperation
    operation_pb2 = gcp_logging_v2.proto.log_entry_pb2.LogEntryOperation(
        id=request_log_id,
        producer='appengine.googleapis.com/request_id',
        first=first,
        last=last)

    return operation_pb2


class GCPStackDriverLogger(object):
    """
    Sends log records to google stack driver logging.  Each thread needs its own copy of this object.
    Buffers up to `buffer_size` log records into one ProtoBuffer to be submitted.
    """

    def __init__(self, buffer_size=_LOG_BUFFER_SIZE):

        self._buffer_size = buffer_size
        self._buffer = collections.deque()

        self._reset()

        self._logging_client = gcp_logging_v2.LoggingServiceV2Client()
        self._operation_pb2 = None

        # Used to determine how long a request took.
        self._first_log_ts = None

    def _reset(self):

        self._first_log_ts = None

        self.log_completion_status = LogCompletionStatusEnum.COMPLETE
        self._operation_pb2 = None

        self._trace = None
        self._start_time = None
        self._end_time = None

        self._request_method = None
        self._request_resource = None
        self._request_agent = None
        self._request_remote_addr = None
        self._request_log_id = None
        self._request_host = None

        # cloud tasks
        self._request_taskname = None
        self._request_queue = None

        self._response_status_code = 200
        self._response_size = None

        self._buffer.clear()

    def setup_from_request(self, _request, initial=False):
        """
        Gather everything we need to log from the request object.
        :param _request: Flask request object
        :param initial: Is this the beginning of a request? If no, this means flask 'begin_request' call failed.
        """
        # send any pending log entries in case 'end_request' was not called.
        if len(self._buffer) and initial:
            self.finalize()

        self._start_time = datetime.now(timezone.utc).isoformat()
        self._request_method = _request.method
        self._request_resource = _request.full_path
        if self._request_resource and self._request_resource.endswith('?'):
            self._request_resource = self._request_resource[:-1]
        self._request_agent = str(_request.user_agent)
        self._request_remote_addr = _request.headers.get(
            'X-Appengine-User-Ip', _request.remote_addr)
        self._request_host = _request.headers.get(
            'X-Appengine-Default-Version-Hostname', _request.host)
        self._request_log_id = _request.headers.get(
            'X-Appengine-Request-Log-Id', 'None')

        self._request_taskname = _request.headers.get('X-Appengine-Taskname',
                                                      None)
        self._request_queue = _request.headers.get('X-Appengine-Queuename',
                                                   None)

        trace_id = _request.headers.get('X-Cloud-Trace-Context', '')
        if trace_id:
            trace_id = trace_id.split('/')[0]
            trace = 'projects/{0}/traces/{1}'.format(
                app_identity.get_application_id(), trace_id)
            self._trace = trace

    def log_event(self, record: logging.LogRecord):
        """
        Capture and store a log event record.
        :param record: Python log record
        """
        self._buffer.appendleft(record)

        if not self._first_log_ts:
            self._first_log_ts = datetime.utcnow()

        if len(self._buffer) >= self._buffer_size:
            if self.log_completion_status == LogCompletionStatusEnum.COMPLETE:
                self.log_completion_status = LogCompletionStatusEnum.PARTIAL_BEGIN
                self._operation_pb2 = update_long_operation(
                    self._request_log_id, self.log_completion_status)

            elif self.log_completion_status == LogCompletionStatusEnum.PARTIAL_BEGIN:
                self.log_completion_status = LogCompletionStatusEnum.PARTIAL_MORE
                self._operation_pb2 = update_long_operation(
                    self._request_log_id, self.log_completion_status)

            self.publish_to_stackdriver()

    def finalize(self, _response=None, _request=None):
        """
        Finalize and send any log entries to StackDriver.
        """
        if not self._start_time and _request:
            self.setup_from_request(_request=_request, initial=False)

        if self.log_completion_status == LogCompletionStatusEnum.COMPLETE:
            if len(self._buffer) == 0 and not _response:
                # nothing to log
                self._reset()
                return
        else:
            self.log_completion_status = LogCompletionStatusEnum.PARTIAL_FINISHED
            self._operation_pb2 = update_long_operation(
                self._request_log_id, self.log_completion_status)

        # _response could be of the exception type if an exception is raised
        if isinstance(_response, Response):
            self._response_status_code = _response.status_code
            self._response_size = len(_response.data)
        else:
            self._response_status_code = None
            self._response_size = None

        self.publish_to_stackdriver()
        self._reset()

    def publish_to_stackdriver(self):
        """
        Send a set of log entries to StackDriver.
        """
        insert_id = \
            ''.join(random.choice(string.ascii_uppercase + string.ascii_lowercase + string.digits) for _ in range(16))
        lines = list()
        index = 0

        while len(self._buffer):
            line = self._buffer.pop()
            lines.append(
                setup_log_line(line, self._request_resource,
                               self._request_method))
            index += 1

        self._end_time = datetime.now(timezone.utc).isoformat()

        log_entry_pb2_args = {
            'resource': setup_logging_resource(),
            'severity': get_highest_severity_level_from_lines(lines),
            'trace': self._trace,
            'insert_id': insert_id,
            'trace_sampled': True if self._trace else False
        }

        if self._response_status_code:
            log_entry_pb2_args[
                'http_request'] = gcp_http_request_pb2.HttpRequest(
                    status=self._response_status_code)
            # Transform the response code to a logging severity level.
            tmp_code = int(round(self._response_status_code / 100, 0) * 100)
            if tmp_code > int(log_entry_pb2_args['severity']):
                log_entry_pb2_args[
                    'severity'] = gcp_logging_v2.gapic.enums.LogSeverity(
                        tmp_code)

        if not self._operation_pb2:
            self.log_completion_status = LogCompletionStatusEnum.COMPLETE
            self._operation_pb2 = update_long_operation(
                self._request_log_id, self.log_completion_status)

        log_entry_pb2_args['operation'] = self._operation_pb2

        proto_payload_args = {
            'startTime': self._start_time,
            'endTime': self._end_time,
            'method': self._request_method,
            'resource': self._request_resource,
            'userAgent': self._request_agent,
            'host': self._request_host,
            'ip': self._request_remote_addr,
            'responseSize': self._response_size,
            'status': self._response_status_code,
            'requestId': self._request_log_id,
            'traceId': self._trace,
            'versionId': os.environ.get('GAE_VERSION', 'devel'),
            'urlMapEntry': 'validation.main.app'
        }

        if self._request_taskname:
            proto_payload_args['taskName'] = self._request_taskname
            proto_payload_args['taskQueueName'] = self._request_queue

        if self._first_log_ts:
            if self._first_log_ts:
                total_time = datetime.utcnow() - self._first_log_ts
            else:
                total_time = 0
            proto_payload_args['latency'] = '{0}.{1}s'.format(
                total_time.seconds, total_time.microseconds)

        proto_payload_pb2 = setup_proto_payload(lines,
                                                self.log_completion_status,
                                                **proto_payload_args)

        log_entry_pb2_args['proto_payload'] = proto_payload_pb2

        # https://cloud.google.com/logging/docs/reference/v2/rpc/google.logging.v2#google.logging.v2.LogEntry
        log_entry_pb2 = gcp_logging_v2.types.log_entry_pb2.LogEntry(
            **log_entry_pb2_args)

        self._logging_client.write_log_entries(
            [log_entry_pb2],
            log_name=LOG_NAME_TEMPLATE.format(
                project_id=app_identity.get_application_id()))


def get_gcp_logger() -> GCPStackDriverLogger:
    """
    Return the GCPStackDriverLogger object for this thread.
    :return: GCPStackDriverLogger object
    """
    if hasattr(_thread_store, 'logger'):
        _logger = getattr(_thread_store, 'logger')
        return _logger

    # We may need to initialize the logger for this thread.
    if 'GAE_ENV' in os.environ:
        _logger = GCPStackDriverLogger()
        setattr(_thread_store, 'logger', _logger)
        return _logger


class GCPLoggingHandler(logging.Handler):

    def emit(self, record: logging.LogRecord):
        """
        Capture and store a log event record.
        :param record: Python log record
        """
        _logger = get_gcp_logger()
        if _logger:
            _logger.log_event(record)
            return

        line = setup_log_line(record)
        print(line)


def initialize_logging(log_level=logging.INFO):
    """
    Setup GCP Stack Driver logging if we are running in App Engine.
    :param log_level: Log level to use.
    """
    if 'GAE_ENV' in os.environ:
        # Configure root logger
        root_logger = logging.getLogger()
        root_logger.setLevel(log_level)
        # Configure StackDriver logging handler
        log_handler = GCPLoggingHandler()
        log_handler.setLevel(log_level)

        # Add StackDriver logging handler to root logger.
        root_logger.addHandler(log_handler)


def begin_request_logging():
    """
    Initialize logging for a new request.  Not guarantied to always be called.
    """
    _logger = get_gcp_logger()
    if _logger:
        _logger.setup_from_request(_request=request, initial=True)


def end_request_logging(response):
    """
    Finalize and send any log entries.  Not guarantied to always be called.
    """
    _logger = get_gcp_logger()
    if _logger:
        _logger.finalize(_response=response, _request=request)
    return response


def flush_request_logs():
    """
    Flush any pending log records.
    """
    _logger = get_gcp_logger()
    if _logger:
        _logger.finalize(_request=request)
