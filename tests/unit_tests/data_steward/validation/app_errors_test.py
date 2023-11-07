"""
A unit test class for the curation/data_steward/validation/app_errors module.
"""
import os
from unittest import TestCase, mock

from googleapiclient.errors import HttpError
import httplib2

from validation import app_errors
from constants.validation import main as main_consts
with mock.patch('google.cloud.logging.Client') as mock_gc_logging_client:
    # mocking the client at the time of import so the script will not check the credential.
    mock_client = mock.MagicMock()
    mock_gc_logging_client.return_value = mock_client

    from validation import main


class AppErrorHandlersTest(TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.message = 'a fake message'
        self.fake_bucket = 'foo'
        self.api_resp = httplib2.Response(dict(status=500))

        self.errors_list = [
            HttpError(self.api_resp, b'500'),
            AttributeError(self.message),
            OSError(self.message),
            app_errors.BucketDoesNotExistError(self.message, self.fake_bucket),
            app_errors.InternalValidationError(self.message)
        ]

        self.handlers_list = [
            app_errors.handle_api_client_errors,
            app_errors.handle_attribute_errors, app_errors.handle_os_errors,
            app_errors.handle_bad_bucket_request,
            app_errors.handle_internal_validation_error
        ]

    def test_log_traceback(self):
        message = f"This is a test Exception"
        exception = ValueError(message)
        alert_msg = app_errors.format_alert_message(
            exception.__class__.__name__, message)

        @app_errors.log_traceback
        def fake_function():
            raise exception

        self.assertRaises(ValueError, fake_function)

    def test_handler_functions(self):
        """
        Test that all the handlers behave as expected.
        """

        expected_alerts = []

        for error, func in zip(self.errors_list, self.handlers_list):

            # test
            view, code = func(error)

            # post condition
            if isinstance(error, HttpError):
                message = '<HttpError 500 "Ok">'
            else:
                message = self.message

            expected_alert = app_errors.format_alert_message(
                error.__class__.__name__, message)

            expected_alerts.append(mock.call(expected_alert))

            self.assertEqual(view, app_errors.DEFAULT_VIEW_MESSAGE)
            self.assertTrue(code, app_errors.DEFAULT_ERROR_STATUS)

    @mock.patch('api_util.check_cron')
    def test_handlers_fire(self, mock_check_cron):
        """
        Test the os handler method fires as expected when an OSError is raised.
        """
        hpo_id = 'no_bucket_exists'
        with main.app.test_client() as tc:
            os.environ.pop(f"BUCKET_NAME_{hpo_id.upper()}", None)
            copy_files_url = main_consts.PREFIX + f'CopyFiles/{hpo_id}'
            response = tc.get(copy_files_url)

            os.environ[f"BUCKET_NAME_{hpo_id.upper()}"] = "bucket_var_set"
            response = tc.get(copy_files_url)

            self.assertEqual(mock_check_cron.call_count, 2)
