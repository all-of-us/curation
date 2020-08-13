"""
A unit test class for the curation/data_steward/validation/app_errors module.
"""
from unittest import TestCase, mock

from googleapiclient.errors import HttpError
import httplib2

from validation import app_errors, main
from constants.validation import main as main_consts


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

    @mock.patch('validation.app_errors.post_message')
    def test_log_traceback(self, mock_post):
        message = f"This is a test Exception"
        exception = ValueError(message)
        alert_msg = f"Exception {exception.__class__}: {message}"

        @app_errors.log_traceback
        def fake_function():
            raise exception

        self.assertRaises(ValueError, fake_function)
        mock_post.assert_called_once_with(alert_msg)

    @mock.patch('validation.app_errors.post_message')
    def test_handler_functions(self, mock_alert_message):
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
            self.assertEqual(code, app_errors.DEFAULT_ERROR_STATUS)
            mock_alert_message.assert_called()

        mock_alert_message.assert_has_calls(expected_alerts)

    @mock.patch('validation.app_errors.post_message')
    @mock.patch('api_util.check_cron')
    def test_handlers_fire(self, mock_check_cron, mock_alert_message):
        """
        Test the os handler method fires as expected when an OSError is raised.
        """
        with main.app.test_client() as tc:
            copy_files_url = main_consts.PREFIX + 'CopyFiles/no_bucket_exists'
            response = tc.get(copy_files_url)

            mock_check_cron.assert_called_once()
            # asserts the handler was called, based on it's contents
            mock_alert_message.assert_called()
            self.assertEqual(response.status_code,
                             app_errors.DEFAULT_ERROR_STATUS)
