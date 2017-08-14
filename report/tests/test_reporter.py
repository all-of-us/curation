import unittest

import mock
from report import main

def dummy(): return

class TestReporter(unittest.TestCase):
    def setUp(self):
        super(TestReporter, self).setUp()

    @mock.patch('report.api_util.check_cron', side_effect=dummy)
    def test_report_check_cron(self, mock_check_cron):
        mock_check_cron.return_value = 0
        main.run_report()
        self.assertEquals(mock_check_cron.call_count, 1)
