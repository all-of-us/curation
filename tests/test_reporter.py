import settings
import unittest
import reporter
import os


class TestReporter(unittest.TestCase):
    def example_path(self, filename):
        return os.path.join(settings.example_path, filename)

    def check_error(self, r, message, actual, expected=None):
        self.assertFalse(r['passed'])
        self.assertTrue(len(r['errors']) == 1)
        e = r['errors'][0]
        self.assertEqual(e['message'], message)
        self.assertEqual(e['actual'], actual)
        if expected is not None:
            self.assertEqual(e['expected'], expected)

    def test_get_cdm_metadata(self):
        cdm_metadata = reporter.get_cdm_table_columns()
        self.assertTrue(cdm_metadata.count_rows() > 0)

    def test_invalid_table_name(self):
        filename = 'cuwmhh_perzon_DataSprint_0.csv'
        submission_filename = self.example_path(filename)
        r = reporter.evaluate_submission(submission_filename)
        self.check_error(r,
                         message=reporter.MSG_CANNOT_PARSE_FILENAME,
                         actual=filename,
                         expected=reporter.FILENAME_FORMAT)

    def test_invalid_hpo_id(self):
        submission_filename = self.example_path('zzzzz_person_DataSprint_0.csv')
        r = reporter.evaluate_submission(submission_filename)
        self.check_error(r,
                         message=reporter.MSG_INVALID_HPO_ID,
                         actual='zzzzz')

    def test_invalid_sprint_num(self):
        submission_filename = self.example_path('cuwmhh_person_DataSprint_1000.csv')
        r = reporter.evaluate_submission(submission_filename)
        self.check_error(r,
                         message=reporter.MSG_INVALID_SPRINT_NUM,
                         actual=1000,
                         expected=settings.sprint_num)
