import settings
import unittest
import reporter
import os


class TestReporter(unittest.TestCase):
    def example_path(self, filename):
        return os.path.join(settings.example_path, filename)

    def test_get_cdm_metadata(self):
        cdm_metadata = reporter.get_cdm_table_columns()
        self.assertTrue(cdm_metadata.count_rows() > 0)

    def test_invalid_table_name(self):
        submission_filename = self.example_path('CUMC_perzon_DataSprint_0.csv')
        r = reporter.evaluate_submission(submission_filename)
        self.assertFalse(r['passed'])
        expected = reporter.MSG_INVALID_TABLE_NAME.format(table_name='perzon')
        self.assertSequenceEqual(r['messages'], [expected])

    def test_invalid_hpo_id(self):
        submission_filename = self.example_path('zzzzz_person_DataSprint_0.csv')
        r = reporter.evaluate_submission(submission_filename)
        self.assertFalse(r['passed'])
        expected = reporter.MSG_INVALID_HPO_ID.format(hpo_id='zzzzz')
        self.assertSequenceEqual(r['messages'], [expected])

    def test_invalid_sprint_num(self):
        submission_filename = self.example_path('CUMC_person_DataSprint_1000.csv')
        r = reporter.evaluate_submission(submission_filename)
        self.assertFalse(r['passed'])
        expected = reporter.MSG_INVALID_SPRINT_NUM.format(sprint_num=1000, settings=settings)
        self.assertSequenceEqual(r['messages'], [expected])
