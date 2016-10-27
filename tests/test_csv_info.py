import settings
import unittest
import csv_info
import os


class TestCsvInfo(unittest.TestCase):

    def example_path(self, filename):
        return os.path.join(settings.example_path, filename)

    def test_bom(self):
        submission_filename = self.example_path('byte_order_mark.csv')
        with open(submission_filename) as input_file:
            info = csv_info.CsvInfo(input_file, 0, 'cumc', 'person')
            self.assertEqual(info.columns[0]['name'], 'person_id')
