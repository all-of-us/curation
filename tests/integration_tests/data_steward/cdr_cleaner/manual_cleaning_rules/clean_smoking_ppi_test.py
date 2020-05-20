import csv
import os
import unittest

import app_identity
import bq_utils
import cdr_cleaner.manual_cleaning_rules.clean_smoking_ppi as smoking_ppi
import resources
from tests import test_util

SELECT_RECORDS = """ SELECT * FROM `{project_id}.{dataset_id}.{table_id}`"""


class CleanSmokingPPITest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = app_identity.get_application_id()
        self.sandbox_dataset_id = bq_utils.get_unioned_dataset_id()

    def test_integration_load_smoking_lookup_table(self):
        csv_file = 'smoking_lookup.csv'
        csv_path = os.path.join(resources.resource_files_path, csv_file)
        with open(csv_path, 'r') as f:
            expected = list(csv.DictReader(f))

        smoking_ppi.load_smoking_lookup_table(self.project_id,
                                              self.sandbox_dataset_id)

        q = SELECT_RECORDS.format(project_id=self.project_id,
                                  dataset_id=self.sandbox_dataset_id,
                                  table_id=smoking_ppi.SMOKING_LOOKUP_TABLE)
        r = bq_utils.query(q)
        actual = bq_utils.response2rows(r)

        for i, _ in enumerate(expected):
            self.assertCountEqual(expected[i], actual[i])

    def tearDown(self):
        test_util.delete_all_tables(self.sandbox_dataset_id)
