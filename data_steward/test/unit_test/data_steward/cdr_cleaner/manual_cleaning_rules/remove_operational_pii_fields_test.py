import csv
import os
import unittest

import app_identity
import bq_utils
import resources
import sandbox
from test.unit_test import test_util
import cdr_cleaner.manual_cleaning_rules.remove_operational_pii_fields as remove_operational_pii_fields
from cdr_cleaner.manual_cleaning_rules.remove_operational_pii_fields import OPERATION_PII_FIELDS_INTERMEDIARY_QUERY, \
    OPERATIONAL_PII_FIELDS_TABLE, INTERMEDIARY_TABLE, DELETE_OPERATIONAL_PII_FIELDS_QUERY

SELECT_RECORDS = """ SELECT * FROM `{project_id}.{dataset_id}.{table_id}`"""


class RemoveOperationalPiiFieldsTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = app_identity.get_application_id()
        self.dataset_id = bq_utils.get_rdr_dataset_id()
        self.sandbox_dataset_id = sandbox.get_sandbox_dataset_id(self.dataset_id)
        sandbox.create_sandbox_dataset(self.project_id, self.dataset_id)

    def test_integration_load_smoking_lookup_table(self):
        csv_file = 'operational_pii_fields.csv'
        csv_path = os.path.join(resources.resource_path, csv_file)
        with open(csv_path, 'r') as f:
            expected = list(csv.DictReader(f))

        remove_operational_pii_fields.load_operational_pii_fields_lookup_table(self.project_id, self.sandbox_dataset_id)

        q = SELECT_RECORDS.format(project_id=self.project_id,
                                  dataset_id=self.sandbox_dataset_id,
                                  table_id=remove_operational_pii_fields.OPERATIONAL_PII_FIELDS_TABLE)
        r = bq_utils.query(q)
        actual = bq_utils.response2rows(r)

        for i, _ in enumerate(expected):
            self.assertCountEqual(expected[i], actual[i])

    def test_parse_intermideary_table_query(self):
        expected_query = OPERATION_PII_FIELDS_INTERMEDIARY_QUERY.format(dataset=self.dataset_id,
                                                                        project=self.project_id,
                                                                        intermediary_table=INTERMEDIARY_TABLE,
                                                                        pii_fields_table=
                                                                        OPERATIONAL_PII_FIELDS_TABLE,
                                                                        sandbox=self.sandbox_dataset_id
                                                                        )

        actual_query = remove_operational_pii_fields.parse_intermediary_table_query(self.dataset_id,
                                                                                    self.project_id,
                                                                                    self.sandbox_dataset_id)

        self.assertCountEqual(expected_query, actual_query)

    def test_parse_delete_query(self):
        expected_query = DELETE_OPERATIONAL_PII_FIELDS_QUERY.format(dataset=self.dataset_id,
                                                                    project=self.project_id,
                                                                    pii_fields_table=
                                                                    OPERATIONAL_PII_FIELDS_TABLE,
                                                                    sandbox=self.sandbox_dataset_id
                                                                    )

        actual_query = remove_operational_pii_fields.parse_delete_query(self.dataset_id,
                                                                        self.project_id,
                                                                        self.sandbox_dataset_id)

        self.assertCountEqual(expected_query, actual_query)

    def tearDown(self):
        test_util.delete_all_tables(self.sandbox_dataset_id)
