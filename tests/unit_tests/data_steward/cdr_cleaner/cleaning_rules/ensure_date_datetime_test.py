"""
Unit Test for the ensure_date_datetime_consistency module.

Remove any nullable date and/or datetimes in RDR and EHR datasets.

Original Issues: DC-614, DC-509, and DC-432
"""

import unittest

import constants.bq_utils as bq_consts
import constants.cdr_cleaner.clean_cdr as cdr_consts
from cdr_cleaner.cleaning_rules import field_mapping
from cdr_cleaner.cleaning_rules.ensure_date_datetime_consistency import EnsureDateDatetimeConsistency, TABLE_DATES, \
    FIX_DATETIME_QUERY, FIX_NULL_OR_INCORRECT_DATETIME_QUERY


class EnsureDateDatetime(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = 'foo_project'
        self.dataset_id = 'bar_project'
        self.sandbox_id = 'baz_sandbox'
        self.client = None

        self.query_class = EnsureDateDatetimeConsistency(
            self.project_id, self.dataset_id, self.sandbox_id)

        self.assertEqual(self.query_class.project_id, self.project_id)
        self.assertEqual(self.query_class.dataset_id, self.dataset_id)
        self.assertEqual(self.query_class.sandbox_dataset_id, self.sandbox_id)

    def test_setup_rule(self):
        # test
        self.query_class.setup_rule(self.client)

        # no errors are raised, nothing happens

    def test_get_cols(self):
        # pre conditions
        self.assertEqual(
            self.query_class.affected_datasets,
            [cdr_consts.RDR, cdr_consts.UNIONED, cdr_consts.COMBINED])

        for table in TABLE_DATES:
            # test
            result_list = self.query_class.get_cols(table)

            # post conditions
            table_fields = field_mapping.get_domain_fields(table)

            expected_list = []
            for field in table_fields:
                if field in TABLE_DATES[table]:
                    expected = FIX_NULL_OR_INCORRECT_DATETIME_QUERY.format(
                        field=field, date_field=TABLE_DATES[table][field])
                else:
                    expected = field
                expected_list.append(expected)

            expected_cols = ', '.join(expected_list)

            self.assertEqual(result_list, expected_cols)

    def test_get_query_specs(self):
        # pre conditions
        self.assertEqual(
            self.query_class.affected_datasets,
            [cdr_consts.RDR, cdr_consts.UNIONED, cdr_consts.COMBINED])

        # test
        result_list = self.query_class.get_query_specs()

        # post conditions
        expected_list = []
        for table in TABLE_DATES:
            query = dict()
            query[cdr_consts.QUERY] = FIX_DATETIME_QUERY.format(
                project_id=self.project_id,
                dataset_id=self.dataset_id,
                table_id=table,
                cols=self.query_class.get_cols(table))
            query[cdr_consts.DESTINATION_TABLE] = table
            query[cdr_consts.DISPOSITION] = bq_consts.WRITE_TRUNCATE
            query[cdr_consts.DESTINATION_DATASET] = self.dataset_id
            expected_list.append(query)
        self.assertEqual(result_list, expected_list)
