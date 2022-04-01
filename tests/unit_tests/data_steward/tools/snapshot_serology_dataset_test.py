# coding=utf-8
"""
Unit tests for snapshot_serology_dataset.py
Issue: DC-2263
"""
# Python imports
from unittest import TestCase

# Project imports
from tools.snapshot_serology_dataset import SEROLOGY_TABLES, PERSON, S_TEST, S_RESULT


class SnapshotSerologyDatasetTest(TestCase):

    @classmethod
    def setUpClass(cls):
        print(
            '\n**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        # input parameters expected by the class
        self.dataset_id = 'fake_dataset'

    def test_table_order(self):
        # Ensure person is before all tables
        for table in SEROLOGY_TABLES:
            if table != PERSON:
                self.assertLess(SEROLOGY_TABLES.index(PERSON),
                                SEROLOGY_TABLES.index(table))

        # Ensure result is before test
        self.assertLess(SEROLOGY_TABLES.index(S_TEST),
                        SEROLOGY_TABLES.index(S_RESULT))
