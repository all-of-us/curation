# Python imports
import copy
import unittest

# Third party imports

# Project imports
import common
from constants import bq_utils as bq_consts
from constants.cdr_cleaner import clean_cdr as clean_consts
from cdr_cleaner.cleaning_rules import drop_rows_for_missing_persons
from cdr_cleaner.cleaning_rules import drop_participants_without_ppi_or_ehr
import resources


class DropParticipantsWithoutPpiOrEhrTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def test_get_queries(self):
        # XXX: Add actual test coverage.
        results = drop_participants_without_ppi_or_ehr.get_queries('foo', 'bar')

        self.assertGreater(len(results), 0)
