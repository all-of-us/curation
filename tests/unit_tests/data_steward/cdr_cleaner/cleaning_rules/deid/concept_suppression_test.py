"""
Unit test for the concept_suppression module's CT+ filter helper

Original Issues: DL2427
"""

# Python imports
import unittest

# Third party imports
import numpy as np
import pandas as pd

# Project imports
from cdr_cleaner.cleaning_rules.deid.concept_suppression import (
    CT_PLUS_SUPPRESSED, keep_ct_plus_suppressed)


class KeepCtPlusSuppressedTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def _frame(self, ct_plus_suppressed):
        """
        A privacy CSV shaped frame with the given ct_plus_suppressed column.
        """
        return pd.DataFrame({
            'concept_id': [1, 2, 3],
            'concept_code': ['a', 'b', 'c'],
            'privacy_rule': ['abortion', 'assault', 'liveborn'],
            CT_PLUS_SUPPRESSED: ct_plus_suppressed
        })

    def test_keeps_only_the_still_suppressed_rows(self):
        result = keep_ct_plus_suppressed(self._frame([True, False, True]))

        self.assertEqual([1, 3], result['concept_id'].tolist())

    def test_preserves_the_other_columns(self):
        result = keep_ct_plus_suppressed(self._frame([True, False, False]))

        self.assertEqual(
            ['concept_id', 'concept_code', 'privacy_rule', CT_PLUS_SUPPRESSED],
            result.columns.tolist())
        self.assertEqual(['abortion'], result['privacy_rule'].tolist())

    def test_all_suppressed_returns_every_row(self):
        """
        The state the CSVs are in before the categories are flipped: the variant
        must load exactly what the base rule loads.
        """
        df = self._frame([True, True, True])

        self.assertEqual(len(df), len(keep_ct_plus_suppressed(df)))

    def test_none_suppressed_returns_an_empty_frame(self):
        result = keep_ct_plus_suppressed(self._frame([False, False, False]))

        self.assertTrue(result.empty)

    def test_missing_column_raises(self):
        df = self._frame([True, False, True]).drop(columns=[CT_PLUS_SUPPRESSED])

        with self.assertRaises(KeyError):
            keep_ct_plus_suppressed(df)

    def test_string_column_raises(self):
        """
        The failure this guard exists for.

        A privacy CSV whose column is read as strings would make every row
        compare unequal to True, releasing concepts that are still suppressed.
        """
        df = self._frame(['true', 'false', 'true'])

        with self.assertRaises(TypeError):
            keep_ct_plus_suppressed(df)

    def test_blank_value_raises(self):
        """
        A blank cell makes the column object dtype rather than boolean.
        """
        df = self._frame([True, np.nan, True])

        with self.assertRaises(TypeError):
            keep_ct_plus_suppressed(df)
