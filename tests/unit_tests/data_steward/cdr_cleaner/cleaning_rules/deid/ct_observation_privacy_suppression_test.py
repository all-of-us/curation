"""
Unit test for ct_observation_privacy_suppression module

Original Issues: DC-3749, DL-2428

The CT+ variant must narrow its lookup to the concepts still suppressed in CT+.
Asserted on the frame directly, so a concept this fixture does not place in a
domain table is still covered.
"""

# Python Imports
import os
import tempfile
import unittest
from unittest import mock

# Project Imports
import cdr_cleaner.cleaning_rules.deid.ct_observation_privacy_suppression as rule_module
from cdr_cleaner.cleaning_rules.deid.concept_suppression import \
    CT_PLUS_LOOKUP_SUFFIX
from cdr_cleaner.cleaning_rules.deid.ct_observation_privacy_suppression import (
    CTObservationPrivacySuppression, CTObservationPrivacySuppressionCtPlus)

# The shipped CSVs carry ct_plus_suppressed = true on every row until the
# expanded categories are flipped, so these fixtures stand in for them. Each
# gets a true and a false row: an all-empty frame concatenates to object dtype
# and the CT+ filter refuses a non-boolean column.
FIXTURE_DIR = tempfile.mkdtemp(prefix='dl2428u_')

CSV_HEADER = ('concept_id,concept_code,privacy_rule,vocabulary_id,date_added,'
              'ct_plus_suppressed\n')


def _fixture_csv(name, rows):
    path = os.path.join(FIXTURE_DIR, name)
    with open(path, 'w') as csv_file:
        csv_file.write(CSV_HEADER)
        csv_file.writelines(rows)
    return path


POSTCOORDINATED_CSV = _fixture_csv('postcoordinated.csv', [
    '111,c111,abortion,ppi,2024-01-01,true\n',
    '222,c222,multiples,ppi,2024-01-01,false\n',
])
ADDITIONAL_CSV = _fixture_csv('additional.csv', [
    '333,c333,abortion,snomed,2024-01-01,true\n',
    '444,c444,assault,snomed,2024-01-01,false\n',
])
PUBLICLY_REPORTABLE_CSV = _fixture_csv('publicly_reportable.csv', [
    '777,c777,abortion,icd10cm,2024-01-01,true\n',
    '888,c888,liveborn,icd10cm,2024-01-01,false\n',
])

PATCHES = [
    mock.patch.object(rule_module, 'CT_OBSERVATION_PRIVACY_CONCEPTS_PATH',
                      POSTCOORDINATED_CSV),
    mock.patch.object(rule_module, 'CT_ADDITIONAL_PRIVACY_CONCEPTS_PATH',
                      ADDITIONAL_CSV),
    mock.patch.object(rule_module, 'CT_RT_PUBLICLY_REPORTABLE_CONCEPTS_PATH',
                      PUBLICLY_REPORTABLE_CSV),
]


class CTObservationPrivacySuppressionCtPlusTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        for patch in PATCHES:
            patch.start()
            self.addCleanup(patch.stop)

        self.args = ('project', 'dataset', 'sandbox')

    def test_base_rule_loads_every_concept(self):
        rule = CTObservationPrivacySuppression(*self.args)

        self.assertEqual(
            [111, 222],
            sorted(rule.get_postc_concepts_df()['concept_id'].tolist()))
        self.assertEqual(
            [333, 444, 777, 888],
            sorted(rule.get_rest_concepts_df()['concept_id'].tolist()))

    def test_variant_narrows_both_lookups(self):
        """
        One expanded concept per lookup, which is what proves both were
        narrowed rather than only the first.
        """
        rule = CTObservationPrivacySuppressionCtPlus(*self.args)

        self.assertEqual(
            [111], sorted(rule.get_postc_concepts_df()['concept_id'].tolist()))
        self.assertEqual(
            [333, 777],
            sorted(rule.get_rest_concepts_df()['concept_id'].tolist()))

    def test_variant_uses_its_own_lookup_tables(self):
        """
        The lookup loads append, so sharing CT's table names in one sandbox
        dataset would union the concept sets.
        """
        base = CTObservationPrivacySuppression(*self.args)
        variant = CTObservationPrivacySuppressionCtPlus(*self.args)

        self.assertEqual(
            base.ct_observation_postc_concept_table + CT_PLUS_LOOKUP_SUFFIX,
            variant.ct_observation_postc_concept_table)
        self.assertEqual(
            base.ct_observation_rest_concept_table + CT_PLUS_LOOKUP_SUFFIX,
            variant.ct_observation_rest_concept_table)
