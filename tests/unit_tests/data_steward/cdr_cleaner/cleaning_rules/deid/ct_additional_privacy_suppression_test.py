"""
Unit test for ct_additional_privacy_suppression module

Original Issues: DC-3749, DL-2427

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
import cdr_cleaner.cleaning_rules.deid.ct_additional_privacy_suppression as rule_module
from cdr_cleaner.cleaning_rules.deid.concept_suppression import \
    CT_PLUS_LOOKUP_SUFFIX
from cdr_cleaner.cleaning_rules.deid.ct_additional_privacy_suppression import (
    CTAdditionalPrivacyConceptSuppression,
    CTAdditionalPrivacyConceptSuppressionCtPlus)

# The shipped CSVs carry ct_plus_suppressed = true on every row until the
# expanded categories are flipped, so these fixtures stand in for them. Each
# gets a true and a false row: an all-empty frame concatenates to object dtype
# and the CT+ filter refuses a non-boolean column.
FIXTURE_DIR = tempfile.mkdtemp(prefix='dl2427u_')

CSV_HEADER = ('concept_id,concept_code,privacy_rule,vocabulary_id,date_added,'
              'ct_plus_suppressed\n')


def _fixture_csv(name, rows):
    path = os.path.join(FIXTURE_DIR, name)
    with open(path, 'w') as csv_file:
        csv_file.write(CSV_HEADER)
        csv_file.writelines(rows)
    return path


ADDITIONAL_CSV = _fixture_csv('additional.csv', [
    '111,c111,abortion,snomed,2024-01-01,true\n',
    '222,c222,assault,snomed,2024-01-01,false\n',
])
POSTCOORDINATED_CSV = _fixture_csv('postcoordinated.csv', [
    '333,c333,abortion,ppi,2024-01-01,true\n',
    '444,c444,multiples,ppi,2024-01-01,false\n',
])
PUBLICLY_REPORTABLE_CSV = _fixture_csv('publicly_reportable.csv', [
    '555,c555,abortion,icd10cm,2024-01-01,true\n',
    '666,c666,liveborn,icd10cm,2024-01-01,false\n',
])

PATCHES = [
    mock.patch.object(rule_module, 'CT_ADDITIONAL_PRIVACY_CONCEPTS_PATH',
                      ADDITIONAL_CSV),
    mock.patch.object(rule_module, 'CT_OBSERVATION_PRIVACY_CONCEPTS_PATH',
                      POSTCOORDINATED_CSV),
    mock.patch.object(rule_module, 'CT_RT_PUBLICLY_REPORTABLE_CONCEPTS_PATH',
                      PUBLICLY_REPORTABLE_CSV),
]


class CTAdditionalPrivacyConceptSuppressionCtPlusTest(unittest.TestCase):

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
        rule = CTAdditionalPrivacyConceptSuppression(*self.args)

        self.assertEqual(
            [111, 222, 333, 444, 555, 666],
            sorted(rule.get_suppression_concepts_df()['concept_id'].tolist()))

    def test_variant_drops_the_expanded_concepts(self):
        rule = CTAdditionalPrivacyConceptSuppressionCtPlus(*self.args)

        self.assertEqual(
            [111, 333, 555],
            sorted(rule.get_suppression_concepts_df()['concept_id'].tolist()))

    def test_variant_uses_its_own_lookup_table(self):
        """
        The lookup load appends, so sharing CT's table name in one sandbox
        dataset would union the two concept sets.
        """
        base = CTAdditionalPrivacyConceptSuppression(*self.args)
        variant = CTAdditionalPrivacyConceptSuppressionCtPlus(*self.args)

        self.assertEqual(
            base.concept_suppression_lookup_table + CT_PLUS_LOOKUP_SUFFIX,
            variant.concept_suppression_lookup_table)
