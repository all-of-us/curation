"""
Assert that every privacy_rule label used by the CT-consumed privacy CSVs has
exactly one definition in privacy_rule_definitions.csv, and that the
definitions file names no label the CSVs do not use.

Original Issue: DL-2494
"""
import csv
import unittest

import resources

# The four CSVs consumed by the controlled tier cleaning rules. The RT-only
# files and ct_nph_observation_concept_suppressions.csv use a different label
# vocabulary and are deliberately not covered here.
CT_CONSUMED_PATHS = [
    resources.CT_ADDITIONAL_PRIVACY_CONCEPTS_PATH,
    resources.CT_RT_PUBLICLY_REPORTABLE_CONCEPTS_PATH,
    resources.CT_OBSERVATION_PRIVACY_CONCEPTS_PATH,
    resources.CT_RETROACTIVE_PRIVACY_CONCEPTS_PATH,
]

DEFINITION_COLUMNS = [
    'privacy_rule', 'data_type', 'definition', 'publicly_reportable',
    'source_ticket'
]


def read_csv(path):
    with open(path, newline='', encoding='utf-8') as csv_file:
        return list(csv.DictReader(csv_file))


def atomic_labels(rows):
    """Split the ';'-joined privacy_rule cells into the atomic labels they hold.

    A cell such as 'liveborn; multiples' names two categories that each get
    their own definition; the combination itself never does.
    """
    labels = set()
    for row in rows:
        for label in row['privacy_rule'].split(';'):
            labels.add(label.strip())
    return labels


class PrivacyRuleDefinitionsTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.definitions = read_csv(resources.PRIVACY_RULE_DEFINITIONS_PATH)
        self.defined_labels = [row['privacy_rule'] for row in self.definitions]
        self.used_labels = set()
        for path in CT_CONSUMED_PATHS:
            self.used_labels |= atomic_labels(read_csv(path))

    def test_every_used_label_is_defined(self):
        missing = sorted(self.used_labels - set(self.defined_labels))
        self.assertEqual(
            [], missing,
            f'privacy_rule values used by the CT privacy CSVs with no row in '
            f'{resources.PRIVACY_RULE_DEFINITIONS_PATH}: {missing}')

    def test_no_definition_names_an_unused_label(self):
        stale = sorted(set(self.defined_labels) - self.used_labels)
        self.assertEqual(
            [], stale,
            f'privacy_rule values defined but no longer used by any CT privacy '
            f'CSV: {stale}')

    def test_each_label_is_defined_exactly_once(self):
        duplicates = sorted({
            label for label in self.defined_labels
            if self.defined_labels.count(label) > 1
        })
        self.assertEqual([], duplicates,
                         f'privacy_rule values defined more than once: '
                         f'{duplicates}')

    def test_definition_columns(self):
        self.assertEqual(DEFINITION_COLUMNS, list(self.definitions[0].keys()))

    def test_every_definition_row_is_populated(self):
        """Every column except privacy_rule carries a value.

        privacy_rule is exempt because one of the labels is the empty string,
        carried by five rows of ct_retroactive_privacy_suppression.csv.
        """
        for row in self.definitions:
            for column in DEFINITION_COLUMNS[1:]:
                self.assertTrue(
                    row[column].strip(), f'{column} is empty for privacy_rule '
                    f'{row["privacy_rule"]!r}')
