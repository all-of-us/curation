"""
Unit test for the CT+ indicators-of-birth concept set and the two CT+ variants that
act on it, BirthInformationSuppressionCtPlus and YearOfBirthRecordsSuppressionCtPlus.

Original Issues: DL-2495
"""

# Python Imports
import os
import tempfile
import unittest
from unittest import mock

# Third party imports
import pandas as pd

# Project Imports
import cdr_cleaner.cleaning_rules.deid.ct_plus_dob_indicators as dob_module
from cdr_cleaner.cleaning_rules.deid.birth_information_suppression import (
    BirthInformationSuppression, BirthInformationSuppressionCtPlus)
from cdr_cleaner.cleaning_rules.deid.year_of_birth_records_suppression import (
    YearOfBirthRecordsSuppression, YearOfBirthRecordsSuppressionCtPlus)
from common import AIAN_LIST, DEID_MAP, OBSERVATION, PERSON, UNDER18_PARTICIPANTS_LOOKUP_TABLE

FIXTURE_DIR = tempfile.mkdtemp(prefix='dl2495u_')

CSV_HEADER = ('concept_id,concept_code,privacy_rule,vocabulary_id,date_added,'
              'ct_plus_suppressed\n')

EXCLUDED_LABELS = [
    'abortion; liveborn', 'liveborn; free text', 'liveborn; location',
    'liveborn; location; free text'
]


def _fixture_csv(name, rows):
    path = os.path.join(FIXTURE_DIR, name)
    with open(path, 'w') as csv_file:
        csv_file.write(CSV_HEADER)
        csv_file.writelines(rows)
    return path


FIXTURE_CSV = _fixture_csv(
    'additional.csv',
    [
        '101,c101,liveborn,snomed,2024-01-01,true\n',
        '102,c102,stillborn,snomed,2024-01-01,true\n',
        # Suppressed wins: 103 also indicates abortion, which stays suppressed
        '103,c103,perinatal,snomed,2024-01-01,true\n',
        '103,c103,abortion,snomed,2024-01-01,true\n',
        # An expanded second label does not conflict
        '104,c104,liveborn; multiples,snomed,2024-01-01,true\n',
        '104,c104,multiples,snomed,2024-01-01,false\n',
        '105,c105,liveborn; free text,snomed,2024-01-01,true\n',
        '106,c106,abortion,snomed,2024-01-01,true\n',
    ])
RETROACTIVE_CSV = _fixture_csv('retroactive.csv', [
    '201,c201,Liveborn infants,snomed,2024-01-01,true\n',
])
PUBLICLY_REPORTABLE_CSV = _fixture_csv(
    'publicly_reportable.csv',
    [
        '301,c301,delivery-proc.csv,icd10pcs,2024-01-01,true\n',
        '44826952,763,06apr20 NIH List,icd9cm,2024-03-25,true\n',
        # A non-perinatal NIH list row is not a birth indicator
        '302,c302,06apr20 NIH List,icd9cm,2024-03-25,true\n',
    ])
FIXTURE_PATHS = [FIXTURE_CSV, RETROACTIVE_CSV, PUBLICLY_REPORTABLE_CSV]


class DobIndicatorConceptSetTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def test_fixture_set(self):
        concept_ids = dob_module.get_dob_indicator_concept_ids(FIXTURE_PATHS)

        # The four NIH list perinatal codes are added whether or not the files
        # carry them, and so is the PPI date-of-birth question.
        self.assertEqual(
            sorted([101, 102, 104, 201, 301, 1585259] +
                   list(dob_module.NIH_LIST_PERINATAL_CONCEPT_IDS)),
            concept_ids)

    def test_shipped_csvs(self):
        """
        Every concept under a birth label is in the set, and no concept under a
        label carrying abortion, free text or location is.
        """
        concept_ids = set(dob_module.get_dob_indicator_concept_ids())
        df = pd.concat([
            pd.read_csv(path, usecols=['concept_id', 'privacy_rule'])
            for path in dob_module.PRIVACY_CONCEPT_PATHS
        ])

        birth = set(df[df['privacy_rule'].isin(
            dob_module.DOB_INDICATOR_LABELS)]['concept_id'])
        excluded = set(
            df[df['privacy_rule'].isin(EXCLUDED_LABELS)]['concept_id'])

        self.assertTrue(birth)
        self.assertLessEqual(birth, concept_ids)
        self.assertFalse(excluded & concept_ids)
        self.assertIn(dob_module.PPI_DATE_OF_BIRTH_CONCEPT_ID, concept_ids)
        self.assertLessEqual(set(dob_module.NIH_LIST_PERINATAL_CONCEPT_IDS),
                             concept_ids)

    def test_drop_dob_indicators(self):
        df = pd.DataFrame({
            'concept_id': [101, 106],
            'privacy_rule': ['a', 'b']
        })

        with mock.patch.object(dob_module,
                               'get_dob_indicator_concept_ids',
                               return_value=[101]):
            kept = dob_module.drop_dob_indicators(df)

        self.assertEqual([106], kept['concept_id'].tolist())


class BirthInformationSuppressionCtPlusTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.rule = BirthInformationSuppressionCtPlus('project', 'dataset',
                                                      'sandbox', 'rdr_sandbox',
                                                      'under18_dataset')

    def test_person_is_never_affected(self):
        """The delete would remove the participant's person row."""
        self.assertNotIn(PERSON, self.rule.affected_tables)
        self.assertIn(OBSERVATION, self.rule.affected_tables)

    def test_sandbox_table_differs_from_ct(self):
        ct_rule = BirthInformationSuppression('project', 'dataset', 'sandbox')

        self.assertNotEqual(ct_rule.sandbox_table_for(OBSERVATION),
                            self.rule.sandbox_table_for(OBSERVATION))

    def test_query_is_scoped_to_excluded_participants(self):
        with mock.patch(
                'cdr_cleaner.cleaning_rules.deid.birth_information_suppression.'
                'get_dob_indicator_concept_ids',
                return_value=[101, 102]):
            query = self.rule.get_sandbox_query(OBSERVATION)['query']

        self.assertIn('UNNEST([101, 102])', query)
        self.assertIn('d.observation_source_concept_id', query)
        self.assertIn(f'rdr_sandbox.{AIAN_LIST}`', query)
        self.assertIn(f'under18_dataset.{UNDER18_PARTICIPANTS_LOOKUP_TABLE}`',
                      query)
        self.assertIn(f'sandbox.{DEID_MAP}`', query)
        self.assertIn('m.research_id = d.person_id', query)
        # Either lookup is enough to exclude a participant
        self.assertIn('OR EXISTS', query)


class YearOfBirthRecordsSuppressionCtPlusTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def _queries(self, rule):
        rule.tables_and_columns = {OBSERVATION: ['observation_date']}
        rule.observation_concept_id_columns = ['observation_concept_id']
        return rule.get_sandbox_queries()[0]['query']

    def test_ct_query_spares_nothing(self):
        rule = YearOfBirthRecordsSuppression('project', 'dataset', 'sandbox')
        rule.concept_id_columns = {OBSERVATION: ['observation_concept_id']}

        self.assertNotIn('NOT EXISTS', self._queries(rule))

    def test_ct_plus_query_spares_birth_rows(self):
        rule = YearOfBirthRecordsSuppressionCtPlus('project', 'dataset',
                                                   'sandbox')
        client = mock.MagicMock()
        client.query.return_value.result.return_value = [
            (OBSERVATION, 'observation_concept_id'),
            (OBSERVATION, 'value_as_concept_id'),
        ]

        with mock.patch.object(YearOfBirthRecordsSuppression, 'setup_rule'), \
                mock.patch(
                    'cdr_cleaner.cleaning_rules.deid.'
                    'year_of_birth_records_suppression.'
                    'get_dob_indicator_concept_ids',
                    return_value=[101, 102]):
            rule.setup_rule(client)

        query = self._queries(rule)

        self.assertIn('AND NOT EXISTS', query)
        self.assertIn('UNNEST([101, 102])', query)
        self.assertIn('c IN (d.observation_concept_id, d.value_as_concept_id)',
                      query)

    def test_ct_plus_query_without_concept_columns_spares_nothing(self):
        rule = YearOfBirthRecordsSuppressionCtPlus('project', 'dataset',
                                                   'sandbox')
        rule.spared_concept_ids = [101]
        rule.concept_id_columns = {}

        self.assertNotIn('NOT EXISTS', self._queries(rule))


if __name__ == '__main__':
    unittest.main()
