# Python imports
import copy
import unittest

# Third party imports

# Project imports
import cleaners.query_generators.person_id_validator as validator


class PersonIDValidatorTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.mapped_tables = copy.copy(validator.MAPPED_VALIDATION_TABLES)
        self.unmapped_tables = copy.copy(validator.UNMAPPED_VALIDATION_TABLES)
        self.all_tables = copy.copy(self.mapped_tables)
        self.all_tables.extend(self.unmapped_tables)

    def test_get_person_id_validation_queries(self):
        # pre conditions

        # test
        results = validator.get_person_id_validation_queries('foo', 'bar')

        # post conditions
        self.assertEqual(len(results), ((len(self.all_tables) * 2) - 1))

        non_consent = validator.NOT_CONSENTING_PERSON_IDS
        orphan_records = validator.DELETE_ORPHANED_PERSON_IDS

        expected = []
        for table in self.mapped_tables:
            expected.append(non_consent.format(
                project='foo', dataset='bar', mapping_dataset='bar', table=table
            ))

        for table in self.all_tables:
            expected.append(orphan_records.format(project='foo', dataset='bar', table=table))

        self.assertEqual(expected, results)

    def test_get_person_id_validation_queries_deid(self):
        # pre conditions

        # test
        results = validator.get_person_id_validation_queries('foo', 'bar_deid')

        # post conditions
        self.assertEqual(len(results), ((len(self.all_tables) * 2) - 1))

        non_consent = validator.NOT_CONSENTING_PERSON_IDS
        orphan_records = validator.DELETE_ORPHANED_PERSON_IDS

        expected = []
        for table in self.mapped_tables:
            expected.append(non_consent.format(
                project='foo', dataset='bar_deid', mapping_dataset='bar', table=table
            ))

        for table in self.all_tables:
            expected.append(orphan_records.format(project='foo', dataset='bar_deid', table=table))

        self.assertEqual(expected, results)
