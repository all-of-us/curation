# Python imports
import copy
import unittest

# Third party imports

# Project imports
import constants.bq_utils as bq_consts
import constants.cleaners.clean_cdr as clean_consts
import cleaners.query_generators.person_id_validator as validator
import resources


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
            field_names = ['entry.' + field['name'] for field in resources.fields_for(table)]
            fields = ', '.join(field_names)

            expected.append(
                {
                    clean_consts.QUERY: non_consent.format(
                        project='foo', dataset='bar', mapping_dataset='bar', table=table, fields=fields
                    ),
                    clean_consts.DESTINATION_TABLE: table,
                    clean_consts.DESTINATION_DATASET: 'bar',
                    clean_consts.DISPOSITION: bq_consts.WRITE_TRUNCATE,
                }
            )

        for table in self.all_tables:
            field_names = [field['name'] for field in resources.fields_for(table)]
            fields = ', '.join(field_names)

            expected.append(
                {
                    clean_consts.QUERY: orphan_records.format(
                        project='foo', dataset='bar', table=table, fields=fields
                    ),
                    clean_consts.DESTINATION_TABLE: table,
                    clean_consts.DESTINATION_DATASET: 'bar',
                    clean_consts.DISPOSITION: bq_consts.WRITE_TRUNCATE,
                }
            )

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
            field_names = ['entry.' + field['name'] for field in resources.fields_for(table)]
            fields = ', '.join(field_names)

            expected.append(
                {
                    clean_consts.QUERY: non_consent.format(
                        project='foo', dataset='bar_deid', mapping_dataset='bar', table=table, fields=fields
                    ),
                    clean_consts.DESTINATION_TABLE: table,
                    clean_consts.DESTINATION_DATASET: 'bar_deid',
                    clean_consts.DISPOSITION: bq_consts.WRITE_TRUNCATE,
                }
            )

        for table in self.all_tables:
            field_names = [field['name'] for field in resources.fields_for(table)]
            fields = ', '.join(field_names)

            expected.append(
                {
                    clean_consts.QUERY: orphan_records.format(
                        project='foo', dataset='bar_deid', table=table, fields=fields
                    ),
                    clean_consts.DESTINATION_TABLE: table,
                    clean_consts.DESTINATION_DATASET: 'bar_deid',
                    clean_consts.DISPOSITION: bq_consts.WRITE_TRUNCATE,
                }
            )

        self.assertEqual(expected, results)
