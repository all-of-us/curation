# Python imports
import copy
import unittest

# Third party imports

# Project imports
import common
from constants import bq_utils as bq_consts
from constants.cdr_cleaner import clean_cdr as clean_consts
from cdr_cleaner.cleaning_rules import drop_rows_for_missing_persons
from cdr_cleaner.cleaning_rules import person_id_validator as validator
import resources

SANDBOX_DDL = """
CREATE OR REPLACE TABLE `{{project}}.{{sandbox_dataset}}.{{sandbox_table}}` AS
SELECT *
"""


# to use method from DropMissingParticipants
def drop_rows_for_missing_persons_sandbox_for(affected_table):
    issue_numbers_str = '_'.join([
        issue_num.lower()
        for issue_num in set(drop_rows_for_missing_persons.ISSUE_NUMBERS)
    ])
    return f'{issue_numbers_str}_{affected_table}'


class PersonIDValidatorTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project = 'foo'
        self.dataset = 'bar'
        self.sandbox = f'{self.dataset}_sandbox'
        self.deid_dataset = 'bar_deid'
        self.deid_sandbox = f'{self.deid_dataset}_sandbox'
        self.mapped_tables = copy.copy(common.MAPPED_CLINICAL_DATA_TABLES)
        self.drop_tables = copy.copy(
            drop_rows_for_missing_persons.TABLES_TO_DELETE_FROM)

    def test_get_person_id_validation_queries(self):
        # pre conditions

        # test
        results = validator.get_person_id_validation_queries(
            self.project, self.dataset, self.sandbox)

        # post conditions
        self.assertEqual(len(results),
                         len(self.mapped_tables) + len(self.drop_tables) * 2)

        existing_and_consenting = validator.EXISTING_AND_VALID_CONSENTING_RECORDS
        existing_in_person_table = drop_rows_for_missing_persons.RECORDS_FOR_NON_EXISTING_PIDS

        expected = []
        for table in self.mapped_tables:
            field_names = [
                'entry.' + field['name']
                for field in resources.fields_for(table)
            ]
            fields = ', '.join(field_names)

            expected.append({
                clean_consts.QUERY:
                    existing_and_consenting.format(project=self.project,
                                                   dataset=self.dataset,
                                                   mapping_dataset=self.dataset,
                                                   table=table,
                                                   fields=fields),
                clean_consts.DESTINATION_TABLE:
                    table,
                clean_consts.DESTINATION_DATASET:
                    self.dataset,
                clean_consts.DISPOSITION:
                    bq_consts.WRITE_TRUNCATE,
            })

        for table in self.drop_tables:
            sandbox_ddl_query = common.JINJA_ENV.from_string(
                SANDBOX_DDL).render(
                    project=self.project,
                    sandbox_dataset=self.sandbox,
                    sandbox_table=drop_rows_for_missing_persons_sandbox_for(
                        table))
            expected.append({
                clean_consts.QUERY:
                    existing_in_person_table.render(
                        query_type=sandbox_ddl_query,
                        project=self.project,
                        dataset=self.dataset,
                        table=table)
            })
            expected.append({
                clean_consts.QUERY:
                    existing_in_person_table.render(query_type='DELETE',
                                                    project=self.project,
                                                    dataset=self.dataset,
                                                    table=table)
            })

        self.assertEqual(expected, results)

    def test_get_person_id_validation_queries_deid(self):
        # pre conditions

        # test
        results = validator.get_person_id_validation_queries(
            self.project, 'bar_deid', 'bar_deid_sandbox')

        # post conditions
        self.assertEqual(len(results),
                         len(self.mapped_tables) + len(self.drop_tables) * 2)

        existing_and_consenting = validator.EXISTING_AND_VALID_CONSENTING_RECORDS
        existing_in_person_table = drop_rows_for_missing_persons.RECORDS_FOR_NON_EXISTING_PIDS

        expected = []
        for table in self.mapped_tables:
            field_names = [
                'entry.' + field['name']
                for field in resources.fields_for(table)
            ]
            fields = ', '.join(field_names)

            expected.append({
                clean_consts.QUERY:
                    existing_and_consenting.format(project=self.project,
                                                   dataset='bar_deid',
                                                   mapping_dataset=self.dataset,
                                                   table=table,
                                                   fields=fields),
                clean_consts.DESTINATION_TABLE:
                    table,
                clean_consts.DESTINATION_DATASET:
                    'bar_deid',
                clean_consts.DISPOSITION:
                    bq_consts.WRITE_TRUNCATE,
            })

        for table in self.drop_tables:
            sandbox_ddl_query = common.JINJA_ENV.from_string(
                SANDBOX_DDL).render(
                    project=self.project,
                    sandbox_dataset=self.deid_sandbox,
                    sandbox_table=drop_rows_for_missing_persons_sandbox_for(
                        table))
            expected.append({
                clean_consts.QUERY:
                    existing_in_person_table.render(
                        query_type=sandbox_ddl_query,
                        project=self.project,
                        dataset=self.deid_dataset,
                        table=table)
            })
            expected.append({
                clean_consts.QUERY:
                    existing_in_person_table.render(query_type='DELETE',
                                                    project=self.project,
                                                    dataset=self.deid_dataset,
                                                    table=table)
            })

        self.assertEqual(expected, results)
