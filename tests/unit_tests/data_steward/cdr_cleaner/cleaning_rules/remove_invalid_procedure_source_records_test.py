# Python imports
import unittest

# Project imports
import cdr_cleaner.cleaning_rules.remove_invalid_procedure_source_records as remove_invalid_procedure_source
import constants.cdr_cleaner.clean_cdr as cdr_consts
from constants.cdr_cleaner import clean_cdr as clean_consts
from constants import bq_utils as bq_consts


class RemoveInvalidProcedureSourceRecordsTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = 'test_project_id'
        self.dataset_id = 'test_dataset_id'
        self.sandbox_dataset_id = 'test_sandbox_id'

    def test_query_generation(self):
        result = remove_invalid_procedure_source.get_remove_invalid_procedure_source_queries(
            self.project_id, self.dataset_id, self.sandbox_dataset_id)

        expected = list()
        expected.append({
            cdr_consts.QUERY:
                remove_invalid_procedure_source.
                INVALID_PROCEDURE_SOURCE_CONCEPT_IDS_QUERY.format(
                    project=self.project_id,
                    dataset=self.dataset_id,
                    table=remove_invalid_procedure_source.TABLE,
                    sandbox_dataset=self.sandbox_dataset_id,
                    intermediary_table=remove_invalid_procedure_source.
                    INTERMEDIARY_TABLE_NAME)
        })
        expected.append({
            cdr_consts.QUERY:
                remove_invalid_procedure_source.
                VALID_PROCEDURE_SOURCE_CONCEPT_IDS_QUERY.format(
                    project=self.project_id,
                    dataset=self.dataset_id,
                    table=remove_invalid_procedure_source.TABLE,
                    sandbox_dataset=self.sandbox_dataset_id,
                    intermediary_table=remove_invalid_procedure_source.
                    INTERMEDIARY_TABLE_NAME),
            clean_consts.DESTINATION_TABLE:
                remove_invalid_procedure_source.TABLE,
            clean_consts.DESTINATION_DATASET:
                self.dataset_id,
            clean_consts.DISPOSITION:
                bq_consts.WRITE_TRUNCATE
        })
        self.assertEquals(result, expected)
