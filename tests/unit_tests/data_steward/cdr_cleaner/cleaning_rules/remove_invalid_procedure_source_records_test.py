# Python imports
import unittest

# Project imports
import cdr_cleaner.cleaning_rules.remove_invalid_procedure_source_records as remove_invalid_procedure_source
import constants.cdr_cleaner.clean_cdr as cdr_consts


class RemoveInvalidProcedureSourceRecordsTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = 'test_project_id'
        self.dataset_id = 'test_dataset_id'

    def test_query_generation(self):
        result = remove_invalid_procedure_source.get_remove_invalid_procedure_source_queries(
            self.project_id, self.dataset_id)

        expected = list()
        expected.append({
            cdr_consts.QUERY:
                remove_invalid_procedure_source.
                INVALID_PROCEDURE_SOURCE_CONCEPT_IDS_QUERY.format(
                    project=self.project_id,
                    dataset=self.dataset_id,
                    sandbox_dataset=remove_invalid_procedure_source.
                    get_sandbox_dataset_id(self.dataset_id),
                    intermediary_table=remove_invalid_procedure_source.
                    INTERMEDIARY_TABLE_NAME)
        })
        expected.append({
            cdr_consts.QUERY:
                remove_invalid_procedure_source.
                VALID_PROCEDURE_SOURCE_CONCEPT_IDS_QUERY.format(
                    project=self.project_id,
                    dataset=self.dataset_id,
                    sandbox_dataset=remove_invalid_procedure_source.
                    get_sandbox_dataset_id(self.dataset_id),
                    intermediary_table=remove_invalid_procedure_source.
                    INTERMEDIARY_TABLE_NAME)
        })
        self.assertEquals(result, expected)
