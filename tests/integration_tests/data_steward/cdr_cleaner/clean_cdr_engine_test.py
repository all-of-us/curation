# Python imports
import os
from unittest import TestCase

# Third party imports
from google.cloud.bigquery import LoadJobConfig, TableReference

# Project imports
import sandbox
from tests import test_util
from utils import bq
from app_identity import get_application_id
import cdr_cleaner.clean_cdr_engine as ce
import cdr_cleaner.cleaning_rules.update_family_history_qa_codes as update_family_history
from cdr_cleaner.cleaning_rules.clean_ppi_numeric_fields_using_parameters import CleanPPINumericFieldsUsingParameters


class CleanCDREngineTest(TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = get_application_id()
        self.dataset_id = os.environ.get('UNIONED_DATASET_ID')
        self.client = bq.get_client()

        self.tearDown()
        self.tables = []
        for table_path in test_util.NYC_FIVE_PERSONS_FILES:
            table_file = os.path.basename(table_path)
            table = table_file.split('.')[0]
            self.tables.append(table)
            bq.create_tables(self.client,
                             self.project_id,
                             [f'{self.project_id}.{self.dataset_id}.{table}'],
                             exists_ok=False,
                             fields=None)
            table_ref = TableReference.from_string(
                f'{self.project_id}.{self.dataset_id}.{table}')
            job_config = LoadJobConfig()
            job_config.source_format = 'CSV'
            job_config.skip_leading_rows = 1
            with open(table_path, 'rb') as f:
                self.client.load_table_from_file(f, table_ref, job_config)

    def test_clean_engine_v1(self):
        jobs = ce.clean_dataset_v1(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            rules=[(CleanPPINumericFieldsUsingParameters,),
                   (update_family_history.get_update_family_history_qa_queries,)
                  ])
        queries = ce.get_query_list(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            rules=[(CleanPPINumericFieldsUsingParameters,),
                   (update_family_history.get_update_family_history_qa_queries,)
                  ])
        self.assertEqual(len(jobs), len(queries))

    def tearDown(self) -> None:
        for table_path in test_util.NYC_FIVE_PERSONS_FILES:
            table_file = os.path.basename(table_path)
            table = table_file.split('.')[0]
            fq_table = f'{self.project_id}.{self.dataset_id}.{table}'
            self.client.delete_table(fq_table, not_found_ok=True)
        sandbox_dataset_id = sandbox.get_sandbox_dataset_id(self.dataset_id)
        self.client.delete_dataset(sandbox_dataset_id,
                                   delete_contents=True,
                                   not_found_ok=True)
