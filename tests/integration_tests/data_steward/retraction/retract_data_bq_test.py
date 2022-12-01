# Python imports
import os
from io import open
from unittest import TestCase, mock
import logging

# Third party imports
from google.cloud import bigquery
import pandas as pd

# Project imports
import app_identity
from common import JINJA_ENV
from gcloud.bq import BigQueryClient
from tests import test_util
from retraction import retract_data_bq as rbq

TABLE_ROWS_QUERY = 'SELECT * FROM {dataset_id}.__TABLES__ '

EXPECTED_ROWS_QUERY = """
SELECT COUNT(*) as count FROM {{dataset_id}}.{{table_id}}
{% if retraction_type == 'only_ehr' and table_id.endswith('person') %}
-- person table does not need retraction when only_ehr --
WHERE 0 = 1
{% else %}
WHERE person_id IN (
    SELECT person_id FROM {{dataset_id}}.{{lookup_table_id}}
)
{% endif %}
"""

INSERT_PID_TABLE = """
INSERT INTO {{dataset_id}}.{{lookup_table_id}} (person_id, research_id)
VALUES {{person_research_ids}}
"""


class RetractDataBqTest(TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.hpo_id = 'fake'
        self.project_id = 'fake-project-id'
        self.test_project_id = app_identity.get_application_id()
        self.lookup_table_id = 'pid_rid_to_retract'
        self.bq_dataset_id = os.environ.get('COMBINED_DATASET_ID')
        self.bq_client = BigQueryClient(self.test_project_id)
        self.dataset_ids = 'all_datasets'
        self.retraction_type = 'rdr_and_ehr'
        self.person_research_ids = [(1, 6890173), (2, 858761),
                                    (1234567, 4589763)]

    @mock.patch('retraction.retract_utils.is_deid_dataset')
    @mock.patch('retraction.retract_utils.is_combined_dataset')
    @mock.patch('retraction.retract_utils.is_unioned_dataset')
    @mock.patch('retraction.retract_utils.is_ehr_dataset')
    @mock.patch('retraction.retract_utils.get_datasets_list')
    def test_integration_queries_to_retract_from_fake_dataset(
        self, mock_list_datasets, mock_is_ehr_dataset, mock_is_unioned_dataset,
        mock_is_combined_dataset, mock_is_deid_dataset):
        mock_list_datasets.return_value = [self.bq_dataset_id]
        mock_is_deid_dataset.return_value = False
        mock_is_combined_dataset.return_value = True
        mock_is_unioned_dataset.return_value = False
        mock_is_ehr_dataset.return_value = False

        # create and load person_ids to pid table
        self.bq_client.create_tables(
            [
                f'{self.test_project_id}.{self.bq_dataset_id}.{self.lookup_table_id}'
            ],
            exists_ok=False,
            fields=[[{
                "type": "integer",
                "name": "person_id",
                "mode": "required",
                "description": "The person_id to retract data for"
            }, {
                "type": "integer",
                "name": "research_id",
                "mode": "nullable",
                "description": "The research_id corresponding to the person_id"
            }]])
        bq_formatted_insert_values = ', '.join([
            f'({person_id}, {research_id})'
            for (person_id, research_id) in self.person_research_ids
        ])
        q = JINJA_ENV.from_string(INSERT_PID_TABLE).render(
            dataset_id=self.bq_dataset_id,
            lookup_table_id=self.lookup_table_id,
            person_research_ids=bq_formatted_insert_values)
        job = self.bq_client.query(q)
        job.result()

        row_count_queries = {}
        # load the cdm files into dataset
        for cdm_file in test_util.NYC_FIVE_PERSONS_FILES:
            cdm_file_name = os.path.basename(cdm_file)
            cdm_table = cdm_file_name.split('.')[0]
            hpo_table = f'{self.hpo_id}_{cdm_table}'
            # store query for checking number of rows to delete
            row_count_queries[hpo_table] = JINJA_ENV.from_string(
                EXPECTED_ROWS_QUERY).render(
                    dataset_id=self.bq_dataset_id,
                    table_id=hpo_table,
                    lookup_table_id=self.lookup_table_id,
                    retraction_type=self.retraction_type)
            logging.info(
                f'Preparing to load table {self.bq_dataset_id}.{hpo_table}')
            with open(cdm_file, 'rb') as f:
                job_config = bigquery.LoadJobConfig()
                job_config.source_format = bigquery.SourceFormat.CSV
                job_config.skip_leading_rows = 1
                job_config.write_disposition = 'WRITE_EMPTY'
                job_config.schema = self.bq_client.get_table_schema(cdm_table)
                load_job = self.bq_client.load_table_from_file(
                    f,
                    f'{self.test_project_id}.{self.bq_dataset_id}.{hpo_table}',
                    job_config=job_config)
                load_job.result()
        logging.info('All tables loaded successfully')

        # use query results to count number of expected row deletions
        expected_row_count = {}
        for table in row_count_queries:
            job = self.bq_client.query(row_count_queries[table])
            result = job.result()
            expected_row_count[table] = result.to_dataframe()['count'].to_list(
            )[0]

        # separate check to find number of actual deleted rows
        q = TABLE_ROWS_QUERY.format(dataset_id=self.bq_dataset_id)
        job = self.bq_client.query(q)
        result = job.result().to_dataframe()
        row_counts_before_retraction = pd.Series(
            result.row_count.values, index=result.table_id).to_dict()

        # perform retraction
        rbq.run_bq_retraction(self.test_project_id, self.bq_dataset_id,
                              self.lookup_table_id, self.hpo_id,
                              self.dataset_ids, self.retraction_type)

        # find actual deleted rows
        job = self.bq_client.query(q)
        result = job.result().to_dataframe()
        row_counts_after_retraction = pd.Series(
            result.row_count.values, index=result.table_id).to_dict()

        for table in expected_row_count:
            self.assertEqual(
                expected_row_count[table], row_counts_before_retraction[table] -
                row_counts_after_retraction[table])

    def tearDown(self):
        for table in self.bq_client.list_tables(self.bq_dataset_id):
            self.bq_client.delete_table(table, not_found_ok=True)
