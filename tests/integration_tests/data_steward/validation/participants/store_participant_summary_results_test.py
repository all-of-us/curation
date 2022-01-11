"""
Integration Test for the store_participant_summary_results module

Ensures that a time-partitioned table is created and populated with participant summary api
results.

Original Issue: DC-1214
"""

# Python imports
import os
import time
from unittest import mock, TestCase

# Third party imports
from pandas import DataFrame
from google.cloud import bigquery

# Project imports
from validation.participants.store_participant_summary_results import get_hpo_info
from utils.bq import get_client
from common import JINJA_ENV, PS_API_VALUES
from app_identity import PROJECT_ID
from constants import bq_utils as bq_consts

PS_API_CONTENTS_QUERY = JINJA_ENV.from_string("""
    SELECT
        person_id, first_name, last_name
    FROM `{{project_id}}.{{dataset_id}}.{{ps_api_table_id}}`
""")


class StoreParticipantSummaryResultsTest(TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        cls.project_id = os.environ.get(PROJECT_ID)
        cls.dataset_id = os.environ.get('COMBINED_DATASET_ID')
        cls.client = get_client(cls.project_id)

        cls.hpo_id = 'fake_hpo'
        cls.org_id = 'fake_org'
        cls.ps_api_table = f'{PS_API_VALUES}_{cls.hpo_id}'

        cls.fq_table_names = [
            f'{cls.project_id}.{cls.dataset_id}.{cls.ps_api_table}'
        ]

    def test_get_hpo_info(self):
        GET_HPO_QUERY = JINJA_ENV.from_string("""
            SELECT LOWER(HPO_ID) AS hpo_id, Org_ID AS org_id
            FROM `{{project_id}}.{{dataset_id}}.{{tablename}}`
            WHERE HPO_ID IS NOT NULL AND Site_Name IS NOT NULL
        """)

        query = GET_HPO_QUERY.render(
            project_id=self.project_id,
            dataset_id=bq_consts.LOOKUP_TABLES_DATASET_ID,
            tablename=bq_consts.HPO_SITE_ID_MAPPINGS_TABLE_ID)

        job = self.client.query(query)
        df = job.result().to_dataframe()
        expected = df.to_dict(orient='records')
        actual = get_hpo_info(self.project_id)

        self.assertCountEqual(actual, expected)

    @mock.patch('tools.store_participant_summary_results.bq.get_table_schema')
    @mock.patch(
        'tools.store_participant_summary_results.get_org_participant_information'
    )
    def test_main(self, mock_get_org_participant_information,
                  mock_get_table_schema):
        data = [{
            'person_id': 1,
            'first_name': 'John',
            'last_name': 'Smith'
        }, {
            'person_id': 2,
            'first_name': 'Jane',
            'last_name': 'Doe'
        }]
        data_df = DataFrame(data)
        mock_get_org_participant_information.return_value = data_df
        mock_get_table_schema.return_value = [
            bigquery.SchemaField('person_id', 'integer'),
            bigquery.SchemaField('first_name', 'string'),
            bigquery.SchemaField('last_name', 'string')
        ]
        main(self.project_id,
             'rdr_project',
             self.org_id,
             self.hpo_id,
             dataset_id=self.dataset_id)

        query = PS_API_CONTENTS_QUERY.render(project_id=self.project_id,
                                             dataset_id=self.dataset_id,
                                             ps_api_table_id=self.ps_api_table)

        job = self.client.query(query)
        results_df = job.result().to_dataframe()
        actual = results_df.to_dict(orient='records')
        expected = data

        self.assertCountEqual(actual, expected)

    def tearDown(self):
        """
        Add a one second delay to teardown to make it less likely to fail due to rate limits.
        """
        time.sleep(1)
        for table in self.fq_table_names:
            self.client.delete_table(table, not_found_ok=True)
