# Python imports
from unittest import TestCase, mock

# Third party imports
import pandas as pd

# Project imports
import app_identity
import constants.bq_utils as bq_consts
from tools import add_hpo
from common import PIPELINE_TABLES, SITE_MASKING_TABLE_ID


class AddHPOTest(TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = 'project_id'
        self.dataset_id = 'dataset_id'
        self.sandbox_dataset_id = 'sandbox_dataset_id'
        self.table_id = 'site_maskings'
        self.hpo_site_mappings_path = 'hpo_site_mappings_path'
        self.us_state = 'PIIState_XY'
        self.value_source_concept_id = 9999999
        self.hpo_id = 1010101

    def test_verify_hpo_mappings_up_to_date(self):
        df_1 = pd.DataFrame({'HPO_ID': ['FAKE_1', 'FAKE_2']})
        df_2 = pd.DataFrame({'hpo_id': ['fake_1', 'fake_2']})
        add_hpo.verify_hpo_mappings_up_to_date(df_1, df_2)
        df_3 = pd.DataFrame({'hpo_id': ['fake_1', 'fake_3']})
        self.assertRaises(ValueError, add_hpo.verify_hpo_mappings_up_to_date,
                          df_1, df_3)

    @mock.patch('bq_utils.get_hpo_info')
    @mock.patch('tools.add_hpo.pd.read_csv')
    def test_add_hpo_site_mappings_file_df(self, mock_read_csv, mock_hpo_info):
        mock_hpo_info.return_value = [{
            'hpo_id': 'fake_1',
            'name': 'fake_name_1'
        }, {
            'hpo_id': 'fake_2',
            'name': 'fake_name_2'
        }, {
            'hpo_id': 'fake_4',
            'name': 'fake_name_4'
        }]
        mock_read_csv.return_value = pd.DataFrame({
            'Org_ID': ['fake_org_1', 'fake_org_2', 'fake_org_4'],
            'HPO_ID': ['fake_1', 'fake_2', 'fake_4'],
            'Site_Name': ['fake_name_1', 'fake_name_2', 'fake_name_4'],
            'Display_Order': [1, 2, 3]
        })

        new_site = {
            'org_id': 'fake_org_3',
            'hpo_id': 'fake_3',
            'hpo_name': 'fake_name_3',
            'display_order': 3
        }
        actual_df = add_hpo.add_hpo_site_mappings_file_df(
            new_site['hpo_id'], new_site['hpo_name'], new_site['org_id'],
            self.hpo_site_mappings_path, new_site['display_order'])

        expected_df = pd.DataFrame({
            'Org_ID': ['fake_org_1', 'fake_org_2', 'fake_org_3', 'fake_org_4'],
            'HPO_ID': ['fake_1', 'fake_2', 'fake_3', 'fake_4'],
            'Site_Name': [
                'fake_name_1', 'fake_name_2', 'fake_name_3', 'fake_name_4'
            ],
            'Display_Order': [1, 2, 3, 4]
        })

        pd.testing.assert_frame_equal(actual_df.reset_index(drop=True),
                                      expected_df.reset_index(drop=True))

        self.assertRaises(ValueError, add_hpo.add_hpo_site_mappings_file_df,
                          new_site['hpo_id'], new_site['hpo_name'],
                          new_site['org_id'], self.hpo_site_mappings_path,
                          new_site['display_order'])

    @mock.patch('tools.add_hpo.BigQueryClient')
    def test_update_site_masking_table(self, mock_bq_client):
        # Mocks the job return
        query_job_reference_results = mock.MagicMock(
            name="query_job_reference_results")
        query_job_reference_results.return_value = query_job_reference_results
        query_job_reference_results.errors = []

        mock_call_response = mock.MagicMock(project=self.project_id)
        mock_call_response.query.return_value = query_job_reference_results
        mock_bq_client.return_value = mock_call_response
        mock_query = mock_bq_client.return_value.query

        mock_query.side_effect = query_job_reference_results

        # Test
        actual_job = add_hpo.update_site_masking_table(
            mock_bq_client(), self.hpo_id, self.us_state,
            self.value_source_concept_id)

        # Post conditions
        update_site_masking_query = add_hpo.UPDATE_SITE_MASKING_QUERY.render(
            project_id=self.project_id,
            pipeline_tables_dataset=PIPELINE_TABLES,
            site_maskings_table=SITE_MASKING_TABLE_ID,
            lookup_tables_dataset=bq_consts.LOOKUP_TABLES_DATASET_ID,
            hpo_site_id_mappings_table=bq_consts.HPO_SITE_ID_MAPPINGS_TABLE_ID,
            us_state=self.us_state,
            hpo_id=self.hpo_id,
            value_source_concept_id=self.value_source_concept_id)

        expected_job = query_job_reference_results
        mock_query.assert_any_call(update_site_masking_query)
        self.assertEqual(actual_job, expected_job)

    def test_check_state_code_format(self):
        us_state = 'XY'
        self.assertRaises(ValueError, add_hpo.check_state_code_format, us_state)

        us_state = 'PIIState_XY'
        add_hpo.check_state_code_format(us_state)
