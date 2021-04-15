# Python imports
from unittest import TestCase, mock

# Third party imports
import pandas as pd

# Project imports
from tools import add_hpo
from cdr_cleaner.cleaning_rules.deid.generate_site_mappings_and_ext_tables import GenerateSiteMappingsAndExtTables


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

        # Tools for mocking the client
        mock_bq_client_patcher = mock.patch('utils.bq.get_client')
        self.mock_bq_client = mock_bq_client_patcher.start()
        self.addCleanup(mock_bq_client_patcher.stop)

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
            new_site['display_order'])

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
                          new_site['org_id'], new_site['display_order'])

    @mock.patch(
        'cdr_cleaner.cleaning_rules.deid.generate_site_mappings_and_ext_tables')
    @mock.patch('utils.bq.get_client')
    def test_update_site_masking_table(self, mock_client,
                                       mock_generate_site_mappings):
        # Preconditions
        client = mock_client.return_value = self.mock_bq_client
        client.side_effects = add_hpo.update_site_masking_table()

        # test
        add_hpo.update_site_masking_table()

        # Post conditions
        self.assertEqual(mock_client.call_count, 2)
