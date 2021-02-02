# Python imports
from unittest import TestCase, mock

# Third party imports
import pandas as pd
from google.cloud.bigquery import TableReference

import constants.retraction.retract_deactivated_pids as consts
# Project imports
from retraction import retract_deactivated_pids as rdp


class RetractDeactivatedEHRDataBqTest(TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = 'fake_project_id'
        self.dataset_id = 'fake_dataset_id'

        self.pids_project_id = 'fake_pids_project_id'
        self.pids_dataset_id = 'fake_pids_dataset_id'
        self.pids_table = 'fake_pids_table'

        self.deactivated_pids_dataset_id = 'fake_deactivated_dataset_id'
        self.deactivated_pids_table = 'fake_deactivated_table'

        mock_bq_client_patcher = mock.patch(
            'retraction.retract_deactivated_pids.bq.get_client')
        self.mock_bq_client = mock_bq_client_patcher.start()
        self.addCleanup(mock_bq_client_patcher.stop)

    def test_get_date_cols_dict(self):
        date_cols = ["visit_date", "measurement_date", "measurement_datetime"]
        expected = {
            consts.DATE: "measurement_date",
            consts.DATETIME: "measurement_datetime"
        }
        actual = rdp.get_date_cols_dict(date_cols)
        self.assertDictEqual(expected, actual)

        date_cols = [
            "verbatim_date", "condition_end_date", "condition_end_datetime",
            "condition_start_datetime", "condition_start_date"
        ]
        expected = {
            consts.START_DATE: "condition_start_date",
            consts.START_DATETIME: "condition_start_datetime",
            consts.END_DATE: "condition_end_date",
            consts.END_DATETIME: "condition_end_datetime"
        }
        actual = rdp.get_date_cols_dict(date_cols)
        self.assertDictEqual(expected, actual)

    @mock.patch('utils.sandbox.check_and_create_sandbox_dataset')
    @mock.patch('retraction.retract_deactivated_pids.get_table_dates_info')
    @mock.patch('retraction.retract_deactivated_pids.get_table_cols_df')
    def test_generate_queries(self, mock_table_cols, mock_table_dates,
                              mock_sandbox):
        table_cols_dict = {
            "condition_occurrence": [
                "condition_start_date", "condition_start_datetime",
                "condition_end_date", "condition_end_datetime"
            ],
            "measurement": ["measurement_date", "measurement_datetime"],
            'drug_exposure': [
                'drug_exposure_start_date', 'drug_exposure_start_datetime',
                'drug_exposure_end_date', 'drug_exposure_end_datetime',
                'verbatim_end_date'
            ]
        }

        table_cols_df = pd.DataFrame.from_dict({
            "table_name": [
                "condition_occurrence", "_mapping_condition_occurrence",
                "measurement", "measurement_ext", "drug_exposure_ext",
                "drug_exposure"
            ]
        })

        mock_table_cols.return_value = table_cols_df
        mock_table_dates.return_value = table_cols_dict
        mock_sandbox.return_value = self.dataset_id + '_sandbox'
        pid_rid_table_ref = TableReference.from_string(
            f"{self.project_id}.{self.pids_dataset_id}.{self.pids_table}")
        deactivated_pids_table_ref = TableReference.from_string(
            f"{self.project_id}.{self.deactivated_pids_dataset_id}.{self.deactivated_pids_table}"
        )

        queries = rdp.generate_queries(self.mock_bq_client, self.project_id,
                                       self.dataset_id, pid_rid_table_ref,
                                       deactivated_pids_table_ref)

        mock_table_cols.called_once_with(self.mock_bq_client, self.project_id,
                                         self.dataset_id)
        mock_table_dates.called_once_with(table_cols_df)

        # count sandbox and clean queries
        self.assertEqual(len(table_cols_dict) * 2, len(queries))

    def test_get_dates_info(self):
        # preconditions
        data = {
            'table_catalog': ['project'] * 13,
            'table_schema': ['dataset'] * 13,
            'table_name': ['observation'] * 5 + ['location'] * 2 +
                          ['drug_exposure'] * 6,
            'column_name': [
                'observation_id', 'person_id', 'observation_concept_id',
                'observation_date', 'observation_datetime', 'location_id',
                'city', 'person_id', 'drug_exposure_start_date',
                'drug_exposure_start_datetime', 'drug_exposure_end_date',
                'drug_exposure_end_datetime', 'verbatim_date'
            ],
        }
        table_cols_df = pd.DataFrame(data,
                                     columns=[
                                         'table_catalog', 'table_schema',
                                         'table_name', 'column_name'
                                     ])

        expected_dict = {
            'observation': ['observation_date', 'observation_datetime'],
            'drug_exposure': [
                'drug_exposure_start_date', 'drug_exposure_start_datetime',
                'drug_exposure_end_date', 'drug_exposure_end_datetime',
                'verbatim_date'
            ]
        }
        actual_dict = rdp.get_table_dates_info(table_cols_df)

        self.assertDictEqual(actual_dict, expected_dict)

    def test_parser(self):
        parser = rdp.get_parser()
        fake_dataset_1 = 'fake_dataset_1'
        fq_deactivated_table = f'{self.project_id}.{self.dataset_id}.{self.pids_table}'
        test_args = [
            '-p', self.project_id, '-d', self.dataset_id, fake_dataset_1, '-a',
            fq_deactivated_table
        ]
        args = parser.parse_args(test_args)
        expected_datasets = [self.dataset_id, fake_dataset_1]
        actual_datasets = args.dataset_ids
        self.assertEqual(expected_datasets, actual_datasets)
        self.assertEqual(fq_deactivated_table, args.fq_deact_table)

        test_args = [
            '-p', self.project_id, '-d', self.dataset_id, fake_dataset_1, '-a',
            f'{self.project_id}.{self.dataset_id}'
        ]
        self.assertRaises((rdp.argparse.ArgumentError, SystemExit),
                          parser.parse_args, test_args)
