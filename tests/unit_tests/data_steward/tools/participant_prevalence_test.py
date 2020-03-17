import unittest

import mock
import pandas as pd

import common
from tools import participant_prevalence as ptpr
from constants.tools import participant_prevalence as consts


class ParticipantPrevalenceTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = 'project_id'
        self.dataset_id = 'dataset_id'
        self.ehr_dataset_id = 'ehr_dataset_id'
        self.pid_project_id = 'pid_project_id'
        self.sandbox_dataset_id = 'sandbox_dataset_id'
        self.pid_table_id = 'pid_table_id'
        self.hpo_id = 'fake'
        self.cdm_pid_tables = [
            common.OBSERVATION, common.VISIT_OCCURRENCE, common.MEASUREMENT,
            common.OBSERVATION_PERIOD, common.CONDITION_OCCURRENCE
        ]
        self.pid_tables = [
            common.PERSON, common.PII_NAME, common.PARTICIPANT_MATCH,
            common.DEATH
        ]
        self.pids_list = [1, 2, 3, 4]
        self.pids_string = str(self.pids_list)[1:-1]
        self.cdm_counts = [0, 0, 0, 5, 0]
        self.non_cdm_counts = [0, 0, 6, 8]
        self.zero_counts = [0 for _ in self.cdm_counts + self.non_cdm_counts]

    def test_get_pids(self):
        expected = ', '.join([str(pid) for pid in self.pids_list])
        actual = ptpr.get_pids(self.pids_list)
        self.assertEqual(expected, actual)

        self.assertRaises(ValueError, ptpr.get_pids)
        self.assertRaises(ValueError, ptpr.get_pids, None, self.pid_project_id,
                          self.sandbox_dataset_id)

        actual = ptpr.get_pids(None, self.pid_project_id,
                               self.sandbox_dataset_id, self.pid_table_id)
        self.assertIn(self.pid_project_id, actual)
        self.assertIn(self.sandbox_dataset_id, actual)
        self.assertIn(self.pid_table_id, actual)

    @mock.patch('utils.bq.query')
    def test_get_cdm_tables_with_person_id(self, mock_bq_query):
        expected = self.cdm_pid_tables
        columns_dict = {consts.TABLE_NAME_COLUMN: self.cdm_pid_tables}
        mock_bq_query.return_value = pd.DataFrame(data=columns_dict)
        actual = ptpr.get_cdm_tables_with_person_id(self.project_id,
                                                    self.dataset_id)
        self.assertEqual(actual, expected)

    @mock.patch(
        'cdr_cleaner.cleaning_rules.sandbox_and_remove_pids.get_tables_with_person_id'
    )
    @mock.patch('tools.participant_prevalence.get_cdm_tables_with_person_id')
    def test_get_pid_counts_query(self, mock_pid_cdm_tables, mock_pid_tables):
        mock_pid_cdm_tables.return_value = self.cdm_pid_tables
        mock_pid_tables.return_value = self.pid_tables + self.cdm_pid_tables

        actual = ptpr.get_pid_counts_query(self.project_id,
                                           self.dataset_id,
                                           self.hpo_id,
                                           self.pids_string,
                                           for_cdm=True)
        self.assertEqual(len(self.cdm_pid_tables),
                         len(actual.split(consts.UNION_ALL)))
        for table in self.cdm_pid_tables:
            self.assertIn(table, actual)

        actual = ptpr.get_pid_counts_query(self.project_id,
                                           self.dataset_id,
                                           self.hpo_id,
                                           self.pids_string,
                                           for_cdm=False)
        self.assertEqual(len(self.pid_tables),
                         len(actual.split(consts.UNION_ALL)))
        for table in self.pid_tables:
            self.assertIn(table, actual)

    @mock.patch('tools.participant_prevalence.get_pid_counts_query')
    def test_get_pid_counts(self, mock_pids_query):
        mock_pids_query.return_value = None
        expected = pd.DataFrame(
            columns=[consts.TABLE_ID, consts.ALL_COUNT, consts.EHR_COUNT])

        actual = ptpr.get_pid_counts(self.project_id,
                                     self.dataset_id,
                                     self.hpo_id,
                                     self.pids_string,
                                     for_cdm=False)
        pd.testing.assert_frame_equal(actual, expected)

    @mock.patch('tools.participant_prevalence.get_pid_counts')
    def test_get_non_zero_counts(self, mock_pid_counts):
        cdm_counts_dict = {
            consts.TABLE_ID: self.cdm_pid_tables,
            consts.EHR_COUNT: self.zero_counts[:5],
            consts.ALL_COUNT: self.cdm_counts
        }
        non_cdm_counts_dict = {
            consts.TABLE_ID: self.pid_tables,
            consts.EHR_COUNT: self.zero_counts[:4],
            consts.ALL_COUNT: self.non_cdm_counts
        }
        cdm_counts_df = pd.DataFrame(data=cdm_counts_dict)
        non_cdm_counts_df = pd.DataFrame(data=non_cdm_counts_dict)

        expected_dict = {
            consts.TABLE_ID: self.cdm_pid_tables + self.pid_tables,
            consts.EHR_COUNT: self.zero_counts,
            consts.ALL_COUNT: self.cdm_counts + self.non_cdm_counts
        }
        expected = pd.DataFrame(data=expected_dict)
        expected = expected[expected[consts.ALL_COUNT] > 0]

        mock_pid_counts.side_effect = [cdm_counts_df, non_cdm_counts_df]
        actual = ptpr.get_non_zero_counts(self.project_id, self.dataset_id,
                                          self.hpo_id, self.pids_string)
        pd.testing.assert_frame_equal(actual.reset_index(drop=True),
                                      expected.reset_index(drop=True))

        expected_dict = {
            consts.TABLE_ID: self.cdm_pid_tables + self.pid_tables,
            consts.EHR_COUNT: self.cdm_counts + self.non_cdm_counts,
            consts.ALL_COUNT: self.cdm_counts + self.non_cdm_counts
        }
        expected = pd.DataFrame(data=expected_dict)
        expected = expected[expected[consts.ALL_COUNT] > 0]

        mock_pid_counts.side_effect = [cdm_counts_df, non_cdm_counts_df]
        actual = ptpr.get_non_zero_counts(self.project_id, self.ehr_dataset_id,
                                          self.hpo_id, self.pids_string)
        pd.testing.assert_series_equal(actual.get(consts.EHR_COUNT),
                                       actual.get(consts.ALL_COUNT),
                                       check_names=False)
        pd.testing.assert_frame_equal(actual.reset_index(drop=True),
                                      expected.reset_index(drop=True))
