# Python imports
import unittest

# Third party imports
import mock
from mock import patch
import pandas as pd

# Project imports
import cdr_cleaner.cleaning_rules.remove_aian_participants as remove_aian


class RemoveAIANParticipantsTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = 'project_id'
        self.dataset_id = 'dataset_id'
        self.sandbox_id = 'sandbox_id'
        self.pids_list = [
            324264993, 307753491, 335484227, 338965846, 354812933, 324983298,
            366423185, 352721597, 352775367, 314281264, 319123185, 325306942,
            324518105, 320577401, 339641873, 329210551, 364674103, 339564778,
            309381334, 352068257, 353001073, 319604059, 336744297, 357830316,
            352653514, 349988031, 349731310, 359249014, 361359486, 315083772,
            358741126, 312045923, 313427389, 341366267, 305170199, 308597253,
            348834424, 325536292, 360363123
        ]
        mock_bq_query_patcher = patch(
            'cdr_cleaner.cleaning_rules.sandbox_and_remove_pids.utils.bq.query')
        self.mock_bq_query = mock_bq_query_patcher.start()
        self.mock_bq_query.return_value = pd.DataFrame()
        self.addCleanup(mock_bq_query_patcher.stop)

    @mock.patch(
        'cdr_cleaner.cleaning_rules.sandbox_and_remove_pids.utils.bq.query')
    def test_get_pids_list(self, mock_query):
        mock_query.return_value = pd.DataFrame(self.pids_list,
                                               columns=['person_id'])
        result = remove_aian.get_pids_list(
            self.project_id, self.dataset_id,
            remove_aian.PIDS_QUERY.format(project=self.project_id,
                                          dataset=self.dataset_id))
        self.assertListEqual(self.pids_list, result)

    @mock.patch(
        'cdr_cleaner.cleaning_rules.sandbox_and_remove_pids.get_sandbox_queries'
    )
    @mock.patch(
        'cdr_cleaner.cleaning_rules.remove_aian_participants.get_pids_list')
    @mock.patch(
        'cdr_cleaner.cleaning_rules.sandbox_and_remove_pids.get_remove_pids_queries'
    )
    def test_query_generation(self, mock_get_sandbox_queries,
                              mock_get_pids_list, mock_get_remove_pids_queries):

        mock_get_pids_list.return_value = self.pids_list
        result = remove_aian.get_queries(self.project_id, self.dataset_id,
                                         self.sandbox_id)

        expected = list()
        expected.extend(mock_get_remove_pids_queries.return_value)
        expected.extend(mock_get_sandbox_queries.return_value)

        self.assertEquals(result, expected)
