import unittest
import mock
import common

import constants.cdr_cleaner.clean_cdr as cdr_consts
from cdr_cleaner.cleaning_rules import validate_missing_participant


class ValidateMissingParticipantTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.ehr_dataset_id = 'ehr_dataset_fake'
        self.dataset_id = 'dataset_id'
        self.project_id = 'project_id'
        self.hpo_id_1 = 'hpo_fake_1'
        self.hpo_id_2 = 'hpo_fake_2'

    @mock.patch('bq_utils.table_exists')
    @mock.patch('bq_utils.get_table_id')
    def test_exist_participant_match(self, mock_get_table_id,
                                     mock_table_exists):

        table_id_1 = self.hpo_id_1 + validate_missing_participant.PARTICIPANT_MATCH
        table_id_2 = self.hpo_id_2 + validate_missing_participant.PARTICIPANT_MATCH
        mock_get_table_id.side_effect = [table_id_1, table_id_2]
        mock_table_exists.side_effect = [True, False]

        self.assertTrue(
            validate_missing_participant.exist_participant_match(
                self.ehr_dataset_id, self.hpo_id_1))

        mock_table_exists.assert_called_with(table_id_1, self.ehr_dataset_id)

        self.assertFalse(
            validate_missing_participant.exist_participant_match(
                self.ehr_dataset_id, self.hpo_id_1))

        mock_table_exists.assert_called_with(table_id_2, self.ehr_dataset_id)

    def test_get_missing_criterion(self):
        pass
