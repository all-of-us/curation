import unittest
import mock
import common

import constants.cdr_cleaner.clean_cdr as cdr_consts
from cdr_cleaner.cleaning_rules.validate_missing_participant import (
    NUM_OF_MISSING_KEY_FIELDS, NUM_OF_MISSING_ALL_FIELDS, CAST_MISSING_COLUMN,
    SELECT_NON_MATCH_PARTICIPANTS_QUERY, CRITERION_COLUMN_TEMPLATE, KEY_FIELDS,
    PARTICIPANT_MATCH_EXCLUDED_FIELD)
from cdr_cleaner.cleaning_rules import validate_missing_participant
from constants.validation.participants import identity_match


class ValidateMissingParticipantTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.ehr_dataset_id = 'ehr_dataset_fake'
        self.validation_dataset_id = 'validation_dataset_id'
        self.project_id = 'project_id'
        self.hpo_id_1 = 'hpo_fake_1'
        self.hpo_id_2 = 'hpo_fake_2'
        self.participant_match_table = 'hpo_fake_1_participant_match'

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
        fields = [
            validate_missing_participant.FIRST_NAME_FIELD,
            validate_missing_participant.LAST_NAME_FIELD
        ]
        expected = validate_missing_participant.CAST_MISSING_COLUMN.format(
            column=fields[0]
        ) + ' + ' + validate_missing_participant.CAST_MISSING_COLUMN.format(
            column=fields[1])
        actual = validate_missing_participant.get_missing_criterion(fields)
        self.assertEqual(expected, actual)

    @mock.patch(
        'cdr_cleaner.cleaning_rules.validate_missing_participant.get_missing_criterion'
    )
    @mock.patch('resources.fields_for')
    def test_get_non_match_participant_query(self, mock_fields_for,
                                             mock_get_missing_criterion):
        fields = [{
            'name': identity_match.PERSON_ID_FIELD
        }, {
            'name': identity_match.FIRST_NAME_FIELD
        }, {
            'name': identity_match.LAST_NAME_FIELD
        }, {
            'name': identity_match.BIRTH_DATE_FIELD
        }, {
            'name': identity_match.EMAIL_FIELD
        }, {
            'name': validate_missing_participant.ALGORITHM_FIELD
        }]
        mock_fields_for.return_value = fields

        returned_missing_criteria = ['criterion_1', 'criterion_2']
        mock_get_missing_criterion.side_effect = returned_missing_criteria

        expected_criterion_one = CRITERION_COLUMN_TEMPLATE.format(
            column_expr=returned_missing_criteria[0],
            num_of_missing=NUM_OF_MISSING_KEY_FIELDS)

        expected_criterion_two = CRITERION_COLUMN_TEMPLATE.format(
            column_expr=returned_missing_criteria[1],
            num_of_missing=NUM_OF_MISSING_ALL_FIELDS)

        expected = SELECT_NON_MATCH_PARTICIPANTS_QUERY.format(
            project_id=self.project_id,
            validation_dataset_id=self.validation_dataset_id,
            participant_match=self.participant_match_table,
            criterion_one_expr=expected_criterion_one,
            criterion_two_expr=expected_criterion_two)

        actual = validate_missing_participant.get_non_match_participant_query(
            project_id=self.project_id,
            validation_dataset_id=self.validation_dataset_id,
            participant_match_table=self.participant_match_table)

        self.assertEqual(expected, actual)

        self.assertEqual(2, mock_get_missing_criterion.call_count)
        mock_get_missing_criterion.assert_any_call(KEY_FIELDS)
        mock_get_missing_criterion.assert_called_with([
            field['name']
            for field in fields
            if field['name'] not in PARTICIPANT_MATCH_EXCLUDED_FIELD
        ])
