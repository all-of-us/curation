import unittest
import mock
import bq_utils
import common

import constants.cdr_cleaner.clean_cdr as cdr_consts
from cdr_cleaner.cleaning_rules.validate_missing_participant import (
    NUM_OF_MISSING_KEY_FIELDS, NUM_OF_MISSING_ALL_FIELDS, PERSON_ID_FIELD,
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
        self.ehr_dataset_id = 'ehr_dataset_id'
        self.combined_dataset_id = 'combined_dataset_fake'
        self.validation_dataset_id = 'validation_dataset_id'
        self.project_id = 'project_id'
        self.hpo_id_1 = 'hpo_fake_1'
        self.hpo_id_2 = 'hpo_fake_2'
        self.participant_match_table = 'hpo_fake_1_participant_match'
        self.non_match_participant_query = 'query'
        self.biqquery_job_id = 'job-id-1'
        self.query_results = {'jobReference': {'jobId': self.biqquery_job_id}}
        self.query_results_rows = [{
            PERSON_ID_FIELD: 1
        }, {
            PERSON_ID_FIELD: 20
        }, {
            PERSON_ID_FIELD: 31
        }]
        self.person_ids = [
            row[PERSON_ID_FIELD] for row in self.query_results_rows
        ]
        self.drop_participant_query_dict = {
            cdr_consts.QUERY: 'drop participants',
            cdr_consts.BATCH: True,
            cdr_consts.DESTINATION_DATASET: self.combined_dataset_id
        }

        self.drop_domain_records_query_dicts = [{
            cdr_consts.QUERY: 'drop condition occurrence',
            cdr_consts.DESTINATION_TABLE: common.CONDITION_OCCURRENCE,
            cdr_consts.DESTINATION_DATASET: self.combined_dataset_id
        }, {
            cdr_consts.QUERY: 'drop procedure occurrence',
            cdr_consts.DESTINATION_TABLE: common.PROCEDURE_OCCURRENCE,
            cdr_consts.DESTINATION_DATASET: self.combined_dataset_id
        }]

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
                self.combined_dataset_id, self.hpo_id_1))

        mock_table_exists.assert_called_with(table_id_1,
                                             self.combined_dataset_id)

        self.assertFalse(
            validate_missing_participant.exist_participant_match(
                self.combined_dataset_id, self.hpo_id_1))

        mock_table_exists.assert_called_with(table_id_2,
                                             self.combined_dataset_id)

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

    def test_get_delete_persons_query(self):
        expected_query = validate_missing_participant.DELETE_PERSON_IDS_QUERY.format(
            project_id=self.project_id,
            dataset_id=self.combined_dataset_id,
            person_ids=','.join(map(str, self.person_ids)))
        expected = {cdr_consts.QUERY: expected_query, cdr_consts.BATCH: True}

        actual = validate_missing_participant.get_delete_persons_query(
            self.project_id, self.combined_dataset_id, self.person_ids)

        self.assertEqual(expected, actual)

    @mock.patch('bq_utils.response2rows')
    @mock.patch('bq_utils.wait_on_jobs')
    @mock.patch('bq_utils.query')
    @mock.patch(
        'cdr_cleaner.cleaning_rules.validate_missing_participant.get_non_match_participant_query'
    )
    @mock.patch('bq_utils.get_table_id')
    def test_get_list_non_match_participants(
        self, mock_get_table_id, mock_get_non_match_participant_query,
        mock_query, mock_wait_on_jobs, mock_response2rows):
        mock_get_table_id.return_value = self.participant_match_table
        mock_get_non_match_participant_query.return_value = self.non_match_participant_query
        mock_query.return_value = self.query_results
        mock_wait_on_jobs.side_effect = [[], [self.biqquery_job_id]]
        mock_response2rows.return_value = self.query_results_rows

        actual = validate_missing_participant.get_list_non_match_participants(
            self.project_id, self.validation_dataset_id, self.hpo_id_1)

        self.assertListEqual(self.person_ids, actual)

        mock_get_table_id.assert_called_with(
            self.hpo_id_1, validate_missing_participant.PARTICIPANT_MATCH)

        mock_get_non_match_participant_query.assert_called_with(
            self.project_id, self.validation_dataset_id,
            self.participant_match_table)

        mock_query.assert_called_with(q=self.non_match_participant_query)
        mock_wait_on_jobs.assert_called_with([self.biqquery_job_id])

        with self.assertRaises(bq_utils.BigQueryJobWaitError):
            validate_missing_participant.get_list_non_match_participants(
                self.project_id, self.validation_dataset_id, self.hpo_id_1)

    @mock.patch(
        'cdr_cleaner.cleaning_rules.drop_rows_for_missing_persons.get_queries')
    @mock.patch(
        'cdr_cleaner.cleaning_rules.validate_missing_participant.get_delete_persons_query'
    )
    @mock.patch(
        'cdr_cleaner.cleaning_rules.validate_missing_participant.get_list_non_match_participants'
    )
    @mock.patch(
        'cdr_cleaner.cleaning_rules.validate_missing_participant.exist_participant_match'
    )
    @mock.patch('validation.participants.readers.get_hpo_site_names')
    def test_delete_records_for_non_matching_participants(
        self, mock_get_hpo_site_names, mock_exist_participant_match,
        mock_get_list_non_match_participants, mock_get_delete_persons_query,
        mock_get_queries):

        mock_get_hpo_site_names.return_value = [self.hpo_id_1, self.hpo_id_2]
        mock_exist_participant_match.side_effect = [True, False]
        mock_get_list_non_match_participants.return_value = self.person_ids
        mock_get_delete_persons_query.return_value = self.drop_participant_query_dict
        mock_get_queries.return_value = self.drop_domain_records_query_dicts

        expected = [self.drop_participant_query_dict
                   ] + self.drop_domain_records_query_dicts
        actual = validate_missing_participant.delete_records_for_non_matching_participants(
            self.project_id, self.ehr_dataset_id, self.validation_dataset_id,
            self.combined_dataset_id)

        self.assertListEqual(expected, actual)

        mock_exist_participant_match.assert_any_call(self.project_id,
                                                     self.ehr_dataset_id,
                                                     self.hpo_id_1)

        mock_exist_participant_match.assert_called_with(self.project_id,
                                                        self.ehr_dataset_id,
                                                        self.hpo_id_2)

        mock_get_list_non_match_participants.assert_called_with(
            self.project_id, self.validation_dataset_id, self.hpo_id_2)

        mock_get_delete_persons_query.assert_called_with(
            self.project_id, self.combined_dataset_id, self.person_ids)
        mock_get_queries.assert_called_with(self.project_id,
                                            self.combined_dataset_id)
