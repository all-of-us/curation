# Python imports
from __future__ import print_function
import os
import unittest

# Third party imports
from mock import call, patch
from mock.mock import MagicMock

# Project imports
from constants import bq_utils as bq_consts
from constants.validation.participants import identity_match as consts

from validation.participants import identity_match as id_match

import test_util


class IdentityMatchTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):

        self.date_string: int = 20190503
        self.project: str = 'fake_project'
        self.dest_dataset: str = f'dest {self.date_string}'
        self.pii_dataset: str = f'pii {self.date_string}'
        self.rdr_dataset: str = f'rdr {self.date_string}'
        self.sites: list = ['fake_site_1', 'fake_site_2', 'fake_site_3']
        self.bucket_ids: list = [
            'fake_bucket_1', 'fake_bucket_2', 'fake_bucket_3'
        ]
        self.dataset_contents: list = [
            f'fake_site_1{consts.PII_NAME_TABLE}',
            f'fake_site_1{consts.PII_EMAIL_TABLE}',
            f'fake_site_1{consts.PII_PHONE_TABLE}',
            f'fake_site_1{consts.PII_ADDRESS_TABLE}',
            f'fake_site_1{consts.EHR_PERSON_TABLE_SUFFIX}',
            f'fake_site_2{consts.PII_NAME_TABLE}',
            f'fake_site_2{consts.PII_EMAIL_TABLE}',
            f'fake_site_2{consts.PII_PHONE_TABLE}',
            f'fake_site_2{consts.PII_ADDRESS_TABLE}',
            f'fake_site_2{consts.EHR_PERSON_TABLE_SUFFIX}',
        ]
        self.internal_bucket_id: str = 'internal_bucket_id'
        self.pid: int = 8888
        self.participant_info: dict = {
            'person_id': self.pid,
            'first': 'Fancy-Nancy',
            'middle': 'K',
            'last': 'Drew',
            'email': 'fancy-nancy_Drew_88@GMAIL.com',
            'phone': '(555) 867-5309',
            'street-one': '88 Lingerlost Rd',
            'street-two': 'Apt.   4E',
            'city': 'Frog Pond',
            'state': 'AL',
            'zip': '05645-1112',
            'sex': 'Female',
            'ehr_birthdate': '1990-01-01 00:00:00+00',
            'rdr_birthdate': '1990-01-01'
        }

        mock_list_ehr_tables = patch(
            'validation.participants.identity_match.bq_utils.list_dataset_contents'
        )
        self.mock_ehr_tables = mock_list_ehr_tables.start()
        self.mock_ehr_tables.return_value = self.dataset_contents
        self.addCleanup(mock_list_ehr_tables.stop)

        mock_dest_dataset_patcher = patch(
            'validation.participants.identity_match.bq_utils.create_dataset')
        self.mock_dest_dataset = mock_dest_dataset_patcher.start()
        self.mock_dest_dataset.return_value = {
            bq_consts.DATASET_REF: {
                bq_consts.DATASET_ID: self.dest_dataset
            }
        }
        self.addCleanup(mock_dest_dataset_patcher.stop)

        mock_match_tables_patcher = patch(
            'validation.participants.identity_match.readers.create_match_values_table'
        )
        self.mock_match_tables = mock_match_tables_patcher.start()
        self.addCleanup(mock_match_tables_patcher.stop)

        mock_site_names_patcher = patch(
            'validation.participants.identity_match.readers.get_hpo_site_names')
        self.mock_site_names = mock_site_names_patcher.start()
        self.mock_site_names.return_value = self.sites
        self.addCleanup(mock_site_names_patcher.stop)

        mock_pii_match_tables_patcher = patch(
            'validation.participants.identity_match.bq_utils.create_table')
        self.mock_pii_match_tables = mock_pii_match_tables_patcher.start()
        self.addCleanup(mock_pii_match_tables_patcher.stop)

        mock_ehr_person_values_patcher = patch(
            'validation.participants.identity_match.readers.get_ehr_person_values'
        )
        self.mock_ehr_person = mock_ehr_person_values_patcher.start()
        self.mock_ehr_person.side_effect = [
            {
                self.pid: 'Female'
            },
            {
                self.pid: self.participant_info.get('ehr_birthdate')
            },
            {
                self.pid: 'female'
            },
            {
                self.pid: self.participant_info.get('ehr_birthdate')
            },
        ]
        self.addCleanup(mock_ehr_person_values_patcher.stop)

        mock_rdr_match_values_patcher = patch(
            'validation.participants.identity_match.readers.get_rdr_match_values'
        )
        self.mock_rdr_values = mock_rdr_match_values_patcher.start()
        self.mock_rdr_values.side_effect = [
            {
                self.pid: self.participant_info.get('first')
            },
            {
                self.pid: self.participant_info.get('last')
            },
            {
                self.pid: self.participant_info.get('zip')
            },
            {
                self.pid: self.participant_info.get('city')
            },
            {
                self.pid: self.participant_info.get('state')
            },
            {
                self.pid: self.participant_info.get('street-one')
            },
            {
                self.pid: self.participant_info.get('street-two')
            },
            {
                self.pid: self.participant_info.get('email')
            },
            {
                self.pid: self.participant_info.get('phone')
            },
            {
                self.pid: 'Female'
            },
            {
                self.pid: self.participant_info.get('rdr_birthdate')
            },
            {
                self.pid: self.participant_info.get('first')
            },
            {
                self.pid: self.participant_info.get('last')
            },
            {
                self.pid: self.participant_info.get('zip')
            },
            {
                self.pid: self.participant_info.get('city')
            },
            {
                self.pid: self.participant_info.get('state')
            },
            {
                self.pid: self.participant_info.get('street-one')
            },
            {
                self.pid: self.participant_info.get('street-two')
            },
            {
                self.pid: self.participant_info.get('email')
            },
            {
                self.pid: self.participant_info.get('phone')
            },
            {
                self.pid: 'male'
            },
            {
                self.pid: self.participant_info.get('rdr_birthdate')
            },
        ]
        self.addCleanup(mock_rdr_match_values_patcher.stop)

        mock_pii_values_patcher = patch(
            'validation.participants.identity_match.readers.get_pii_values')
        self.mock_pii_values = mock_pii_values_patcher.start()
        self.mock_pii_values.side_effect = [
            [(self.pid, self.participant_info.get('first'))],
            [(self.pid, self.participant_info.get('last'))],
            [(self.pid, self.participant_info.get('email'))],
            [(self.pid, self.participant_info.get('phone'))],
            [(self.pid, self.participant_info.get('first'))],
            [(self.pid, self.participant_info.get('last'))],
            [(self.pid, self.participant_info.get('email'))],
            [(self.pid, self.participant_info.get('phone'))],
        ]
        self.addCleanup(mock_pii_values_patcher.stop)

        mock_write_to_result_table = patch(
            'validation.participants.identity_match.writers.write_to_result_table'
        )
        self.mock_table_write = mock_write_to_result_table.start()
        self.addCleanup(mock_write_to_result_table.stop)

        mock_location_pii_patcher = patch(
            'validation.participants.identity_match.readers.get_location_pii')
        self.mock_location_pii = mock_location_pii_patcher.start()
        self.mock_location_pii.side_effect = [
            [(self.pid, self.participant_info.get('zip'))],
            [(self.pid, self.participant_info.get('city'))],
            [(self.pid, self.participant_info.get('state'))],
            [(self.pid, self.participant_info.get('street-one'))],
            [(self.pid, self.participant_info.get('street-two'))],
            [(self.pid, self.participant_info.get('zip'))],
            [(self.pid, self.participant_info.get('city'))],
            [(self.pid, self.participant_info.get('state'))],
            [(self.pid, self.participant_info.get('street-one'))],
            [(self.pid, self.participant_info.get('street-two'))],
        ]
        self.addCleanup(mock_location_pii_patcher.stop)

        mock_validation_report_patcher = patch(
            'validation.participants.identity_match.writers.create_site_validation_report'
        )
        self.mock_validation_report = mock_validation_report_patcher.start()
        self.mock_validation_report.return_value = 0
        self.addCleanup(mock_validation_report_patcher.stop)

        self.mock_hpo_blobs: list = [MagicMock() for _ in range(3)]
        self.mock_drc_blob = MagicMock()
        self.mock_hpo_bucket = MagicMock()
        self.hpo_iterator = MagicMock()
        self.mock_drc_bucket = MagicMock()

        mock_client_patcher = patch(
            'validation.participants.identity_match.StorageClient')
        self.mock_client = mock_client_patcher.start()
        self.mock_client.get_drc_bucket.return_value = self.mock_drc_bucket
        self.mock_client.get_hpo_bucket.return_value = self.mock_hpo_bucket
        self.mock_drc_bucket.blob = self.mock_drc_blob
        self.mock_hpo_bucket.blob.side_effect = self.mock_hpo_blobs
        self.hpo_iterator.blob.side_effect = self.mock_hpo_blobs
        self.addCleanup(mock_client_patcher.stop)

    def test_match_participants_same_participant(self):
        # pre conditions

        # test
        id_match.match_participants(self.project, self.rdr_dataset,
                                    self.pii_dataset, self.dest_dataset)

        # post conditions
        self.assertEqual(self.mock_dest_dataset.call_count, 1)
        self.assertEqual(
            self.mock_dest_dataset.assert_called_with(
                dataset_id=self.dest_dataset,
                description=consts.DESTINATION_DATASET_DESCRIPTION.format(
                    version='',
                    rdr_dataset=self.rdr_dataset,
                    ehr_dataset=self.pii_dataset),
                overwrite_existing=True), None)

        self.assertEqual(self.mock_match_tables.call_count, 1)
        self.assertEqual(
            self.mock_match_tables.assert_called_with(self.project,
                                                      self.rdr_dataset,
                                                      self.dest_dataset), None)

        self.assertEqual(self.mock_site_names.call_count, 1)
        self.assertEqual(self.mock_site_names.assert_called_once_with(), None)

        num_sites: int = len(self.sites)
        self.assertEqual(self.mock_pii_match_tables.call_count, num_sites)

        self.assertEqual(self.mock_ehr_person.call_count, (num_sites - 1) * 2)
        self.assertEqual(self.mock_rdr_values.call_count, (num_sites - 1) * 11)
        self.assertEqual(self.mock_pii_values.call_count, (num_sites - 1) * 4)
        self.assertEqual(self.mock_table_write.call_count, num_sites)
        self.assertEqual(self.mock_location_pii.call_count, (num_sites - 1) * 5)
        self.assertEqual(self.mock_hpo_bucket.call_count, 0)
        self.assertEqual(self.mock_drc_bucket.call_count, 0)
        self.assertEqual(self.mock_validation_report.call_count, 0)

    def test_match_participants_same_participant_simulate_ehr_read_errors(self):
        # pre conditions
        self.mock_ehr_person.side_effect = test_util.mock_google_http_error(
            status_code=500, content=b'content', reason='reason')

        # test
        id_match.match_participants(self.project, self.rdr_dataset,
                                    self.pii_dataset, self.dest_dataset)

        # post conditions
        self.assertEqual(self.mock_dest_dataset.call_count, 1)
        self.assertEqual(
            self.mock_dest_dataset.assert_called_with(
                dataset_id=self.dest_dataset,
                description=consts.DESTINATION_DATASET_DESCRIPTION.format(
                    version='',
                    rdr_dataset=self.rdr_dataset,
                    ehr_dataset=self.pii_dataset),
                overwrite_existing=True), None)

        self.assertEqual(self.mock_match_tables.call_count, 1)
        self.assertEqual(
            self.mock_match_tables.assert_called_with(self.project,
                                                      self.rdr_dataset,
                                                      self.dest_dataset), None)

        self.assertEqual(self.mock_site_names.call_count, 1)
        self.assertEqual(self.mock_site_names.assert_called_once_with(), None)

        num_sites: int = len(self.sites)
        self.assertEqual(self.mock_pii_match_tables.call_count, num_sites)

        self.assertEqual(self.mock_ehr_person.call_count, (num_sites - 1) * 2)
        self.assertEqual(self.mock_rdr_values.call_count, (num_sites - 1) * 11)
        self.assertEqual(self.mock_pii_values.call_count, (num_sites - 1) * 4)
        self.assertEqual(self.mock_table_write.call_count, num_sites)
        self.assertEqual(self.mock_location_pii.call_count, (num_sites - 1) * 5)
        self.assertEqual(self.mock_hpo_bucket.call_count, 0)
        self.assertEqual(self.mock_drc_bucket.call_count, 0)
        self.assertEqual(self.mock_validation_report.call_count, 0)

    def test_match_participants_same_participant_simulate_write_errors(self):
        # pre conditions
        self.mock_table_write.side_effect = test_util.mock_google_http_error(
            status_code=500, content=b'content', reason='reason')

        # test
        id_match.match_participants(self.project, self.rdr_dataset,
                                    self.pii_dataset, self.dest_dataset)

        # post conditions
        self.assertEqual(self.mock_dest_dataset.call_count, 1)
        self.assertEqual(
            self.mock_dest_dataset.assert_called_with(
                dataset_id=self.dest_dataset,
                description=consts.DESTINATION_DATASET_DESCRIPTION.format(
                    version='',
                    rdr_dataset=self.rdr_dataset,
                    ehr_dataset=self.pii_dataset),
                overwrite_existing=True), None)

        self.assertEqual(self.mock_match_tables.call_count, 1)
        self.assertEqual(
            self.mock_match_tables.assert_called_with(self.project,
                                                      self.rdr_dataset,
                                                      self.dest_dataset), None)

        self.assertEqual(self.mock_site_names.call_count, 1)
        self.assertEqual(self.mock_site_names.assert_called_once_with(), None)

        num_sites: int = len(self.sites)
        self.assertEqual(self.mock_pii_match_tables.call_count, num_sites)

        self.assertEqual(self.mock_ehr_person.call_count, (num_sites - 1) * 2)
        self.assertEqual(self.mock_rdr_values.call_count, (num_sites - 1) * 11)
        self.assertEqual(self.mock_pii_values.call_count, (num_sites - 1) * 4)
        self.assertEqual(self.mock_table_write.call_count, num_sites)
        self.assertEqual(self.mock_location_pii.call_count, (num_sites - 1) * 5)
        self.assertEqual(self.mock_hpo_bucket.call_count, 0)
        self.assertEqual(self.mock_drc_bucket.call_count, 0)
        self.assertEqual(self.mock_validation_report.call_count, 0)

    def test_match_participants_same_participant_simulate_location_pii_read_errors(
        self):
        # pre conditions
        self.mock_location_pii.side_effect = test_util.mock_google_http_error(
            status_code=500, content=b'content', reason='reason')

        # test
        id_match.match_participants(self.project, self.rdr_dataset,
                                    self.pii_dataset, self.dest_dataset)

        # post conditions
        self.assertEqual(self.mock_dest_dataset.call_count, 1)
        self.assertEqual(
            self.mock_dest_dataset.assert_called_with(
                dataset_id=self.dest_dataset,
                description=consts.DESTINATION_DATASET_DESCRIPTION.format(
                    version='',
                    rdr_dataset=self.rdr_dataset,
                    ehr_dataset=self.pii_dataset),
                overwrite_existing=True), None)

        self.assertEqual(self.mock_match_tables.call_count, 1)
        self.assertEqual(
            self.mock_match_tables.assert_called_with(self.project,
                                                      self.rdr_dataset,
                                                      self.dest_dataset), None)

        self.assertEqual(self.mock_site_names.call_count, 1)
        self.assertEqual(self.mock_site_names.assert_called_once_with(), None)

        num_sites: int = len(self.sites)
        self.assertEqual(self.mock_pii_match_tables.call_count, num_sites)

        self.assertEqual(self.mock_ehr_person.call_count, (num_sites - 1) * 2)
        self.assertEqual(self.mock_rdr_values.call_count, (num_sites - 1) * 11)
        self.assertEqual(self.mock_pii_values.call_count, (num_sites - 1) * 4)
        self.assertEqual(self.mock_table_write.call_count, num_sites)
        self.assertEqual(self.mock_location_pii.call_count, (num_sites - 1) * 4)
        self.assertEqual(self.mock_hpo_bucket.call_count, 0)
        self.assertEqual(self.mock_drc_bucket.call_count, 0)
        self.assertEqual(self.mock_validation_report.call_count, 0)

    def test_match_participants_same_participant_simulate_pii_read_errors(self):
        # pre conditions
        self.mock_pii_values.side_effect = test_util.mock_google_http_error(
            status_code=500, content=b'content', reason='reason')

        # test
        id_match.match_participants(self.project, self.rdr_dataset,
                                    self.pii_dataset, self.dest_dataset)

        # post conditions
        self.assertEqual(self.mock_dest_dataset.call_count, 1)
        self.assertEqual(
            self.mock_dest_dataset.assert_called_with(
                dataset_id=self.dest_dataset,
                description=consts.DESTINATION_DATASET_DESCRIPTION.format(
                    version='',
                    rdr_dataset=self.rdr_dataset,
                    ehr_dataset=self.pii_dataset),
                overwrite_existing=True), None)

        self.assertEqual(self.mock_match_tables.call_count, 1)
        self.assertEqual(
            self.mock_match_tables.assert_called_with(self.project,
                                                      self.rdr_dataset,
                                                      self.dest_dataset), None)

        self.assertEqual(self.mock_site_names.call_count, 1)
        self.assertEqual(self.mock_site_names.assert_called_once_with(), None)

        num_sites: int = len(self.sites)
        self.assertEqual(self.mock_pii_match_tables.call_count, num_sites)

        self.assertEqual(self.mock_ehr_person.call_count, (num_sites - 1) * 2)
        self.assertEqual(self.mock_rdr_values.call_count, (num_sites - 1) * 11)
        self.assertEqual(self.mock_pii_values.call_count, (num_sites - 1) * 4)
        self.assertEqual(self.mock_table_write.call_count, num_sites)
        self.assertEqual(self.mock_location_pii.call_count, (num_sites - 1) * 5)
        self.assertEqual(self.mock_hpo_bucket.call_count, 0)
        self.assertEqual(self.mock_drc_bucket.call_count, 0)
        self.assertEqual(self.mock_validation_report.call_count, 0)

    def test_write_results_to_site_buckets(self):

        # test
        id_match.write_results_to_site_buckets(self.mock_client,
                                               self.dest_dataset)

        # post conditions
        num_sites: int = len(self.sites)
        self.assertEqual(self.mock_client.get_hpo_bucket.call_count, num_sites)

        expected_report_calls = [
            call(self.mock_client, self.dest_dataset, [self.sites[0]],
                 self.hpo_iterator.blob()),
            call(self.mock_client, self.dest_dataset, [self.sites[1]],
                 self.hpo_iterator.blob()),
            call(self.mock_client, self.dest_dataset, [self.sites[2]],
                 self.hpo_iterator.blob())
        ]

        self.assertEqual(self.mock_validation_report.mock_calls,
                         expected_report_calls)

    def test_write_results_to_site_buckets_simulate_errors(self):
        # pre conditions
        self.mock_validation_report.return_value = 1

        # test
        id_match.write_results_to_site_buckets(self.mock_client,
                                               self.dest_dataset)

        # post conditions
        num_sites: int = len(self.sites)
        self.assertEqual(self.mock_client.get_hpo_bucket.call_count, num_sites)

        expected_report_calls = [
            call(self.mock_client, self.dest_dataset, [self.sites[0]],
                 self.hpo_iterator.blob()),
            call(self.mock_client, self.dest_dataset, [self.sites[1]],
                 self.hpo_iterator.blob()),
            call(self.mock_client, self.dest_dataset, [self.sites[2]],
                 self.hpo_iterator.blob()),
        ]
        self.assertEqual(self.mock_validation_report.mock_calls,
                         expected_report_calls)

    def test_write_results_to_site_buckets_None_dataset(self):
        # test
        self.assertRaises(RuntimeError, id_match.write_results_to_site_buckets,
                          self.project, None)

    def test_write_results_to_drc_bucket(self):
        # test
        id_match.write_results_to_drc_bucket(self.mock_client,
                                             self.dest_dataset)

        # post conditions
        self.assertEqual(self.mock_client.get_drc_bucket.call_count, 1)

        drc_filename: str = os.path.join(
            self.dest_dataset,
            consts.REPORT_DIRECTORY.format(date=self.date_string),
            consts.REPORT_TITLE)

        self.mock_drc_blob.name = drc_filename
        expected_report_calls: list = [
            call(self.mock_client, self.dest_dataset, self.sites,
                 self.mock_client.get_drc_bucket().blob())
        ]

        self.assertEqual(self.mock_validation_report.mock_calls,
                         expected_report_calls)

    def test_write_results_to_drc_bucket_simulate_error(self):
        # pre conditions
        self.mock_validation_report.return_value = 2  # error count

        # test
        id_match.write_results_to_drc_bucket(self.mock_client,
                                             self.dest_dataset)

        # post conditions
        self.assertEqual(self.mock_client.get_drc_bucket.call_count, 1)

        expected_report_calls: list = [
            call(self.mock_client, self.dest_dataset, self.sites,
                 self.mock_client.get_drc_bucket().blob())
        ]
        self.assertEqual(self.mock_validation_report.mock_calls,
                         expected_report_calls)

    def test_write_results_to_drc_bucket_None_dataset(self):
        # test
        self.assertRaises(RuntimeError, id_match.write_results_to_drc_bucket,
                          self.project, None)
