import mock
import unittest

import validation.participants.consts as consts
import validation.participants.identity_match as id_match


class IdentityMatchTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.dataset = 'combined20190211'
        self.pid = 8888
        self.participant_info = {
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
            'ehr_birthdate': '1990-01-01 00:00:00+00',
            'reported_birthdate': '1990-01-01'
        }

        self.observation_values = {
            consts.OBS_PII_NAME_FIRST:
                {self.pid: self.participant_info.get('first')},
            consts.OBS_PII_NAME_MIDDLE:
                {self.pid: self.participant_info.get('middle')},
            consts.OBS_PII_NAME_LAST:
                {self.pid: self.participant_info.get('last')},
            consts.OBS_PII_EMAIL_ADDRESS:
                {self.pid: self.participant_info.get('email')},
            consts.OBS_PII_PHONE:
                {self.pid: self.participant_info.get('phone')},
            consts.OBS_PII_STREET_ADDRESS_ONE:
                {self.pid: self.participant_info.get('street-one')},
            consts.OBS_PII_STREET_ADDRESS_TWO:
                {self.pid: self.participant_info.get('street-two')},
            consts.OBS_PII_STREET_ADDRESS_CITY:
                {self.pid: self.participant_info.get('city')},
            consts.OBS_PII_STREET_ADDRESS_STATE:
                {self.pid: self.participant_info.get('state')},
            consts.OBS_PII_STREET_ADDRESS_ZIP:
                {self.pid: self.participant_info.get('zip')},
            consts.OBS_EHR_BIRTH_DATETIME:
                {self.pid: self.participant_info.get('ehr_birthdate')},
            consts.OBS_PII_BIRTH_DATETIME:
                {self.pid: self.participant_info.get('reported_birthdate')},
        }

        mock_obs_values_patcher = mock.patch('validation.participants.identity_match._get_observation_match_values')
        mock_site_names_patcher = mock.patch('validation.participants.identity_match._get_hpo_site_names')
        mock_pii_names_patcher = mock.patch('validation.participants.identity_match._get_pii_names')
        mock_pii_emails_patcher = mock.patch('validation.participants.identity_match._get_pii_emails')
        mock_pii_phone_numbers_patcher = mock.patch('validation.participants.identity_match._get_pii_phone_numbers')
        mock_pii_addresses_patcher = mock.patch('validation.participants.identity_match._get_pii_addresses')

        self.mock_obs_values = mock_obs_values_patcher.start()
        self.mock_obs_values.return_value = self.observation_values
        self.addCleanup(mock_obs_values_patcher.stop)

        self.mock_site_names = mock_site_names_patcher.start()
        self.mock_site_names.return_value = ['bogus-site']
        self.addCleanup(mock_site_names_patcher.stop)

        self.mock_pii_names = mock_pii_names_patcher.start()
        self.mock_pii_names.return_value = [
            (self.pid,
             self.participant_info.get('first'),
             self.participant_info.get('middle'),
             self.participant_info.get('last'))]
        self.addCleanup(mock_pii_names_patcher.stop)

        self.mock_pii_emails = mock_pii_emails_patcher.start()
        self.mock_pii_emails.return_value = [
            (self.pid,
             self.participant_info.get('email'))]
        self.addCleanup(mock_pii_emails_patcher.stop)

        self.mock_pii_phone_numbers = mock_pii_phone_numbers_patcher.start()
        self.mock_pii_phone_numbers.return_value = [
            (self.pid,
             self.participant_info.get('phone'))]
        self.addCleanup(mock_pii_phone_numbers_patcher.stop)

        self.mock_pii_addresses = mock_pii_addresses_patcher.start()
        self.mock_pii_addresses.return_value = [
            (self.pid,
             self.participant_info.get('street-one'),
             self.participant_info.get('street-two'),
             self.participant_info.get('city'),
             self.participant_info.get('state'),
             self.participant_info.get('zip'))]
        self.addCleanup(mock_pii_addresses_patcher.stop)

        self.expected = {self.pid:
            {
                consts.FIRST_NAME: consts.MATCH,
                consts.MIDDLE_NAME: consts.MATCH,
                consts.LAST_NAME: consts.MATCH,
                consts.EMAIL: consts.MATCH,
                consts.CONTACT_PHONE: consts.MATCH,
                consts.STREET_ONE: consts.MATCH,
                consts.STREET_TWO: consts.MATCH,
                consts.CITY: consts.MATCH,
                consts.STATE: consts.MATCH,
                consts.ZIP: consts.MATCH,
                consts.BIRTHDATE: consts.MATCH,
            }
        }

    def test_match_participants_same_participant(self):
        # pre conditions

        # test
        results = id_match.match_participants(self.dataset)

        # post conditions
        self.assertEqual(results, self.expected)

    def test_match_participants_different_names(self):
        # pre conditions
        self.mock_pii_names.return_value = [(self.pid, 'George', '', 'Costanza')]

        # test
        results = id_match.match_participants(self.dataset)

        # post conditions
        self.expected[self.pid][consts.FIRST_NAME] = consts.MISMATCH
        self.expected[self.pid][consts.MIDDLE_NAME] = consts.MISMATCH
        self.expected[self.pid][consts.LAST_NAME] = consts.MISMATCH

        self.assertEqual(results, self.expected)

    def test_match_participants_none_and_integer_name(self):
        # pre conditions
        self.mock_pii_names.return_value = [(self.pid, 99, 'k', None)]

        # test
        results = id_match.match_participants(self.dataset)

        # post conditions
        self.expected[self.pid][consts.FIRST_NAME] = consts.MISMATCH
        self.expected[self.pid][consts.LAST_NAME] = consts.MISMATCH

        self.assertEqual(results, self.expected)

    def test_match_participants_different_emails(self):
        # pre conditions
        self.mock_pii_emails.return_value = [(self.pid, 'oscar-THE-grouch@aol.com')]

        # test
        results = id_match.match_participants(self.dataset)

        # post conditions
        self.expected[self.pid][consts.EMAIL] = consts.MISMATCH
        self.assertEqual(results, self.expected)

    def test_match_participants_none_emails(self):
        # pre conditions
        self.mock_pii_emails.return_value = [(self.pid, None)]

        # test
        results = id_match.match_participants(self.dataset)

        # post conditions
        self.expected[self.pid][consts.EMAIL] = consts.MISMATCH
        self.assertEqual(results, self.expected)

    def test_match_participants_integer_emails(self):
        # pre conditions
        self.mock_pii_emails.return_value = [(self.pid, 99)]

        # test
        results = id_match.match_participants(self.dataset)

        # post conditions
        self.expected[self.pid][consts.EMAIL] = consts.MISMATCH
        self.assertEqual(results, self.expected)

    def test_match_participants_different_phone_numbers(self):
        # pre conditions
        self.mock_pii_phone_numbers.return_value = [(self.pid, '867-5309')]

        # test
        results = id_match.match_participants(self.dataset)

        # post conditions
        self.expected[self.pid][consts.CONTACT_PHONE] = consts.MISMATCH
        self.assertEqual(results, self.expected)

    def test_match_participants_none_phone_numbers(self):
        # pre conditions
        self.mock_pii_phone_numbers.return_value = [(self.pid, None)]

        # test
        results = id_match.match_participants(self.dataset)

        # post conditions
        self.expected[self.pid][consts.CONTACT_PHONE] = consts.MISMATCH
        self.assertEqual(results, self.expected)

    def test_match_participants_integer_phone_numbers(self):
        # pre conditions
        self.mock_pii_phone_numbers.return_value = [(self.pid, 5558675309)]

        # test
        results = id_match.match_participants(self.dataset)

        # post conditions
        self.assertEqual(results, self.expected)

    def test_match_participants_formatted_phone_numbers(self):
        # pre conditions
        self.mock_pii_phone_numbers.return_value = [(self.pid, '(555) 867-5309')]

        # test
        results = id_match.match_participants(self.dataset)

        # post conditions
        self.assertEqual(results, self.expected)

    def test_match_participants_different_address_fields(self):
        # pre conditions
        self.mock_pii_addresses.return_value = [
            (self.pid,
             '99 Problems',
             '',
             'Oakland',
             'MI',
             '90210')]

        # test
        results = id_match.match_participants(self.dataset)

        # post conditions
        self.expected[self.pid][consts.STREET_ONE] = consts.MISMATCH
        self.expected[self.pid][consts.STREET_TWO] = consts.MISMATCH
        self.expected[self.pid][consts.CITY] = consts.MISMATCH
        self.expected[self.pid][consts.STATE] = consts.MISMATCH
        self.expected[self.pid][consts.ZIP] = consts.MISMATCH
        self.assertEqual(results, self.expected)

    def test_match_participants_none_address_fields(self):
        # pre conditions
        self.mock_pii_addresses.return_value = [
            (self.pid,
             None,
             None,
             None,
             None,
             None)]

        # test
        results = id_match.match_participants(self.dataset)

        # post conditions
        self.expected[self.pid][consts.STREET_ONE] = consts.MISMATCH
        self.expected[self.pid][consts.STREET_TWO] = consts.MISMATCH
        self.expected[self.pid][consts.CITY] = consts.MISMATCH
        self.expected[self.pid][consts.STATE] = consts.MISMATCH
        self.expected[self.pid][consts.ZIP] = consts.MISMATCH
        self.assertEqual(results, self.expected)

    def test_match_participants_integer_address_fields(self):
        # pre conditions
        self.mock_pii_addresses.return_value = [
            (self.pid,
             1,
             2,
             3,
             44,
             90210)]

        # test
        results = id_match.match_participants(self.dataset)

        # post conditions
        self.expected[self.pid][consts.STREET_ONE] = consts.MISMATCH
        self.expected[self.pid][consts.STREET_TWO] = consts.MISMATCH
        self.expected[self.pid][consts.CITY] = consts.MISMATCH
        self.expected[self.pid][consts.STATE] = consts.MISMATCH
        self.expected[self.pid][consts.ZIP] = consts.MISMATCH
        self.assertEqual(results, self.expected)

    def test_match_participants_split_but_same_street_address(self):
        # pre conditions
        self.mock_pii_addresses.return_value = [
            (self.pid,
             '88 Lingerlost Road   Apt.   4E',
             '',
             'Frog Pond',
             'AL',
             '05645')]

        # test
        results = id_match.match_participants(self.dataset)

        # post conditions
        self.assertEqual(results, self.expected)

    def test_match_participants_different_birthdates(self):
        # pre conditions
        self.observation_values[consts.OBS_EHR_BIRTH_DATETIME][self.pid] = '2010-01-01'
        self.mock_obs_values.return_value = self.observation_values

        # test
        results = id_match.match_participants(self.dataset)

        # post conditions
        self.expected[self.pid][consts.BIRTHDATE] = consts.MISMATCH
        self.assertEqual(results, self.expected)

    def test_match_participants_none_birthdate(self):
        # pre conditions
        self.observation_values[consts.OBS_EHR_BIRTH_DATETIME][self.pid] = None
        self.mock_obs_values.return_value = self.observation_values

        # test
        results = id_match.match_participants(self.dataset)

        # post conditions
        self.expected[self.pid][consts.BIRTHDATE] = consts.MISMATCH
        self.assertEqual(results, self.expected)

    def test_match_participants_none_birthdates(self):
        # pre conditions
        self.observation_values[consts.OBS_EHR_BIRTH_DATETIME][self.pid] = None
        self.observation_values[consts.OBS_PII_BIRTH_DATETIME][self.pid] = None
        self.mock_obs_values.return_value = self.observation_values

        # test
        results = id_match.match_participants(self.dataset)

        # post conditions
        self.assertEqual(results, self.expected)

    def test_match_participants_epoch_birthdates(self):
        # pre conditions
        # TODO cover epoch datetimes if necessary
        self.observation_values[consts.OBS_PII_BIRTH_DATETIME][self.pid] = 631170000
        self.mock_obs_values.return_value = self.observation_values

        # test
        results = id_match.match_participants(self.dataset)

        # post conditions
        self.expected[self.pid][consts.BIRTHDATE] = consts.MISMATCH
        self.assertEqual(results, self.expected)

    def test_match_participants_middle_initial(self):
        # pre conditions
        self.mock_pii_names.return_value = [(self.pid, 'Fancy-Nancy', 'Knight', 'Drew')]

        # test
        results = id_match.match_participants(self.dataset)

        # post conditions
        self.expected[self.pid][consts.FIRST_NAME] = consts.MATCH
        self.expected[self.pid][consts.MIDDLE_NAME] = consts.MISMATCH
        self.expected[self.pid][consts.LAST_NAME] = consts.MATCH
        self.assertEqual(results, self.expected)

    def test_match_participants_zero_zipcode_address_fields(self):
        # pre conditions
        # mock seems to read zero prefixed integers wrong (different base system)
        self.mock_pii_addresses.return_value = [
            (self.pid,
             '1',
             '2',
             '3',
             '44',
             '5645')]

        # test
        results = id_match.match_participants(self.dataset)

        # post conditions
        self.expected[self.pid][consts.STREET_ONE] = consts.MISMATCH
        self.expected[self.pid][consts.STREET_TWO] = consts.MISMATCH
        self.expected[self.pid][consts.CITY] = consts.MISMATCH
        self.expected[self.pid][consts.STATE] = consts.MISMATCH
        self.expected[self.pid][consts.ZIP] = consts.MATCH
        self.assertEqual(results, self.expected)
