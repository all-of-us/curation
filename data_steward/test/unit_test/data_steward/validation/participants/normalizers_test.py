import unittest

import validation.participants.normalizers as normalizer


class NormalizersTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def test_normalize_None_city(self):
        # test
        actual = normalizer.normalize_city_name(None)

        # post conditions
        expected = ''
        self.assertEqual(actual, expected)

    def test_normalize_non_string_city(self):
        # test
        actual = normalizer.normalize_city_name(88.321)

        # post conditions
        expected = '88321'
        self.assertEqual(actual, expected)

    def test_normalize_city_with_punctuation_and_spaces(self):
        # test
        actual = normalizer.normalize_city_name('St. Paul\'s Place')

        # post conditions
        expected = 'saint pauls place'
        self.assertEqual(actual, expected)

    def test_normalize_city_name_mixed_case(self):
        # test
        actual = normalizer.normalize_city_name('bIrMiNgHaM')

        expected = 'birmingham'
        # post conditions
        self.assertEqual(actual, expected)

    def test_nomalize_city_with_unknown_abbreviation(self):
        # test
        actual = normalizer.normalize_city_name('L8t. Made Up Place')

        # post conditions
        expected = 'l8t made up place'
        self.assertEqual(actual, expected)

    def test_normalize_street(self):
        # test
        actual = normalizer.normalize_street('Elm Street')

        # post condition
        expected = 'elm street'
        self.assertEqual(actual, expected)

    def test_normalize_street_abbreviations(self):
        # test
        actual = normalizer.normalize_street('Elm St BTM BND ALy')

        # post condition
        expected = 'elm street bottom bend alley'
        self.assertEqual(actual, expected)

    def test_normalize_street_with_puntcuations(self):
        # test
        actual = normalizer.normalize_street('El-lm Str. Blvd.')

        # post condition
        expected = 'el lm street boulevard'
        self.assertEqual(actual, expected)

    def test_normalize_street_with_numeric_endings(self):
        # test
        actual = normalizer.normalize_street('71st Street')

        # post condition
        expected = '71 street'
        self.assertEqual(actual, expected)

    def test_normalize_street_with_alpha_numerics(self):
        # test
        actual = normalizer.normalize_street('Apt. 50a Bldg. 2')

        # post condition
        expected = 'apartment 50 a building 2'
        self.assertEqual(actual, expected)

    def test_normalize_None_street(self):
        # test
        actual = normalizer.normalize_street(None)

        # post condition
        expected = ''
        self.assertEqual(actual, expected)

    def test_normalize_non_string_street(self):
        # test
        actual = normalizer.normalize_street(1492.0)

        # post condition
        expected = '1492 0'
        self.assertEqual(actual, expected)

    def test_normalize_None_state(self):
        # test
        actual = normalizer.normalize_state(None)

        # post condition
        expected = ''
        self.assertEqual(actual, expected)

    def test_normalize_non_string_state(self):
        # test
        actual = normalizer.normalize_state(1492.0)

        # post condition
        expected = ''
        self.assertEqual(actual, expected)

    def test_normalize_mixed_case_state(self):
        #test
        actual = normalizer.normalize_state('aL')

        # post condition
        expected = 'al'
        self.assertEqual(actual, expected)

    def test_normalize_full_state_name(self):
        # test
        actual = normalizer.normalize_state('Colorado')

        #post condition
        expected = ''
        self.assertEqual(actual, expected)

    def test_normalize_None_zip(self):
        # test
        actual = normalizer.normalize_zip(None)

        # post condition
        expected = ''
        self.assertEqual(actual, expected)

    def test_normalize_non_string_zip(self):
        # test
        actual = normalizer.normalize_zip(35401)

        # post condition
        expected = '35401'
        self.assertEqual(actual, expected)

    def test_normalize_hyphenated_zip(self):
        #test
        actual = normalizer.normalize_zip('37010-1112')

        # post condition
        expected = '37010'
        self.assertEqual(actual, expected)

    def test_normalize_spaced_zip(self):
        # test
        actual = normalizer.normalize_zip('37010 1112')

        #post condition
        expected = '37010'
        self.assertEqual(actual, expected)

    def test_normalize_short_zip(self):
        # test
        actual = normalizer.normalize_zip('370')

        #post condition
        expected = '00370'
        self.assertEqual(actual, expected)

    def test_normalize_phone_with_punctuation(self):
        #test
        actual = normalizer.normalize_phone('+1(256) 555-5309')

        # post conditions
        expected = '12565555309'
        self.assertEqual(actual, expected)

    def test_normalize_None_phone(self):
        # test
        actual = normalizer.normalize_phone(None)

        # pos-condition
        expected = ''
        self.assertEqual(actual, expected)

    def test_normalize_non_string_phone(self):
        # test
        actual = normalizer.normalize_phone(5558675309)

        # post condition
        expected = '5558675309'
        self.assertEqual(actual, expected)

    def test_normalize_None_email(self):
        # test
        actual = normalizer.normalize_email(None)

        # post condition
        expected = ''
        self.assertEqual(actual, expected)

    def test_normalize_non_string_email(self):
        # test
        actual = normalizer.normalize_email(1992)

        # post condition
        expected = ''
        self.assertEqual(actual, expected)

    def test_normalize_email_with_extra_spaces(self):
        #test
        actual = normalizer.normalize_email('     willy.wonka@chocolate.ORG   ')

        # post condition
        expected = 'willy.wonka@chocolate.org'
        self.assertEqual(actual, expected)

    def test_normalize_None_name(self):
        # test
        actual = normalizer.normalize_name(None)

        # post condition
        expected = ''
        self.assertEqual(actual, expected)

    def test_normalize_non_string_name(self):
        # test
        actual = normalizer.normalize_name(11)

        # post-condition
        expected = ''
        self.assertEqual(actual, expected)

    def test_normalize_name_mixed_case(self):
        # test
        actual = normalizer.normalize_name('O\'Keefe')

        # post condition
        expected = 'okeefe'
        self.assertEqual(actual, expected)

    def test_normalize_hyphenated_name(self):
        # test
        actual = normalizer.normalize_name('Foo-Bar')

        # post condition
        expected = 'foobar'
        self.assertEqual(actual, expected)

    def test_normalize_spaced_name(self):
        # test
        actual = normalizer.normalize_name('Jo Anne')

        # post condition
        expected = 'joanne'
        self.assertEqual(actual, expected)
