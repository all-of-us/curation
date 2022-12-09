from argparse import ArgumentTypeError
import unittest

from utils import parameter_validators as pv


class ParameterValidatorsTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        pass

    def test_validate_release_tag_param(self):
        # casing error
        self.assertRaises(ArgumentTypeError, pv.validate_release_tag_param,
                          '1111Q1R1')

        # not enough initial digits error
        self.assertRaises(ArgumentTypeError, pv.validate_release_tag_param,
                          '111q1r1')

        # malformed quarter digit error
        self.assertRaises(ArgumentTypeError, pv.validate_release_tag_param,
                          '1111q6r1')

        # malformed quarter digit error
        self.assertRaises(ArgumentTypeError, pv.validate_release_tag_param,
                          '1111qr1')

        # malformed revision digit error
        self.assertRaises(ArgumentTypeError, pv.validate_release_tag_param,
                          '1111q6r111')

        # unknown alphabetic characters error
        self.assertRaises(ArgumentTypeError, pv.validate_release_tag_param,
                          'a1111q1r1')

        # unknown punctuation error
        self.assertRaises(ArgumentTypeError, pv.validate_release_tag_param,
                          '_1111q1r1')

        # well formed single digit revision example
        self.assertEqual(pv.validate_release_tag_param('1111q1r1'), '1111q1r1')

        # well formed double digit revision example
        self.assertEqual(pv.validate_release_tag_param('1111q1r11'),
                         '1111q1r11')

    def test_validate_output_release_tag_param(self):
        # casing error
        self.assertRaises(ArgumentTypeError,
                          pv.validate_output_release_tag_param, '1111q1r1')

        # not enough initial digits error
        self.assertRaises(ArgumentTypeError,
                          pv.validate_output_release_tag_param, '111Q1R1')

        # malformed Quarter digit error
        self.assertRaises(ArgumentTypeError,
                          pv.validate_output_release_tag_param, '1111Q6R1')

        # malformed quarter digit error
        self.assertRaises(ArgumentTypeError,
                          pv.validate_output_release_tag_param, '1111QR1')

        # malformed revision digit error
        self.assertRaises(ArgumentTypeError,
                          pv.validate_output_release_tag_param, '1111Q6R111')

        # unknown alphabetic characters error
        self.assertRaises(ArgumentTypeError,
                          pv.validate_output_release_tag_param, 'a1111Q1R1')

        # unknown punctuation error
        self.assertRaises(ArgumentTypeError,
                          pv.validate_output_release_tag_param, '_1111Q1R1')

        # well formed single digit revision example
        self.assertEqual(pv.validate_output_release_tag_param('1111Q1R1'),
                         '1111Q1R1')

        # well formed double digit revision example
        self.assertEqual(pv.validate_output_release_tag_param('1111Q1R11'),
                         '1111Q1R11')

    def test_validate_bucket_filepath(self):
        # off limits name
        self.assertRaises(ArgumentTypeError, pv.validate_bucket_filepath,
                          'gs://.well-known/acme-challenge/')

        # unix like . for this
        self.assertRaises(ArgumentTypeError, pv.validate_bucket_filepath,
                          'gs://.')

        # unix like .. for parent
        self.assertRaises(ArgumentTypeError, pv.validate_bucket_filepath,
                          'gs://..')

        # unix like wildcard character
        self.assertRaises(ArgumentTypeError, pv.validate_bucket_filepath,
                          'gs://foo.bar!')

        # missing leading locator
        self.assertRaises(ArgumentTypeError, pv.validate_bucket_filepath,
                          '//foo.bar')

        # well formed bucket filepath
        self.assertEqual(pv.validate_bucket_filepath('gs://foo.bar'),
                         'gs://foo.bar')

    def test_validate_qualified_bq_tablename(self):
        # short project name
        self.assertRaises(ArgumentTypeError, pv.validate_qualified_bq_tablename,
                          'abc.def.ghi')

        # long project name
        self.assertRaises(ArgumentTypeError, pv.validate_qualified_bq_tablename,
                          'abcdefghijklmnopqrstuvwxyz12345.abc.def')

        # short dataset name
        self.assertRaises(ArgumentTypeError, pv.validate_qualified_bq_tablename,
                          'abcd..efg')

        # short table name
        self.assertRaises(ArgumentTypeError, pv.validate_qualified_bq_tablename,
                          'abcd.e.')

        # hyphen in dataset name
        self.assertRaises(ArgumentTypeError, pv.validate_qualified_bq_tablename,
                          'abcd.e-f.gh')

        # well formed
        self.assertEqual(
            pv.validate_qualified_bq_tablename('aA-_0!\'.aA0.aA_-0'),
            'aA-_0!\'.aA0.aA_-0')
