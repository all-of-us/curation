import StringIO
import os
import shutil
import tempfile
import unittest

import mock

import test_util
from common import DELIMITER, LINE_TERMINATOR
from resources import AOU_GENERAL_CONCEPT_CSV_PATH
from vocabulary import _transform_csv, format_date_str, get_aou_general_vocabulary_row, \
    append_vocabulary, append_concepts


class VocabularyTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        super(VocabularyTest, self).setUp()

    @staticmethod
    def do_transform_file(input_text):
        in_fp = StringIO.StringIO(input_text)
        out_fp = StringIO.StringIO()
        _transform_csv(in_fp, out_fp)
        return out_fp.getvalue()

    def test_transform_file(self):
        header = ['concept_id', 'valid_start_date', 'valid_end_date', 'invalid_reason']
        row1 = ['1', '2017-05-17', '2099-12-31', '']
        row2 = ['2', '2019-01-01', '2099-12-31', '']
        rows = [header, row1, row2]
        lines = [DELIMITER.join(row) for row in rows]

        # after transform should output valid csv with date format yyyy-mm-dd
        expected_text = LINE_TERMINATOR.join(lines) + LINE_TERMINATOR
        input_text = expected_text.replace('-', '')
        actual_text = VocabularyTest.do_transform_file(input_text)
        msg_fmt = 'Dates were not formatted as expected.\nExpected:\n{0}\nActual:\n{1}'
        self.assertEqual(expected_text,
                         actual_text,
                         msg_fmt.format(expected_text, actual_text))

        # windows line endings are replaced with linux ones
        input_text = expected_text.replace(LINE_TERMINATOR, '\r\n')
        actual_text = VocabularyTest.do_transform_file(input_text)
        self.assertEqual(expected_text,
                         actual_text,
                         'Windows line endings were not replaced as expected.')

    def test_format_date_str(self):
        expected = '2019-01-23'
        msg_fmt = 'Date not formatted as expected.\nExpected:\n{0}\nActual:\n{1}'
        actual = format_date_str('20190123')
        self.assertEqual(expected, actual, msg_fmt.format(expected, actual))
        actual = format_date_str('2019-01-23')
        self.assertEqual(expected, actual, msg_fmt.format(expected, actual))
        with self.assertRaises(ValueError):
            format_date_str('201901234')

    def test_append_vocabulary(self):
        in_path = test_util.TEST_VOCABULARY_VOCABULARY_CSV
        out_dir = tempfile.mkdtemp()
        out_path = os.path.join(out_dir, 'VOCABULARY_1.CSV')
        out_path_2 = os.path.join(out_dir, 'VOCABULARY_2.CSV')
        expected_last_row = get_aou_general_vocabulary_row()

        try:
            append_vocabulary(in_path, out_path)
            with open(in_path, 'rb') as in_fp, open(out_path, 'rb') as out_fp:
                # same content as input
                for in_row in in_fp:
                    out_row = out_fp.readline()
                    self.assertEqual(in_row, out_row)
                # new row added
                actual_last_row = out_fp.readline()
                self.assertEqual(actual_last_row, expected_last_row)
                # end of file
                self.assertEqual('', out_fp.readline())

            # should warn when concepts already in input
            with mock.patch('warnings.warn') as warn_call:
                append_vocabulary(out_path, out_path_2)
                warn_call.assert_called()
        finally:
            shutil.rmtree(out_dir)

    def test_append_concepts(self):
        in_path = test_util.TEST_VOCABULARY_CONCEPT_CSV
        out_dir = tempfile.mkdtemp()
        out_path = os.path.join(out_dir, 'CONCEPT_1.CSV')
        out_path_2 = os.path.join(out_dir, 'CONCEPT_2.CSV')

        try:
            append_concepts(in_path, out_path)
            with open(in_path, 'rb') as in_fp, open(AOU_GENERAL_CONCEPT_CSV_PATH, 'rb') as add_fp:
                # Note: Test files are small so memory usage here is acceptable
                original_lines = in_fp.readlines()
                all_lines = add_fp.readlines()
                expected_lines = original_lines + all_lines[1:]
                with open(out_path, 'rb') as out_fp:
                    actual_lines = out_fp.readlines()
                    self.assertSequenceEqual(actual_lines, expected_lines)

            # should warn when concepts already in input
            with mock.patch('warnings.warn') as warn_call:
                append_concepts(out_path, out_path_2)
                warn_call.assert_called()
        finally:
            shutil.rmtree(out_dir)
