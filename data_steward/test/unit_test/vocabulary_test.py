import unittest
import StringIO
from vocabulary import _transform_csv, DELIMITER, LINE_TERMINATOR, format_date_str


class VocabularyTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        super(VocabularyTest, self).setUp()

    @staticmethod
    def do_transform_csv(input_text):
        in_fp = StringIO.StringIO(input_text)
        out_fp = StringIO.StringIO()
        _transform_csv(in_fp, out_fp)
        return out_fp.getvalue()

    def test_transform_csv(self):
        header = ['concept_id', 'valid_start_date', 'valid_end_date', 'invalid_reason']
        row1 = ['1', '2017-05-17', '2099-12-31', '']
        row2 = ['2', '2019-01-01', '2099-12-31', '']
        rows = [header, row1, row2]
        lines = map(lambda r: DELIMITER.join(r), rows)

        # after transform should output valid csv with date format yyyy-mm-dd
        expected_text = LINE_TERMINATOR.join(lines) + LINE_TERMINATOR
        input_text = expected_text.replace('-', '')
        actual_text = VocabularyTest.do_transform_csv(input_text)
        self.assertEqual(expected_text, actual_text)

        # windows line endings are replaced with linux ones
        input_text = expected_text.replace(LINE_TERMINATOR, '\r\n')
        actual_text = VocabularyTest.do_transform_csv(input_text)
        self.assertEqual(expected_text, actual_text)

    def test_format_date_str(self):
        self.assertEqual('2019-01-23', format_date_str('20190123'))
        self.assertEqual('2019-01-23', format_date_str('2019-01-23'))
        with self.assertRaises(ValueError):
            format_date_str('201901234')
