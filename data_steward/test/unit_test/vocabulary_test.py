import unittest
import StringIO
from vocabulary import _transform_csv, DELIMITER, LINE_TERMINATOR


class VocabularyTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        super(VocabularyTest, self).setUp()

    def test_transform_csv(self):
        header = ['concept_id', 'valid_start_date', 'valid_end_date', 'invalid_reason']
        row1 = ['1', '2017-05-17', '2099-12-31', '']
        row2 = ['2', '2019-01-01', '2099-12-31', '']
        rows = [header, row1, row2]
        lines = map(lambda r: DELIMITER.join(r), rows)

        # after transform, dates should have format yyyy-mm-dd
        expected_text = LINE_TERMINATOR.join(lines) + LINE_TERMINATOR

        # dates have format yyyymmdd in csv files downloaded from athena
        input_text = expected_text.replace('-', '')

        in_fp = StringIO.StringIO(input_text)
        out_fp = StringIO.StringIO()
        _transform_csv(in_fp, out_fp)
        actual_text = out_fp.getvalue()
        self.assertEqual(expected_text, actual_text)
