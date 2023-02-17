import unittest

from mock import patch
import cdm


class CDMTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.test_dataset = 'test_dataset'
        self.test_component = 'test_component'
        self.test_table = 'test_table'

    @patch('common.VOCABULARY', ['mock_vocabulary'])
    @patch('common.ACHILLES', ['mock_achilles'])
    def test_parser_with_bad_ags(self, mock_achilles, mock_vocabulary):
        """
        Case 1: No dataset / arg specified
        Case 2: bad table argument
        Case 3: bad component argument
        Case 4: bad table-component mutual exclusion violation
        """

        parser = cdm.get_parser()

        test_args = []
        with self.assertRaises(SystemExit):
            _ = parser.parse_args(test_args)

        test_args = [self.test_dataset, '--table', 'bad_table']
        with self.assertRaises(SystemExit):
            _ = parser.parse_args(test_args)

        test_args = [self.test_dataset, '--component', 'bad_component']
        with self.assertRaises(SystemExit):
            _ = parser.parse_args(test_args)

        test_args = [
            self.test_dataset, '--component', self.test_component, '--table',
            self.test_table
        ]
        with self.assertRaises(SystemExit):
            _ = parser.parse_args(test_args)

    @patch('common.VOCABULARY', ['mock_vocabulary'])
    @patch('common.ACHILLES', ['mock_achilles'])
    def test_parser_with_good_args(self, mock_achilles, mock_vocabulary):
        """
        Case 1: Only a dataset specified
        Case 2: Dataset with table
        Case 3: Dataset with common.vocabulary component
        Case 4: Dataset with non-matching component
        """

        parser = cdm.get_parser()

        test_args = [self.test_dataset]
        result_args = parser.parse_args(test_args)

        test_args = [self.test_dataset, '--table', mock_achilles]
        result_args = parser.parse_args(test_args)

        test_args = [self.test_dataset, '--component', mock_vocabulary]
        result_args = parser.parse_args(test_args)