import unittest
from mock import patch

import cdm, common


class CDMTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.mock_dataset = 'mock_dataset'

        self.mock_table = 'mock_table'
        self.mock_component = 'mock_component'

        self.mock_achilles = common.ACHILLES
        self.mock_vocabulary = 'mock_vocabulary'

    @patch('common.CDM_COMPONENTS')
    @patch('resources.CDM_TABLES')
    def test_parser_with_bad_args(self, mock_cdm_tables, mock_cdm_components):
        """
        Case 1: No dataset
        Case 2: Bad table
        Case 3: Bad component
        Case 4: Using Achilles (NYI)
        Case 5: table/component mutual exclusion violation with good args
        """

        mock_cdm_tables.return_value = [self.mock_table]
        mock_cdm_components.return_value = [self.mock_component]

        parser = cdm.get_parser()

        test_args = []
        with self.assertRaises(SystemExit):
            _ = parser.parse_args(test_args)

        test_args = [self.mock_dataset, '--table', 'bad_table']
        with self.assertRaises(SystemExit):
            _ = parser.parse_args(test_args)

        test_args = [self.mock_dataset, '--component', 'bad_component']
        with self.assertRaises(SystemExit):
            _ = parser.parse_args(test_args)

        test_args = [self.mock_dataset, '--component', self.mock_achilles]
        with self.assertRaises(SystemExit):
            _ = parser.parse_args(test_args)

        test_args = [
            self.mock_dataset, '--component', self.mock_component, '--table',
            self.mock_table
        ]
        with self.assertRaises(SystemExit):
            _ = parser.parse_args(test_args)

    @patch('common.VOCABULARY')
    def test_parser_with_good_args(self, mock_vocabulary):
        """
        Case 1: Only a dataset specified
        Case 2: Dataset with table
        Case 3: Dataset with common.vocabulary component
        Case 4: Dataset with no matching component
        """

        mock_vocabulary.return_value = self.mock_vocabulary

        parser = cdm.get_parser()

        test_args = [self.mock_dataset]
        result_args = parser.parse_args(test_args)

        test_args = [self.mock_dataset, '--component', 'test_vocabulary']
        result_args = parser.parse_args(test_args)

        test_args = [
            self.mock_dataset,
            '--table',
        ]
        result_args = parser.parse_args(test_args)
