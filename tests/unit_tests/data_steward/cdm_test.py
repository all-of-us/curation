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
        self.mock_dataset = 'mock_dataset'

        self.mock_table = 'mock_table'
        self.mock_component = 'mock_component'

        self.mock_achilles = 'mock_achilles'
        self.mock_vocabulary = 'mock_vocabulary'

    @patch('cdm.common')  # components
    @patch('cdm.resources')  # tables
    def test_parser_with_bad_args(self, mock_resources, mock_common):
        """
        Case 1: No dataset
        Case 2: Bad table
        Case 3: Bad component
        Case 4: Using Achilles (NYI)
        Case 5: table/component mutual exclusion violation with good args
        """

        mock_common.CDM_COMPONENTS = [self.mock_component, self.mock_achilles]
        mock_common.ACHILLES = [self.mock_achilles]
        mock_resources.CDM_TABLES = [self.mock_table]

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

        # Incorrect, targets parser, not main()
        # test_args = [self.mock_dataset, '--component', self.mock_achilles]
        # with self.assertRaises(NotImplementedError):
        #     _ = parser.parse_args(test_args)
        #     cdm.main()

        test_args = [
            self.mock_dataset, '--component', self.mock_component, '--table',
            self.mock_table
        ]
        with self.assertRaises(SystemExit):
            _ = parser.parse_args(test_args)

    @patch('cdm.common')  # components
    @patch('cdm.resources')  # tables
    def test_parser_with_good_args(self, mock_resources, mock_common):
        """
        Case 1: Only a dataset specified -> create_all()
        Case 2: Dataset with table -> create_table()
        Case 3: Dataset with component -> create_vocabulary()
        Case 4: Dataset with no matching component
        """

        mock_common.CDM_COMPONENTS = [self.mock_component, self.mock_achilles]
        mock_common.ACHILLES = [self.mock_achilles]
        mock_resources.CDM_TABLES = [self.mock_table]

        parser = cdm.get_parser()

        test_args = [self.mock_dataset]
        result = parser.parse_args(test_args)
        self.assertIn(self.mock_dataset, list(vars(result).values()))

        test_args = [self.mock_dataset, '--table', self.mock_table]
        result = parser.parse_args(test_args)
        self.assertIn(self.mock_table, list(vars(result).values()))

        test_args = [self.mock_dataset, '--component', self.mock_component]
        result = parser.parse_args(test_args)
        self.assertIn(self.mock_component, list(vars(result).values()))
