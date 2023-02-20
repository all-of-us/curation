import unittest
from mock import patch
from types import SimpleNamespace

import cdm


class CDMTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.mock_dataset_id = 'mock_dataset'

        self.mock_table = 'mock_table'
        self.mock_component = 'mock_component'

        self.mock_achilles = 'mock_achilles'
        self.mock_vocabulary = 'mock_vocabulary'

    @patch('cdm.common')  # components
    @patch('cdm.resources')  # tables
    def test_parser_with_bad_args(self, mock_resources, mock_common):
        """
        Case 1: No dataset, good table
        Case 2: Bad table, good dataset
        Case 3: Bad component, good dataset
        Case 4: Create a NotYetImplemented case, good dataset
        Case 5: table/component mutual exclusion violation, good args
        """

        mock_common.CDM_COMPONENTS = [self.mock_component, self.mock_achilles]
        mock_common.ACHILLES = self.mock_achilles
        mock_resources.CDM_TABLES = [self.mock_table]

        parser = cdm.create_parser()

        # Case 1
        test_args = ['--table', self.mock_table]
        with self.assertRaises(SystemExit):
            _ = parser.parse_args(test_args)

        # Case 2
        test_args = [self.mock_dataset_id, '--table', 'bad_table']
        with self.assertRaises(SystemExit):
            _ = parser.parse_args(test_args)

        # Case 3
        test_args = [self.mock_dataset_id, '--component', 'bad_component']
        with self.assertRaises(SystemExit):
            _ = parser.parse_args(test_args)

        # Case 4
        sn = SimpleNamespace()
        sn.table = None
        sn.component = self.mock_achilles
        sn.dataset_id = self.mock_dataset_id
        with self.assertRaises(NotImplementedError), patch(
                'argparse.ArgumentParser.parse_args') as mock_args:

            mock_args.return_value = sn
            cdm.main()

        # Case 5
        test_args = [
            self.mock_dataset_id, '--component', self.mock_component, '--table',
            self.mock_table
        ]
        with self.assertRaises(SystemExit):
            _ = parser.parse_args(test_args)

    @patch('cdm.common')  # components
    @patch('cdm.resources')  # tables
    def test_parser_with_good_args(self, mock_resources, mock_common):
        """
        Case 1: Only a dataset specified -> create_all_tables(dataset_id)
        Case 2: Dataset with no matching component -> pass
        Case 3: Dataset with component -> create_vocabulary()
        Case 4: Dataset with table -> create_table()
        """

        mock_common.VOCABULARY = self.mock_vocabulary
        mock_common.CDM_COMPONENTS = [self.mock_component]
        mock_resources.CDM_TABLES = [self.mock_table]

        sn = SimpleNamespace()
        sn.table = None
        sn.component = None
        sn.dataset_id = self.mock_dataset_id

        # Case 1
        with patch('cdm.create_all_tables') as mock_create_all, patch(
                'argparse.ArgumentParser.parse_args') as mock_args:

            mock_args.return_value = sn  # Pre
            cdm.main()  # Test
            mock_create_all.assert_called_once_with(
                self.mock_dataset_id)  # Post

        # Case 2
        sn.component = 'no_matching_table'
        with patch('argparse.ArgumentParser.parse_args') as mock_args:

            mock_args.return_value = sn  # Pre
            cdm.main()  # Test
            pass  # Post

        # Case 3
        sn.component = self.mock_vocabulary
        with patch('cdm.create_vocabulary_tables') as mock_create_vocab, patch(
                'argparse.ArgumentParser.parse_args') as mock_args:

            mock_args.return_value = sn  # Pre
            cdm.main()  # Test
            mock_create_vocab.assert_called_once_with(self.mock_dataset_id)

        # Case 4
        sn.table = self.mock_table
        with patch('cdm.create_table') as mock_create_table, patch(
                'argparse.ArgumentParser.parse_args') as mock_args:

            mock_args.return_value = sn  # Pre
            cdm.main()  # Test
            mock_create_table.assert_called_once_with(self.mock_table,
                                                      self.mock_dataset_id)
