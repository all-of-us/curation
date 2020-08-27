import unittest

from cdr_cleaner.clean_cdr import get_parser
from constants.cdr_cleaner.clean_cdr import DataStage


class CleanCDRTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = 'test project'
        self.dataset_id = 'test dataset'
        self.sandbox_dataset_id = 'test sandbox'
        self.dest_table = 'dest_table'

    def test_data_stage(self):
        actual_stages = [value for item, value in DataStage.__members__.items()]
        expected_stages = list([s for s in DataStage])
        self.assertEqual(actual_stages, expected_stages)

    def test_parser(self):
        stage = ['--data_stage', 'ehr']
        parser = get_parser()
        args = parser.parse_args(stage)
        self.assertIn(args.data_stage, DataStage)

        stage = ['--data_stage', 'unspecified']
        parser = get_parser()
        self.assertRaises(SystemExit, parser.parse_args, stage)

        stage = ['--data_stage', 'unknown']
        parser = get_parser()
        self.assertRaises(SystemExit, parser.parse_args, stage)
