# Python imports
import unittest

# Project imports
from tools.copy_dataset_to_output_prod import get_arg_parser, DEID_STAGE_LIST


class CopyDatasetToOutputProdTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.parser = get_arg_parser()

    def test_parser(self):

        # Valid arguments
        test_args = [
            "--run_as", "myemail.com", "-s", "mysrcproject", "-o",
            "mydestproject", "-d", "mysrcdataset", "-r", "2021Q2R4", "-t",
            "controlled", "--deid_stage", "base"
        ]

        args = self.parser.parse_args(test_args)
        self.assertIn(args.deid_stage, DEID_STAGE_LIST)

        # lowercase release tag
        test_args = [
            "--run_as", "myemail.com", "-s", "mysrcproject", "-o",
            "mydestproject", "-d", "mysrcdataset", "-r", "2021q2r4", "-t",
            "controlled", "--deid_stage", "base"
        ]

        self.assertRaises(SystemExit, self.parser.parse_args, test_args)

        # unknown stage
        test_args = [
            "--run_as", "myemail.com", "-s", "mysrcproject", "-o",
            "mydestproject", "-d", "mysrcdataset", "-r", "2021Q2R4", "-t",
            "controlled", "--deid_stage", "extra_cleaned"
        ]

        self.assertRaises(SystemExit, self.parser.parse_args, test_args)

        # invalid tier
        test_args = [
            "--run_as", "myemail.com", "-s", "mysrcproject", "-o",
            "mydestproject", "-d", "mysrcdataset", "-r", "2021Q2R4", "-t",
            "CONTROLLED", "--deid_stage", "clean"
        ]

        self.assertRaises(SystemExit, self.parser.parse_args, test_args)
