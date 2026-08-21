# Python imports
import unittest
import mock

# Project imports
from common import CONTROLLED, CONTROLLED_PLUS, TIER_LIST
from tools.copy_dataset_to_output_prod import (get_arg_parser, get_dataset_name,
                                               generate_output_prod,
                                               DEID_STAGE_LIST)


class CopyDatasetToOutputProdTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.parser = get_arg_parser()

    def test_new_parser(self):

        # Valid arguments
        test_args = [
            "new", "--run_as", "myemail.com", "-s", "mysrcproject", "-o",
            "mydestproject", "-d", "mysrcdataset", "-r", "2021Q2R4", "-t",
            "controlled", "--deid_stage", "base", "--fitbit_dataset",
            "myfitbitdataset"
        ]

        args = self.parser.parse_args(test_args)
        self.assertIn(args.deid_stage, DEID_STAGE_LIST)

        # lowercase release tag
        test_args = [
            "new", "--run_as", "myemail.com", "-s", "mysrcproject", "-o",
            "mydestproject", "-d", "mysrcdataset", "-r", "2021q2r4", "-t",
            "controlled", "--deid_stage", "base", "--fitbit_dataset",
            "myfitbitdataset"
        ]

        self.assertRaises(SystemExit, self.parser.parse_args, test_args)

        # unknown stage
        test_args = [
            "new", "--run_as", "myemail.com", "-s", "mysrcproject", "-o",
            "mydestproject", "-d", "mysrcdataset", "-r", "2021Q2R4", "-t",
            "controlled", "--deid_stage", "extra_cleaned", "--fitbit_dataset",
            "myfitbitdataset"
        ]

        self.assertRaises(SystemExit, self.parser.parse_args, test_args)

        # invalid tier
        test_args = [
            "new", "--run_as", "myemail.com", "-s", "mysrcproject", "-o",
            "mydestproject", "-d", "mysrcdataset", "-r", "2021Q2R4", "-t",
            "CONTROLLED", "--deid_stage", "clean", "--fitbit_dataset",
            "myfitbitdataset"
        ]

        self.assertRaises(SystemExit, self.parser.parse_args, test_args)

    def test_hotfix_parser(self):
        # Valid arguments
        test_args = [
            "hotfix", "--run_as", "myemail.com", "-s", "mysrcproject", "-o",
            "mydestproject", "-d", "mysrcdataset", "-r", "2021Q2R4", "-t",
            "controlled", "--deid_stage", "base"
        ]

        args = self.parser.parse_args(test_args)
        self.assertIn(args.deid_stage, DEID_STAGE_LIST)

        # Invalid fitbit dataset argument
        test_args = [
            "hotfix", "--run_as", "myemail.com", "-s", "mysrcproject", "-o",
            "mydestproject", "-d", "mysrcdataset", "-r", "2021Q2R4", "-t",
            "controlled", "--deid_stage", "base", "--fitbit_dataset",
            "myfitbitdataset"
        ]

        self.assertRaises(SystemExit, self.parser.parse_args, test_args)


class CopyDatasetToOutputProdControlledPlusTest(unittest.TestCase):
    """
    Covers the tier-generic output naming and the serology gate for CT+ (DL-2462).
    """

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.parser = get_arg_parser()
        self.release_tag = '2021Q2R4'

    def test_parser_accepts_controlled_plus(self):
        test_args = [
            "new", "--run_as", "myemail.com", "-s", "mysrcproject", "-o",
            "mydestproject", "-d", "mysrcdataset", "-r", self.release_tag, "-t",
            CONTROLLED_PLUS, "--deid_stage", "clean", "--fitbit_dataset",
            "myfitbitdataset"
        ]

        args = self.parser.parse_args(test_args)
        self.assertEqual(args.tier, CONTROLLED_PLUS)

    def test_output_prod_names_do_not_collide(self):
        """CT+ publishes CP{tag}; the tier[0].upper() bug published C{tag}."""
        names = {
            tier: get_dataset_name(tier, self.release_tag, 'clean')
            for tier in TIER_LIST
        }
        self.assertEqual(names[CONTROLLED_PLUS], f'CP{self.release_tag}')
        self.assertEqual(names[CONTROLLED], f'C{self.release_tag}')
        self.assertEqual(len(set(names.values())), len(TIER_LIST))

    def test_no_pipeline_suffix_is_applied_here(self):
        """The source arrives through -d, so the destination is the published name."""
        self.assertNotIn(
            '_pre_rekey',
            get_dataset_name(CONTROLLED_PLUS, self.release_tag, 'clean'))

    @mock.patch('tools.copy_dataset_to_output_prod.populate_death')
    @mock.patch('tools.copy_dataset_to_output_prod.update_person')
    @mock.patch('tools.copy_dataset_to_output_prod.BigQueryClient')
    @mock.patch('tools.copy_dataset_to_output_prod.auth'
                '.get_impersonation_credentials')
    def test_serology_is_controlled_tier_only(self, mock_creds, mock_client,
                                              mock_update_person,
                                              mock_populate_death):
        """CT+ must not receive serology, so this gate stays an equality check."""
        for tier, expect_serology in ((CONTROLLED, True), (CONTROLLED_PLUS,
                                                           False)):
            with self.subTest(tier=tier):
                bq_client = mock.MagicMock()
                mock_client.return_value = bq_client

                generate_output_prod(tier,
                                     self.release_tag,
                                     'clean',
                                     'mysrcproject',
                                     'mysrc_deid_clean',
                                     'mydestproject',
                                     'myemail.com',
                                     copy_fitbit=False)

                copied = [
                    call.args for call in bq_client.copy_dataset.call_args_list
                ]
                serology_copied = any(
                    'antibody_quest' in str(args) for args in copied)
                self.assertEqual(serology_copied, expect_serology)
