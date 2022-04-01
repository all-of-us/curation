import unittest

from tools import generate_ehr_upload_pids as eup


class GenerateEhrUploadPids(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = "fake_project"
        self.dataset_id = "fake_dataset"

    def test_get_excluded_hpo_ids_str(self):
        hpo_ids = ["hpo_id_1", "hpo_id_2"]
        expected = "'HPO_ID_1', 'HPO_ID_2', ''"
        actual = eup.get_excluded_hpo_ids_str(hpo_ids)
        self.assertEqual(actual, expected)

        hpo_ids = None
        expected = "''"
        actual = eup.get_excluded_hpo_ids_str(hpo_ids)
        self.assertEqual(actual, expected)

    def test_get_args_parser(self):
        parser = eup.get_args_parser()

        input_args = [
            '-p', self.project_id, '-d', self.dataset_id, '-i', 'hpo_id_1',
            'hpo_id_2'
        ]
        expected_hpo_ids = ['hpo_id_1', 'hpo_id_2']
        args = parser.parse_args(input_args)
        self.assertListEqual(args.excluded_hpo_ids, expected_hpo_ids)

        input_args = [
            '-p', self.project_id, '-d', self.dataset_id, '-i', 'hpo_id_1'
        ]
        expected_hpo_ids = ['hpo_id_1']
        args = parser.parse_args(input_args)
        self.assertListEqual(args.excluded_hpo_ids, expected_hpo_ids)

        input_args = ['-p', self.project_id, '-d', self.dataset_id]
        args = parser.parse_args(input_args)
        self.assertIsNone(args.excluded_hpo_ids)
