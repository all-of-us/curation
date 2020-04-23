import unittest

from cdr_cleaner.manual_cleaning_rules import dc732_suppress


class Dc732SuppressTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = "project_id"
        self.dataset_id = "dataset_id"
        self.sandbox_dataset_id = "sandbox_dataset_id"
        self.concept_lookup_table = "dataset_id.concept_lookup_table"

    def test_to_ext_query(self):
        delete_query = """
        DELETE FROM `project.dataset.condition_occurrence` t WHERE EXISTS(
         SELECT 1 FROM dc732_suppress_rows WHERE
              project.sandbox.dataset_id = "dataset"
          AND table = "condition_occurrence"
          AND t.condition_occurrence_id = row_id)
        """
        expected = """
        DELETE FROM `project.dataset.condition_occurrence_ext` t WHERE EXISTS(
         SELECT 1 FROM dc732_suppress_rows WHERE
              project.sandbox.dataset_id = "dataset"
          AND table = "condition_occurrence"
          AND t.condition_occurrence_id = row_id)
        """
        actual = dc732_suppress.to_ext_query(delete_query)
        self.assertEqual(expected, actual)

    def test_arg_parser(self):
        # raise error with invalid table reference
        with self.assertRaises(ValueError):
            dc732_suppress.parse_args(
                ['-p', self.project_id, 'setup', 'missing_dataset_qualifier'])

        # setup command
        args = dc732_suppress.parse_args(
            ['-p', self.project_id, 'setup', self.concept_lookup_table])
        self.assertEqual('setup', args.cmd)
        self.assertEqual(self.concept_lookup_table,
                         args.concept_lookup_dest_table)
        self.assertEqual(self.project_id, args.project_id)

        dataset_id_1 = 'dataset1'
        dataset_id_2 = 'dataset2'
        # retract command with one target dataset
        args = dc732_suppress.parse_args([
            '-p', self.project_id, 'retract', '-s', self.sandbox_dataset_id,
            '-c', self.concept_lookup_table, '-d', dataset_id_1
        ])
        self.assertEqual(self.concept_lookup_table, args.concept_lookup_table)
        self.assertEqual(self.project_id, args.project_id)
        self.assertEqual(self.sandbox_dataset_id, args.sandbox_dataset_id)
        # retract command with multiple target datasets
        args = dc732_suppress.parse_args([
            '-p', self.project_id, 'retract', '-s', self.sandbox_dataset_id,
            '-c', self.concept_lookup_table, '-d', dataset_id_1, dataset_id_2
        ])
        self.assertEqual([dataset_id_1, dataset_id_2], args.dataset_ids)
