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
        self.concept_lookup_table = "concept_lookup_table"

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
        parser = dc732_suppress.get_arg_parser()

        dataset_id = ['dataset1']
        parser.parse_args(['-p', self.project_id,
                           '-s', self.sandbox_dataset_id,
                           '-c', self.concept_lookup_table,
                           '-d'] + dataset_id)
        dataset_ids = ['dataset1', 'dataset2']
        args = parser.parse_args(['-p', self.project_id,
                                  '-s', self.sandbox_dataset_id,
                                  '-c', self.concept_lookup_table,
                                  '-d'] + dataset_ids)
        self.assertEqual(args.dataset_ids, dataset_ids)
