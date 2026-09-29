"""Unit tests for the RT ID regeneration query builders."""
import unittest

from common import MEASUREMENT, PERSON
from tools import regenerate_rt_ids as rt


class RegenerateRtIds(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = 'fake_project'
        self.input_dataset_id = 'fake_deid'
        self.mapping_dataset_id = 'fake_mapping'
        self.pipeline_dataset_id = 'fake_pipeline'
        self.ids_view = 'fake_ids_view'

    def _mapping_query(self, table_name, append=False):
        return rt.mapping_query(table_name, self.input_dataset_id,
                                self.project_id, self.pipeline_dataset_id,
                                self.ids_view, self.input_dataset_id,
                                self.mapping_dataset_id, append)

    def test_person_mapping_reads_rt_columns(self):
        """The deid input's person_id is research_id, re-keyed to registered_tier_id."""
        actual = self._mapping_query(PERSON)

        self.assertIn('research_id AS src_person_id', actual)
        self.assertIn('registered_tier_id AS person_id', actual)
        self.assertIn(
            f'FROM `{self.project_id}.{self.pipeline_dataset_id}.{self.ids_view}`',
            actual)

    def test_person_mapping_excludes_rows_missing_either_id(self):
        """The ids view left-joins, so a NULL id must be filtered, not joined away."""
        for append in (False, True):
            with self.subTest(append=append):
                actual = self._mapping_query(PERSON, append)
                where_clause = actual.split('WHERE', 1)[1]

                self.assertIn('research_id IS NOT NULL', where_clause)
                self.assertIn('registered_tier_id IS NOT NULL', where_clause)

    def test_domain_mapping_is_not_filtered_by_the_person_guard(self):
        """The guard belongs to the person mapping only."""
        actual = self._mapping_query(MEASUREMENT)

        self.assertNotIn('IS NOT NULL', actual)
        self.assertNotIn(self.ids_view, actual)


if __name__ == '__main__':
    unittest.main()
