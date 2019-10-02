import re
import unittest

from tools import snapshot_by_query

WHITESPACE = '[\t\n\\s]+'
SPACE = ' '


class RetractDataGcsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def test_get_copy_table_query(self):
        actual_query = snapshot_by_query.get_copy_table_query('test-project', 'test-dataset', 'person')
        expected_query = '''SELECT person_id, gender_concept_id, year_of_birth, 
        month_of_birth, day_of_birth, birth_datetime, race_concept_id, ethnicity_concept_id, location_id, 
        provider_id, care_site_id, person_source_value, gender_source_value, gender_source_concept_id, 
        race_source_value, race_source_concept_id, ethnicity_source_value, ethnicity_source_concept_id 
        FROM `test-project.test-dataset.person`'''
        expected_query = re.sub(WHITESPACE, SPACE, expected_query)
        self.assertEqual(actual_query, expected_query)

        actual_query = snapshot_by_query.get_copy_table_query('test-project', 'test-dataset', 'non_cdm_table')
        expected_query = '''SELECT * FROM `test-project.test-dataset.non_cdm_table`'''
        self.assertEqual(actual_query, expected_query)
