import re
import unittest

from tools import snapshot_by_query

WHITESPACE = '[\t\n\\s]+'
SPACE = ' '


class SnapshotByQueryTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def test_get_copy_table_query(self):
        actual_query = snapshot_by_query.get_copy_table_query(
            'test-project', 'test-dataset', 'person')
        expected_query = """SELECT
  CAST(person_id AS INT64) AS person_id,
  CAST(gender_concept_id AS INT64) AS gender_concept_id,
  CAST(year_of_birth AS INT64) AS year_of_birth,
  CAST(month_of_birth AS INT64) AS month_of_birth,
  CAST(day_of_birth AS INT64) AS day_of_birth,
  CAST(birth_datetime AS TIMESTAMP) AS birth_datetime,
  CAST(race_concept_id AS INT64) AS race_concept_id,
  CAST(ethnicity_concept_id AS INT64) AS ethnicity_concept_id,
  CAST(location_id AS INT64) AS location_id,
  CAST(provider_id AS INT64) AS provider_id,
  CAST(care_site_id AS INT64) AS care_site_id,
  CAST(person_source_value AS STRING) AS person_source_value,
  CAST(gender_source_value AS STRING) AS gender_source_value,
  CAST(gender_source_concept_id AS INT64) AS gender_source_concept_id,
  CAST(race_source_value AS STRING) AS race_source_value,
  CAST(race_source_concept_id AS INT64) AS race_source_concept_id,
  CAST(ethnicity_source_value AS STRING) AS ethnicity_source_value,
  CAST(ethnicity_source_concept_id AS INT64) AS ethnicity_source_concept_id
FROM
  `test-project.test-dataset.person`"""
        expected_query = re.sub(WHITESPACE, SPACE, expected_query)
        self.assertEqual(re.sub(WHITESPACE, SPACE, actual_query),
                         re.sub(WHITESPACE, SPACE, expected_query))

        actual_query = snapshot_by_query.get_copy_table_query(
            'test-project', 'test-dataset', 'non_cdm_table')
        expected_query = '''SELECT * FROM `test-project.test-dataset.non_cdm_table`'''
        self.assertEqual(actual_query, expected_query)
