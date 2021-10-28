import re
import unittest
import mock

import resources
from tools import migrate_cdm52_to_cdm531

WHITESPACE = '[\t\n\\s]+'
SPACE = ' '


class SnapshotByQueryTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        cls.mock_client = ''

    def get_mock_fields(self, table_name):
        field_names = []
        for field in resources.fields_for(table_name):
            if field[
                    'name'] in migrate_cdm52_to_cdm531.MODIFIED_FIELD_NAMES.keys(
                    ):
                field_names.append(migrate_cdm52_to_cdm531.MODIFIED_FIELD_NAMES[
                    field['name']]['old_name'])
            else:
                field_names.append(field['name'])
        return field_names

    @mock.patch('tools.snapshot_by_query.get_source_fields')
    def test_get_copy_table_query(self, mock_get_source_fields):
        mock_get_source_fields.return_value = self.get_mock_fields('provider')

        actual_query = migrate_cdm52_to_cdm531.get_copy_table_query(
            'test-project', 'test-dataset', 'provider', self.mock_client)
        expected_query = """SELECT
  CAST(provider_id AS INT64) AS provider_id,
  CAST(provider_name AS STRING) AS provider_name,
  CAST(NPI AS STRING) AS npi,
  CAST(DEA AS STRING) AS dea,
  CAST(specialty_concept_id AS INT64) AS specialty_concept_id,
  CAST(care_site_id AS INT64) AS care_site_id,
  CAST(year_of_birth AS INT64) AS year_of_birth,
  CAST(gender_concept_id AS INT64) AS gender_concept_id,
  CAST(provider_source_value AS STRING) AS provider_source_value,
  CAST(specialty_source_value AS STRING) AS specialty_source_value,
  CAST(specialty_source_concept_id AS INT64) AS specialty_source_concept_id,
  CAST(gender_source_value AS STRING) AS gender_source_value,
  CAST(gender_source_concept_id AS INT64) AS gender_source_concept_id
FROM
  `test-project.test-dataset.provider`"""
        expected_query = re.sub(WHITESPACE, SPACE, expected_query)
        self.assertEqual(re.sub(WHITESPACE, SPACE, actual_query),
                         re.sub(WHITESPACE, SPACE, expected_query))

        mock_get_source_fields.return_value = self.get_mock_fields(
            'procedure_occurrence')
        actual_query = migrate_cdm52_to_cdm531.get_copy_table_query(
            'test-project', 'test-dataset', 'procedure_occurrence',
            self.mock_client)
        expected_query = """SELECT
  CAST(procedure_occurrence_id AS INT64) AS procedure_occurrence_id,
  CAST(person_id AS INT64) AS person_id,
  CAST(procedure_concept_id AS INT64) AS procedure_concept_id,
  CAST(procedure_date AS DATE) AS procedure_date,
  CAST(procedure_datetime AS TIMESTAMP) AS procedure_datetime,
  CAST(procedure_type_concept_id AS INT64) AS procedure_type_concept_id,
  CAST(modifier_concept_id AS INT64) AS modifier_concept_id,
  CAST(quantity AS INT64) AS quantity,
  CAST(provider_id AS INT64) AS provider_id,
  CAST(visit_occurrence_id AS INT64) AS visit_occurrence_id,
  CAST(visit_detail_id AS INT64) AS visit_detail_id,
  CAST(procedure_source_value AS STRING) AS procedure_source_value,
  CAST(procedure_source_concept_id AS INT64) AS procedure_source_concept_id,
  CAST(qualifier_source_value AS STRING) AS modifier_source_value
FROM
  `test-project.test-dataset.procedure_occurrence`"""
        expected_query = re.sub(WHITESPACE, SPACE, expected_query)
        self.assertEqual(re.sub(WHITESPACE, SPACE, actual_query),
                         re.sub(WHITESPACE, SPACE, expected_query))

        actual_query = migrate_cdm52_to_cdm531.get_copy_table_query(
            'test-project', 'test-dataset', 'non_cdm_table', self.mock_client)
        expected_query = '''SELECT * FROM `test-project.test-dataset.non_cdm_table`'''
        self.assertEqual(actual_query, expected_query)
