import re
import unittest
import mock

import resources
from common import PII_TABLES
from tools import migrate_cdm52_to_cdm531

WHITESPACE = '[\t\n\\s]+'
SPACE = ' '


class MigrateByQueryTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self) -> None:
        self.project_id = 'test-project'
        self.mock_client = mock.MagicMock()
        self.mock_client.project = self.project_id
        self.dataset_id = 'test-dataset'
        self.snapshot_dataset_id = 'test-snapshot-dataset'

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

    def test_get_field_cast_expr_with_schema_change(self):

        expected = 'CAST(DEA AS STRING) AS dea'
        actual = migrate_cdm52_to_cdm531.get_field_cast_expr_with_schema_change(
            dict(name='dea', mode='nullable', type='STRING'),
            source_fields=['DEA', 'procedure_occurrence_id'])
        self.assertEqual(expected, actual)

        expected = 'CAST(dea AS STRING) AS dea'
        actual = migrate_cdm52_to_cdm531.get_field_cast_expr_with_schema_change(
            dict(name='dea', mode='nullable', type='STRING'),
            source_fields=['dea', 'procedure_occurrence_id'])
        self.assertEqual(expected, actual)

        # optional dest field missing from source should result in NULL
        expected = 'CAST(NULL AS STRING) AS dea'
        actual = migrate_cdm52_to_cdm531.get_field_cast_expr_with_schema_change(
            dict(name='dea', mode='nullable', type='STRING'),
            source_fields=['procedure_occurrence_id'])
        self.assertEqual(expected, actual)

        # required dest field missing from source should result in error
        with self.assertRaises(RuntimeError) as e:
            migrate_cdm52_to_cdm531.get_field_cast_expr_with_schema_change(
                dict(name='procedure_occurrence_id',
                     mode='required',
                     type='INT64'),
                source_fields=['something'])

    @mock.patch('tools.snapshot_by_query.get_source_fields')
    def test_get_upgrade_table_query(self, mock_get_source_fields):
        mock_get_source_fields.return_value = self.get_mock_fields('provider')

        actual_query = migrate_cdm52_to_cdm531.get_upgrade_table_query(
            self.mock_client, self.dataset_id, 'provider')
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
        actual_query = migrate_cdm52_to_cdm531.get_upgrade_table_query(
            self.mock_client, self.dataset_id, 'procedure_occurrence')
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

        actual_query = migrate_cdm52_to_cdm531.get_upgrade_table_query(
            self.mock_client, self.dataset_id, 'non_cdm_table')
        expected_query = '''SELECT * FROM `test-project.test-dataset.non_cdm_table`'''
        self.assertEqual(actual_query, expected_query)

    @mock.patch('utils.bq.get_client')
    @mock.patch('tools.snapshot_by_query.create_empty_cdm_tables')
    @mock.patch('tools.snapshot_by_query.get_source_fields')
    def test_get_upgrade_hpo_table_query(self, mock_source_fields,
                                         mock_create_tables, mock_get_client):
        hpo_id = 'fake'
        mock_client = mock.MagicMock()
        mock_client.project = self.project_id
        mock_get_client.return_value = mock_client
        # Setup mocks
        mocked_fields = []
        all_tables = []
        mock_query_calls = []
        for table in resources.CDM_TABLES + PII_TABLES:
            fields = self.get_mock_fields(table)
            # called twice so mock twice
            mocked_fields.extend([fields, fields])
            tables = [
                mock.MagicMock(table_id=table),
                mock.MagicMock(table_id=f'{hpo_id}_{table}'),
                mock.MagicMock(table_id=f'unioned_{table}'),
                mock.MagicMock(table_id=f'_mapping_{table}')
            ]
            all_tables.extend(tables)
            call_q = mock.call(
                migrate_cdm52_to_cdm531.get_upgrade_table_query(
                    mock_client, self.dataset_id, f'{hpo_id}_{table}', hpo_id),
                mock.ANY)
            mock_query_calls.append(call_q)
        mock_source_fields.side_effect = mocked_fields
        mock_client.list_tables.return_value = all_tables
        mock_job = mock.MagicMock(job_id=None, result=lambda: None)
        mock_client.query.return_value = mock_job
        migrate_cdm52_to_cdm531.schema_upgrade_cdm52_to_cdm531(
            self.project_id, self.dataset_id, self.snapshot_dataset_id, hpo_id)
        mock_client.query.assert_has_calls(mock_query_calls)
