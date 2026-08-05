"""Unit tests for the CT+ ID regeneration query builders."""
import unittest
from unittest import mock

from common import (CARE_SITE, CONDITION_OCCURRENCE, DRUG_EXPOSURE, MEASUREMENT,
                    OBSERVATION, PERSON, PROCEDURE_OCCURRENCE, PROVIDER,
                    SURVEY_CONDUCT, VISIT_OCCURRENCE)
from tools import regenerate_ct_plus_ids as ct


class RegenerateCtPlusIds(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = 'fake_project'
        self.input_dataset_id = 'fake_controlled_tier'
        self.output_dataset_id = 'fake_controlled_tier_plus'
        self.mapping_dataset_id = 'fake_mapping'
        self.pipeline_dataset_id = 'fake_pipeline'
        self.ids_view = 'fake_ids_view'

    def _mapping_query(self, table_name, append=False):
        return ct.mapping_query(table_name, self.input_dataset_id,
                                self.project_id, self.pipeline_dataset_id,
                                self.ids_view, ct.DEFAULT_MAPPING_NAMESPACE,
                                self.mapping_dataset_id, append)

    def _table_query(self, table_name):
        return ct.table_query(table_name, self.input_dataset_id,
                              self.output_dataset_id, self.project_id,
                              self.pipeline_dataset_id, self.ids_view,
                              self.mapping_dataset_id,
                              ct.DEFAULT_MAPPING_NAMESPACE)

    def test_person_mapping_reads_ct_plus_columns(self):
        """The CT input's person_id is controlled_tier_id, not research_id."""
        actual = self._mapping_query(PERSON)

        self.assertIn('controlled_tier_id AS src_person_id', actual)
        self.assertIn('controlled_tier_plus_id AS person_id', actual)
        self.assertNotIn('research_id AS src_person_id', actual)
        self.assertNotIn('registered_tier_id', actual)

    def test_person_mapping_drops_duplicate_view_rows(self):
        """The ids view carries byte identical duplicates; a LEFT JOIN would fan out."""
        actual = self._mapping_query(PERSON)

        self.assertIn('SELECT DISTINCT', actual)

    def test_person_mapping_src_table_id_is_ct_plus_specific(self):
        """An RT person mapping must not satisfy the CT+ join predicate."""
        src_table_id = ct.person_mapping_src_table_id(self.pipeline_dataset_id,
                                                      self.ids_view)

        self.assertTrue(src_table_id.endswith(ct.CT_PLUS_PERSON_MAPPING_SUFFIX))
        self.assertNotEqual(src_table_id,
                            f'{self.pipeline_dataset_id}.{self.ids_view}')
        self.assertIn(src_table_id, self._table_query(OBSERVATION))

    def test_row_ids_are_a_random_permutation(self):
        """Ordering by the source id would let CT and CT+ be aligned by sorting."""
        actual = self._mapping_query(MEASUREMENT)

        self.assertIn('ROW_NUMBER() OVER (ORDER BY shuffle_key)', actual)
        self.assertIn('RAND() AS shuffle_key', actual)
        self.assertNotIn('ORDER BY src_measurement_id', actual)

    def test_row_id_shuffle_key_is_drawn_outside_the_distinct(self):
        """RAND() inside the DISTINCT would let duplicate source ids survive it."""
        actual = self._mapping_query(MEASUREMENT)
        distinct_clause = actual.split('SELECT DISTINCT')[1].split('FROM')[0]

        self.assertNotIn('RAND()', distinct_clause)

    def test_first_release_draws_ids_without_an_offset(self):
        """With no prior mapping there is nothing to preserve or sit above."""
        actual = self._mapping_query(MEASUREMENT)

        self.assertNotIn('MAX(', actual)
        self.assertNotIn('NOT EXISTS', actual)
        self.assertIn('0 + ROW_NUMBER()', actual)

    def test_later_releases_preserve_existing_ids(self):
        """A row published in one release must keep its CT+ id in the next."""
        actual = self._mapping_query(MEASUREMENT, append=True)

        self.assertIn('COALESCE(MAX(measurement_id), 0)', actual)
        self.assertIn('NOT EXISTS', actual)
        self.assertIn('mt.src_measurement_id = t.src_measurement_id', actual)

    def test_append_filter_and_load_filter_use_the_same_namespace(self):
        """A per release stamp would strand earlier ids and reassign them silently."""
        stamp = f"'{ct.DEFAULT_MAPPING_NAMESPACE}.{MEASUREMENT}'"

        self.assertIn(f'mt.src_table_id = {stamp}',
                      self._mapping_query(MEASUREMENT, append=True))
        self.assertIn(stamp, self._table_query(MEASUREMENT))
        self.assertNotIn(f"'{self.input_dataset_id}.{MEASUREMENT}'",
                         self._mapping_query(MEASUREMENT, append=True))

    def test_survey_conduct_remaps_its_foreign_keys(self):
        actual = self._table_query(SURVEY_CONDUCT)

        self.assertIn('m.survey_conduct_id', actual)
        self.assertIn('CAST(m.survey_conduct_id AS STRING)', actual)
        self.assertIn('mpr.provider_id', actual)
        self.assertIn('mvo.visit_occurrence_id', actual)
        self.assertIn('rvo.visit_occurrence_id AS response_visit_occurrence_id',
                      actual)
        self.assertNotIn('t.provider_id,', actual)
        self.assertNotIn('t.visit_occurrence_id,', actual)
        self.assertNotIn('t.response_visit_occurrence_id,', actual)

    def test_questionnaire_response_id_follows_survey_conduct(self):
        actual = self._table_query(OBSERVATION)

        self.assertIn('mqr.survey_conduct_id AS questionnaire_response_id',
                      actual)
        self.assertIn(ct.mapping_table_for(SURVEY_CONDUCT), actual)

    def test_provider_id_is_remapped_wherever_it_is_referenced(self):
        for table_name in (VISIT_OCCURRENCE, MEASUREMENT, CONDITION_OCCURRENCE):
            with self.subTest(table_name=table_name):
                actual = self._table_query(table_name)

                self.assertIn('mpr.provider_id', actual)
                self.assertIn(ct.mapping_table_for(PROVIDER), actual)

    def test_fact_relationship_resolves_both_sides_per_domain(self):
        actual = self._table_query(ct.FACT_RELATIONSHIP)

        for domain_concept_id, domain_table in ct.DOMAIN_CONCEPT_ID_TO_TABLE.items(
        ):
            with self.subTest(domain_table=domain_table):
                self.assertIn(ct.mapping_table_for(domain_table), actual)
                self.assertIn(f'domain_concept_id_1 = {domain_concept_id}',
                              actual)
                self.assertIn(f'domain_concept_id_2 = {domain_concept_id}',
                              actual)

        self.assertIn('WHERE fact_id_1 IS NOT NULL', actual)
        self.assertIn('AND fact_id_2 IS NOT NULL', actual)

    def test_fact_relationship_domain_map_covers_the_observed_domains(self):
        """Domains seen in the CT input. Anything else resolves to NULL and is dropped."""
        expected = {
            10: PROCEDURE_OCCURRENCE,
            13: DRUG_EXPOSURE,
            19: CONDITION_OCCURRENCE,
            21: MEASUREMENT,
            27: OBSERVATION,
            57: CARE_SITE,
        }

        self.assertDictEqual(ct.DOMAIN_CONCEPT_ID_TO_TABLE, expected)

    def test_aou_death_id_is_preserved(self):
        """aou_death_id is a GUID and is shared with CT, matching RT."""
        actual = self._table_query('aou_death')

        self.assertIn('t.aou_death_id', actual)
        self.assertIn('mp.person_id', actual)

    def test_person_mapping_from_another_tier_is_rejected(self):
        client = mock.MagicMock()
        client.table_exists.return_value = True
        client.query.return_value.result.return_value = [{
            'src_table_id': f'{self.pipeline_dataset_id}.{self.ids_view}'
        }]

        with self.assertRaises(RuntimeError):
            ct.assert_person_mapping_is_ct_plus(client, self.project_id,
                                                self.mapping_dataset_id,
                                                self.pipeline_dataset_id,
                                                self.ids_view)

    def test_person_mapping_from_a_previous_ct_plus_run_is_accepted(self):
        client = mock.MagicMock()
        client.table_exists.return_value = True
        client.query.return_value.result.return_value = [{
            'src_table_id':
                ct.person_mapping_src_table_id(self.pipeline_dataset_id,
                                               self.ids_view)
        }]

        ct.assert_person_mapping_is_ct_plus(client, self.project_id,
                                            self.mapping_dataset_id,
                                            self.pipeline_dataset_id,
                                            self.ids_view)

    @mock.patch('tools.regenerate_ct_plus_ids.bq_utils.query')
    @mock.patch('tools.regenerate_ct_plus_ids.BigQueryClient')
    def test_existing_domain_mapping_is_appended_to(self, mock_client,
                                                    mock_query):
        """Truncating would hand every previously published row a new CT+ id."""
        mock_client.return_value.table_exists.return_value = True

        ct.mapping(MEASUREMENT, self.input_dataset_id, self.output_dataset_id,
                   self.project_id, self.pipeline_dataset_id, self.ids_view,
                   None, self.mapping_dataset_id)

        self.assertEqual(mock_query.call_args.kwargs['write_disposition'],
                         'WRITE_APPEND')
        self.assertIn('NOT EXISTS', mock_query.call_args.args[0])

    @mock.patch('tools.regenerate_ct_plus_ids.bq_utils.query')
    @mock.patch('tools.regenerate_ct_plus_ids.BigQueryClient')
    def test_person_mapping_is_rebuilt_from_the_view_every_run(
            self, mock_client, mock_query):
        """Skipping it would leave participants new in a later release unmapped."""
        mock_client.return_value.table_exists.return_value = True

        ct.mapping(PERSON, self.input_dataset_id, self.output_dataset_id,
                   self.project_id, self.pipeline_dataset_id, self.ids_view,
                   None, self.mapping_dataset_id)

        self.assertEqual(mock_query.call_args.kwargs['write_disposition'],
                         'WRITE_TRUNCATE')
        self.assertIn(ct.CT_PLUS_PERSON_ID_COLUMN, mock_query.call_args.args[0])

    @staticmethod
    def _person_mapping_client(row_count, src_count, ct_plus_count):
        client = mock.MagicMock()
        client.table_exists.return_value = True
        client.query.return_value.result.return_value = [{
            'row_count': row_count,
            'src_count': src_count,
            'ct_plus_count': ct_plus_count
        }]
        return client

    def test_one_to_one_person_mapping_is_accepted(self):
        client = self._person_mapping_client(1000, 1000, 1000)

        ct.assert_person_mapping_is_one_to_one(client, self.project_id,
                                               self.mapping_dataset_id)

    def test_participant_with_two_ct_plus_ids_stops_the_run(self):
        """DISTINCT does not collapse these, and the LEFT JOIN would double the rows."""
        client = self._person_mapping_client(1001, 1000, 1001)

        with self.assertRaises(RuntimeError) as ctx:
            ct.assert_person_mapping_is_one_to_one(client, self.project_id,
                                                   self.mapping_dataset_id)

        self.assertIn('more than one CT+ id', str(ctx.exception))

    def test_ct_plus_id_shared_by_two_participants_stops_the_run(self):
        """Loading this would merge two people into one in the released data."""
        client = self._person_mapping_client(1000, 1000, 999)

        with self.assertRaises(RuntimeError) as ctx:
            ct.assert_person_mapping_is_one_to_one(client, self.project_id,
                                                   self.mapping_dataset_id)

        self.assertIn('shared by more than one participant', str(ctx.exception))

    def test_missing_person_mapping_skips_the_one_to_one_check(self):
        client = mock.MagicMock()
        client.table_exists.return_value = False

        ct.assert_person_mapping_is_one_to_one(client, self.project_id,
                                               self.mapping_dataset_id)

        client.query.assert_not_called()

    def test_non_empty_output_dataset_stops_the_run(self):
        """main() drops every CDM table in the output dataset before loading."""
        client = mock.MagicMock()
        client.list_tables.return_value = [
            mock.Mock(table_id='observation'),
            mock.Mock(table_id='measurement')
        ]

        with self.assertRaises(RuntimeError) as ctx:
            ct.assert_output_dataset_is_safe(client, self.output_dataset_id,
                                             False)

        self.assertIn(self.output_dataset_id, str(ctx.exception))

    def test_empty_output_dataset_is_accepted(self):
        client = mock.MagicMock()
        client.list_tables.return_value = []

        ct.assert_output_dataset_is_safe(client, self.output_dataset_id, False)

    def test_absent_output_dataset_is_accepted(self):
        """main() creates it further down, so a missing dataset is not an error."""
        client = mock.MagicMock()
        client.list_tables.side_effect = Exception('404 Not found')

        ct.assert_output_dataset_is_safe(client, self.output_dataset_id, False)

    def test_allow_replace_skips_the_check_without_listing(self):
        client = mock.MagicMock()

        ct.assert_output_dataset_is_safe(client, self.output_dataset_id, True)

        client.list_tables.assert_not_called()

    def test_missing_person_mapping_is_accepted(self):
        client = mock.MagicMock()
        client.table_exists.return_value = False

        ct.assert_person_mapping_is_ct_plus(client, self.project_id,
                                            self.mapping_dataset_id,
                                            self.pipeline_dataset_id,
                                            self.ids_view)

        client.query.assert_not_called()
