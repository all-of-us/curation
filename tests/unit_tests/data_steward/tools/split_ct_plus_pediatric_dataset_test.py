"""
Unit test for split_ct_plus_pediatric_dataset, and the add-on dataset naming helper it
introduces.
"""
# Python imports
import unittest
from unittest import mock

# Third party imports
from google.cloud.bigquery import SchemaField

# Project imports
import common
from common import (CARE_SITE, CT_PLUS_DOB_INDICATORS, CT_PLUS_ELEMENT_DELTA,
                    CT_PLUS_PEDIATRICS, CT_PLUS_ZIP5, FACT_RELATIONSHIP,
                    MEASUREMENT, NOTE, NOTE_NLP, OBSERVATION,
                    PEDIATRIC_RELATIONSHIP_EXT, PERSON, PROVIDER,
                    SURVEY_CONDUCT, VISIT_OCCURRENCE,
                    get_ct_plus_addon_dataset_name)
from tools import split_ct_plus_pediatric_dataset as split


class GetCtPlusAddonDatasetNameTest(unittest.TestCase):

    def test_one_dataset_components_take_their_settled_suffix(self):
        """zip5 and dob_indicators are named by suffix, not by data element."""
        for component, expected in [
            (CT_PLUS_PEDIATRICS, 'CP2025q4r7_pediatrics'),
            (CT_PLUS_ZIP5, 'CP2025q4r7_zip5'),
            (CT_PLUS_DOB_INDICATORS, 'CP2025q4r7_dob'),
        ]:
            with self.subTest(component=component):
                self.assertEqual(
                    get_ct_plus_addon_dataset_name('2025q4r7', component),
                    expected)

    def test_element_delta_is_named_after_its_data_element(self):
        self.assertEqual(
            get_ct_plus_addon_dataset_name('2025q4r7', CT_PLUS_ELEMENT_DELTA,
                                           'drowning'), 'CP2025q4r7_drowning')

    def test_element_delta_without_a_data_element_is_refused(self):
        with self.assertRaises(ValueError):
            get_ct_plus_addon_dataset_name('2025q4r7', CT_PLUS_ELEMENT_DELTA)

    def test_a_data_element_on_a_one_dataset_component_is_refused(self):
        """dob_indicators is carried by four data elements and is still one
        dataset, so naming it after one of them is an error."""
        with self.assertRaises(ValueError):
            get_ct_plus_addon_dataset_name('2025q4r7', CT_PLUS_DOB_INDICATORS,
                                           'dob')

    def test_unknown_component_is_refused(self):
        for component in ['geolocation', 'none', 'mainline']:
            with self.subTest(component=component):
                with self.assertRaises(ValueError):
                    get_ct_plus_addon_dataset_name('2025q4r7', component)


class SplitCtPlusPediatricDatasetTest(unittest.TestCase):

    def setUp(self):
        self.project_id = 'fake_project'
        self.input_dataset_id = 'CP2025q4r7_deid_clean_pre_split'
        self.cohort_dataset_id = 'CP2025q4r7_deid_sandbox'
        self.output_dataset_id = 'CP2025q4r7_pediatrics'
        self.params = dict(project=self.project_id,
                           input_dataset=self.input_dataset_id,
                           output_dataset=self.output_dataset_id,
                           cohort_dataset=self.cohort_dataset_id,
                           cohort_table=common.CT_PLUS_PEDIATRIC_COHORT)

    def test_pre_split_input_of_the_release_is_accepted(self):
        split.validate_dataset_names('2025q4r7', self.input_dataset_id)

    def test_other_inputs_are_refused(self):
        """A published dataset, another release's input, and the pipeline output
        all hold the wrong rows."""
        for input_dataset_id in [
                'CP2025q4r7_deid_clean', 'CP2025q4r6_deid_clean_pre_split',
                'CP2025q4r7_deid_clean_pre_rekey',
                'C2025q4r7_deid_clean_pre_split'
        ]:
            with self.subTest(input_dataset_id=input_dataset_id):
                with self.assertRaises(ValueError):
                    split.validate_dataset_names('2025q4r7', input_dataset_id)

    def test_guardian_about_clause_is_on_observation_only(self):
        observation = split.PERSON_KEYED_ROWS.render(
            table=OBSERVATION,
            guardian_about=True,
            survey_conduct=SURVEY_CONDUCT,
            **self.params)
        measurement = split.PERSON_KEYED_ROWS.render(
            table=MEASUREMENT,
            guardian_about=False,
            survey_conduct=SURVEY_CONDUCT,
            **self.params)

        self.assertIn('t.questionnaire_response_id IN', observation)
        self.assertIn(f'{self.input_dataset_id}.{SURVEY_CONDUCT}', observation)
        self.assertNotIn('questionnaire_response_id', measurement)
        for query in (observation, measurement):
            self.assertIn(
                f'{self.cohort_dataset_id}.{common.CT_PLUS_PEDIATRIC_COHORT}',
                query)

    def test_fact_relationship_claims_either_side(self):
        query = split.FACT_RELATIONSHIP_ROWS.render(
            fact_relationship=FACT_RELATIONSHIP,
            claimed_domains=[(21, MEASUREMENT), (56, PERSON)],
            **self.params)

        for side in ('1', '2'):
            self.assertIn(f'fr.domain_concept_id_{side} = 21', query)
            self.assertIn(f'fr.domain_concept_id_{side} = 56', query)
        self.assertIn(
            f'SELECT person_id FROM `{self.project_id}.'
            f'{self.output_dataset_id}.{PERSON}`', query)

    def test_care_site_never_makes_a_fact_relationship_row_pediatric(self):
        self.assertNotIn(57, split.FACT_DOMAIN_TO_TABLE)
        self.assertNotIn(CARE_SITE, split.FACT_DOMAIN_TO_TABLE.values())

    @staticmethod
    def _cohort_client(schema_fields, cohort_count, matched_count):
        client = mock.MagicMock()
        client.get_table.return_value.schema = [
            SchemaField(name, 'INTEGER') for name in schema_fields
        ]
        client.query.return_value.result.return_value = [{
            'cohort_count': cohort_count,
            'matched_count': matched_count
        }]
        return client

    def _assert_cohort(self, client):
        split.assert_cohort_is_usable(client, self.project_id,
                                      self.cohort_dataset_id,
                                      self.input_dataset_id)

    def test_converted_cohort_is_accepted(self):
        self._assert_cohort(
            self._cohort_client(['participant_id', 'person_id'], 3, 3))

    def test_partial_overlap_is_logged_not_refused(self):
        with self.assertLogs(split.LOGGER, level='WARNING'):
            self._assert_cohort(
                self._cohort_client(['participant_id', 'person_id'], 3, 2))

    def test_unconverted_cohort_is_refused(self):
        client = self._cohort_client(['participant_id', 'age_band'], 3, 3)

        with self.assertRaises(RuntimeError) as ctx:
            self._assert_cohort(client)

        self.assertIn('ConvertCtPlusPediatricCohortIds', str(ctx.exception))
        client.query.assert_not_called()

    def test_empty_cohort_is_refused(self):
        with self.assertRaises(RuntimeError) as ctx:
            self._assert_cohort(
                self._cohort_client(['participant_id', 'person_id'], 0, 0))

        self.assertIn('is empty', str(ctx.exception))

    def test_cohort_outside_the_input_id_space_is_refused(self):
        with self.assertRaises(RuntimeError) as ctx:
            self._assert_cohort(
                self._cohort_client(['participant_id', 'person_id'], 3, 0))

        self.assertIn('different ID spaces', str(ctx.exception))

    def test_populated_output_dataset_is_refused(self):
        client = mock.MagicMock()
        table = mock.MagicMock()
        table.table_id = PERSON
        client.list_tables.return_value = [table]

        with self.assertRaises(RuntimeError):
            split.assert_output_dataset_is_empty(client, self.project_id,
                                                 self.output_dataset_id)

    @mock.patch.object(split, 'write_table', return_value=1)
    @mock.patch.object(split, 'get_person_keyed_tables')
    def test_split_claims_the_expected_tables_in_order(self, mock_person_keyed,
                                                       mock_write_table):
        """Person-keyed tables first, since everything after reads them back out
        of the output. provider and care_site stay in the base."""
        input_tables = [
            PERSON, VISIT_OCCURRENCE, OBSERVATION, SURVEY_CONDUCT, NOTE,
            NOTE_NLP, 'observation_ext', 'note_nlp_ext', 'person_ext',
            'care_site_ext', FACT_RELATIONSHIP, PROVIDER, CARE_SITE,
            PEDIATRIC_RELATIONSHIP_EXT
        ]
        person_keyed = [
            NOTE, OBSERVATION, PERSON, 'person_ext', SURVEY_CONDUCT,
            VISIT_OCCURRENCE
        ]
        mock_person_keyed.return_value = person_keyed
        client = mock.MagicMock()
        tables = []
        for table_id in input_tables:
            table = mock.MagicMock()
            table.table_id = table_id
            tables.append(table)
        client.list_tables.return_value = tables

        counts = split.split(client, self.project_id, self.input_dataset_id,
                             self.cohort_dataset_id, self.output_dataset_id)

        written = [call.args[4] for call in mock_write_table.call_args_list]
        self.assertEqual(written[:len(person_keyed)], person_keyed)
        self.assertEqual(written[len(person_keyed):], [
            NOTE_NLP, 'note_nlp_ext', 'observation_ext', FACT_RELATIONSHIP,
            PEDIATRIC_RELATIONSHIP_EXT
        ])
        for table in (PROVIDER, CARE_SITE, 'care_site_ext'):
            self.assertNotIn(table, counts)

        queries = {
            call.args[4]: call.args[5]
            for call in mock_write_table.call_args_list
        }
        self.assertIn('questionnaire_response_id', queries[OBSERVATION])
        self.assertNotIn('questionnaire_response_id', queries[PERSON])

    @mock.patch.object(split, 'write_table', return_value=1)
    @mock.patch.object(split, 'get_person_keyed_tables', return_value=[PERSON])
    def test_missing_relationship_table_is_logged(self, _, __):
        client = mock.MagicMock()
        table = mock.MagicMock()
        table.table_id = PERSON
        client.list_tables.return_value = [table]

        with self.assertLogs(split.LOGGER, level='WARNING') as logs:
            split.split(client, self.project_id, self.input_dataset_id,
                        self.cohort_dataset_id, self.output_dataset_id)

        self.assertIn(PEDIATRIC_RELATIONSHIP_EXT, ''.join(logs.output))


if __name__ == '__main__':
    unittest.main()
