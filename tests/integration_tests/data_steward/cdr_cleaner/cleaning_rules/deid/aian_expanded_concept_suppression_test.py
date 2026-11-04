"""
Integration test for AianExpandedConceptSuppressionCtPlus

Original Issues: DL-2538

Runs the rule through the clean engine with person_id already holding CT research ids,
as it does after CtPlusPIDtoRID. The concept set is the shipped one, and the vocabulary
tables are empty, so the vehicle lists add nothing.
"""

# Python Imports
import os

# Third party imports
from google.cloud.bigquery import Table

# Project Imports
import cdr_cleaner.clean_cdr_engine as clean_engine
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.deid.aian_expanded_concept_suppression import \
    AianExpandedConceptSuppressionCtPlus
from cdr_cleaner.cleaning_rules.deid.ct_plus_expanded_concepts import \
    VEHICLE_LOOKUPS
from common import (AIAN_LIST, CONCEPT, CONCEPT_ANCESTOR, CONCEPT_RELATIONSHIP,
                    CONDITION_OCCURRENCE, CT_PLUS_EXPANDED_CONCEPTS, DEID_MAP,
                    JINJA_ENV, NOTE, NOTE_NLP, PERSON)
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import \
    BaseTest

# Filed under suicide.csv, flagged expanded
SUICIDE = 432507
# Essential hypertension, on no privacy list
HYPERTENSION = 320128

PERSON_ID_SCHEMA = [{
    "type": "integer",
    "name": "person_id",
    "mode": "nullable"
}]

DEID_MAP_SCHEMA = PERSON_ID_SCHEMA + [{
    "type": "integer",
    "name": "research_id",
    "mode": "nullable"
}]

# Participant ids are 10x, CT research ids x. Participant 3 is AIAN.
#   condition 1: eligible participant's expanded row, kept
#   condition 2: AIAN participant's expanded row, removed
#   condition 3: AIAN participant's row with no expanded concept, kept
#   note_nlp 11: expanded, on the AIAN participant's note, removed
#   note_nlp 12: expanded, on the eligible participant's note, kept
LOAD_DATA = JINJA_ENV.from_string("""
INSERT INTO `{{project}}.{{dataset}}.person`
(person_id, gender_concept_id, year_of_birth, race_concept_id,
 ethnicity_concept_id)
VALUES (1, 0, 1980, 0, 0), (3, 0, 1975, 0, 0);

INSERT INTO `{{project}}.{{dataset}}.condition_occurrence`
(condition_occurrence_id, person_id, condition_concept_id,
 condition_start_date, condition_start_datetime, condition_type_concept_id)
VALUES
  (1, 1, {{suicide}}, '2000-01-01', '2000-01-01 00:00:00', 0),
  (2, 3, {{suicide}}, '2000-01-01', '2000-01-01 00:00:00', 0),
  (3, 3, {{hypertension}}, '2000-01-01', '2000-01-01 00:00:00', 0);

INSERT INTO `{{project}}.{{dataset}}.note`
(note_id, person_id, note_date, note_datetime, note_type_concept_id,
 note_class_concept_id, note_title, note_text, encoding_concept_id,
 language_concept_id)
VALUES
  (21, 3, '2000-01-01', '2000-01-01 00:00:00', 0, 0, '', '', 0, 0),
  (22, 1, '2000-01-01', '2000-01-01 00:00:00', 0, 0, '', '', 0, 0);

INSERT INTO `{{project}}.{{dataset}}.note_nlp`
(note_nlp_id, note_id, lexical_variant, note_nlp_concept_id, nlp_date)
VALUES
  (11, 21, '', {{suicide}}, '2000-01-01'),
  (12, 22, '', {{suicide}}, '2000-01-01')
""")


class AianExpandedConceptSuppressionCtPlusTest(BaseTest.CleaningRulesTestBase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        super().initialize_class_vars()

        cls.project_id = os.environ.get(PROJECT_ID)
        cls.dataset_id = os.environ.get('COMBINED_DATASET_ID')
        cls.sandbox_id = f'{cls.dataset_id}_sandbox'
        cls.rdr_dataset_id = os.environ.get('RDR_DATASET_ID')

        cls.kwargs = {'rdr_sandbox_id': cls.rdr_dataset_id}
        cls.rule_instance = AianExpandedConceptSuppressionCtPlus(
            cls.project_id, cls.dataset_id, cls.sandbox_id, cls.rdr_dataset_id)

        for table in [
                PERSON, CONDITION_OCCURRENCE, NOTE, NOTE_NLP, CONCEPT,
                CONCEPT_ANCESTOR, CONCEPT_RELATIONSHIP
        ]:
            cls.fq_table_names.append(
                f'{cls.project_id}.{cls.dataset_id}.{table}')

        for table in [CONDITION_OCCURRENCE, NOTE_NLP]:
            cls.fq_sandbox_table_names.append(
                f'{cls.project_id}.{cls.sandbox_id}.'
                f'{cls.rule_instance.sandbox_table_for(table)}')
        for table in [CT_PLUS_EXPANDED_CONCEPTS, *VEHICLE_LOOKUPS.values()]:
            cls.fq_sandbox_table_names.append(
                f'{cls.project_id}.{cls.sandbox_id}.{table}')

        # Not via fq_table_names, whose tables are created from schema files by
        # name. setUp creates these and tearDown drops them.
        cls.extra_tables = {
            f'{cls.project_id}.{cls.rdr_dataset_id}.{AIAN_LIST}':
                (PERSON_ID_SCHEMA, 'INSERT INTO `{fq}` (person_id) VALUES (103)'
                ),
            f'{cls.project_id}.{cls.sandbox_id}.{DEID_MAP}':
                (DEID_MAP_SCHEMA, 'INSERT INTO `{fq}` (person_id, research_id) '
                 'VALUES (101, 1), (103, 3)'),
        }

        super().setUpClass()

        # BaseTest creates only the datasets of fq_table_names and
        # fq_sandbox_table_names, and aian_list lives in the RDR dataset.
        desc = (f'dataset created by {cls.__name__} to test a cleaning rule.  '
                f'deletion candidate.')
        dataset = cls.client.define_dataset(cls.rdr_dataset_id, desc,
                                            {'test': ''})
        cls.client.create_dataset(dataset, exists_ok=True)

    def setUp(self):
        super().setUp()

        queries = [
            LOAD_DATA.render(project=self.project_id,
                             dataset=self.dataset_id,
                             suicide=SUICIDE,
                             hypertension=HYPERTENSION)
        ]
        for fq_table, (schema, data) in self.extra_tables.items():
            self.client.create_table(Table(fq_table, schema))
            queries.append(data.format(fq=fq_table))

        self.load_test_data(queries)

    def tearDown(self):
        for fq_table in self.extra_tables:
            self.client.delete_table(fq_table, not_found_ok=True)

        super().tearDown()

    def test_only_aian_expanded_rows_are_removed(self):
        clean_engine.clean_dataset(self.project_id, self.dataset_id,
                                   self.sandbox_id,
                                   [(AianExpandedConceptSuppressionCtPlus,)],
                                   **self.kwargs)

        fq_dataset = f'{self.project_id}.{self.dataset_id}'
        self.assertTableValuesMatch(f'{fq_dataset}.{CONDITION_OCCURRENCE}',
                                    ['condition_occurrence_id', 'person_id'],
                                    [(1, 1), (3, 3)])
        self.assertTableValuesMatch(f'{fq_dataset}.{NOTE_NLP}',
                                    ['note_nlp_id', 'note_id'], [(12, 22)])
