"""
Integration test for split_ct_plus_pediatric_dataset.

Original Issues: DL2537

Participant 1 is an adult and participant 2 is pediatric. The fixture carries a shared
care_site row, a guardian-about observation record keyed to the adult but belonging to
the child's survey, and adult-child linkage rows in fact_relationship.
"""
# Python imports
import os

# Third party imports
from google.cloud.bigquery import Table

# Project imports
from app_identity import PROJECT_ID
from common import (CARE_SITE, CT_PLUS_PEDIATRIC_COHORT, FACT_RELATIONSHIP,
                    JINJA_ENV, OBSERVATION, PERSON, SURVEY_CONDUCT,
                    VISIT_OCCURRENCE)
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import \
    BaseTest
from tools import split_ct_plus_pediatric_dataset as split

INPUT_TABLES = [
    PERSON, VISIT_OCCURRENCE, SURVEY_CONDUCT, OBSERVATION, CARE_SITE,
    FACT_RELATIONSHIP
]

COHORT_SCHEMA = [{
    "type": "integer",
    "name": "participant_id",
    "mode": "nullable"
}, {
    "type": "string",
    "name": "age_band",
    "mode": "nullable"
}, {
    "type": "integer",
    "name": "controlled_tier_id",
    "mode": "nullable"
}, {
    "type": "integer",
    "name": "person_id",
    "mode": "nullable"
}]

LOAD_DATA = JINJA_ENV.from_string("""
INSERT INTO `{{project}}.{{cohort_dataset}}.{{cohort_table}}`
(participant_id, age_band, controlled_tier_id, person_id)
VALUES (9002, '0-6', 8002, 2);

INSERT INTO `{{project}}.{{input}}.person`
(person_id, gender_concept_id, year_of_birth, race_concept_id, ethnicity_concept_id)
VALUES (1, 0, 1985, 0, 0), (2, 0, 2021, 0, 0);

INSERT INTO `{{project}}.{{input}}.care_site` (care_site_id)
VALUES (7);

INSERT INTO `{{project}}.{{input}}.visit_occurrence`
(visit_occurrence_id, person_id, visit_concept_id, visit_start_date, visit_end_date,
 visit_type_concept_id, care_site_id)
VALUES
  (11, 1, 0, '2024-01-01', '2024-01-01', 0, 7),
  (21, 2, 0, '2024-01-01', '2024-01-01', 0, 7);

INSERT INTO `{{project}}.{{input}}.survey_conduct`
(survey_conduct_id, person_id, survey_concept_id, survey_end_datetime,
 assisted_concept_id, respondent_type_concept_id, timing_concept_id,
 collection_method_concept_id, survey_source_concept_id, validated_survey_concept_id)
VALUES
  (101, 1, 0, '2024-01-01 00:00:00', 0, 0, 0, 0, 0, 0),
  (201, 2, 0, '2024-01-01 00:00:00', 0, 0, 0, 0, 0, 0);

INSERT INTO `{{project}}.{{input}}.observation`
(observation_id, person_id, observation_concept_id, observation_date,
 observation_type_concept_id, questionnaire_response_id)
VALUES
  /* the adult's own survey answer, stays in the base */
  (1001, 1, 0, '2024-01-01', 0, 101),
  /* the child's own answer */
  (2001, 2, 0, '2024-01-01', 0, 201),
  /* guardian-about: keyed to the adult, answered on the child's survey */
  (2002, 1, 0, '2024-01-01', 0, 201);

INSERT INTO `{{project}}.{{input}}.fact_relationship`
(domain_concept_id_1, fact_id_1, domain_concept_id_2, fact_id_2, relationship_concept_id)
VALUES
  /* adult-child linkage, both directions */
  (56, 1, 56, 2, 4326600),
  (56, 2, 56, 1, 4326600),
  /* a pair of the child's observation rows */
  (27, 2001, 27, 2002, 581411),
  /* adult only, stays in the base */
  (27, 1001, 57, 7, 0)
""")


class SplitCtPlusPediatricDatasetTest(BaseTest.BigQueryTestBase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        super().initialize_class_vars()

        cls.project_id = os.environ.get(PROJECT_ID)
        # Dedicated datasets: the split copies every person-keyed table it finds,
        # so a shared dataset would pull in other tests' tables.
        base = os.environ.get('COMBINED_DATASET_ID')
        cls.input_dataset_id = f'{base}_peds_split_input'
        cls.cohort_dataset_id = f'{base}_peds_split_sandbox'
        cls.output_dataset_id = f'{base}_peds_split_output'

        cls.fq_table_names = [
            f'{cls.project_id}.{cls.input_dataset_id}.{table}'
            for table in INPUT_TABLES
        ]
        cls.fq_cohort = (f'{cls.project_id}.{cls.cohort_dataset_id}.'
                         f'{CT_PLUS_PEDIATRIC_COHORT}')
        cls.fq_sandbox_table_names = [cls.fq_cohort] + [
            f'{cls.project_id}.{cls.output_dataset_id}.{table}'
            for table in INPUT_TABLES
        ]

        super().setUpClass()

    def setUp(self):
        super().setUp()

        self.client.create_table(Table(self.fq_cohort, COHORT_SCHEMA))
        self.load_test_data([
            LOAD_DATA.render(project=self.project_id,
                             input=self.input_dataset_id,
                             cohort_dataset=self.cohort_dataset_id,
                             cohort_table=CT_PLUS_PEDIATRIC_COHORT)
        ])

    def _output(self, table):
        return f'{self.project_id}.{self.output_dataset_id}.{table}'

    def test_split_writes_the_pediatric_dataset(self):
        split.assert_cohort_is_usable(self.client, self.project_id,
                                      self.cohort_dataset_id,
                                      self.input_dataset_id)
        split.assert_output_dataset_is_empty(self.client, self.project_id,
                                             self.output_dataset_id)

        split.split(self.client, self.project_id, self.input_dataset_id,
                    self.cohort_dataset_id, self.output_dataset_id)

        # The child's own rows, with row IDs and person IDs unchanged, and no
        # adult person row
        self.assertTableValuesMatch(self._output(PERSON), ['person_id'], [(2,)])
        self.assertTableValuesMatch(self._output(VISIT_OCCURRENCE),
                                    ['visit_occurrence_id', 'person_id'],
                                    [(21, 2)])
        self.assertTableValuesMatch(self._output(SURVEY_CONDUCT),
                                    ['survey_conduct_id', 'person_id'],
                                    [(201, 2)])

        # The guardian-about record rides with the child's survey_conduct row
        self.assertTableValuesMatch(
            self._output(OBSERVATION),
            ['observation_id', 'person_id', 'questionnaire_response_id'],
            [(2001, 2, 201), (2002, 1, 201)])

        # Linkage in both directions and the child's observation pair
        self.assertTableValuesMatch(self._output(FACT_RELATIONSHIP), [
            'domain_concept_id_1', 'fact_id_1', 'domain_concept_id_2',
            'fact_id_2'
        ], [(56, 1, 56, 2), (56, 2, 56, 1), (27, 2001, 27, 2002)])

        # care_site stays in the base, where the child's care_site_id resolves
        self.assertTableDoesNotExist(self._output(CARE_SITE))
