"""
Integration test for the route_pediatric_adult_observations module.

Original Issues: DL-2483

Asserts that each of the guardian-about Pediatric Basics items produces an observation
record on the linked adult's person_id, that the pediatric participant's own row is
left in place, and that an item about the child is not emitted. Also asserts the three
ways a linkage can fail to resolve, each of which must record the response and leave it
unchanged rather than emitting a record: no person domain linkage row, more than one
linked adult, and a counterpart who is themselves a pediatric participant.
"""
# Python Imports
import os

# Project Imports
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.route_pediatric_adult_observations import (
    RoutePediatricAdultObservations)
from common import (FACT_RELATIONSHIP, JINJA_ENV, MAPPING_PREFIX, OBSERVATION,
                    SURVEY_CONDUCT)
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest

OBSERVATION_TMPL = JINJA_ENV.from_string("""
INSERT INTO `{{project}}.{{dataset}}.observation`
(observation_id, person_id, observation_concept_id, observation_date, observation_datetime,
 observation_type_concept_id, observation_source_value, observation_source_concept_id,
 value_source_value, questionnaire_response_id)
VALUES
-- Adult 100's own TheBasics answer. Same PPI code as the pediatric item, so this is
-- what proves the survey filter and not the concept list is doing the work. --
  (601, 100, 40771091, DATE('2020-01-01'), TIMESTAMP('2020-01-01'), 45905771,
   'EducationLevel_HighestGrade', 1585940, 'HighestGrade_CollegeGraduate', 5005),
-- Child 101, guardian-about item, mapped by the export. Emitted onto adult 100. --
  (602, 101, 40771091, DATE('2026-01-01'), TIMESTAMP('2026-01-01'), 45905771,
   'EducationLevel_HighestGrade', 1585940, 'HighestGrade_AdvancedDegree', 5001),
-- Child 101, an item about the child. Outside the twelve, must not be emitted. --
  (603, 101, 0, DATE('2026-01-01'), TIMESTAMP('2026-01-01'), 45905771,
   'aou_1', 0, 'PMI_Skip', 5001),
-- Child 101, guardian-about item the export left unmapped. The declared standard
-- concept 46235933 fills in, which is the only case where it is used. --
  (604, 101, 0, DATE('2026-01-01'), TIMESTAMP('2026-01-01'), 45905771,
   'Income_AnnualIncome', 1585375, 'AnnualIncome_50k75k', 5001),
-- Child 102, a second child of the same adult 100. --
  (605, 102, 1585370, DATE('2026-01-01'), TIMESTAMP('2026-01-01'), 45905771,
   'HomeOwn_CurrentHomeOwn', 1585370, 'CurrentHomeOwn_Own', 5002),
-- Child 200 has no person domain linkage row at all. --
  (606, 200, 40771091, DATE('2026-01-01'), TIMESTAMP('2026-01-01'), 45905771,
   'EducationLevel_HighestGrade', 1585940, 'HighestGrade_TwelveOrGED', 5003),
-- Child 300 is linked to two adults, so no single adult can be chosen. --
  (607, 300, 40771091, DATE('2026-01-01'), TIMESTAMP('2026-01-01'), 45905771,
   'EducationLevel_HighestGrade', 1585940, 'HighestGrade_CollegeOnetoThree', 5004),
-- Child 400's only counterpart is child 101, who is themselves pediatric. A sibling
-- pair must not be read as an adult link. --
  (608, 400, 40771091, DATE('2026-01-01'), TIMESTAMP('2026-01-01'), 45905771,
   'EducationLevel_HighestGrade', 1585940, 'HighestGrade_NineThroughEleven', 5006)
""")

SURVEY_CONDUCT_TMPL = JINJA_ENV.from_string("""
INSERT INTO `{{project}}.{{dataset}}.survey_conduct`
(survey_conduct_id, person_id, survey_concept_id, survey_source_value)
VALUES
-- The pediatric survey concept has not been minted, so survey_concept_id arrives as 0
-- and survey_source_value is what identifies the instrument. --
  (5001, 101, 0, 'ped_basics'),
  (5002, 102, 0, 'ped_basics'),
  (5003, 200, 0, 'ped_basics'),
  (5004, 300, 0, 'ped_basics'),
  (5005, 100, 1586134, 'TheBasics'),
  (5006, 400, 0, 'ped_basics')
""")

FACT_RELATIONSHIP_TMPL = JINJA_ENV.from_string("""
INSERT INTO `{{project}}.{{dataset}}.fact_relationship`
(domain_concept_id_1, fact_id_1, domain_concept_id_2, fact_id_2, relationship_concept_id)
VALUES
-- Both directions are emitted for every pair, which is why reading only side 1 must
-- not double count. --
  (56, 101, 56, 100, 4053608),
  (56, 100, 56, 101, 4326600),
  (56, 102, 56, 100, 4053608),
  (56, 100, 56, 102, 4326600),
-- Child 300 linked to two adults. --
  (56, 300, 56, 301, 4053608),
  (56, 300, 56, 302, 4053608),
-- Child 400 linked only to child 101. --
  (56, 400, 56, 101, 4218412),
-- A measurement domain pair, which the rule must ignore entirely. --
  (21, 900, 21, 901, 581436)
""")

MAPPING_TMPL = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project}}.{{dataset}}._mapping_observation`
    (observation_id INT64, src_id STRING)
    ;
INSERT INTO `{{project}}.{{dataset}}._mapping_observation`
(observation_id, src_id)
VALUES
  (601, 'src_adult'),
  (602, 'src_peds'),
  (603, 'src_peds'),
  (604, 'src_peds'),
  (605, 'src_peds'),
  (606, 'src_peds'),
  (607, 'src_peds'),
  (608, 'src_peds')
""")


class RoutePediatricAdultObservationsTest(BaseTest.CleaningRulesTestBase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        super().initialize_class_vars()

        cls.project_id = os.environ.get(PROJECT_ID)
        cls.dataset_id = os.environ.get('RDR_DATASET_ID')
        cls.sandbox_id = f'{cls.dataset_id}_sandbox'

        cls.rule_instance = RoutePediatricAdultObservations(
            cls.project_id, cls.dataset_id, cls.sandbox_id)

        for table_name in cls.rule_instance.get_sandbox_tablenames():
            cls.fq_sandbox_table_names.append(
                f'{cls.project_id}.{cls.sandbox_id}.{table_name}')

        # _mapping_observation is kept out of fq_table_names because its columns come
        # from create_rdr_snapshot.py rather than the resource_files schema, the same
        # reason the backfill tests create it by hand.
        cls.fq_mapping_table_name = (
            f'{cls.project_id}.{cls.dataset_id}.{MAPPING_PREFIX}{OBSERVATION}')

        cls.fq_table_names = [
            f'{cls.project_id}.{cls.dataset_id}.{OBSERVATION}',
            f'{cls.project_id}.{cls.dataset_id}.{SURVEY_CONDUCT}',
            f'{cls.project_id}.{cls.dataset_id}.{FACT_RELATIONSHIP}',
        ]

        super().setUpClass()

    def setUp(self):
        super().setUp()

        self.load_test_data([
            OBSERVATION_TMPL.render(project=self.project_id,
                                    dataset=self.dataset_id),
            SURVEY_CONDUCT_TMPL.render(project=self.project_id,
                                       dataset=self.dataset_id),
            FACT_RELATIONSHIP_TMPL.render(project=self.project_id,
                                          dataset=self.dataset_id),
            MAPPING_TMPL.render(project=self.project_id,
                                dataset=self.dataset_id),
        ])

    def test_route_pediatric_adult_observations(self):
        """
        Eight loaded responses over six participants.

        Three are emitted onto adult 100: two from child 101 and one from child 102.
        Emitted observation_ids continue from the loaded maximum of 608, ordered by
        the pediatric person_id then the source observation_id, so 602 becomes 609,
        604 becomes 610 and 605 becomes 611.

        Every loaded row survives unchanged, including the pediatric participants'
        own rows, which the PRD requires be preserved.
        """
        tables_and_counts = [{
            'fq_table_name':
                self.fq_table_names[0],
            'fq_sandbox_table_name':
                self.fq_sandbox_table_names[0],
            'loaded_ids': [601, 602, 603, 604, 605, 606, 607, 608],
            'sandboxed_ids': [609, 610, 611],
            'fields': [
                'observation_id', 'person_id', 'observation_concept_id',
                'observation_source_value', 'observation_source_concept_id',
                'questionnaire_response_id'
            ],
            'cleaned_values': [
                (601, 100, 40771091, 'EducationLevel_HighestGrade', 1585940,
                 5005),
                (602, 101, 40771091, 'EducationLevel_HighestGrade', 1585940,
                 5001),
                (603, 101, 0, 'aou_1', 0, 5001),
                (604, 101, 0, 'Income_AnnualIncome', 1585375, 5001),
                (605, 102, 1585370, 'HomeOwn_CurrentHomeOwn', 1585370, 5002),
                (606, 200, 40771091, 'EducationLevel_HighestGrade', 1585940,
                 5003),
                (607, 300, 40771091, 'EducationLevel_HighestGrade', 1585940,
                 5004),
                (608, 400, 40771091, 'EducationLevel_HighestGrade', 1585940,
                 5006),
                # emitted onto the adult, carrying the pediatric survey's
                # questionnaire_response_id so they stay distinguishable from 601
                (609, 100, 40771091, 'EducationLevel_HighestGrade', 1585940,
                 5001),
                # the export left 604 unmapped, so the declared standard concept
                # 46235933 fills in here
                (610, 100, 46235933, 'Income_AnnualIncome', 1585375, 5001),
                (611, 100, 1585370, 'HomeOwn_CurrentHomeOwn', 1585370, 5002),
            ]
        }]

        self.default_test(tables_and_counts)

        # Every response whose linkage did not resolve is recorded, with the reason.
        self.assertTableValuesMatch(
            self.fq_sandbox_table_names[1],
            ['observation_id', 'person_id', 'unresolved_reason'],
            [(606, 200, 'no person domain linkage row for this participant'),
             (607, 300, 'linked to more than one adult'),
             (608, 400, 'no person domain linkage row for this participant')])

        # Each emitted record inherits the src_id of the pediatric response it came
        # from. A record missing here would carry a dangling observation_id.
        self.assertTableValuesMatch(self.fq_mapping_table_name,
                                    ['observation_id', 'src_id'],
                                    [(601, 'src_adult'), (602, 'src_peds'),
                                     (603, 'src_peds'), (604, 'src_peds'),
                                     (605, 'src_peds'), (606, 'src_peds'),
                                     (607, 'src_peds'), (608, 'src_peds'),
                                     (609, 'src_peds'), (610, 'src_peds'),
                                     (611, 'src_peds')])
