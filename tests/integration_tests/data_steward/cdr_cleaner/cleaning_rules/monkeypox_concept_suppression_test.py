"""
Integration test for MonkeypoxConceptSuppression module

Original Issue: DC-2711
"""
# Python Imports
import os
import mock

# Project imports
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.monkeypox_concept_suppression import (
    MonkeypoxConceptSuppression, LOGGER, SUPPRESSION_RULE_CONCEPT_TABLE)
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest
from common import (AOU_DEATH, CARE_SITE, DEATH, DEID_MAP, FACT_RELATIONSHIP,
                    JINJA_ENV, OBSERVATION, SURVEY_CONDUCT)


class MonkeypoxConceptSuppressionTest(BaseTest.CleaningRulesTestBase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        super().initialize_class_vars()

        cls.project_id = os.environ.get(PROJECT_ID)
        cls.dataset_id = os.environ.get('COMBINED_DATASET_ID')
        cls.mapping_dataset_id = os.environ.get('COMBINED_DATASET_ID')
        cls.sandbox_id = f'{cls.dataset_id}_sandbox'

        cls.rule_instance = MonkeypoxConceptSuppression(cls.project_id,
                                                        cls.dataset_id,
                                                        cls.sandbox_id,
                                                        cls.mapping_dataset_id)

        cls.kwargs.update({'mapping_dataset_id': cls.mapping_dataset_id})

        cls.fq_table_names = [
            f'{cls.project_id}.{cls.dataset_id}.{OBSERVATION}',
            f'{cls.project_id}.{cls.dataset_id}.{SURVEY_CONDUCT}',
            f'{cls.project_id}.{cls.dataset_id}.{AOU_DEATH}',
            f'{cls.project_id}.{cls.dataset_id}.{DEATH}',
            f'{cls.project_id}.{cls.dataset_id}.{CARE_SITE}',
            f'{cls.project_id}.{cls.dataset_id}.{FACT_RELATIONSHIP}',
            f'{cls.project_id}.{cls.mapping_dataset_id}.{DEID_MAP}',
        ]

        cls.fq_sandbox_table_names = [
            f'{cls.project_id}.{cls.sandbox_id}.{cls.rule_instance.sandbox_table_for(OBSERVATION)}',
            f'{cls.project_id}.{cls.sandbox_id}.{cls.rule_instance.sandbox_table_for(SURVEY_CONDUCT)}',
            f'{cls.project_id}.{cls.sandbox_id}.{cls.rule_instance.sandbox_table_for(AOU_DEATH)}',
            f'{cls.project_id}.{cls.sandbox_id}.{cls.rule_instance.sandbox_table_for(DEATH)}',
            f'{cls.project_id}.{cls.sandbox_id}.{SUPPRESSION_RULE_CONCEPT_TABLE}',
        ]

        # call super to set up the client, create datasets, and create
        # empty test tables
        # NOTE:  does not create empty sandbox tables.
        super().setUpClass()

    @mock.patch(
        'cdr_cleaner.cleaning_rules.monkeypox_concept_suppression.MonkeypoxConceptSuppression.affected_tables',
        [
            OBSERVATION, SURVEY_CONDUCT, AOU_DEATH, DEATH, CARE_SITE,
            FACT_RELATIONSHIP
        ])
    def test_monkeypox_concept_suppression(self):
        """
        Tests that the specifications perform as designed.

        OBSERVATION:
            11... Not suppress: Not a monkeypox record.
            12... Not suppress: A monkeypox record but the un-shifted date is before the suppression start date.
            13... Not suppress: A monkeypox record but the un-shifted date is after the suppression end date.
            14... Suppress: A monkeypox record and the un-shifted date within the suppression period.
            15... Suppress: A monkeypox record, different concept column from 4.

        SURVEY_CONDUCT:
            21... Not suppress: Not a monkeypox record.
            22... Not suppress: A monkeypox record but the un-shifted date/datetime is before the suppression start date.
            23... Not suppress: A monkeypox record but the un-shifted date/datetime is after the suppression end date.
            24... Suppress: A monkeypox record and the un-shifted start datetime within the suppression period.
            25... Suppress: A monkeypox record and the un-shifted end datetime within the suppression period.

        AOU_DEATH: This is an AOU custom table. Its primary key is "aou_death_id".
            31... Not suppress: Not a monkeypox record.
            32... Suppress: A monkeypox record and the un-shifted death date within the suppression period.

        DEATH: Unlike other tables, its primary key is "person_id", not "death_id".
            31... Not suppress: Not a monkeypox record.
            32... Suppress: A monkeypox record and the un-shifted death date within the suppression period.

        CARE_SITE, FACT_RELATIONSHIP: No date or datetime fields in this table. This CR skips it.

        """

        INSERT_DEID_MAP_QUERY = JINJA_ENV.from_string("""
            INSERT INTO `{{project_id}}.{{mapping_dataset_id}}._deid_map`
                (person_id, shift)
            VALUES
                (101, 1),
                (102, 2),
                (103, 3),
                (104, 4),
                (105, 5)
        """).render(project_id=self.project_id,
                    mapping_dataset_id=self.mapping_dataset_id)

        INSERT_OBSERVATIONS_QUERY = JINJA_ENV.from_string("""
            INSERT INTO `{{project_id}}.{{dataset_id}}.observation`
                (observation_id, person_id, observation_concept_id, observation_source_concept_id, 
                 observation_date, observation_type_concept_id)
            VALUES
                (11, 101, 12345, 0, date('2022-09-01'), 1),
                (12, 102, 443405, 0, date('2022-05-14'), 2),
                (13, 103, 443405, 0, date('2023-05-15'), 3),
                (14, 104, 443405, 0, date('2022-05-13'), 4),
                (15, 105, 0, 3119802, date('2023-05-12'), 5)
        """).render(project_id=self.project_id, dataset_id=self.dataset_id)

        INSERT_SURVEY_CONDUCT_QUERY = JINJA_ENV.from_string("""
            INSERT INTO `{{project_id}}.{{dataset_id}}.survey_conduct`
                (survey_conduct_id, person_id, survey_concept_id,
                 survey_start_date, survey_start_datetime,
                 survey_end_date, survey_end_datetime,
                 assisted_concept_id, respondent_type_concept_id, timing_concept_id,
                 collection_method_concept_id, survey_source_concept_id,
                 validated_survey_concept_id)
            VALUES
                (21, 101, 12345, date('2022-09-01'), timestamp('2022-09-01 00:00:00'), date('2022-09-02'), timestamp('2022-09-02 00:00:00'), 0, 0, 0, 0, 0, 0),
                (22, 102, 443405, date('2022-05-13'), timestamp('2022-05-13 00:00:00'), date('2022-05-14'), timestamp('2022-05-14 00:00:00'), 0, 0, 0, 0, 0, 0),
                (23, 103, 443405, date('2023-05-15'), timestamp('2023-05-15 00:00:00'), date('2023-05-16'), timestamp('2023-05-16 00:00:00'), 0, 0, 0, 0, 0, 0),
                (24, 104, 443405, NULL, timestamp('2023-05-13 00:00:00'), NULL, timestamp('2023-05-15 00:00:00'), 0, 0, 0, 0, 0, 0),
                (25, 105, 443405, NULL, NULL, NULL, timestamp('2023-05-12 00:00:00'), 0, 0, 0, 0, 0, 0)
        """).render(project_id=self.project_id, dataset_id=self.dataset_id)

        INSERT_AOU_DEATH_QUERY = JINJA_ENV.from_string("""
            INSERT INTO `{{project_id}}.{{dataset_id}}.aou_death`
                (aou_death_id, person_id, death_date, death_type_concept_id, cause_concept_id, src_id, primary_death_record)
            VALUES
                ('aaa', 101, date('2022-09-01'), 0, 0, 'foo', True),
                ('bbb', 102, date('2022-09-01'), 0, 443405, 'bar', True)
        """).render(project_id=self.project_id, dataset_id=self.dataset_id)

        INSERT_DEATH_QUERY = JINJA_ENV.from_string("""
            INSERT INTO `{{project_id}}.{{dataset_id}}.death`
                (person_id, death_date, death_type_concept_id, cause_concept_id)
            VALUES
                (101, date('2022-09-01'), 0, 0),
                (102, date('2022-09-01'), 0, 443405)
        """).render(project_id=self.project_id, dataset_id=self.dataset_id)

        queries = [
            INSERT_DEID_MAP_QUERY, INSERT_OBSERVATIONS_QUERY,
            INSERT_SURVEY_CONDUCT_QUERY, INSERT_AOU_DEATH_QUERY,
            INSERT_DEATH_QUERY
        ]

        self.load_test_data(queries)

        tables_and_counts = [{
            'fq_table_name':
                '.'.join([self.project_id, self.dataset_id, OBSERVATION]),
            'fq_sandbox_table_name':
                self.fq_sandbox_table_names[0],
            'loaded_ids': [11, 12, 13, 14, 15],
            'sandboxed_ids': [14, 15],
            'fields': ['observation_id'],
            'cleaned_values': [(11,), (12,), (13,)]
        }, {
            'fq_table_name':
                '.'.join([self.project_id, self.dataset_id, SURVEY_CONDUCT]),
            'fq_sandbox_table_name':
                self.fq_sandbox_table_names[1],
            'loaded_ids': [21, 22, 23, 24, 25],
            'sandboxed_ids': [24, 25],
            'fields': ['survey_conduct_id'],
            'cleaned_values': [(21,), (22,), (23,)]
        }, {
            'fq_table_name':
                '.'.join([self.project_id, self.dataset_id, AOU_DEATH]),
            'fq_sandbox_table_name':
                self.fq_sandbox_table_names[2],
            'loaded_ids': ['aaa', 'bbb'],
            'sandboxed_ids': ['bbb'],
            'fields': ['aou_death_id'],
            'cleaned_values': [('aaa',)]
        }, {
            'fq_table_name':
                '.'.join([self.project_id, self.dataset_id, DEATH]),
            'fq_sandbox_table_name':
                self.fq_sandbox_table_names[3],
            'loaded_ids': [101, 102],
            'sandboxed_ids': [102],
            'fields': ['person_id'],
            'cleaned_values': [(101,)]
        }]

        with self.assertLogs(LOGGER, level='INFO') as cm:
            self.default_test(tables_and_counts)

        self.assertIn(
            'INFO:cdr_cleaner.cleaning_rules.monkeypox_concept_suppression:Skipping care_site. care_site has no date or datetime fields.',
            cm.output)

        self.assertIn(
            'INFO:cdr_cleaner.cleaning_rules.monkeypox_concept_suppression:Skipping fact_relationship. fact_relationship has no date or datetime fields.',
            cm.output)
