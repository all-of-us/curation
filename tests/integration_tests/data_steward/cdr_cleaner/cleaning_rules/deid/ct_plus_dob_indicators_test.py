"""
Integration test for the CT+ indicators-of-birth rules

Original Issues: DL-2495

Runs BirthInformationSuppressionCtPlus, YearOfBirthRecordsSuppressionCtPlus and
CTAdditionalPrivacyConceptSuppressionCtPlus in their pipeline order through the clean
engine, with person_id already holding CT research ids, as it does after
CtPlusPIDtoRID. The concept set is the shipped one.
"""

# Python Imports
import os

# Third party imports
from google.cloud.bigquery import Table

# Project Imports
import cdr_cleaner.clean_cdr_engine as clean_engine
from app_identity import PROJECT_ID
from common import (AIAN_LIST, CONDITION_OCCURRENCE, DEID_MAP, JINJA_ENV,
                    PERSON, UNDER18_PARTICIPANTS_LOOKUP_TABLE)
from cdr_cleaner.cleaning_rules.deid.birth_information_suppression import \
    BirthInformationSuppressionCtPlus
from cdr_cleaner.cleaning_rules.deid.ct_additional_privacy_suppression import \
    CTAdditionalPrivacyConceptSuppressionCtPlus
from cdr_cleaner.cleaning_rules.deid.year_of_birth_records_suppression import (
    LOOKUP_TABLE as YOB_LOOKUP_TABLE, YearOfBirthRecordsSuppressionCtPlus)
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import \
    BaseTest

# O99.814, filed under liveborn
LIVEBORN = 45582600
# Filed under abortion, which stays suppressed in CT+
ABORTION = 433826
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

# Participant ids are 10x, CT research ids x.
#   1: eligible, born 1980
#   3: AIAN
#   4: pediatric
PERSON_DATA = JINJA_ENV.from_string("""
INSERT INTO `{{project_id}}.{{dataset_id}}.person`
(person_id, gender_concept_id, year_of_birth, race_concept_id,
 ethnicity_concept_id)
VALUES (1, 0, 1980, 0, 0), (3, 0, 1975, 0, 0), (4, 0, 2022, 0, 0)
""")

# 1: eligible participant's birth-year birth row, kept by all three rules
# 2: AIAN participant's birth row, removed by the birth information variant
# 3: pediatric participant's birth row, removed by the birth information variant
# 4: eligible birth row that also carries abortion, removed by the additional
#    privacy variant, since suppressed wins
# 5: eligible participant's birth-year row with no birth concept, removed by the
#    year-of-birth variant as in CT
# 6: eligible participant's adult row with no birth concept, kept
CONDITION_DATA = JINJA_ENV.from_string("""
INSERT INTO `{{project_id}}.{{dataset_id}}.condition_occurrence`
(condition_occurrence_id, person_id, condition_concept_id,
 condition_start_date, condition_start_datetime, condition_type_concept_id,
 condition_source_concept_id)
VALUES
  (1, 1, 0, '1980-05-17', '1980-05-17 00:00:00', 0, {{liveborn}}),
  (2, 3, 0, '2000-01-01', '2000-01-01 00:00:00', 0, {{liveborn}}),
  (3, 4, 0, '2023-01-01', '2023-01-01 00:00:00', 0, {{liveborn}}),
  (4, 1, {{abortion}}, '2000-01-01', '2000-01-01 00:00:00', 0, {{liveborn}}),
  (5, 1, {{hypertension}}, '1980-06-01', '1980-06-01 00:00:00', 0, 0),
  (6, 1, {{hypertension}}, '2000-01-01', '2000-01-01 00:00:00', 0, 0)
""")


class CtPlusDobIndicatorsTest(BaseTest.CleaningRulesTestBase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        super().initialize_class_vars()

        cls.project_id = os.environ.get(PROJECT_ID)
        cls.dataset_id = os.environ.get('COMBINED_DATASET_ID')
        cls.sandbox_id = f'{cls.dataset_id}_sandbox'
        # The two lookups share the RDR dataset
        cls.rdr_dataset_id = os.environ.get('RDR_DATASET_ID')

        cls.kwargs = {
            'rdr_sandbox_id': cls.rdr_dataset_id,
            'under18_lookup_dataset_id': cls.rdr_dataset_id
        }
        cls.rules = [(BirthInformationSuppressionCtPlus,),
                     (YearOfBirthRecordsSuppressionCtPlus,),
                     (CTAdditionalPrivacyConceptSuppressionCtPlus,)]

        for table in [PERSON, CONDITION_OCCURRENCE]:
            cls.fq_table_names.append(
                f'{cls.project_id}.{cls.dataset_id}.{table}')

        for rule_class, in cls.rules:
            rule = rule_class(
                cls.project_id, cls.dataset_id, cls.sandbox_id,
                **clean_engine.get_custom_kwargs(rule_class, **cls.kwargs))
            cls.fq_sandbox_table_names.append(
                f'{cls.project_id}.{cls.sandbox_id}.'
                f'{rule.sandbox_table_for(CONDITION_OCCURRENCE)}')
            if hasattr(rule, 'concept_suppression_lookup_table'):
                cls.fq_sandbox_table_names.append(
                    f'{cls.project_id}.{cls.sandbox_id}.'
                    f'{rule.concept_suppression_lookup_table}')
        # The year-of-birth rule writes its column lookup to the sandbox too
        yob_rule = YearOfBirthRecordsSuppressionCtPlus(cls.project_id,
                                                       cls.dataset_id,
                                                       cls.sandbox_id)
        cls.fq_sandbox_table_names.append(
            f'{cls.project_id}.{cls.sandbox_id}.'
            f'{yob_rule.sandbox_table_for(YOB_LOOKUP_TABLE)}')

        # Not via fq_table_names, whose tables are created from schema files by
        # name. setUp creates these and tearDown drops them.
        cls.extra_tables = {
            f'{cls.project_id}.{cls.rdr_dataset_id}.{AIAN_LIST}':
                (PERSON_ID_SCHEMA, 'INSERT INTO `{fq}` (person_id) VALUES (103)'
                ),
            f'{cls.project_id}.{cls.rdr_dataset_id}.{UNDER18_PARTICIPANTS_LOOKUP_TABLE}':
                (PERSON_ID_SCHEMA, 'INSERT INTO `{fq}` (person_id) VALUES (104)'
                ),
            f'{cls.project_id}.{cls.sandbox_id}.{DEID_MAP}':
                (DEID_MAP_SCHEMA, 'INSERT INTO `{fq}` (person_id, research_id) '
                 'VALUES (101, 1), (103, 3), (104, 4)'),
        }

        super().setUpClass()

        # BaseTest creates only the datasets of fq_table_names and
        # fq_sandbox_table_names, and the two lookups above live in the RDR
        # dataset, which neither list names.
        desc = (f'dataset created by {cls.__name__} to test a cleaning rule.  '
                f'deletion candidate.')
        dataset = cls.client.define_dataset(cls.rdr_dataset_id, desc,
                                            {'test': ''})
        cls.client.create_dataset(dataset, exists_ok=True)

    def setUp(self):
        super().setUp()

        queries = [
            PERSON_DATA.render(project_id=self.project_id,
                               dataset_id=self.dataset_id),
            CONDITION_DATA.render(project_id=self.project_id,
                                  dataset_id=self.dataset_id,
                                  liveborn=LIVEBORN,
                                  abortion=ABORTION,
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

    def test_eligible_birth_rows_stay_inline(self):
        clean_engine.clean_dataset(self.project_id, self.dataset_id,
                                   self.sandbox_id, self.rules, **self.kwargs)

        self.assertTableValuesMatch(
            f'{self.project_id}.{self.dataset_id}.{CONDITION_OCCURRENCE}',
            ['condition_occurrence_id', 'person_id'], [(1, 1), (6, 1)])
