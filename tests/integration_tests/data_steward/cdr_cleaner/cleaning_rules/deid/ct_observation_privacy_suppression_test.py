"""
Integration test for ct_observation_privacy_suppression module

Original Issues: DC-3749, DL-2428

The rule builds two lookups: a post-coordinated one that only applies to EHR
rows, and one for everything else. The CT+ variant narrows both, so an expanded
concept survives whichever path would have caught it.
"""

# Python Imports
import os
import tempfile
from unittest import mock

# Project Imports
import cdr_cleaner.cleaning_rules.deid.ct_observation_privacy_suppression as rule_module
from app_identity import PROJECT_ID
from common import OBSERVATION, JINJA_ENV
from cdr_cleaner.cleaning_rules.deid.ct_observation_privacy_suppression import (
    CTObservationPrivacySuppression, CTObservationPrivacySuppressionCtPlus)
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import \
    BaseTest

OBSERVATION_EXT = f'{OBSERVATION}_ext'

# The shipped CSVs carry ct_plus_suppressed = true on every row until the
# expanded categories are flipped, so the fixtures below stand in for them.
# Each file gets both a true and a false row: an empty frame would concatenate
# to object dtype and the CT+ filter refuses a non-boolean column.
FIXTURE_DIR = tempfile.mkdtemp(prefix='dl2428_')

POSTCOORDINATED_CSV = os.path.join(FIXTURE_DIR, 'postcoordinated.csv')
ADDITIONAL_CSV = os.path.join(FIXTURE_DIR, 'additional.csv')
PUBLICLY_REPORTABLE_CSV = os.path.join(FIXTURE_DIR, 'publicly_reportable.csv')

CSV_HEADER = ('concept_id,concept_code,privacy_rule,vocabulary_id,date_added,'
              'ct_plus_suppressed\n')

# Concepts 111 and 222 are reached through the post-coordinated lookup, which
# only matches EHR rows; 333 and 444 through the other lookup, which matches
# regardless of source. The even-hundreds are the expanded ones.
POSTCOORDINATED_CONCEPTS = [111, 222]
REST_CONCEPTS = [333, 444]
EXPANDED_IN_CT_PLUS = [222, 444]
ALL_CONCEPTS = POSTCOORDINATED_CONCEPTS + REST_CONCEPTS


def _write_fixture_csv(path, rows):
    with open(path, 'w') as csv_file:
        csv_file.write(CSV_HEADER)
        csv_file.writelines(rows)


_write_fixture_csv(POSTCOORDINATED_CSV, [
    '111,c111,abortion,ppi,2024-01-01,true\n',
    '222,c222,multiples,ppi,2024-01-01,false\n',
])
_write_fixture_csv(ADDITIONAL_CSV, [
    '333,c333,abortion,snomed,2024-01-01,true\n',
    '444,c444,assault,snomed,2024-01-01,false\n',
])
# Carries no observation concept of its own, but must still be a boolean frame
# because the rule concatenates it with the one above.
_write_fixture_csv(PUBLICLY_REPORTABLE_CSV, [
    '777,c777,abortion,icd10cm,2024-01-01,true\n',
    '888,c888,liveborn,icd10cm,2024-01-01,false\n',
])

OBSERVATION_DATA_TEMPLATE = JINJA_ENV.from_string("""
INSERT INTO `{{project_id}}.{{dataset_id}}.observation`
(observation_id, person_id, observation_concept_id, observation_date,
 observation_type_concept_id, value_as_concept_id, qualifier_concept_id,
 unit_concept_id, observation_source_concept_id, value_source_concept_id)
VALUES
      /* Row id is the concept id. The post-coordinated pair is EHR sourced, */
      /* which is the only way the first lookup can reach them. */
      (111, 1, 111, '2020-01-01', 0, 0, 0, 0, 0, 0),
      (222, 1, 222, '2020-01-01', 0, 0, 0, 0, 0, 0),
      (333, 2, 333, '2020-01-01', 0, 0, 0, 0, 0, 0),
      (444, 2, 444, '2020-01-01', 0, 0, 0, 0, 0, 0),
      /* A concept in no privacy CSV. Neither rule may touch it. */
      (999, 2, 999, '2020-01-01', 0, 0, 0, 0, 0, 0)
""")

OBSERVATION_EXT_DATA_TEMPLATE = JINJA_ENV.from_string("""
INSERT INTO `{{project_id}}.{{dataset_id}}.observation_ext`
(observation_id, src_id)
VALUES
      (111, 'EHR site nyc'),
      (222, 'EHR site nyc'),
      (333, 'PPI/PM'),
      (444, 'PPI/PM'),
      (999, 'PPI/PM')
""")


class CTObservationPrivacySuppressionTest(BaseTest.CleaningRulesTestBase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        super().initialize_class_vars()

        # Set the test project identifier
        cls.project_id = os.environ.get(PROJECT_ID)

        # Set the expected test datasets
        cls.dataset_id = os.environ.get('COMBINED_DATASET_ID')
        cls.sandbox_id = f'{cls.dataset_id}_sandbox'

        cls.rule_instance = CTObservationPrivacySuppression(
            cls.project_id, cls.dataset_id, cls.sandbox_id)

        for table_name in [OBSERVATION, OBSERVATION_EXT]:
            cls.fq_table_names.append(
                f'{cls.project_id}.{cls.dataset_id}.{table_name}')

        # The observation sandbox table, plus the two concept lookups the rule
        # builds. The lookups are not in get_sandbox_tablenames, so they are
        # listed here to get torn down: the sandbox query is a plain CREATE
        # TABLE and the lookup loads append, so leftovers break the next test.
        cls.fq_sandbox_table_names.append(
            f'{cls.project_id}.{cls.sandbox_id}.'
            f'{cls.rule_instance.sandbox_table_for(OBSERVATION)}')
        # All four lookup names: the base rule and the variant build their
        # own pair, so a leftover from one test would append into the next.
        for rule in [
                cls.rule_instance,
                CTObservationPrivacySuppressionCtPlus(cls.project_id,
                                                      cls.dataset_id,
                                                      cls.sandbox_id)
        ]:
            for lookup_table in [
                    rule.ct_observation_postc_concept_table,
                    rule.ct_observation_rest_concept_table
            ]:
                cls.fq_sandbox_table_names.append(
                    f'{cls.project_id}.{cls.sandbox_id}.{lookup_table}')

        # call super to set up the client, create datasets
        cls.up_class = super().setUpClass()

    def setUp(self):
        """
        Create empty tables for the rule to run on
        """
        super().setUp()

        observation_data_query = OBSERVATION_DATA_TEMPLATE.render(
            project_id=self.project_id, dataset_id=self.dataset_id)
        observation_ext_data_query = OBSERVATION_EXT_DATA_TEMPLATE.render(
            project_id=self.project_id, dataset_id=self.dataset_id)

        # Load test data
        self.load_test_data([
            f'''{observation_data_query};
                {observation_ext_data_query}'''
        ])

    def _tables_and_counts(self, surviving_concepts):
        """
        default_test expectations, given the concepts a rule leaves in place.

        999 always survives, since no privacy CSV names it.
        """
        lookup_tables = [
            f'{self.project_id}.{self.sandbox_id}.'
            f'{self.rule_instance.ct_observation_postc_concept_table}',
            f'{self.project_id}.{self.sandbox_id}.'
            f'{self.rule_instance.ct_observation_rest_concept_table}'
        ]
        return [{
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{OBSERVATION}',
            'fq_sandbox_table_name':
                f'{self.project_id}.{self.sandbox_id}.'
                f'{self.rule_instance.sandbox_table_for(OBSERVATION)}',
            'loaded_ids':
                ALL_CONCEPTS + [999],
            'sandboxed_ids': [
                concept for concept in ALL_CONCEPTS
                if concept not in surviving_concepts
            ],
            'fields': ['observation_id', 'observation_concept_id'],
            'cleaned_values': [
                (concept, concept) for concept in surviving_concepts + [999]
            ],
            'tables_created_on_setup':
                lookup_tables
        }, {
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{OBSERVATION_EXT}',
            'loaded_ids':
                ALL_CONCEPTS + [999],
            'fields': ['observation_id'],
            'cleaned_values': [(concept,) for concept in ALL_CONCEPTS + [999]],
            'tables_created_on_setup':
                lookup_tables
        }]

    @mock.patch.object(rule_module, 'CT_OBSERVATION_PRIVACY_CONCEPTS_PATH',
                       POSTCOORDINATED_CSV)
    @mock.patch.object(rule_module, 'CT_ADDITIONAL_PRIVACY_CONCEPTS_PATH',
                       ADDITIONAL_CSV)
    @mock.patch.object(rule_module, 'CT_RT_PUBLICLY_REPORTABLE_CONCEPTS_PATH',
                       PUBLICLY_REPORTABLE_CSV)
    def test_base_rule_suppresses_through_both_lookups(self):
        """
        The CT rule is unchanged by the refactor: it loads every CSV row, and
        removes the EHR-sourced post-coordinated rows as well as the rest.
        """
        self.rule_instance = CTObservationPrivacySuppression(
            self.project_id, self.dataset_id, self.sandbox_id)

        self.default_test(self._tables_and_counts(surviving_concepts=[]))

    @mock.patch.object(rule_module, 'CT_OBSERVATION_PRIVACY_CONCEPTS_PATH',
                       POSTCOORDINATED_CSV)
    @mock.patch.object(rule_module, 'CT_ADDITIONAL_PRIVACY_CONCEPTS_PATH',
                       ADDITIONAL_CSV)
    @mock.patch.object(rule_module, 'CT_RT_PUBLICLY_REPORTABLE_CONCEPTS_PATH',
                       PUBLICLY_REPORTABLE_CSV)
    def test_ct_plus_variant_keeps_the_expanded_concepts(self):
        """
        One expanded concept per lookup survives, which is what proves both
        lookups were narrowed rather than just the first.
        """
        self.rule_instance = CTObservationPrivacySuppressionCtPlus(
            self.project_id, self.dataset_id, self.sandbox_id)

        self.default_test(
            self._tables_and_counts(surviving_concepts=EXPANDED_IN_CT_PLUS))
