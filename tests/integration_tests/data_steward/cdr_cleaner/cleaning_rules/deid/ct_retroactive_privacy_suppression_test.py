"""
Integration test for ct_retroactive_privacy_suppression module

Original Issues: DC-3812, DL2429

The rule runs at deid_base and deid_clean, which is where a late privacy fix
would otherwise remove a concept the CT+ deid-stage variant deliberately kept.
The CT+ variant narrows its lookup the same way.
"""

# Python Imports
import os
import tempfile
from unittest import mock

# Project Imports
import cdr_cleaner.cleaning_rules.deid.ct_retroactive_privacy_suppression as rule_module
from app_identity import PROJECT_ID
from common import CONDITION_OCCURRENCE, JINJA_ENV
from cdr_cleaner.cleaning_rules.deid.ct_retroactive_privacy_suppression import (
    CTRetroactivePrivacyConceptSuppression,
    CTRetroactivePrivacyConceptSuppressionCtPlus)
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import \
    BaseTest

# The shipped CSV carries ct_plus_suppressed = true on every row until the
# expanded categories are flipped, so the fixture below stands in for it.
FIXTURE_DIR = tempfile.mkdtemp(prefix='dl2429_')
RETROACTIVE_CSV = os.path.join(FIXTURE_DIR, 'retroactive.csv')

CSV_HEADER = ('concept_id,concept_code,privacy_rule,vocabulary_id,date_added,'
              'ct_plus_suppressed\n')

SUPPRESSED_IN_CT_PLUS = [111]
EXPANDED_IN_CT_PLUS = [222]
ALL_CONCEPTS = SUPPRESSED_IN_CT_PLUS + EXPANDED_IN_CT_PLUS

with open(RETROACTIVE_CSV, 'w') as csv_file:
    csv_file.write(CSV_HEADER)
    csv_file.writelines([
        '111,c111,abortion,snomed,2024-01-01,true\n',
        '222,c222,assault,snomed,2024-01-01,false\n',
    ])

CONDITION_DATA_TEMPLATE = JINJA_ENV.from_string("""
INSERT INTO `{{project_id}}.{{dataset_id}}.condition_occurrence`
(condition_occurrence_id, person_id, condition_concept_id, condition_start_date,
 condition_start_datetime, condition_type_concept_id, condition_status_concept_id,
 condition_source_concept_id)
VALUES
      /* Row id is the concept id. */
      (111, 1, 111, '2020-01-01', '2020-01-01 00:00:00 UTC', 0, 0, 0),
      (222, 1, 222, '2020-01-01', '2020-01-01 00:00:00 UTC', 0, 0, 0),
      /* A concept the CSV does not name. Neither rule may touch it. */
      (999, 2, 999, '2020-01-01', '2020-01-01 00:00:00 UTC', 0, 0, 0)
""")


class CTRetroactivePrivacyConceptSuppressionTest(BaseTest.CleaningRulesTestBase
                                                ):

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

        cls.rule_instance = CTRetroactivePrivacyConceptSuppression(
            cls.project_id, cls.dataset_id, cls.sandbox_id)

        cls.fq_table_names.append(
            f'{cls.project_id}.{cls.dataset_id}.{CONDITION_OCCURRENCE}')

        # The domain sandbox table, plus the concept lookup the rule builds.
        # Both must be torn down between tests: the lookup load appends, and
        # the base and variant runs would otherwise share one table.
        cls.fq_sandbox_table_names.append(
            f'{cls.project_id}.{cls.sandbox_id}.'
            f'{cls.rule_instance.sandbox_table_for(CONDITION_OCCURRENCE)}')
        cls.fq_sandbox_table_names.append(
            f'{cls.project_id}.{cls.sandbox_id}.'
            f'{cls.rule_instance.concept_suppression_lookup_table}')

        # call super to set up the client, create datasets
        cls.up_class = super().setUpClass()

    def setUp(self):
        """
        Create empty tables for the rule to run on
        """
        super().setUp()

        condition_data_query = CONDITION_DATA_TEMPLATE.render(
            project_id=self.project_id, dataset_id=self.dataset_id)

        # Load test data
        self.load_test_data([condition_data_query])

    def _tables_and_counts(self, surviving_concepts):
        """
        default_test expectations, given the concepts a rule leaves in place.

        999 always survives, since the CSV does not name it.
        """
        return [{
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{CONDITION_OCCURRENCE}',
            'fq_sandbox_table_name':
                f'{self.project_id}.{self.sandbox_id}.'
                f'{self.rule_instance.sandbox_table_for(CONDITION_OCCURRENCE)}',
            'loaded_ids':
                ALL_CONCEPTS + [999],
            'sandboxed_ids': [
                concept for concept in ALL_CONCEPTS
                if concept not in surviving_concepts
            ],
            'fields': ['condition_occurrence_id', 'condition_concept_id'],
            'cleaned_values': [
                (concept, concept) for concept in surviving_concepts + [999]
            ],
            'tables_created_on_setup': [
                f'{self.project_id}.{self.sandbox_id}.'
                f'{self.rule_instance.concept_suppression_lookup_table}'
            ]
        }]

    @mock.patch.object(rule_module, 'PRIVACY_CONCEPTS_PATH', RETROACTIVE_CSV)
    def test_base_rule_suppresses_every_retroactive_concept(self):
        """
        The CT rule is unchanged by the refactor: it loads every CSV row.
        """
        self.rule_instance = CTRetroactivePrivacyConceptSuppression(
            self.project_id, self.dataset_id, self.sandbox_id)

        self.default_test(self._tables_and_counts(surviving_concepts=[]))

    @mock.patch.object(rule_module, 'PRIVACY_CONCEPTS_PATH', RETROACTIVE_CSV)
    def test_ct_plus_variant_keeps_the_expanded_concept(self):
        """
        Without this variant a retroactive fix would remove at deid_base what
        the deid-stage CT+ variant kept.
        """
        self.rule_instance = CTRetroactivePrivacyConceptSuppressionCtPlus(
            self.project_id, self.dataset_id, self.sandbox_id)

        self.default_test(
            self._tables_and_counts(surviving_concepts=EXPANDED_IN_CT_PLUS))

    @mock.patch.object(rule_module, 'PRIVACY_CONCEPTS_PATH', RETROACTIVE_CSV)
    def test_variant_lookup_holds_no_expanded_concept(self):
        """
        Assert on the lookup itself, not only on what survives: this rule
        applies to every CDM table, and the fixture covers one.
        """
        rule = CTRetroactivePrivacyConceptSuppressionCtPlus(
            self.project_id, self.dataset_id, self.sandbox_id)

        self.assertEqual(
            SUPPRESSED_IN_CT_PLUS,
            rule.get_suppression_concepts_df()['concept_id'].tolist())
