"""
Unit test for the cleaning_rules.ehr_submission_data_cutoff.py module

Original Issue: DC-1445

Intent of this unit test is to ensure that the data cutoff for the PPI data in all CDM tables is enforced by sandboxing
 and removing any records that persist after the data cutoff date.
"""

# Python imports
import unittest
from mock import patch
from datetime import datetime

# Project imports
import common
from constants import bq_utils as bq_consts
from constants.cdr_cleaner import clean_cdr as cdr_consts
import cdr_cleaner.cleaning_rules.ehr_submission_data_cutoff as data_cutoff


class EhrSubmissionDataCutoffTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = 'foo_project'
        self.dataset_id = 'bar_dataset'
        self.sandbox_id = 'baz_sandbox'
        self.client = None

        self.date_fields = ['visit_start_date', 'visit_end_date']
        self.datetime_fields = ['visit_start_datetime', 'visit_end_datetime']
        self.cutoff_date = str(datetime.now().date())

        self.rule_instance = data_cutoff.EhrSubmissionDataCutoff(
            self.project_id, self.dataset_id, self.sandbox_id)

        self.assertEqual(self.rule_instance.project_id, self.project_id)
        self.assertEqual(self.rule_instance.dataset_id, self.dataset_id)
        self.assertEqual(self.rule_instance.sandbox_dataset_id, self.sandbox_id)

    def test_setup_rule(self):
        # Test
        self.rule_instance.setup_rule(self.client)

        # No errors are raised, nothing will happen

    def test_get_affected_tables(self):
        """
        Will test that the affected tables generated do no include the person table
        """
        # Pre conditions
        expected_tables = [
            'observation_period', 'visit_cost', 'drug_cost',
            'procedure_occurrence', 'payer_plan_period', 'device_cost',
            'device_exposure', 'procedure_cost', 'source_to_concept_map',
            'observation', 'location', 'cohort', 'cost', 'death',
            'drug_exposure', 'measurement', 'condition_era', 'note',
            'cohort_definition', 'dose_era', 'care_site', 'fact_relationship',
            'cohort_attribute', 'provider', 'condition_occurrence',
            'cdm_source', 'attribute_definition', 'visit_occurrence',
            'drug_era', 'specimen', 'cope_survey_semantic_version_map'
        ]

        actual_tables = self.rule_instance.get_affected_tables()

        self.assertEqual(expected_tables, actual_tables)

    @patch.object(data_cutoff.EhrSubmissionDataCutoff, 'get_affected_tables')
    def test_get_query_specs(self, mock_get_affected_tables):
        # Pre conditions
        mock_get_affected_tables.return_value = [common.VISIT_OCCURRENCE]

        table = common.VISIT_OCCURRENCE

        sandbox_query = {
            cdr_consts.QUERY:
                data_cutoff.SANDBOX_QUERY.render(
                    project_id=self.project_id,
                    sandbox_id=self.sandbox_id,
                    intermediary_table=self.rule_instance.sandbox_table_for(table),
                    dataset_id=self.dataset_id,
                    cdm_table=table,
                    date_fields=(", ".join(self.date_fields)),
                    datetime_fields=(", ".join(self.datetime_fields)),
                    cutoff_date=self.cutoff_date),
        }

        date_cutoff_query = {
            cdr_consts.QUERY:
                data_cutoff.DATE_CUTOFF_QUERY.render(
                    project_id=self.project_id,
                    dataset_id=self.dataset_id,
                    cdm_table=table,
                    sandbox_id=self.sandbox_id,
                    intermediary_table=self.rule_instance.sandbox_table_for(table)),
            cdr_consts.DESTINATION_TABLE:
                table,
            cdr_consts.DESTINATION_DATASET:
                self.dataset_id,
            cdr_consts.DISPOSITION:
                bq_consts.WRITE_TRUNCATE
        }

        expected_list = [sandbox_query] + [date_cutoff_query]

        # Test
        results_list = self.rule_instance.get_query_specs()

        # Post conditions
        self.assertEqual(results_list, expected_list)




