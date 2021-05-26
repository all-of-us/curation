"""
Unit test for drop_zero_concept_ids module

Original Issues: DC-975

As part of the effort to continue to improve data quality, rows with 0 source and standard concept_ids need to be
removed from the clean dataset

Affected tables and columns (both columns need to be zero or NULL):
condition_occurrence
    - condition_source_concept_id
    - condition_concept_id
procedure_occurrence
    - procedure_source_concept_id
    - procedure_concept_id
visit_occurrence
    - visit_source_concept_id
    - visit_concept_id
drug_exposure
    - drug_source_concept_id
    - drug_concept_id
device_exposure
    - device_source_concept_id
    - device_concept_id
observation
    - observation_source_concept_id
    - observation_concept_id
measurement
    - measurement_source_concept_id
    - measurement_concept_id

Remove those rows from the clean dataset
Archive/sandbox those rows

As of DC-1661, the death table has been removed.
This allows the death table with suppressed cause_concept_id and
cause_source_concept_id to persist without being deleted.
"""

# Python imports
import unittest

# Project imports
from cdr_cleaner.cleaning_rules.drop_zero_concept_ids import DropZeroConceptIDs, SANDBOX_ZERO_CONCEPT_IDS_QUERY, \
    DROP_ZERO_CONCEPT_IDS_QUERY, tables, unique_identifier, concept_id_columns, source_concept_id_columns
from constants.cdr_cleaner import clean_cdr as clean_consts


class DropZeroConceptIDsTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = 'test_project'
        self.dataset_id = 'test_dataset'
        self.sandbox_id = 'test_sandbox'
        self.client = None

        self.rule_instance = DropZeroConceptIDs(self.project_id,
                                                self.dataset_id,
                                                self.sandbox_id)

        self.assertEqual(self.rule_instance.project_id, self.project_id)
        self.assertEqual(self.rule_instance.dataset_id, self.dataset_id)
        self.assertEqual(self.rule_instance.sandbox_dataset_id, self.sandbox_id)

    def test_setup_rule(self):
        # Test
        self.rule_instance.setup_rule(self.client)

    def test_get_query_specs(self):
        # Pre conditions
        self.assertEqual(
            self.rule_instance.affected_datasets,
            [clean_consts.DEID_CLEAN, clean_consts.CONTROLLED_TIER_DEID_CLEAN])

        # Test
        results_list = self.rule_instance.get_query_specs()

        # Post conditions
        expected_sandbox_queries_list = []
        expected_drop_queries_list = []

        for i, table in enumerate(tables):
            expected_sandbox_queries_list.append({
                clean_consts.QUERY:
                    SANDBOX_ZERO_CONCEPT_IDS_QUERY.render(
                        project=self.project_id,
                        sandbox_dataset=self.sandbox_id,
                        sandbox_table=self.rule_instance.get_sandbox_tablenames(
                        )[i],
                        dataset=self.dataset_id,
                        table=table,
                        source_concept_id=source_concept_id_columns[table],
                        concept_id=concept_id_columns[table])
            })

            expected_drop_queries_list.append({
                clean_consts.QUERY:
                    DROP_ZERO_CONCEPT_IDS_QUERY.render(
                        project=self.project_id,
                        dataset=self.dataset_id,
                        table=table,
                        unique_identifier=unique_identifier[table],
                        sandbox_dataset=self.sandbox_id,
                        sandbox_table=self.rule_instance.get_sandbox_tablenames(
                        )[i])
            })

        self.assertEqual(
            results_list,
            expected_sandbox_queries_list + expected_drop_queries_list)
