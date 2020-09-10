"""
Unit test for update_fields_numbers_as_strings cleaning rule

Original Issues: DC-1052

Background
It has been discovered that the field type for some PPI survey answers is incorrect: there are several instances of
numeric answers being saved as ‘string’ field types. The expected long-term fix is for PTSC to correct the field type
on their end; however, there is no anticipated timeline for the completion of this work. As a result, the Curation team
will need to create a cleaning rule to correct these errors.

Cleaning rule to fill null values in value_as_number with values in value_as_string,
EXCEPT when it’s a ‘PMI Skip’ for each of the observation_source_value

Rule should be applied to the RDR export
"""

# Python imports
import unittest

# Project imports
from cdr_cleaner.cleaning_rules.update_fields_numbers_as_strings import UpdateFieldsNumbersAsStrings, \
    OBSERVATION_SOURCE_VALUES, SANDBOX_QUERY, NUMBERS_AS_STRINGS_QUERY, tables
from constants.bq_utils import WRITE_TRUNCATE
from constants.cdr_cleaner import clean_cdr as clean_consts
import constants.cdr_cleaner.clean_cdr as cdr_consts


class UpdateFieldsNumbersAsStringsTest(unittest.TestCase):

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

        self.query_class = UpdateFieldsNumbersAsStrings(self.project_id,
                                                        self.dataset_id,
                                                        self.sandbox_id)

        self.assertEqual(self.query_class.project_id, self.project_id)
        self.assertEqual(self.query_class.dataset_id, self.dataset_id)
        self.assertEqual(self.query_class.sandbox_dataset_id, self.sandbox_id)

    def test_setup_rule(self):
        # Test
        self.query_class.setup_rule(self.client)

    def test_get_query_specs(self):
        # Pre conditions
        self.assertEqual(self.query_class.affected_datasets, [clean_consts.RDR])

        # Test
        results_list = self.query_class.get_query_specs()

        # Post conditions
        expected_query_list = []

        for i, table in enumerate(tables):
            expected_query_list.append({
                clean_consts.QUERY:
                    SANDBOX_QUERY.render(
                        project=self.project_id,
                        sandbox_dataset=self.sandbox_id,
                        sandbox_table=self.query_class.get_sandbox_tablenames()
                        [i],
                        dataset=self.dataset_id)
            })
            expected_query_list.append({
                clean_consts.QUERY:
                    NUMBERS_AS_STRINGS_QUERY.render(
                        project=self.project_id,
                        dataset=self.dataset_id,
                        observation_source_values=OBSERVATION_SOURCE_VALUES),
                cdr_consts.DESTINATION_TABLE:
                    table,
                cdr_consts.DESTINATION_DATASET:
                    self.dataset_id,
                cdr_consts.DISPOSITION:
                    WRITE_TRUNCATE
            })

        self.assertEqual(results_list, expected_query_list)
