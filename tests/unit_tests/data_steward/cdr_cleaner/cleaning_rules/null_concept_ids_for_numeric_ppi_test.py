"""
Unit Test for the null_concept_ids_for_numeric_ppi module.

Nullify concept ids for numeric PPIs from the RDR observation dataset

Original Issues: DC-537, DC-703

The intent is to null concept ids (value_source_concept_id, value_as_concept_id, value_source_value,
value_as_string) from the RDR observation dataset. The changed records should be archived in the
dataset sandbox.
"""

# Python imports
import unittest

# Third party imports

# Project imports
from constants.bq_utils import WRITE_TRUNCATE
from constants.cdr_cleaner import clean_cdr as clean_consts
from cdr_cleaner.cleaning_rules.null_concept_ids_for_numeric_ppi import NullConceptIDForNumericPPI
from sandbox import check_and_create_sandbox_dataset

class NullConceptIDForNumericPPITest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = 'foo_dataset'
        self.dataset_id = 'bar_dataset'
        self.sandbox_id = 'baz_dataset'

        self.query_class = NullConceptIDForNumericPPI(
            self.project_id, self.dataset_id, self.sandbox_id)

        self.assertEqual(self.query_class.get_project_id(), self.project_id)
        self.assertEqual(self.query_class.get_dataset_id(), self.dataset_id)
        self.assertEqual(self.query_class.g)
