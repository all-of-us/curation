"""
Unit Test for the null_concept_ids_for_numeric_ppi module.

Nullify concept ids for numeric PPIs from the RDR observation dataset

Original Issues: DC-537, DC-703

The intent is to null concept ids (value_source_concept_id, value_as_concept_id, value_source_value,
value_as_string) from the RDR observation dataset. The changed records should be archived in the
dataset sandbox.
"""

# Python Imports
import os

# Third Party Imports

# Project Imports
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.null_concept_ids_for_numeric_ppi import NullConceptIDForNumericPPI
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base import BaseTest


class NullConceptIDForNumericPPITest(BaseTest.CleaningRulesTestBase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        super().initialize_class_vars()

        cls.insert_fake_participants_template = [
            cls.jinja_env.from_string("""
        INSERT INTO `{{fake_table_name}}`
        (""")
        ]

        # Set the test project identifier
        project_id = os.environ.get(PROJECT_ID)
        cls.project_id = project_id
