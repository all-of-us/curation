"""
Remove all the data from drug_era, condition_era, and dose_era tables.

Original Issue: DC-2786
"""

# Python Imports
import logging

# Project Imports
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule


class TruncateEraTablesData(BaseCleaningRule):

    def __init__(self,
                 project_id,
                 dataset_id,
                 sandbox_dataset_id,
                 table_namer=None):
        """
        Initialize the class with proper information.

        Set the issue numbers, description and affected datasets. As other tickets may affect
        this SQL, append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """
        desc = (
            'All the data from drug_era, condition_era, and dose_era tables is dropped'
        )