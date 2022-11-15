"""
Remove all the data from drug_era, condition_era, and dose_era tables.

Original Issue: DC-2786
"""

# Python Imports
import logging

# Project Imports
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule
import constants.cdr_cleaner.clean_cdr as cdr_consts
from common import DOSE_ERA, DRUG_ERA, CONDITION_ERA

LOGGER = logging.getLogger(__name__)


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
        super().__init__(issue_numbers=['DC1046'],
                         description=desc,
                         affected_datasets=[cdr_consts.CONTROLLED_TIER_DEID],
                         affected_tables=[DOSE_ERA, DRUG_ERA, CONDITION_ERA],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         table_namer=table_namer)
