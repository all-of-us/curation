"""
"""

# Project imports
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule
from constants.cdr_cleaner import clean_cdr as cdr_consts
from common import FITBIT_TABLES, JINJA_ENV

ISSUE_NUMBERS = ['DC3337']

# Query template to update src_ids in fitbit tables
UPDATE_SRC_IDS_QUERY = JINJA_ENV.from_string("""
UPDATE 
    `{{project_id}}.{{dataset_id}}.{{fitbit_table}}` ft
SET
    ft.src_id = sm.src_id
FROM
    `{{project_id}}.{{pipeline_tables}}.{{site_maskings}}` sm
WHERE
    ft.src_id = sm.hpo_id
""")


class FitbitDeidSrcID(BaseCleaningRule):

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
        desc = ('')

        super().__init__(issue_numbers=ISSUE_NUMBERS,
                         description=desc,
                         affected_datasets=[cdr_consts.FITBIT],
                         affected_tables=FITBIT_TABLES,
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         table_namer=table_namer)

    def get_query_specs(self):
        pass

    def get_sandbox_tablenames(self):
        pass

    def setup_rule(self, client):
        pass

    def setup_validation(self, client):
        pass

    def validate_rule(self, client):
        pass