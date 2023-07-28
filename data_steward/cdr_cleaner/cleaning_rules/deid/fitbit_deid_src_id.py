"""
"""

# Project imports
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule
from constants.cdr_cleaner import clean_cdr as cdr_consts
from common import FITBIT_TABLES, JINJA_ENV

ISSUE_NUMBERS = ['DC3337']

# Query template to sandbox records
SANDBOX_SRC_IDS_QUERY = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE 
    `{{project_id}}.{{sandbox_dataset_id}}.{{sandbox_table}}` ft
AS (
    SELECT
        *
    FROM
        `{{project_id}}.{{dataset_id}}.{{fitbit_table}}`
)
""")

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
        desc = ('Update src_id values in fitbit tables.')

        super().__init__(issue_numbers=ISSUE_NUMBERS,
                         description=desc,
                         affected_datasets=[cdr_consts.FITBIT],
                         affected_tables=FITBIT_TABLES,
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         table_namer=table_namer)

    def get_query_specs(self):
        """
        Return a list of dictionary query specifications.

        :return:  A list of dictionaries.  Each dictionary contains a
            single query and a specification for how to execute that query.
            The specifications are optional but the query is required.
        """
        update_queries, sandbox_queries = [], []

        for table in self.affected_tables:
            sandbox_query = {
                cdr_consts.QUERY:
                    SANDBOX_SRC_IDS_QUERY.render(
                        project_id=self.project_id,
                        sandbox_dataset_id=self.sandbox_dataset_id,
                        sandbox_table=self.sandbox_table_for(table),
                        dataset_id=self.dataset_id,
                        fitbit_table=table)
            }
            update_query = {
                cdr_consts.QUERY:
                    UPDATE_SRC_IDS_QUERY.render(
                        project_id=self.project_id,
                        dataset_id=self.dataset_id,
                        fitbit_table=table,
                        pipeline_tables=PIPELINE_TABLES,
                        site_maskings=SITE_MASKING_TABLE_ID)
            }
            sandbox_queries.append(sandbox_query)
            update_queries.append(update_query)

        return update_queries + sandbox_queries

    def get_sandbox_tablenames(self):
        pass

    def setup_rule(self, client):
        pass

    def setup_validation(self, client):
        pass

    def validate_rule(self, client):
        pass