# Project imports
import constants.cdr_cleaner.clean_cdr as cdr_consts
from common import JINJA_ENV
from gcloud.bq import BigQueryClient
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule

# Query to create tables in sandbox with rows that will be removed per cleaning rule
SANDBOX_QUERY = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project}}.{{sandbox_dataset}}.{{intermediary_table}}` AS (
SELECT *
FROM `{{project}}.{{dataset}}.{{table}}`
WHERE person_id IN (
    SELECT person_id FROM `{{project}}.{{sandbox_dataset}}.{{lookup_table}}`
))
""")

# Query to truncate existing tables to remove PIDs based on cleaning rule criteria
CLEAN_QUERY = JINJA_ENV.from_string("""
DELETE
FROM `{{project}}.{{dataset}}.{{table}}`
WHERE person_id IN (
    SELECT distinct person_id FROM `{{project}}.{{sandbox_dataset}}.{{lookup_table}}`
)
""")

# Query to list all tables within a dataset that contains person_id in the schema
PERSON_TABLE_QUERY = JINJA_ENV.from_string("""
SELECT table_name
FROM `{{project}}.{{dataset}}.INFORMATION_SCHEMA.COLUMNS`
WHERE COLUMN_NAME = 'person_id'
""")


class SandboxAndRemovePids(BaseCleaningRule):
    """
    Removes records with person_ids that are not validated by sites and non-matching.
    """

    def __init__(self,
                 issue_numbers=None,
                 description: str = None,
                 affected_datasets=None,
                 project_id: str = None,
                 dataset_id: str = None,
                 sandbox_dataset_id: str = None,
                 depends_on=None,
                 affected_tables=None,
                 table_namer: str = None):
        """
        Initialize the class with proper information.
        """

        desc = (
            'Abstract class to sandbox and remove participant records from the '
            'given dataset.  This is a class that can be extended for other rules.'
        )
        description = description if description else desc

        super().__init__(issue_numbers=issue_numbers,
                         description=description,
                         affected_datasets=affected_datasets,
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         affected_tables=affected_tables,
                         depends_on=depends_on,
                         table_namer=table_namer)

    def setup_rule(self, client: BigQueryClient):
        """
        Get list of tables that have a person_id column, excluding mapping tables
        """
        person_table_query = PERSON_TABLE_QUERY.render(project=self.project_id,
                                                       dataset=self.dataset_id)
        person_tables = client.query(person_table_query).result()

        self.affected_tables = [
            table.get('table_name')
            for table in person_tables
            if '_mapping' not in table.get('table_name')
        ]

    def get_sandbox_queries(self, lookup_table: str = None) -> list:
        """
        Returns a list of queries of all tables to be added to the datasets sandbox. These tables include all rows from all
        effected tables that include PIDs that will be removed by a specific cleaning rule.

        :param lookup_table: name of the lookup table
        :return: list of CREATE OR REPLACE queries to create tables in sandbox
        """
        if self.sandbox_dataset_id is None:
            raise RuntimeError(
                f"sandbox_dataset_id is None.  This is not allowed.")

        queries_list = []

        for table in self.affected_tables:
            queries_list.append({
                cdr_consts.QUERY:
                    SANDBOX_QUERY.render(
                        dataset=self.dataset_id,
                        project=self.project_id,
                        table=table,
                        sandbox_dataset=self.sandbox_dataset_id,
                        intermediary_table=self.sandbox_table_for(table),
                        lookup_table=lookup_table)
            })

        return queries_list

    def get_remove_pids_queries(self, lookup_table=None):
        """
        Returns a list of queries in which the table will be truncated with clean data, ie: all removed PIDs from all
        datasets based on a cleaning rule.

        :param lookup_table: name of the lookup table
        :return: list of select statements that will truncate the existing tables with clean data
        """
        queries_list = []

        for table in self.affected_tables:
            queries_list.append({
                cdr_consts.QUERY:
                    CLEAN_QUERY.render(project=self.project_id,
                                       dataset=self.dataset_id,
                                       table=table,
                                       sandbox_dataset=self.sandbox_dataset_id,
                                       lookup_table=lookup_table)
            })

        return queries_list
