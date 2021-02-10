"""
DEID rule to change PIDs to RIDs for specific tables
"""
# Python Imports
import logging

# Third party imports
import google.cloud.bigquery as gbq
from google.cloud.exceptions import NotFound

# Project imports
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule
from constants.bq_utils import WRITE_TRUNCATE
from constants.cdr_cleaner import clean_cdr as cdr_consts
from common import JINJA_ENV, PERSON, DEID_MAP, PID_RID_MAPPING, PIPELINE_TABLES

LOGGER = logging.getLogger(__name__)

PID_RID_QUERY = """
SELECT
    t.* EXCEPT (person_id),
    d.research_id as person_id
FROM `{{input_table.project}}.{{input_table.dataset_id}}.{{input_table.table_id}}` t
LEFT JOIN `{{deid_map.project}}.{{deid_map.dataset_id}}.{{deid_map.table_id}}` d
ON t.person_id = d.person_id
"""

PID_RID_QUERY_TMPL = JINJA_ENV.from_string(PID_RID_QUERY)

VALIDATE_QUERY = """
SELECT person_id
FROM `{{input_table.project}}.{{input_table.dataset_id}}.{{input_table.table_id}}`
WHERE person_id NOT IN 
(SELECT research_id
FROM `{{deid_map.project}}.{{deid_map.dataset_id}}.{{deid_map.table_id}}`)
"""

VALIDATE_QUERY_TMPL = JINJA_ENV.from_string(VALIDATE_QUERY)


class PIDtoRID(BaseCleaningRule):
    """
    Use RID instead of PID for specific tables
    """

    def __init__(self, project_id, dataset_id, sandbox_dataset_id,
                 mapping_dataset_id, mapping_table_id, affected_tables,
                 issue_numbers):
        """
        Initialize the class with proper info.

        Set the issue numbers, description and affected datasets.  As other
        tickets may affect this SQL, append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """
        desc = f'Change PIDs to RIDs in specified tables'
        super().__init__(issue_numbers=issue_numbers,
                         description=desc,
                         affected_datasets=[dataset_id],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         affected_tables=affected_tables)

        self.pid_tables = [
            gbq.TableReference.from_string(
                f'{self.project_id}.{self.dataset_id}.{table_id}')
            for table_id in affected_tables
        ]
        fq_deid_map_table = f'{self.project_id}.{mapping_dataset_id}.{mapping_table_id}'
        self.deid_map = gbq.TableReference.from_string(fq_deid_map_table)
        self.person = gbq.TableReference.from_string(
            f'{self.project_id}.{mapping_dataset_id}.{PERSON}')

    def get_query_specs(self):
        """
        Return a list of dictionary query specifications.

        :return:  A list of dictionaries.  Each dictionary contains a
            single query and a specification for how to execute that query.
            The specifications are optional but the query is required.
        """
        queries = []

        for table in self.pid_tables:
            table_query = {
                cdr_consts.QUERY:
                    PID_RID_QUERY_TMPL.render(input_table=table,
                                              deid_map=self.deid_map),
                cdr_consts.DESTINATION_TABLE:
                    table.table_id,
                cdr_consts.DESTINATION_DATASET:
                    self.dataset_id,
                cdr_consts.DISPOSITION:
                    WRITE_TRUNCATE
            }
            queries.append(table_query)

        return queries

    def get_sandbox_tablenames(self):
        return []

    def setup_rule(self, client):
        """
        Function to run any data upload options before executing a query.
        """
        try:
            client.get_table(self.deid_map)
        except NotFound:
            job = client.copy_table(
                f'{self.project_id}.{PIPELINE_TABLES}.{PID_RID_MAPPING}',
                f'{self.project_id}.{self.sandbox_dataset_id}.{DEID_MAP}')
            job.result()
            LOGGER.info(
                f'Copied {PIPELINE_TABLES}.{PID_RID_MAPPING} to {self.sandbox_dataset_id}.{DEID_MAP}'
            )

    def setup_validation(self, client):
        """
        Run required steps for validation setup
        """
        pass

    def validate_rule(self, client):
        """
        Validates the cleaning rule which deletes or updates the data from the tables
        """
        for table in self.pid_tables:
            query = VALIDATE_QUERY_TMPL.render(
                input_table=table,
                deid_map=self.deid_map,
            )
            result = client.query(query).result()
            if result.total_rows > 0:
                raise RuntimeError(
                    f'PIDs {result.total_rows} not converted to research_ids')
