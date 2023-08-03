"""

Original Issues: DC-2650, DC-2651
"""

import logging

# Project imports
import constants.cdr_cleaner.clean_cdr as cdr_consts
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule
from common import JINJA_ENV, MEASUREMENT, IDENTICAL_LABS_LOOKUP_TABLE
from cdr_cleaner.cleaning_rules.store_new_duplicate_measurement_concept_ids import \
    StoreNewDuplicateMeasurementConceptIds

# Third party imports
from google.cloud import bigquery

LOGGER = logging.getLogger(__name__)

JIRA_ISSUE_NUMBERS = ['DC2651', 'DC2650', 'DC2358']
PIPELINE_TABLES = 'pipeline_tables'

SANDBOX_QUERY = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE
  `{{project}}.{{sandbox_dataset}}.{{sandbox_table}}` AS (
  SELECT
    m.*
  FROM
    `{{project}}.{{dataset}}.measurement` m
  JOIN
    `{{project}}.{{sandbox_dataset}}.{{identical_labs_table}}` lm
  ON
    m.value_as_concept_id = lm.value_as_concept_id
  WHERE
    m.value_as_concept_id <> lm.aou_standard_vac )
""")

UPDATE_QUERY = JINJA_ENV.from_string("""
UPDATE
  `{{project}}.{{dataset}}.measurement` m
SET
  value_as_concept_id = lm.aou_standard_vac
FROM
  `{{project}}.{{sandbox_dataset}}.{{identical_labs_table}}` lm
WHERE
  m.value_as_concept_id = lm.value_as_concept_id
""")


class DedupMeasurementValueAsConceptId(BaseCleaningRule):

    def __init__(self,
                 project_id,
                 dataset_id,
                 sandbox_dataset_id=None,
                 table_namer=None):
        """
        Initialize the class with proper information.

        Set the issue numbers, description and affected datasets. As other tickets may affect
        this SQL, append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """
        desc = ('Corrects VALUE AS CONCEPT ID to be the ANSWER concept ID')

        super().__init__(issue_numbers=JIRA_ISSUE_NUMBERS,
                         description=desc,
                         depends_on=[StoreNewDuplicateMeasurementConceptIds],
                         affected_datasets=[cdr_consts.COMBINED],
                         affected_tables=[MEASUREMENT],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         table_namer=table_namer)

    def get_query_specs(self):
        """
        Return a list of dictionary query specifications.

        :return:  A list of dictionaries. Each dictionary contains a single query
            and a specification for how to execute that query. The specifications
            are optional but the query is required.
        """
        queries_list = []

        sandbox_query, update_query = dict(), dict()

        sandbox_query[cdr_consts.QUERY] = SANDBOX_QUERY.render(
            sandbox_dataset=self.sandbox_dataset_id,
            sandbox_table=self.sandbox_table_for(MEASUREMENT),
            project=self.project_id,
            dataset=self.dataset_id,
            pipeline_tables=PIPELINE_TABLES,
            identical_labs_table=IDENTICAL_LABS_LOOKUP_TABLE)
        queries_list.append(sandbox_query)

        update_query[cdr_consts.QUERY] = UPDATE_QUERY.render(
            project=self.project_id,
            dataset=self.dataset_id,
            sandbox_dataset=self.sandbox_dataset_id,
            identical_labs_table=IDENTICAL_LABS_LOOKUP_TABLE)
        queries_list.append(update_query)

        return queries_list

    def setup_rule(self, client, *args, **keyword_args):
        job = client.copy_table(
            f'{self.project_id}.{PIPELINE_TABLES}.{IDENTICAL_LABS_LOOKUP_TABLE}',
            f'{self.project_id}.{self.sandbox_dataset_id}.{IDENTICAL_LABS_LOOKUP_TABLE}',
            job_config=bigquery.job.CopyJobConfig(
                write_disposition=bigquery.job.WriteDisposition.WRITE_TRUNCATE))
        job.result()
        LOGGER.info(
            f'Copied {PIPELINE_TABLES}.{IDENTICAL_LABS_LOOKUP_TABLE} to '
            f'{self.sandbox_dataset_id}.{IDENTICAL_LABS_LOOKUP_TABLE}')

    def setup_validation(self, client):
        raise NotImplementedError("Please fix me.")

    def validate_rule(self, client):
        raise NotImplementedError("Please fix me.")

    def get_sandbox_tablenames(self):
        return [self.sandbox_table_for(table) for table in self.affected_tables]


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.parse_args()

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(
            ARGS.project_id, ARGS.dataset_id, ARGS.sandbox_dataset_id,
            [(DedupMeasurementValueAsConceptId,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(DedupMeasurementValueAsConceptId,)])
