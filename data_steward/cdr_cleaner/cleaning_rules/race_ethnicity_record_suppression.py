"""
Sandbox and record suppress all records with an observation_source_concept_id in the following list (as of 2021/02/11):

| observation_source_concept_id |
|-------------------------------|
| 1586151                       |
| 1586150                       |
| 1586152                       |
| 1586153                       |
| 1586154                       |
| 1586155                       |
| 1586156                       |
| 1586149                       |

Original Issue: DC-1365

The intent of this cleaning rule is to suppress all records with observation_source_concept_id in the above table which
exist in the observation table
"""

# Python imports
import logging

# Project imports
from common import JINJA_ENV, OBSERVATION
from utils import pipeline_logging
from constants.bq_utils import WRITE_TRUNCATE
from constants.cdr_cleaner import clean_cdr as cdr_consts
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule

LOGGER = logging.getLogger(__name__)

# Keeps all the records that do not contain any observation_source_concept_id in the module description table
DROP_RECORDS_QUERY = JINJA_ENV.from_string("""
SELECT * FROM `{{project_id}}.{{dataset_id}}.observation`
WHERE observation_id NOT IN (
SELECT observation_id FROM `{{project_id}}.{{sandbox_id}}.{{sandbox_table}}`)
""")

# Selects all the records that will be dropped. The records that will be dropped contain observation_source_concept_id
# in the module description table
SANDBOX_RECORDS_QUERY = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project_id}}.{{sandbox_id}}.{{sandbox_table}}` AS (
SELECT * FROM `{{project_id}}.{{dataset_id}}.observation`
WHERE observation_source_concept_id IN (1586151, 1586150, 1586152, 1586153, 1586154, 1586155, 1586156, 1586149))
""")


class RaceEthnicityRecordSuppression(BaseCleaningRule):
    """
    Any record with an observation_source_concept_id in the description table should be sandboxed and dropped from the
    observation table.
    """

    def __init__(self, project_id, dataset_id, sandbox_dataset_id):
        """
        Initialize the class with proper information.

        Set the issue numbers, description and affected datasets. As other tickets may affect
        this SQL, append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """
        desc = 'Any record with an observation_source_concept_id equal to any of these values (1586151, 1586150, ' \
               '1586152, 1586153, 1586154, 1586155, 1586156, 1586149) will be sandboxed and dropped from ' \
               'the observation table'
        super().__init__(issue_numbers=['DC1365'],
                         description=desc,
                         affected_datasets=[cdr_consts.CONTROLLED_TIER_DEID],
                         affected_tables=OBSERVATION,
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id)

    def get_query_specs(self, *args, **keyword_args):
        """
        Return a list of dictionary query specifications.

        :return:  A list of dictionaries. Each dictionary contains a single query
            and a specification for how to execute that query. The specifications
            are optional but the query is required.
        """

        sandbox_records_query = {
            cdr_consts.QUERY:
                SANDBOX_RECORDS_QUERY.render(
                    project_id=self.project_id,
                    sandbox_id=self.sandbox_dataset_id,
                    sandbox_table=self.sandbox_table_for(OBSERVATION),
                    dataset_id=self.dataset_id)
        }

        drop_records_query = {
            cdr_consts.QUERY:
                DROP_RECORDS_QUERY.render(
                    project_id=self.project_id,
                    dataset_id=self.dataset_id,
                    sandbox_id=self.sandbox_dataset_id,
                    sandbox_table=self.sandbox_table_for(OBSERVATION)),
            cdr_consts.DESTINATION_TABLE:
                OBSERVATION,
            cdr_consts.DESTINATION_DATASET:
                self.dataset_id,
            cdr_consts.DISPOSITION:
                WRITE_TRUNCATE
        }

        return [sandbox_records_query, drop_records_query]

    def setup_rule(self, client, *args, **keyword_args):
        """
        Function to run any data upload options before executing a query.
        """
        pass

    def get_sandbox_tablenames(self):
        pass

    def setup_validation(self, client, *args, **keyword_args):
        """
        Run required steps for validation setup
        """
        raise NotImplementedError("Please fix me.")

    def validate_rule(self, client, *args, **keyword_args):
        """
        Validates the cleaning rule which deletes or updates the data from the tables
        """
        raise NotImplementedError("Please fix me.")


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.parse_args()
    pipeline_logging.configure(level=logging.DEBUG, add_console_handler=True)

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(
            ARGS.project_id, ARGS.dataset_id, ARGS.sandbox_dataset_id,
            [(RaceEthnicityRecordSuppression,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(RaceEthnicityRecordSuppression,)])
