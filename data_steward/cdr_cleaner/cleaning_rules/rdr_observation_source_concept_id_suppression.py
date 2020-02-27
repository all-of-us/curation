"""
Removing three irrelevant observation_source_concept_ids from the RDR dataset.

Original Issue:  DC-529

The intent is to remove PPI records from the observation table in the RDR
export where observation_source_concept_id in (43530490, 43528818, 43530333).
The records for removal should be archived in the dataset sandbox.
"""
# Python Imports
import logging

# Project imports
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule
from constants.bq_utils import WRITE_TRUNCATE
import constants.cdr_cleaner.clean_cdr as cdr_consts

LOGGER = logging.getLogger(__name__)

SAVE_TABLE_NAME = 'dc_529_obs_rows_dropped'

DROP_SELECTION_QUERY = (
    'CREATE OR REPLACE TABLE `{project}.{sandbox}.{drop_table}` AS '
    'SELECT * '
    'FROM `{project}.{dataset}.observation` '
    'WHERE observation_source_concept_id IN (43530490, 43528818, 43530333)')

DROP_QUERY = (
    'SELECT * FROM `{project}.{dataset}.observation` AS o '
    'WHERE NOT EXISTS ( '
    '  SELECT 1 '
    '  FROM `{project}.{dataset}.observation` AS n '
    '  WHERE o.observation_id = n.observation_id AND '
    '  n.observation_source_concept_id IN (43530490, 43528818, 43530333) '
    ')')


class ObservationSourceConceptIDRowSuppression(BaseCleaningRule):
    """
    Suppress rows by values in the observation_source_concept_id field.
    """

    def __init__(self, project_id, dataset_id, sandbox_dataset_id):
        """
        Initialize the class with proper info.

        Set the issue numbers, description and affected datasets.  As other
        tickets may affect this SQL, append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """
        desc = (
            'Remove records from the rdr dataset where '
            'observation_source_concept_id in (43530490, 43528818, 43530333)')
        BaseCleaningRule.__init__(self,
                                  jira_issue_numbers=['DC-529'],
                                  description=desc,
                                  affected_datasets=[cdr_consts.RDR],
                                  project_id=project_id,
                                  dataset_id=dataset_id,
                                  sandbox_dataset_id=sandbox_dataset_id)

    def get_query_dictionary_list(self):
        """
        Get dictionary list of queries to execute.

        :param project_id:  The project to query.
        :param dataset_id:  The dataset to query.
        :param sandbox_id:  The sandbox dataset id to store intermediate
            results in.

        :return:  A list of dictionaries.  Each contains a single query.
            They may contain optional parameters describing the query.
        """
        save_dropped_rows = {
            cdr_consts.QUERY:
                DROP_SELECTION_QUERY.format(
                    project=self.get_project_id(),
                    dataset=self.get_dataset_id(),
                    sandbox=self.get_sandbox_dataset_id(),
                    drop_table=SAVE_TABLE_NAME),
        }

        drop_rows_query = {
            cdr_consts.QUERY:
                DROP_QUERY.format(project=self.get_project_id(),
                                  dataset=self.get_dataset_id()),
            cdr_consts.DESTINATION_TABLE:
                'observation',
            cdr_consts.DESTINATION_DATASET:
                self.get_dataset_id(),
            cdr_consts.DISPOSITION:
                WRITE_TRUNCATE
        }

        return [save_dropped_rows, drop_rows_query]


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.parse_args()
    clean_engine.add_console_logging(ARGS.console_log)
    rdr_cleaner = ObservationSourceConceptIDRowSuppression(
        ARGS.project_id, ARGS.dataset_id, ARGS.sandbox_dataset_id)
    query_list = rdr_cleaner.get_query_dictionary_list()

    if ARGS.list_queries:
        rdr_cleaner.print_queries()
    else:
        clean_engine.clean_dataset(ARGS.project_id, query_list)
