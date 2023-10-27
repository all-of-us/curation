"""
Dataset (currently limited to fitbit dataset) should only contain pids that exist in the corresponding CDR
"""
import logging

# Project imports
from common import JINJA_ENV, FITBIT_TABLES, DEVICE
from constants.cdr_cleaner import clean_cdr as cdr_consts
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule, query_spec_list
from cdr_cleaner.clean_cdr_utils import get_tables_in_dataset

LOGGER = logging.getLogger(__name__)

JIRA_ISSUE_NUMBERS = ['DC1787', 'DC2138']

SANDBOX_NON_EXISTING_PIDS = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project_id}}.{{sandbox_dataset_id}}.{{sandbox_table_id}}` AS
SELECT *
FROM `{{project_id}}.{{dataset_id}}.{{table_id}}`
WHERE person_id NOT IN
(SELECT person_id
FROM `{{project_id}}.{{reference_dataset_id}}.person`)
""")

DELETE_NON_EXISTING_PIDS = JINJA_ENV.from_string("""
DELETE
FROM `{{project_id}}.{{dataset_id}}.{{table_id}}`
WHERE person_id NOT IN
(SELECT person_id
FROM `{{project_id}}.{{reference_dataset_id}}.person`)
""")


class RemoveNonExistingPids(BaseCleaningRule):

    def __init__(self,
                 project_id,
                 dataset_id,
                 sandbox_dataset_id,
                 table_namer=None,
                 reference_dataset_id=None):
        """
        Initialize the class with proper information.

        Set the issue numbers, description and affected datasets. As other tickets may affect
        this SQL, append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """
        desc = (
            'Remove records for PIDs not belonging in the corresponding CDR')

        if not reference_dataset_id:
            raise TypeError("`reference_dataset_id` cannot be empty")

        self.reference_dataset_id = reference_dataset_id

        super().__init__(
            issue_numbers=JIRA_ISSUE_NUMBERS,
            description=desc,
            affected_datasets=[cdr_consts.FITBIT],
            #  affected_tables=FITBIT_TABLES,
            affected_tables=[DEVICE],
            project_id=project_id,
            dataset_id=dataset_id,
            sandbox_dataset_id=sandbox_dataset_id,
            table_namer=table_namer)

    def get_query_specs(self, *args, **keyword_args) -> query_spec_list:

        sandbox_queries, queries = [], []
        # iterate through the list of tables
        for table in self.affected_tables:
            sandbox_queries.append({
                cdr_consts.QUERY:
                    SANDBOX_NON_EXISTING_PIDS.render(
                        project_id=self.project_id,
                        dataset_id=self.dataset_id,
                        sandbox_dataset_id=self.sandbox_dataset_id,
                        sandbox_table_id=self.sandbox_table_for(table),
                        reference_dataset_id=self.reference_dataset_id,
                        table_id=table)
            })
            queries.append({
                cdr_consts.QUERY:
                    DELETE_NON_EXISTING_PIDS.render(
                        project_id=self.project_id,
                        dataset_id=self.dataset_id,
                        reference_dataset_id=self.reference_dataset_id,
                        table_id=table)
            })

        return sandbox_queries + queries

    def get_sandbox_tablenames(self):
        return [
            self.sandbox_table_for(affected_table)
            for affected_table in self.affected_tables
        ]

    def setup_rule(self, client, *args, **keyword_args):
        self.affected_tables = get_tables_in_dataset(client, self.project_id,
                                                     self.dataset_id,
                                                     self.affected_tables)

    def setup_validation(self, client, *args, **keyword_args):
        pass

    def validate_rule(self, client, *args, **keyword_args):
        pass


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine
    from utils import pipeline_logging

    ext_parser = parser.get_argument_parser()
    ext_parser.add_argument(
        '-r',
        '--reference_dataset_id',
        dest='reference_dataset_id',
        action='store',
        help='CT or RT dataset to use as reference containing valid PIDs',
        required=True,
    )

    ARGS = ext_parser.parse_args()
    pipeline_logging.configure(level=logging.DEBUG, add_console_handler=True)

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(
            ARGS.project_id,
            ARGS.dataset_id,
            ARGS.sandbox_dataset_id, [(RemoveNonExistingPids,)],
            table_namer='',
            reference_dataset_id=ARGS.reference_dataset_id)
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(
            ARGS.project_id,
            ARGS.dataset_id,
            ARGS.sandbox_dataset_id, [(RemoveNonExistingPids,)],
            table_namer='',
            reference_dataset_id=ARGS.reference_dataset_id)
