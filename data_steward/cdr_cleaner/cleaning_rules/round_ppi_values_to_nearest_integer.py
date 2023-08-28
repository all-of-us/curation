"""
Numeric PPI free text questions should only have integer values, but there are some non-integer values present that
needs to be updated.
For the following observation_source_concept_ids, we have to make sure if the value_as_number field is an integer.
If it is not, It should be rounded to the nearest integer:

1585889
1585890
1585795
1585802
1585820
1585864
1585870
1585873
1586159
1586162
1333015
1333023

"""
import logging

import constants.cdr_cleaner.clean_cdr as cdr_consts
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule
from common import JINJA_ENV

LOGGER = logging.getLogger(__name__)

OBSERVATION = 'observation'

JIRA_ISSUE_NUMBERS = ['DC538', 'DC1276']
ROUND_PPI_VALUES_QUERY = JINJA_ENV.from_string("""
UPDATE
  `{{project}}.{{dataset}}.observation`
SET
  value_as_number = CAST(ROUND(value_as_number) AS INT64)
WHERE
  observation_source_concept_id IN (1585889,
    1585890,
    1585795,
    1585802,
    1585820,
    1585864,
    1585870,
    1585873,
    1586159,
    1586162,
    1333015,
    1333023
    )
  AND value_as_number IS NOT NULL
""")


class RoundPpiValuesToNearestInteger(BaseCleaningRule):
    """
    Runs the query which make sure if the value_as_number field is an integer.
    If it is not, It will be rounded to the nearest integer in observation table
    """

    def __init__(self, project_id, dataset_id, sandbox_dataset_id):
        """
        Initialize the class with proper information.

        Set the issue numbers, description and affected datasets. As other tickets may affect
        this SQL, append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """
        desc = ("Runs the query which make sure if the value_as_number field "
                "is an integer.  If it is not, It will be rounded to the "
                "nearest integer in observation table.")

        super().__init__(issue_numbers=JIRA_ISSUE_NUMBERS,
                         description=desc,
                         affected_datasets=[cdr_consts.RDR],
                         affected_tables=[OBSERVATION],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         run_for_synthetic=True)

    def get_query_specs(self):
        """
        Return a list of dictionary query specifications.

        :return:  A list of dictionaries. Each dictionary contains a single query
            and a specification for how to execute that query. The specifications
            are optional but the query is required.
        """

        queries_list = []
        query = dict()

        query[cdr_consts.QUERY] = ROUND_PPI_VALUES_QUERY.render(
            dataset=self.dataset_id, project=self.project_id)

        queries_list.append(query)

        return queries_list

    def setup_rule(self, client):
        """
        Function to run any data upload options before executing a query.
        """
        pass

    def setup_validation(self, client):
        """
        Run required steps for validation setup
        """
        raise NotImplementedError("Please fix me.")

    def validate_rule(self, client):
        """
        Validates the cleaning rule which deletes or updates the data from the tables
        """
        raise NotImplementedError("Please fix me.")

    def get_sandbox_tablenames(self):
        raise NotImplementedError("Please fix me.")


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.parse_args()

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(
            ARGS.project_id, ARGS.dataset_id, ARGS.sandbox_dataset_id,
            [(RoundPpiValuesToNearestInteger,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(RoundPpiValuesToNearestInteger,)])
