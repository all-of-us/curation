"""
This cleaning rule generalizes all PPI concepts containing zip codes (observation_source_concept_id = 1585250)
to their primary three digit representation, (e.g. 35400 → 354**).

This cleaning rule is specific to the controlled tier.

Original Issues: DC-1376
"""

# Python imports
import logging

# Project imports
import constants.bq_utils as bq_consts
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule
from constants.cdr_cleaner import clean_cdr as cdr_consts
from common import JINJA_ENV, OBSERVATION
from utils import pipeline_logging

LOGGER = logging.getLogger(__name__)

GENERALIZE_ZIP_CODES_QUERY = JINJA_ENV.from_string("""
SELECT
    * REPLACE(
        IF (
            observation_source_concept_id IN (1585250),
            RPAD(
                SUBSTR(value_as_string, 1, 3),
                LENGTH(value_as_string),
                '*'
            ),
            value_as_string
        )  AS value_as_string       
    )
FROM `{{project_id}}.{{dataset_id}}.{{obs_table}}`
""")


class GeneralizeZipCodes(BaseCleaningRule):

    def __init__(self, project_id, dataset_id, sandbox_dataset_id):
        """
        Initialize the class with proper information.

        Set the issue numbers, description and affected datasets. As other tickets may affect
        this SQL, append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """
        desc = 'This cleaning rule transform zip codes to a generalized, partially obscured form.'
        super().__init__(issue_numbers=['DC1376'],
                         description=desc,
                         affected_datasets=[cdr_consts.CONTROLLED_TIER_DEID],
                         affected_tables=[OBSERVATION],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id)

    def validate_rule(self, client, *args, **keyword_args):
        """
        Validates the cleaning rule which deletes or updates the data from the tables

        Method to run validation on cleaning rules that will be updating the values.
        For example:
        if your class updates all the datetime fields you should be implementing the
        validation that checks if the date time values that needs to be updated no
        longer exists in the table.

        if your class deletes a subset of rows in the tables you should be implementing
        the validation that checks if the count of final final row counts + deleted rows
        should equals to initial row counts of the affected tables.

        Raises RunTimeError if the validation fails.
        """

        raise NotImplementedError("Please fix me.")

    def get_sandbox_tablenames(self):
        return [self.sandbox_table_for(OBSERVATION)]

    def setup_rule(self, client, *args, **keyword_args):
        """
        Load required resources prior to executing cleaning rule queries.

        Method to run data upload options before executing the first cleaning
        rule of a class.  For example, if your class requires loading a static
        table, that load operation should be defined here.  It SHOULD NOT BE
        defined as part of get_query_specs().
        """
        pass

    def get_query_specs(self, *args, **keyword_args):
        """
        Interface to return a list of query dictionaries.

        :returns:  a list of query dictionaries.  Each dictionary specifies
            the query to execute and how to execute.  The dictionaries are
            stored in list order and returned in list order to maintain
            an ordering.
        """
        update_query = dict()
        update_query[cdr_consts.QUERY] = GENERALIZE_ZIP_CODES_QUERY.render(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            obs_table=OBSERVATION)
        update_query[cdr_consts.DESTINATION_TABLE] = OBSERVATION
        update_query[cdr_consts.DESTINATION_DATASET] = self.dataset_id
        update_query[cdr_consts.DISPOSITION] = bq_consts.WRITE_TRUNCATE

        return [update_query]

    def setup_validation(self, client, *args, **keyword_args):
        """
        Run required steps for validation setup

        Method to run to setup validation on cleaning rules that will be updating or deleting the values.
        For example:
        if your class updates all the datetime fields you should be implementing the
        logic to get the initial list of values which adhere to a condition we are looking for.

        if your class deletes a subset of rows in the tables you should be implementing
        the logic to get the row counts of the tables prior to applying cleaning rule

        """
        raise NotImplementedError("Please fix me.")


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.parse_args()
    pipeline_logging.configure(level=logging.DEBUG, add_console_handler=True)

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(ARGS.project_id,
                                                 ARGS.dataset_id,
                                                 ARGS.sandbox_dataset_id,
                                                 [(GeneralizeZipCodes,)])

        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(GeneralizeZipCodes,)])
