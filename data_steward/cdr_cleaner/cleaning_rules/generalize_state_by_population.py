"""
This cleaning rule will generalize participant states that do not meet a
threshold of participant size.

Original Issues: DC-1614
"""

# Python imports
import logging

# Project imports
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule
from cdr_cleaner.cleaning_rules.deid.conflicting_hpo_state_generalization import ConflictingHpoStateGeneralize
from constants.cdr_cleaner import clean_cdr as cdr_consts
from common import JINJA_ENV, OBSERVATION
from utils import pipeline_logging

LOGGER = logging.getLogger(__name__)

PARTICIPANT_THRESH = 200

STATE_GENERALIZATION_QUERY = JINJA_ENV.from_string("""
    UPDATE `{{project_id}}.{{dataset_id}}.observation`
    SET value_source_concept_id = 2000000011,
        value_as_concept_id = 2000000011
    WHERE value_source_concept_id IN (
        SELECT
            value_source_concept_id
        FROM `{{project_id}}.{{dataset_id}}.observation`
        WHERE observation_source_concept_id = 1585249
        GROUP BY value_source_concept_id
        HAVING COUNT(*) < {{threshold}}
    )
    and observation_source_concept_id = 1585249
""")


class GeneralizeStateByPopulation(BaseCleaningRule):

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
        desc = "This cleaning rules generalizes states that don't achieve a set threshold."
        super().__init__(issue_numbers=['DC1614'],
                         description=desc,
                         affected_datasets=[cdr_consts.REGISTERED_TIER_DEID],
                         affected_tables=[OBSERVATION],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         depends_on=[ConflictingHpoStateGeneralize],
                         table_namer=table_namer)

    def get_sandbox_tablenames(self):
        return []

    def get_query_specs(self, *args, **keyword_args):
        """
        Interface to return a list of query dictionaries.

        :returns:  a list of query dictionaries.  Each dictionary specifies
            the query to execute and how to execute.  The dictionaries are
            stored in list order and returned in list order to maintain
            an ordering.
        """
        state_generalization_query = dict()
        state_generalization_query[
            cdr_consts.QUERY] = STATE_GENERALIZATION_QUERY.render(
                project_id=self.project_id,
                dataset_id=self.dataset_id,
                threshold=PARTICIPANT_THRESH)

        return [state_generalization_query]

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

    def setup_rule(self, client, *args, **keyword_args):
        """
        Load required resources prior to executing cleaning rule queries.

        Method to run data upload options before executing the first cleaning
        rule of a class.  For example, if your class requires loading a static
        table, that load operation should be defined here.  It SHOULD NOT BE
        defined as part of get_query_specs().
        """
        pass

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
        query_list = clean_engine.get_query_list(
            ARGS.project_id, ARGS.dataset_id, ARGS.sandbox_dataset_id,
            [(GeneralizeStateByPopulation,)])

        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(GeneralizeStateByPopulation,)])
