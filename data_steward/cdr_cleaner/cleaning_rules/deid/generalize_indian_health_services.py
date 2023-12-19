"""
Participants answer multiple choice insurance questions on multiple surveys.
One of the available insurance selections is "Indian Health Services" (or a variant).
This option does not necessarily identify a participant as a self identifying 
AI/AN participant. In an abundance of caution, the program is requesting 
generalization of these responses to the same questions' associated “Other” 
response.

This should be done at Registered Tier de-identification and should drop 
duplicate responses if they are created by this generalization.

NOTE This cleaning rule only updates `value_source_concept_id` and `value_as_concept_id`.
    String fields will be unchanged. `StringFieldsSuppression` will take care of the 
    string suppression.

Original Issues: DC-3597
"""

# Python imports
import logging

# Project imports
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule
from constants.cdr_cleaner import clean_cdr as cdr_consts
from common import JINJA_ENV, OBSERVATION
from utils import pipeline_logging

LOGGER = logging.getLogger(__name__)

SANDBOX_QUERY = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project_id}}.{{sandbox_id}}.{{sandbox_table}}` AS (
    SELECT * FROM `{{project_id}}.{{dataset_id}}.observation`
    WHERE value_source_concept_id IN (1384516, 1585396, 43529111)
)
""")

GENERALIZATION_QUERY = JINJA_ENV.from_string("""
UPDATE `{{project_id}}.{{dataset_id}}.observation`
SET
    value_source_concept_id = CASE 
        WHEN value_source_concept_id = 1384516 THEN 1384595 
        WHEN value_source_concept_id = 1585396 THEN 1585398 
        WHEN value_source_concept_id = 43529111 THEN 43528423 
    END,
    value_as_concept_id = CASE 
        WHEN value_source_concept_id = 1384516 THEN 1384595 
        WHEN value_source_concept_id = 1585396 THEN 45876762 
        WHEN value_source_concept_id = 43529111 THEN 43528423 
    END
WHERE value_source_concept_id IN (1384516, 1585396, 43529111)
""")

REMOVE_DUPLICATE_GENERALIZED_ANSWERS = JINJA_ENV.from_string("""
DELETE FROM `{{project_id}}.{{dataset_id}}.observation`
WHERE observation_id IN (
    SELECT observation_id
    FROM (
        SELECT
            observation_id,
            ROW_NUMBER() OVER(
                PARTITION BY person_id, value_source_concept_id, value_as_concept_id 
                ORDER BY observation_date DESC, observation_id
            ) AS rn
        FROM `{{project_id}}.{{dataset_id}}.observation`
        WHERE (value_source_concept_id = 1384595 AND value_as_concept_id = 1384595)
        OR (value_source_concept_id = 1585398 AND value_as_concept_id = 45876762)
        OR (value_source_concept_id = 43528423 AND value_as_concept_id = 43528423)
    ) WHERE rn <> 1
)
""")


class GeneralizeIndianHealthServices(BaseCleaningRule):

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
        desc = (
            "Generalizes answers that indicate Indian Health Services (or a variant)."
            "Generalizes only ID fields. String fields will be suppressed by `StringFieldsSuppression`."
        )
        super().__init__(issue_numbers=['DC3597'],
                         description=desc,
                         affected_datasets=[cdr_consts.REGISTERED_TIER_DEID],
                         affected_tables=[OBSERVATION],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         table_namer=table_namer)

    def get_sandbox_tablenames(self):
        """
        Returns a list of sandbox table names.
        """
        return [self.sandbox_table_for(OBSERVATION)]

    def get_query_specs(self, *args, **keyword_args):
        """
        Interface to return a list of query dictionaries.

        :returns:  a list of query dictionaries.  Each dictionary specifies
            the query to execute and how to execute.  The dictionaries are
            stored in list order and returned in list order to maintain
            an ordering.
        """

        sandbox_query = dict()
        sandbox_query[cdr_consts.QUERY] = SANDBOX_QUERY.render(
            project_id=self.project_id,
            sandbox_id=self.sandbox_dataset_id,
            sandbox_table=self.sandbox_table_for(OBSERVATION),
            dataset_id=self.dataset_id)

        generalization_query = dict()
        generalization_query[cdr_consts.QUERY] = GENERALIZATION_QUERY.render(
            project_id=self.project_id, dataset_id=self.dataset_id)

        generalized_answers_deduplication_query = dict()
        generalized_answers_deduplication_query[
            cdr_consts.QUERY] = REMOVE_DUPLICATE_GENERALIZED_ANSWERS.render(
                project_id=self.project_id, dataset_id=self.dataset_id)

        return [
            sandbox_query, generalization_query,
            generalized_answers_deduplication_query
        ]

    def validate_rule(self, client, *args, **keyword_args):
        """
        Validates the cleaning rule which deletes or updates the data from the tables
        """
        raise NotImplementedError("Please fix me.")

    def setup_rule(self, client, *args, **keyword_args):
        """
        Load required resources prior to executing cleaning rule queries.
        """
        pass

    def setup_validation(self, client, *args, **keyword_args):
        """
        Run required steps for validation setup
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
            [(GeneralizeIndianHealthServices,)])

        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(GeneralizeIndianHealthServices,)])
