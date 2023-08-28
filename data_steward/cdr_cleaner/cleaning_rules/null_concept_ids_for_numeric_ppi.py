"""
Nullify concept ids for numeric PPIs from the RDR observation dataset

Original Issues: DC-537, DC-703, DC-1098

The intent is to null concept ids (value_source_concept_id, value_as_concept_id, value_source_value,
value_as_string) for numeric PPIs from the RDR observation dataset. The changed records should be
archived in the dataset sandbox.
"""

# Python imports
import logging

# Project imports
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule
from common import OBSERVATION, JINJA_ENV
from constants.cdr_cleaner import clean_cdr as cdr_consts

LOGGER = logging.getLogger(__name__)

# Query to create tables in sandbox with the rows that will be removed per cleaning rule
SANDBOX_QUERY = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE
    `{{project}}.{{sandbox_dataset}}.{{intermediary_table}}` AS (
SELECT *
FROM
    `{{project}}.{{dataset}}.observation`
WHERE
    questionnaire_response_id IS NOT NULL
AND
    value_as_number IS NOT NULL
AND
    (value_source_concept_id IS NOT NULL OR value_as_concept_id IS NOT NULL))
""")

CLEAN_NUMERIC_PPI_QUERY = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project}}.{{dataset}}.observation` AS (
    SELECT
        observation_id,
        person_id,
        observation_concept_id,
        observation_date,
        observation_datetime,
        observation_type_concept_id,
        value_as_number,
    CASE
        WHEN
        questionnaire_response_id IS NOT NULL AND value_as_number IS NOT NULL AND (value_source_concept_id IS NOT NULL OR value_as_concept_id IS NOT NULL) THEN NULL
        ELSE value_as_string
    END AS
        value_as_string,
    CASE
        WHEN
        questionnaire_response_id IS NOT NULL AND value_as_number IS NOT NULL AND (value_source_concept_id IS NOT NULL OR value_as_concept_id IS NOT NULL) THEN NULL
        ELSE value_as_concept_id
    END AS
        value_as_concept_id,
        qualifier_concept_id,
        unit_concept_id,
        provider_id,
        visit_occurrence_id,
        visit_detail_id,
        observation_source_value,
        observation_source_concept_id,
        unit_source_value,
        qualifier_source_value,
    CASE
        WHEN
        questionnaire_response_id IS NOT NULL AND value_as_number IS NOT NULL AND (value_source_concept_id IS NOT NULL OR value_as_concept_id IS NOT NULL) THEN NULL
        ELSE value_source_concept_id
    END AS
        value_source_concept_id,
    CASE
        WHEN
        questionnaire_response_id IS NOT NULL AND value_as_number IS NOT NULL AND (value_source_concept_id IS NOT NULL OR value_as_concept_id IS NOT NULL) THEN NULL
        ELSE value_source_value
    END AS
        value_source_value,
        questionnaire_response_id
    FROM
        {{project}}.{{dataset}}.observation
)""")


class NullConceptIDForNumericPPI(BaseCleaningRule):
    """
    Nulls answer concept_ids for numeric PPI questions
    """

    def __init__(self, project_id, dataset_id, sandbox_dataset_id):
        """
        Initialize the class with proper information.

        Set the issue numbers, description and affected datasets. As other tickets may affect
        this SQL, append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """
        desc = (
            'Nulls answer concept_ids for numeric PPI questions if:\n'
            '(1) questionnaire_response_id is not null\n'
            '(2) value_as_number is not null\n'
            '(3) value_source_concept_id or value_as_concept_id is not null')
        super().__init__(issue_numbers=['DC-537', 'DC-703', 'DC-1098'],
                         description=desc,
                         affected_datasets=[cdr_consts.RDR],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         affected_tables=[OBSERVATION],
                         run_for_synthetic=True)

    def get_query_specs(self):
        """
        Return a list of dictionary query specifications.

        :return:  A list of dictionaries. Each dictionary contains a single query
            and a specification for how to execute that query. The specifications
            are optional but the query is required.
        """
        save_changed_rows = {
            cdr_consts.QUERY:
                SANDBOX_QUERY.render(
                    project=self.project_id,
                    dataset=self.dataset_id,
                    sandbox_dataset=self.sandbox_dataset_id,
                    intermediary_table=self.get_sandbox_tablenames()[0]),
        }

        clean_numeric_ppi_query = {
            cdr_consts.QUERY:
                CLEAN_NUMERIC_PPI_QUERY.render(project=self.project_id,
                                               dataset=self.dataset_id),
        }

        return [save_changed_rows, clean_numeric_ppi_query]

    def setup_rule(self, client):
        """
        Function to run any data upload options before executing a query.
        """
        pass

    def get_sandbox_tablenames(self):
        return [self.sandbox_table_for(table) for table in self.affected_tables]

    def setup_validation(self, client):
        """
        Run required steps for validation setup

        This abstract method was added to the base class after this rule was authored.
        This rule needs to implement logic to setup validation on cleaning rules that
        will be updating or deleting the values.
        Until done no issue exists for this yet.
        """
        raise NotImplementedError("Please fix me.")

    def validate_rule(self, client):
        """
        Validates the cleaning rule which deletes or updates the data from the tables

        This abstract method was added to the base class after this rule was authored.
        This rule needs to implement logic to run validation on cleaning rules that will
        be updating or deleting the values.
        Until done no issue exists for this yet.
        """
        raise NotImplementedError("Please fix me.")


if __name__ == '__main__':
    import cdr_cleaner.clean_cdr_engine as clean_engine
    import cdr_cleaner.args_parser as parser

    ARGS = parser.parse_args()

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(
            ARGS.project_id, ARGS.dataset_id, ARGS.sandbox_dataset_id,
            [(NullConceptIDForNumericPPI,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(NullConceptIDForNumericPPI,)])
