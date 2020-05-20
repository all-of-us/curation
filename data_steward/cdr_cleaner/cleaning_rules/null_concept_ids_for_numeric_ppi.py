"""
Nullify concept ids for numeric PPIs from the RDR observation dataset

Original Issues: DC-537, DC-703

The intent is to null concept ids (value_source_concept_id, value_as_concept_id, value_source_value,
value_as_string) for numeric PPIs from the RDR observation dataset. The changed records should be
archived in the dataset sandbox.
"""

# Python imports
import logging

# Third party imports
from jinja2 import Environment

from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule
# Project imports
from common import OBSERVATION
from constants.bq_utils import WRITE_TRUNCATE
from constants.cdr_cleaner import clean_cdr as cdr_consts

LOGGER = logging.getLogger(__name__)

jinja_env = Environment(
    # help protect against cross-site scripting vulnerabilities
    autoescape=True,
    # block tags on their own lines
    # will not cause extra white space
    trim_blocks=True,
    lstrip_blocks=True,
    # syntax highlighting should be better
    # with these comment delimiters
    comment_start_string='--',
    comment_end_string=' --')

SAVE_TABLE_NAME = "dc_703_obs_changed_rows_saved"

# Query to create tables in sandbox with the rows that will be removed per cleaning rule
SANDBOX_QUERY = jinja_env.from_string("""
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

CLEAN_NUMERIC_PPI_QUERY = jinja_env.from_string("""
SELECT
    observation_id,
    person_id,
    observation_concept_id,
    observation_date,
    observation_datetime,
    observation_type_concept_id,
    value_as_number,
CASE WHEN 
    value_as_number IS NOT NULL AND value_source_concept_id IS NOT NULL AND (value_source_concept_id IS NOT NULL OR value_as_concept_id IS NOT NULL) THEN NULL
END AS 
    value_as_string,
CASE WHEN
    value_as_number IS NOT NULL AND value_source_concept_id IS NOT NULL AND (value_source_concept_id IS NOT NULL OR value_as_concept_id IS NOT NULL) THEN NULL
END AS
    value_as_concept_id,
    qualifier_concept_id,
    unit_concept_id,
    provider_id,
    visit_occurrence_id,
    observation_source_value,
    observation_source_concept_id,
    unit_source_value,
    qualifier_source_value,
CASE WHEN
    value_as_number IS NOT NULL AND value_source_concept_id IS NOT NULL AND (value_source_concept_id IS NOT NULL OR value_as_concept_id IS NOT NULL) THEN NULL
END AS
    value_source_concept_id,
CASE WHEN
    value_as_number IS NOT NULL AND value_source_concept_id IS NOT NULL AND (value_source_concept_id IS NOT NULL OR value_as_concept_id IS NOT NULL) THEN NULL
END AS
    value_source_value,
    questionnaire_response_id
FROM
    {{project}}.{{dataset}}.observation""")


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
            'Nulls answer concept_ids for numeric PPI questions if:'
            '(1) questionnaire_response_id is not null'
            '(2) value_as_number is not null'
            '(3) value_source_concept_id or value_as_concept_id is not null')
        super().__init__(issue_numbers=['DC-537', 'DC-703'],
                         description=desc,
                         affected_datasets=[cdr_consts.RDR],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         affected_tables=[OBSERVATION])

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
                    project=self.get_project_id(),
                    dataset=self.get_dataset_id(),
                    sandbox_dataset=self.get_sandbox_dataset_id(),
                    intermediary_table=SAVE_TABLE_NAME),
        }

        clean_numeric_ppi_query = {
            cdr_consts.QUERY:
                CLEAN_NUMERIC_PPI_QUERY.render(project=self.get_project_id(),
                                               dataset=self.get_dataset_id()),
            cdr_consts.DESTINATION_TABLE:
                'observation',
            cdr_consts.DESTINATION_DATASET:
                self.get_dataset_id(),
            cdr_consts.DISPOSITION:
                WRITE_TRUNCATE
        }

        return [save_changed_rows, clean_numeric_ppi_query]

    def setup_rule(self):
        """
        Function to run any data upload options before executing a query.
        """
        pass

    def get_sandbox_tablenames(self):
        return [SAVE_TABLE_NAME]

    def setup_validation(self):
        """
        Run required steps for validation setup

        This abstract method was added to the base class after this rule was authored.
        This rule needs to implement logic to setup validation on cleaning rules that
        will be updating or deleting the values.
        Until done no issue exists for this yet.
        """
        raise NotImplementedError("Please fix me.")

    def validate_rule(self):
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

    clean_engine.add_console_logging(ARGS.console_log)
    rdr_cleaner = NullConceptIDForNumericPPI(ARGS.project_id, ARGS.dataset_id,
                                             ARGS.sandbox_dataset_id)
    query_list = rdr_cleaner.get_query_specs()

    if ARGS.list_queries:
        rdr_cleaner.log_queries()
    else:
        clean_engine.clean_dataset(ARGS.project_id, query_list)
