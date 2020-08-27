"""
Apply value ranges to ensure that values are reasonable and to minimize the likelihood
of sensitive information (like phone numbers) within the free text fields.

Original Issues: DC-827, DC-502, DC-487

The intent is to ensure that numeric free-text fields that are not manipulated by de-id
have value range restrictions applied to the value_as_number field across the entire dataset.
"""

# Python imports
import logging

# Third party imports
from jinja2 import Environment

# Project imports
import constants.cdr_cleaner.clean_cdr as cdr_consts
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule
from constants.bq_utils import WRITE_TRUNCATE

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

# Query to create tables in sandbox with the rows that will be removed per cleaning rule
SANDBOX_QUERY = jinja_env.from_string("""
CREATE OR REPLACE TABLE
    `{{project}}.{{sandbox_dataset}}.{{intermediary_table}}` AS (
SELECT *
FROM
    `{{project}}.{{dataset}}.observation`
WHERE
    (observation_concept_id = 1585889 AND (value_as_number < 0 OR value_as_number > 20))
OR
    (observation_concept_id = 1585890 AND (value_as_number < 0 OR value_as_number > 20))
OR
    (observation_concept_id = 1585795 AND (value_as_number < 0 OR value_as_number > 99))
OR
    (observation_concept_id = 1585802 AND (value_as_number < 0 OR value_as_number > 99))
OR
    (observation_concept_id = 1585820 AND (value_as_number < 0 OR value_as_number > 255))
OR
    (observation_concept_id = 1585864 AND (value_as_number < 0 OR value_as_number > 99))
OR
    (observation_concept_id = 1585870 AND (value_as_number < 0 OR value_as_number > 99))
OR 
    (observation_concept_id = 1585873 AND (value_as_number < 0 OR value_as_number > 99))
OR
    (observation_concept_id = 1586159 AND (value_as_number < 0 OR value_as_number > 99))
OR
    (observation_concept_id = 1586162 AND (value_as_number < 0 OR value_as_number > 99)))
""")

CLEAN_PPI_NUMERIC_FIELDS_QUERY = jinja_env.from_string("""
SELECT 
    observation_id,
    person_id,
    observation_concept_id,
    observation_date,
    observation_datetime,
    observation_type_concept_id,
CASE
    WHEN observation_concept_id IN (1585889, 1585890) AND (value_as_number < 0 OR value_as_number > 20) THEN NULL
    WHEN observation_concept_id IN (1585795, 1585802, 1585864, 1585870, 1585873, 1586159, 1586162) AND (value_as_number < 0 OR value_as_number > 99) THEN NULL
    WHEN observation_concept_id = 1585820 AND (value_as_number < 0 OR value_as_number > 255) THEN NULL
  ELSE value_as_number
END AS
    value_as_number,
    value_as_string,
CASE
    WHEN observation_concept_id IN (1585889, 1585890) AND (value_as_number < 0 OR value_as_number > 20) THEN 2000000010
    WHEN observation_concept_id IN (1585795, 1585802, 1585864, 1585870, 1585873, 1586159, 1586162) AND (value_as_number < 0 OR value_as_number > 99) THEN 2000000010
    WHEN observation_concept_id = 1585820 AND (value_as_number < 0 OR value_as_number > 255) THEN 2000000010
  ELSE value_as_concept_id
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
    value_source_concept_id,
    value_source_value,
    questionnaire_response_id
FROM
    {{project}}.{{dataset}}.observation""")


class CleanPPINumericFieldsUsingParameters(BaseCleaningRule):
    """
    Apply value ranges to ensure that values are reasonable and to minimize the likelihood
    of sensitive information (like phone numbers) within the free text fields.
    """

    def __init__(self, project_id, dataset_id, sandbox_dataset_id):
        """
        Initialize the class with proper information.

        Set the issue numbers, description and affected datasets. As other tickets may affect
        this SQL, append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """
        desc = (
            'Sets value_as_number to NULL and value_as_concept_id and observation_type_concept_id '
            'to new AOU custom concept 2000000010')
        super().__init__(issue_numbers=['DC827', 'DC502', 'DC487'],
                         description=desc,
                         affected_datasets=[cdr_consts.RDR],
                         affected_tables=['observation'],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id)

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

        clean_ppi_numeric_fields_query = {
            cdr_consts.QUERY:
                CLEAN_PPI_NUMERIC_FIELDS_QUERY.render(project=self.project_id,
                                                      dataset=self.dataset_id),
            cdr_consts.DESTINATION_TABLE:
                'observation',
            cdr_consts.DESTINATION_DATASET:
                self.dataset_id,
            cdr_consts.DISPOSITION:
                WRITE_TRUNCATE
        }

        return [save_changed_rows, clean_ppi_numeric_fields_query]

    def setup_rule(self, client):
        """
        Function to run any data upload options before executing a query.
        """
        pass

    def setup_validation(self, client):
        """
        Run required steps for validation setup
        """
        pass

    def validate_rule(self, client):
        """
        Validates the cleaning rule which deletes or updates the data from the tables
        """
        pass

    def get_sandbox_tablenames(self):
        sandbox_table_names = list()
        sandbox_table_names.append(self._issue_numbers[0] +
                                   self._affected_tables[0])
        return sandbox_table_names


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.parse_args()

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(
            ARGS.project_id, ARGS.dataset_id,
            [(CleanPPINumericFieldsUsingParameters,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   [(CleanPPINumericFieldsUsingParameters,)])
