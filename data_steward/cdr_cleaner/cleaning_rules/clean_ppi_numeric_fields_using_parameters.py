"""
Apply value ranges to ensure that values are reasonable and to minimize the likelihood
of sensitive information (like phone numbers) within the free text fields.

Original Issues: DC-1058, DC-1061, DC-827, DC-502, DC-487, DC-2475, DC-2649, DC-3052

The intent is to ensure that numeric free-text fields that are not manipulated by de-id
have value range restrictions applied to the value_as_number field across the entire dataset.
"""

# Python imports
import logging

# Project imports
import constants.cdr_cleaner.clean_cdr as cdr_consts
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule
from constants.bq_utils import WRITE_TRUNCATE
from common import JINJA_ENV

LOGGER = logging.getLogger(__name__)

# Query to create tables in sandbox with the rows that will be removed per cleaning rule
INVALID_VALUES_SANDBOX_QUERY = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE
    `{{project}}.{{sandbox_dataset}}.{{intermediary_table}}` AS (
SELECT *
FROM
    `{{project}}.{{dataset}}.observation`
WHERE
    (observation_source_concept_id = 1585795 AND (value_as_number < 0 OR value_as_number > 99))
OR
    (observation_source_concept_id = 1585802 AND (value_as_number < 0 OR value_as_number > 99))
OR
    (observation_source_concept_id = 1585820 AND (value_as_number < 0 OR value_as_number > 255))
OR
    (observation_source_concept_id = 1585864 AND (value_as_number < 0 OR value_as_number > 99))
OR
    (observation_source_concept_id = 1585870 AND (value_as_number < 0 OR value_as_number > 99))
OR 
    (observation_source_concept_id = 1585873 AND (value_as_number < 0 OR value_as_number > 99))
OR
    (observation_source_concept_id = 1586159 AND (value_as_number < 0 OR value_as_number > 99))
OR
    (observation_source_concept_id = 1586162 AND (value_as_number < 0 OR value_as_number > 99))
OR
    -- from dc1061: sandbox any participant data who have a household size greater than 11 --
    (observation_source_concept_id IN (1333015, 1585889) AND (value_as_number < 0 OR value_as_number > 10))
OR
    -- from dc1058: sandbox any participant data who have 6 or more members under 18 in their household --
    (observation_source_concept_id IN (1333023, 1585890) AND (value_as_number < 0 OR value_as_number > 5))
OR
    (observation_source_concept_id = 1333023 AND value_as_number IS NULL AND value_as_string IS NOT NULL))
""")

CLEAN_INVALID_VALUES_QUERY = JINJA_ENV.from_string("""
SELECT 
    observation_id,
    person_id,
    observation_concept_id,
    observation_date,
    observation_datetime,
    observation_type_concept_id,
CASE
    WHEN observation_source_concept_id IN (1585795, 1585802, 1585864, 1585870, 1585873, 1586159, 1586162) AND (value_as_number < 0 OR value_as_number > 99) THEN NULL
    WHEN observation_source_concept_id = 1585820 AND (value_as_number < 0 OR value_as_number > 255) THEN NULL
    
    -- from dc1058: will null invalid values for value_as_number if participant household size is greater than 11 --
    WHEN observation_source_concept_id IN (1333015, 1585889) AND (value_as_number < 0 OR value_as_number > 10) THEN NULL
    
    -- from dc1061: will null invalid values for value_as_number if participant household has 6 or more members under the age of 18 --
    WHEN observation_source_concept_id IN (1333023, 1585890) AND (value_as_number < 0 OR value_as_number > 5) THEN NULL
  ELSE value_as_number
END AS
    value_as_number,
    CASE WHEN observation_source_concept_id = 1333023 AND value_as_number IS NULL AND value_as_string IS NOT NULL THEN NULL
    ELSE value_as_string
    END AS value_as_string,
CASE
    WHEN observation_source_concept_id IN (1585890, 1333023, 1333015, 1585889) 
        AND (
            value_as_number < 0 
            OR value_as_number >= 20 
            OR (value_as_number IS NULL AND value_as_string IS NOT NULL AND TRIM(LOWER(value_as_string)) NOT IN ('pmi_skip', 'pmi skip', ''))
        )
        THEN 2000000010
    WHEN observation_source_concept_id IN (1585795, 1585802, 1585864, 1585870, 1585873, 1586159, 1586162) AND (value_as_number < 0 OR value_as_number > 99) THEN 2000000010
    WHEN observation_source_concept_id = 1585820 AND (value_as_number < 0 OR value_as_number > 255) THEN 2000000010
    
    -- from dc1058: if the observation_source_concept_id is 1585889 or 1333015 and has between less than 11 members in the household --
    -- will set value_as_concept_id to the new custom concept --
    WHEN observation_source_concept_id IN (1585889, 1333015) AND (value_as_number < 20 AND value_as_number > 10) THEN 2000000013
    
    -- from dc1061: if the observation_source_concept_id is 1333023 or 1585890 and less than 6 members in the household --
    -- is under the age of 18, will set value_as_concept_id to the new custom concept --
    WHEN observation_source_concept_id IN (1333023, 1585890) AND (value_as_number < 20 AND value_as_number > 5) THEN 2000000012
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
        WHEN observation_source_concept_id IN (1585890, 1333023, 1333015, 1585889) 
            AND (
                value_as_number < 0 
                OR value_as_number >= 20 
                OR (value_as_number IS NULL AND value_as_string IS NOT NULL AND TRIM(LOWER(value_as_string)) NOT IN ('pmi_skip', 'pmi skip', ''))
            )
            THEN 2000000010
        WHEN observation_source_concept_id IN (1585795, 1585802, 1585864, 1585870, 1585873, 1586159, 1586162) AND (value_as_number < 0 OR value_as_number > 99) THEN 2000000010
        WHEN observation_source_concept_id = 1585820 AND (value_as_number < 0 OR value_as_number > 255) THEN 2000000010
    
        -- from dc1058: if the observation_source_concept_id is 1585889 or 1333015 and has between less than 11 members in the household --
        -- will set value_as_concept_id to the new custom concept --
        WHEN observation_source_concept_id IN (1585889, 1333015) AND (value_as_number < 20 AND value_as_number > 10) THEN 2000000013
    
        -- from dc1061: if the observation_source_concept_id is 1333023 or 1585890 and less than 6 members in the household --
        -- is under the age of 18, will set value_as_concept_id to the new custom concept --
        WHEN observation_source_concept_id IN (1333023, 1585890) AND (value_as_number < 20 AND value_as_number > 5) THEN 2000000012
    ELSE value_source_concept_id
    END AS
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
            'Sets value_as_number to NULL and value_as_concept_id and value_as_number '
            'to new AOU custom concept 2000000010 for responses with invalid values.'
            'Sets value_as_number to NULL and value_as_concept_id and value_as_number '
            'to new AOU custom concept 2000000013 for households with high amount of individuals.'
            'Sets value_as_number to NULL and value_as_concept_id and value_as_number '
            'to new AOU custom concept 2000000012 for households with 6 or more individuals '
            'under the age of 18')
        super().__init__(issue_numbers=[
            'DC1058', 'DC1061', 'DC827', 'DC502', 'DC487', 'DC2475', 'DC2649'
        ],
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
        invalid_values_sandbox_query = {
            cdr_consts.QUERY:
                INVALID_VALUES_SANDBOX_QUERY.render(
                    project=self.project_id,
                    dataset=self.dataset_id,
                    sandbox_dataset=self.sandbox_dataset_id,
                    intermediary_table=self.get_sandbox_tablenames()),
        }

        clean_invalid_values_query = {
            cdr_consts.QUERY:
                CLEAN_INVALID_VALUES_QUERY.render(project=self.project_id,
                                                  dataset=self.dataset_id),
            cdr_consts.DESTINATION_TABLE:
                'observation',
            cdr_consts.DESTINATION_DATASET:
                self.dataset_id,
            cdr_consts.DISPOSITION:
                WRITE_TRUNCATE
        }

        return [invalid_values_sandbox_query, clean_invalid_values_query]

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
        return f'{self._issue_numbers[0].lower()}_{self._affected_tables[0]}'


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.parse_args()

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(
            ARGS.project_id, ARGS.dataset_id, ARGS.sandbox_dataset_id,
            [(CleanPPINumericFieldsUsingParameters,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(CleanPPINumericFieldsUsingParameters,)])
