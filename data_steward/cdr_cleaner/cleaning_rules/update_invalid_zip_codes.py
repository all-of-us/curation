"""
Sandbox and update invalid zip codes found in the observation table.

Original Issues: DC-1633, DC-1645

The intent of this cleaning rule is to remove any leading/trailing whitespace in the zip code string then sandbox and
update any invalid zip code in the observation table. A zip code is considered invalid if it:
        Is less than 5 digits in length
        Is alpha-numeric
        Does not match any zip3 code in the master zip3 lookup table
If zip code is deemed invalid the record is sandboxed and updated to have the following information:
        value_as_string and value_source_value = 'Response removed due to invalid value'
        value_as_number = 0
        value_source_concept_id = 2000000010
"""

# Python imports
import logging

# Project imports
from utils import pipeline_logging
from constants.cdr_cleaner import clean_cdr as cdr_consts
from common import JINJA_ENV, OBSERVATION, PIPELINE_TABLES, ZIP3_LOOKUP
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule

LOGGER = logging.getLogger(__name__)

ZIPS_WITH_WHITESPACE_SANDBOX = 'dc1633_zips_with_whitespace'

# Creates sandbox that contains any zips that have leading/trailing whitespace
SANDBOX_ZIPS_WITH_WHITESPACE = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project_id}}.{{sandbox_dataset_id}}.{{whitespace_sandbox}}` AS
SELECT * FROM `{{project_id}}.{{dataset_id}}.{{obs_table}}`
WHERE observation_source_concept_id = 1585250 
AND REGEXP_CONTAINS(value_as_string, ' ')
""")

# Creates sandbox that contains all invalid zip codes which are deemed invalid because they:
# 1. Are not 5 digits in length
# 2. Are are alpha-numeric
# 3. Do not match a zip3 code in the master zip3 lookup table
SANDBOX_INVALID_ZIP_CODES = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project_id}}.{{sandbox_dataset_id}}.{{sandbox_table}}` AS (
-- Selects all zips that are less than 5 digits in length and/or alpha-numeric --
SELECT * FROM `{{project_id}}.{{dataset_id}}.{{obs_table}}`
WHERE observation_source_concept_id = 1585250 
AND (LENGTH(value_as_string) < 5 
OR NOT REGEXP_CONTAINS(value_as_string, r'^[0-9]{5}(?:-[0-9]{4})?$'))
-- Using union distinct to prevent duplicates --
UNION DISTINCT (
-- Selects all zips that do not match one in the master zip3 lookup table --
SELECT o.* FROM `{{project_id}}.{{dataset_id}}.{{obs_table}}` o
LEFT JOIN `{{project_id}}.{{pipeline_tables}}.{{zip3_lookup}}` z
ON SUBSTR(o.value_as_string, 1, 3) = CAST(z.zip3 AS STRING)
WHERE (observation_source_concept_id = 1585250 AND z.zip3 IS NULL)))
""")

CLEAN_ZIPS_OF_WHITESPACE = JINJA_ENV.from_string("""
UPDATE `{{project_id}}.{{dataset_id}}.{{obs_table}}` SET
value_as_string = TRIM(value_as_string)
WHERE observation_id IN (
SELECT observation_id FROM `{{project_id}}.{{sandbox_dataset_id}}.{{whitespace_sandbox}}`)
""")

UPDATE_INVALID_ZIP_CODES = JINJA_ENV.from_string("""
UPDATE `{{project_id}}.{{dataset_id}}.{{obs_table}}` SET
value_as_string = 'Response removed due to invalid value',
value_source_value = 'Response removed due to invalid value',
value_as_number = 0,
value_source_concept_id = 2000000010
WHERE observation_id IN (
SELECT observation_id FROM `{{project_id}}.{{sandbox_dataset_id}}.{{sandbox_table}}`)
""")


class UpdateInvalidZipCodes(BaseCleaningRule):
    """
    Any invalid zip code will be sandboxed and updated.
    """

    def __init__(self, project_id, dataset_id, sandbox_dataset_id):
        """
        Initialize the class with proper information.

        Set the issue numbers, description and affected datasets. As other tickets may affect
        this SQL, append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """
        desc = 'Sandbox and update invalid zip codes found in the observation table.'
        super().__init__(issue_numbers=['DC1633'],
                         description=desc,
                         affected_datasets=[cdr_consts.RDR],
                         affected_tables=[OBSERVATION],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id)

    def get_query_specs(self, *args, **keyword_args):
        """
        Return a list of dictionary query specifications.

        :return:  A list of dictionaries. Each dictionary contains a single query
            and a specification for how to execute that query. The specifications
            are optional but the query is required.
        """
        sandbox_zips_with_whitespace = {
            cdr_consts.QUERY:
                SANDBOX_ZIPS_WITH_WHITESPACE.render(
                    project_id=self.project_id,
                    sandbox_dataset_id=self.sandbox_dataset_id,
                    whitespace_sandbox=ZIPS_WITH_WHITESPACE_SANDBOX,
                    dataset_id=self.dataset_id,
                    obs_table=OBSERVATION)
        }

        sandbox_invalid_zip_codes = {
            cdr_consts.QUERY:
                SANDBOX_INVALID_ZIP_CODES.render(
                    project_id=self.project_id,
                    sandbox_dataset_id=self.sandbox_dataset_id,
                    sandbox_table=self.sandbox_table_for(OBSERVATION),
                    dataset_id=self.dataset_id,
                    obs_table=OBSERVATION,
                    pipeline_tables=PIPELINE_TABLES,
                    zip3_lookup=ZIP3_LOOKUP)
        }

        clean_zips_of_whitespace = {
            cdr_consts.QUERY:
                CLEAN_ZIPS_OF_WHITESPACE.render(
                    project_id=self.project_id,
                    dataset_id=self.dataset_id,
                    obs_table=OBSERVATION,
                    sandbox_dataset_id=self.sandbox_dataset_id,
                    whitespace_sandbox=ZIPS_WITH_WHITESPACE_SANDBOX)
        }

        update_zip_codes = {
            cdr_consts.QUERY:
                UPDATE_INVALID_ZIP_CODES.render(
                    project_id=self.project_id,
                    dataset_id=self.dataset_id,
                    obs_table=OBSERVATION,
                    sandbox_dataset_id=self.sandbox_dataset_id,
                    sandbox_table=self.sandbox_table_for(OBSERVATION))
        }

        return [
            sandbox_zips_with_whitespace, clean_zips_of_whitespace,
            sandbox_invalid_zip_codes, update_zip_codes
        ]

    def setup_rule(self, client, *args, **keyword_args):
        """
        Function to run any data upload options before executing a query.
        """
        pass

    def get_sandbox_tablenames(self):
        return [
            ZIPS_WITH_WHITESPACE_SANDBOX,
            self.sandbox_table_for(OBSERVATION)
        ]

    def setup_validation(self, client, *args, **keyword_args):
        """
        Run required steps for validation setup
        """
        raise NotImplementedError("Please fix me.")

    def validate_rule(self, client, *args, **keyword_args):
        """
        Validates the cleaning rule which deletes or updates the data from the tables
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
                                                 [(UpdateInvalidZipCodes,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(UpdateInvalidZipCodes,)])
