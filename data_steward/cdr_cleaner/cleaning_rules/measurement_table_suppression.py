"""
Removing irrelevant observation records from the RDR dataset.

Original Issue:  DC-481, 699

The intent is to clean data in the measurement table. The rule intends to
reset invalid fields values to null, drop records that do not provide
meaningful data, and drop duplicate records.
"""
# Python Imports
import logging

# Third party imports
from jinja2 import Environment

# Project imports
from common import MEASUREMENT
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule
from constants.bq_utils import WRITE_TRUNCATE
from constants.cdr_cleaner import clean_cdr as cdr_consts

LOGGER = logging.getLogger(__name__)

ISSUE_NUMBERS = ['DC-699', 'DC-481']
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

# Save rows that will be altered to a sandbox dataset.
NULL_VALUES_SAVE_QUERY = jinja_env.from_string("""
CREATE OR REPLACE TABLE `{{project}}.{{sandbox}}.{{save_table}}` AS
SELECT *
FROM `{{project}}.{{dataset}}.measurement`
WHERE value_as_number = 9999999
""")

# Alter rows by changing 9999999 to NULL
NULL_VALUES_UPDATE_QUERY = jinja_env.from_string("""
SELECT 
measurement_id, person_id, measurement_concept_id, measurement_date,
measurement_datetime, measurement_type_concept_id, operator_concept_id, 
CASE
WHEN value_as_number = 9999999 THEN NULL
ELSE value_as_number
END AS value_as_number, 
value_as_concept_id, unit_concept_id, range_low, range_high, provider_id, 
visit_occurrence_id, measurement_source_value, measurement_source_concept_id, 
unit_source_value, value_source_value 
from `{{project}}.{{dataset}}.measurement`
""")

# Identify sites who submitted "junk" data.  Only one value.  Likely 0.
SITES_TO_REMOVE_DATA_FOR = jinja_env.from_string("""
-- join the measurment and mapping table and only store EHR site records --
WITH joined_table AS (
SELECT *
FROM `{{project}}.{{dataset}}.measurement` AS m
JOIN `{{project}}.{{dataset}}..measurement_ext` AS me
USING (measurement_id)
WHERE src_id LIKE 'EHR site%'
),

-- get the src_id of sites having something greater than 0 in the value_as_number field --
values_containing_srcs AS(
SELECT DISTINCT(src_id)
FROM joined_table AS jt
GROUP BY src_id, value_as_number
HAVING value_as_number > 0
),

-- get the src_id of sites having either 0 or null in the value_as_number field --
junk_srcs AS (
SELECT DISTINCT(src_id)
FROM joined_table AS jt
GROUP BY src_id, value_as_number
HAVING value_as_number = 0 OR value_as_number IS NULL)

-- select those src_ids from junk_srcs that do not exist in value containing sources --
-- this means the site never submitted anything other than 0 or null in the --
-- value_as_number field --
SELECT js.src_id
FROM junk_srcs AS js
WHERE js.src_id NOT IN (SELECT src_id FROM values_containing_srcs)""")

# store data from sites that will be dropped.
ZERO_VALUES_SAVE_QUERY = jinja_env.from_string("""
CREATE OR REPLACE TABLE `{{project}}.{{sandbox}}.{{save_table}}` AS
SELECT *
FROM `{{project}}.{{dataset}}.measurement`
WHERE value_as_number = 9999999
""")

# Query uses 'NOT EXISTS' because the observation_source_concept_id field
# is nullable.
DROP_QUERY = """
SELECT * FROM `{{project}}.{{dataset}}.observation` AS o
WHERE NOT EXISTS (
    SELECT 1
    FROM `{{project}}.{{dataset}}.observation` AS n
    WHERE o.observation_id = n.observation_id AND
    n.observation_source_concept_id IN ({{obs_concepts}})
)"""


class MeasurementRecordsSuppression(BaseCleaningRule):
    """
    Suppress measurement rows by values.

    Suppress measurement rows if:
    1.  Convert value_as_number = 9999999 to value_as_number IS NULL,
    2.  value_as_number IS NULL AND value_as_concept_id IS NULL,
    3.  drop all measurement data from a site if value_as_number = 0 for all
    records submitted by the site.
    4.  Eliminate duplicate rows based on the fields:
        person_id, measurement_source_concept_id, unit_concept_id,
        measurement_concept_id, measurement_datetime, value_as_number,
        and value_as_concept_id.
    """

    def __init__(self, project_id, dataset_id, sandbox_dataset_id):
        """
        Initialize the class with proper info.

        Set the issue numbers, description and affected datasets.  As other
        tickets may affect this SQL, append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """
        desc = (f'Clean the measurement table after de-identifying it.  '
                f'Remove rows that do not contribute high quality data.')
        super().__init__(issue_numbers=ISSUE_NUMBERS,
                         description=desc,
                         affected_datasets=[cdr_consts.DEID_BASE],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         affected_tables=[MEASUREMENT])

    def get_query_specs(self):
        """
        Return a list of dictionary query specifications.

        :return:  A list of dictionaries.  Each dictionary contains a
            single query and a specification for how to execute that query.
            The specifications are optional but the query is required.
        """
        save_dropped_rows = {cdr_consts.QUERY: ''}

        drop_rows_query = {
            cdr_consts.QUERY: '',
            cdr_consts.DESTINATION_TABLE: MEASUREMENT,
            cdr_consts.DESTINATION_DATASET: self.dataset_id,
            cdr_consts.DISPOSITION: WRITE_TRUNCATE
        }

        return [save_dropped_rows, drop_rows_query]

    def setup_rule(self, client):
        """
        Function to run any data upload options before executing a query.
        """
        pass

    def setup_validation(self, client):
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

    def get_sandbox_tablenames(self):
        issue_numbers = self.issue_numbers
        primary_issue = issue_numbers[0].replace(
            '-', '_').lower() if issue_numbers else self.__class__.__name__

        sandbox_table_name = f"{primary_issue}_{MEASUREMENT}"
        return [sandbox_table_name]


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.parse_args()
    clean_engine.add_console_logging(ARGS.console_log)
    meas_cleaner = MeasurementRecordsSuppression(ARGS.project_id,
                                                 ARGS.dataset_id,
                                                 ARGS.sandbox_dataset_id)
    #    query_list = meas_cleaner.get_query_specs()
    #
    #    if ARGS.list_queries:
    #        meas_cleaner.log_queries()
    #    else:
    #        clean_engine.clean_dataset(ARGS.project_id, query_list)
    meas_cleaner.get_sandbox_tablenames()
