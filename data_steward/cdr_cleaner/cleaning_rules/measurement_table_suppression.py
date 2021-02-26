"""
Removing irrelevant observation records from the RDR dataset.

Original Issue:  DC-481, 699

The intent is to clean data in the measurement table. The rule intends to
reset invalid fields values to null, drop records that do not provide
meaningful data, and drop duplicate records.
"""
# Python Imports
import logging

# Project imports
from common import MEASUREMENT, JINJA_ENV
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule
from constants.bq_utils import WRITE_TRUNCATE
from constants.cdr_cleaner import clean_cdr as cdr_consts

LOGGER = logging.getLogger(__name__)

ISSUE_NUMBERS = ['DC-699', 'DC-481']

INVALID_VALUES_RECORDS = 'dc699_save_9999999_as_null'
SITES_WITH_ONLY_BAD_DATA = 'dc699_sites_with_only_null_or_zero_meas_data'
SAVE_BAD_SITE_DATA = 'dc699_save_bad_site_data'
SAVE_NULL_VALUE_RECORDS = 'dc699_save_null_records_from_measurement'
SAVE_DUPLICATE_RECORDS = 'dc699_save_measurement_duplicates'

# Save rows that will be altered to a sandbox dataset.
NULL_VALUES_SAVE_QUERY = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project}}.{{sandbox}}.{{save_table}}` AS (
SELECT *
FROM `{{project}}.{{dataset}}.measurement`
WHERE value_as_number = 9999999
)""")

# Alter rows by changing 9999999 to NULL
NULL_VALUES_UPDATE_QUERY = JINJA_ENV.from_string("""
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

# Identify sites who submitted "junk" data.  Either all nulls or zero values in
# the value_as_number field.  These sites should be saved to a table to make
# programmatic access easier
SITES_TO_REMOVE_DATA_FOR = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project}}.{{sandbox}}.{{save_table}}` AS (
-- join the measurment and mapping table and only store EHR site records --
WITH joined_table AS (
SELECT *
FROM `{{project}}.{{dataset}}.measurement` AS m
JOIN `{{project}}.{{dataset}}.measurement_ext` AS me
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
WHERE js.src_id NOT IN (SELECT src_id FROM values_containing_srcs)
)""")

# store data from sites that will be dropped.
NULL_AND_ZERO_VALUES_SAVE_QUERY = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project}}.{{sandbox}}.{{save_table}}` AS (
SELECT *
FROM `{{project}}.{{dataset}}.measurement` AS m
JOIN `{{project}}.{{dataset}}.measurement_ext` AS me
USING (measurement_id)
WHERE me.src_id IN (SELECT src_id FROM `{{project}}.{{sandbox}}.{{id_table}}`)
AND m.value_as_number = 0
)""")

# Update value_as_number for any site that has only submitted junk, i.e. 0 or null
# for value_as_number
SET_NULL_WHEN_ONLY_ZEROS_SUBMITTED = JINJA_ENV.from_string("""
SELECT
  measurement_id,
  person_id,
  measurement_concept_id,
  measurement_date,
  measurement_datetime,
  measurement_type_concept_id,
  operator_concept_id,
  value_as_concept_id,
  unit_concept_id,
  range_low,
  range_high,
  provider_id,
  visit_occurrence_id,
  measurement_source_value,
  measurement_source_concept_id,
  unit_source_value,
  value_source_value,
  CASE
    WHEN value_as_number = 0 AND me.src_id IN (SELECT src_id FROM `{{project}}.{{sandbox}}.{{id_table}}`) THEN NULL
  ELSE
  value_as_number
END
  AS value_as_number
FROM `{{project}}.{{dataset}}.measurement` AS m
JOIN `{{project}}.{{dataset}}.measurement_ext` AS me
USING (measurement_id)
""")

# Save records that will be dropped when
# value_as_number IS NULL AND value_as_concept_id IS NULL
SAVE_NULL_DROP_RECORDS = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project}}.{{sandbox}}.{{save_table}}` AS (
SELECT *
FROM `{{project}}.{{dataset}}.measurement` AS m
WHERE m.value_as_number IS NULL AND m.value_as_concept_id IS NULL
)""")

# Only select records that we want to keep
SELECT_RECORDS_WITH_VALID_DATA = JINJA_ENV.from_string("""
SELECT *
FROM `{{project}}.{{dataset}}.measurement` AS m
WHERE m.value_as_number IS NOT NULL OR m.value_as_concept_id IS NOT NULL
""")

# Sandbox duplicate records based on the fields:  person_id,
# measurement_source_concept_id, unit_concept_id, measurement_concept_id,
# measurement_datetime, value_as_number, value_as_concept_id
# Had to use grouping because ROW_NUMBER OVER cannot partition by value_as_number
SANDBOX_DUPLICATES = JINJA_ENV.from_string("""
-- identify duplicates with this context table statement --
-- only add duplicate field identifiers to this statement --
CREATE OR REPLACE TABLE `{{project}}.{{sandbox}}.{{save_table}}` AS (
WITH
  cte AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY person_id, measurement_source_concept_id, unit_concept_id, measurement_concept_id, measurement_datetime, CAST(value_as_number AS string),
      value_as_concept_id
    ORDER BY
      person_id,
      measurement_source_concept_id,
      unit_concept_id,
      measurement_concept_id,
      measurement_datetime,
      value_as_number,
      value_as_concept_id,
      measurement_id) AS row_num
  FROM
    `{{project}}.{{dataset}}.measurement`
)

-- select all fields from the table for sandboxing --
SELECT *
FROM
  cte
WHERE row_num > 1
)""")

REMOVE_DUPLICATES = JINJA_ENV.from_string("""
-- Select only the records that have not been sandboxed --
SELECT *
FROM `{{project}}.{{dataset}}.measurement`
WHERE measurement_id NOT IN
  (SELECT measurement_id FROM `{{project}}.{{sandbox}}.{{id_table}}`)
""")


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
        desc = (f'Clean the measurement table after it was de-identified.  '
                f'Remove rows that do not contribute high quality data.')
        super().__init__(issue_numbers=ISSUE_NUMBERS,
                         description=desc,
                         affected_datasets=[
                             cdr_consts.DEID_BASE,
                             cdr_consts.CONTROLLED_TIER_DEID_CLEAN
                         ],
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
        save_null_values = {
            cdr_consts.QUERY:
                NULL_VALUES_SAVE_QUERY.render(
                    project=self.project_id,
                    dataset=self.dataset_id,
                    sandbox=self.sandbox_dataset_id,
                    save_table=INVALID_VALUES_RECORDS),
        }

        update_to_null_values = {
            cdr_consts.QUERY:
                NULL_VALUES_UPDATE_QUERY.render(project=self.project_id,
                                                dataset=self.dataset_id),
            cdr_consts.DESTINATION_TABLE:
                MEASUREMENT,
            cdr_consts.DESTINATION_DATASET:
                self.dataset_id,
            cdr_consts.DISPOSITION:
                WRITE_TRUNCATE
        }

        identify_bad_sites = {
            cdr_consts.QUERY:
                SITES_TO_REMOVE_DATA_FOR.render(
                    project=self.project_id,
                    dataset=self.dataset_id,
                    sandbox=self.sandbox_dataset_id,
                    save_table=SITES_WITH_ONLY_BAD_DATA)
        }

        save_data_from_bad_sites = {
            cdr_consts.QUERY:
                NULL_AND_ZERO_VALUES_SAVE_QUERY.render(
                    project=self.project_id,
                    dataset=self.dataset_id,
                    sandbox=self.sandbox_dataset_id,
                    save_table=SAVE_BAD_SITE_DATA,
                    id_table=SITES_WITH_ONLY_BAD_DATA)
        }

        set_null_for_zero_from_bad_sites = {
            cdr_consts.QUERY:
                SET_NULL_WHEN_ONLY_ZEROS_SUBMITTED.render(
                    project=self.project_id,
                    dataset=self.dataset_id,
                    sandbox=self.sandbox_dataset_id,
                    id_table=SITES_WITH_ONLY_BAD_DATA)
        }

        save_null_records_before_dropping = {
            cdr_consts.QUERY:
                SAVE_NULL_DROP_RECORDS.render(
                    project=self.project_id,
                    dataset=self.dataset_id,
                    sandbox=self.sandbox_dataset_id,
                    save_table=SAVE_NULL_VALUE_RECORDS)
        }

        keep_records_with_good_data = {
            cdr_consts.QUERY:
                SELECT_RECORDS_WITH_VALID_DATA.render(project=self.project_id,
                                                      dataset=self.dataset_id),
            cdr_consts.DESTINATION_TABLE:
                MEASUREMENT,
            cdr_consts.DESTINATION_DATASET:
                self.dataset_id,
            cdr_consts.DISPOSITION:
                WRITE_TRUNCATE
        }

        sandbox_duplicates = {
            cdr_consts.QUERY:
                SANDBOX_DUPLICATES.render(project=self.project_id,
                                          dataset=self.dataset_id,
                                          sandbox=self.sandbox_dataset_id,
                                          save_table=SAVE_DUPLICATE_RECORDS)
        }
        remove_duplicates = {
            cdr_consts.QUERY:
                REMOVE_DUPLICATES.render(project=self.project_id,
                                         dataset=self.dataset_id,
                                         sandbox=self.sandbox_dataset_id,
                                         id_table=SAVE_DUPLICATE_RECORDS),
            cdr_consts.DESTINATION_TABLE:
                MEASUREMENT,
            cdr_consts.DESTINATION_DATASET:
                self.dataset_id,
            cdr_consts.DISPOSITION:
                WRITE_TRUNCATE
        }

        return [
            save_null_values, update_to_null_values, identify_bad_sites,
            save_data_from_bad_sites, set_null_for_zero_from_bad_sites,
            save_null_records_before_dropping, keep_records_with_good_data,
            sandbox_duplicates, remove_duplicates
        ]

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
        return [
            INVALID_VALUES_RECORDS, SITES_WITH_ONLY_BAD_DATA,
            SAVE_BAD_SITE_DATA, SAVE_NULL_VALUE_RECORDS, SAVE_DUPLICATE_RECORDS
        ]


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.parse_args()

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(
            ARGS.project_id, ARGS.dataset_id, ARGS.sandbox_dataset_id,
            [(MeasurementRecordsSuppression,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(MeasurementRecordsSuppression,)])
