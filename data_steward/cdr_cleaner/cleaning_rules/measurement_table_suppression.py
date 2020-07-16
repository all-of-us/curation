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
-- group site input by the site src_id and the value_as_number --
-- only do this for sites, not PPI or PM --
WITH site_aggregates AS (
    SELECT 
    me.src_id,
    m.value_as_number
    FROM `{{project}}.{{dataset}}.measurement_ext` as me
    JOIN `{{project}}.{{dataset}}.measurement` as m
    USING (measurement_id)
    WHERE m.value_as_number IS NOT NULL
    AND me.src_id LIKE 'EHR site%'
    GROUP BY me.src_id, m.value_as_number
),

-- count the rows for each site src_id in site_aggregates --
-- more than one row in the aggregate indicates the site submitted more than --
-- one value type in value_as_number --
site_count_measurements AS (
    SELECT ms.src_id,
    COUNT(src_id) AS site_count
    FROM site_aggregates AS ms
    GROUP BY ms.src_id
)

-- select src_id from sites that have only one row in site_count_measurements --
-- this means the site only listed one type of value for value_as_number --
SELECT scm.src_id
FROM site_count_measurements AS scm
WHERE scm.site_count < 2""")

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
        save_dropped_rows = {
            cdr_consts.QUERY:
                DROP_SELECTION_QUERY_TMPL.render(
                    project=self.project_id,
                    dataset=self.dataset_id,
                    sandbox=self.sandbox_dataset_id,
                    drop_table=self.get_sandbox_tablenames()[0],
                    obs_concepts=OBS_SRC_CONCEPTS),
        }

        drop_rows_query = {
            cdr_consts.QUERY:
                DROP_QUERY_TMPL.render(project=self.project_id,
                                       dataset=self.dataset_id,
                                       obs_concepts=OBS_SRC_CONCEPTS),
            cdr_consts.DESTINATION_TABLE:
                OBSERVATION,
            cdr_consts.DESTINATION_DATASET:
                self.dataset_id,
            cdr_consts.DISPOSITION:
                WRITE_TRUNCATE
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
