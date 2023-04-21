"""
Removes data containing death_dates which fall outside of the AoU program dates or after the current date

Original Issues: DC-431, DC-822

The intent is to ensure there are no death dates that occur before the start of the AoU program or after the current
date. A death date is considered "valid" if it is after the program start date and before the current date. Allowing for
 more flexibility, we chose Jan 1, 2017 as the program start date.
"""

# Python imports
import logging

# Project imports
from common import AOU_DEATH, DEATH, JINJA_ENV
from constants.bq_utils import WRITE_TRUNCATE
from constants.cdr_cleaner import clean_cdr as cdr_consts
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule

LOGGER = logging.getLogger(__name__)

program_start_date = '2017-01-01'
current_date = 'CURRENT_DATE()'

# Keeps any rows where the death_date is after the AoU program start or before the current date by comparing person_ids
# of the death table and sandbox tables. If the person_id is not in the sandbox table the row is kept, else, dropped.
KEEP_VALID_DEATH_DATE_ROWS = JINJA_ENV.from_string("""
SELECT * FROM `{{project_id}}.{{dataset_id}}.{{table}}` 
{% if table == 'death' %}
WHERE person_id NOT IN (SELECT person_id 
{% elif table == 'aou_death' %}
WHERE aou_death_id NOT IN (SELECT aou_death_id 
{% endif %}
FROM `{{project_id}}.{{sandbox_id}}.{{sandbox_table}}`)
""")

# Selects all the invalid rows. Invalid means the death_date occurs before the AoU program start
# or after the current date.
SANDBOX_INVALID_DEATH_DATE_ROWS = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project_id}}.{{sandbox_id}}.{{sandbox_table}}` AS (
SELECT d.*
FROM `{{project_id}}.{{dataset_id}}.{{table}}` d
-- Find the latest PPI observation for each person --
LEFT JOIN (
        SELECT
            person_id, MAX(o.observation_date) last_ppi_date
        FROM `{{project_id}}.{{dataset_id}}.observation` o
        JOIN `{{project_id}}.{{dataset_id}}.concept` c
            ON c.concept_id =  o.observation_source_concept_id
        WHERE c.vocabulary_id = 'PPI'
        GROUP BY person_id
) last_ppi_date
    ON last_ppi_date.person_id = d.person_id
WHERE death_date < '{{program_start_date}}' OR death_date > {{current_date}}
    -- Sandbox death record if it is >=1 day before latest PPI observation or no PPI observation exists --
    OR (
        last_ppi_date.person_id IS NULL 
        OR DATE_DIFF(last_ppi_date.last_ppi_date, d.death_date, DAY) >= 1
    ) 
)
""")


class ValidDeathDates(BaseCleaningRule):
    """
    Any row with a death_date that occurs before the start of the AoU program (Jan 1, 2017 for simplicity) or after
    the current date should be sandboxed and dropped from the death table.
    """

    def __init__(self, project_id, dataset_id, sandbox_dataset_id):
        """
        Initialize the class with proper information.

        Set the issue numbers, description and affected datasets. As other tickets may affect
        this SQL, append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """
        desc = 'All rows in the death and aou_death tables that contain a death_date which occurs before the start of the AoU ' \
               'program (Jan 1, 2017) or after the current date will be sandboxed and dropped.' \
               'Valid Death dates needs to be applied before no data after death as running no data after death is' \
               ' wiping out the needed consent related data for cleaning'
        super().__init__(issue_numbers=['DC822'],
                         description=desc,
                         affected_datasets=[cdr_consts.COMBINED],
                         affected_tables=[AOU_DEATH, DEATH],
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
        keep_valid_death_dates = [{
            cdr_consts.QUERY:
                KEEP_VALID_DEATH_DATE_ROWS.render(
                    project_id=self.project_id,
                    dataset_id=self.dataset_id,
                    table=table,
                    sandbox_id=self.sandbox_dataset_id,
                    sandbox_table=self.sandbox_table_for(table)),
            cdr_consts.DESTINATION_TABLE:
                table,
            cdr_consts.DESTINATION_DATASET:
                self.dataset_id,
            cdr_consts.DISPOSITION:
                WRITE_TRUNCATE
        } for table in self.affected_tables]

        sandbox_invalid_death_dates = [{
            cdr_consts.QUERY:
                SANDBOX_INVALID_DEATH_DATE_ROWS.render(
                    project_id=self.project_id,
                    sandbox_id=self.sandbox_dataset_id,
                    sandbox_table=self.sandbox_table_for(table),
                    dataset_id=self.dataset_id,
                    table=table,
                    program_start_date=program_start_date,
                    current_date=current_date)
        } for table in self.affected_tables]

        return sandbox_invalid_death_dates + keep_valid_death_dates

    def setup_rule(self, client, *args, **keyword_args):
        """
        Function to run any data upload options before executing a query.
        """
        pass

    def get_sandbox_tablenames(self):
        pass

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

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(ARGS.project_id,
                                                 ARGS.dataset_id,
                                                 ARGS.sandbox_dataset_id,
                                                 [(ValidDeathDates,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(ValidDeathDates,)])
