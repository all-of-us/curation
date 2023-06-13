"""
Generate research_device_ids for each fitbit person_id/device_id pair.

Original Issue: DC-3229

Device_id is required by privacy to be deidentified in RT and CT. This CR updates the mapping table with newly generated
 research_device_ids.
"""

# Python Imports
import logging

# Project Imports
from common import JINJA_ENV, PIPELINE_TABLES
from constants.cdr_cleaner.clean_cdr import QUERY, FITBIT
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule

LOGGER = logging.getLogger(__name__)

ISSUE_NUMBERS = ['DC3229']

# Capture new person_id/device_id pairs, generate research_device_ids and update the masking table
APPEND_MASKING_TABLE_QUERY = JINJA_ENV.from_string("""
INSERT INTO `{{project_id}}.{{pipeline_tables}}.wearables_device_id_masking` 
(person_id, device_id, research_device_id, wearable_type, import_date)
SELECT 
  d.person_id, 
  d.device_id,
  GENERATE_UUID() as research_device_id, 
  'fitbit' as wearable_type, 
  CURRENT_DATE() as import_date
FROM (SELECT DISTINCT person_id, device_id 
      FROM `{{project_id}}.{{fitbit_dataset}}.device`) d
LEFT JOIN `{{project_id}}.{{pipeline_tables}}.wearables_device_id_masking` wdim
ON wdim.person_id = d.person_id AND wdim.device_id = d.device_id
WHERE wdim.person_id IS NULL  AND wdim.device_id IS NULL
""")


class GenerateResearchDeviceIds(BaseCleaningRule):
    """
    Capture new person_id/device_id pairs, generate research_device_ids and update the masking table
    """

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
            f'New research_device_ids will be appended to the wearables_device_id_masking pipeline table.'
        )
        super().__init__(issue_numbers=ISSUE_NUMBERS,
                         description=desc,
                         affected_datasets=[FITBIT],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         table_namer=table_namer)

    def get_query_specs(self):
        """
        Return a list of dictionary query specifications.

        :return:  A list of dictionaries. Each dictionary contains a single query
            and a specification for how to execute that query. The specifications
            are optional but the query is required.
        """

        query_list = []

        append_query = {
            QUERY:
                APPEND_MASKING_TABLE_QUERY.render(
                    project_id=self.project_id,
                    fitbit_dataset=self.dataset_id,
                    pipeline_tables=PIPELINE_TABLES,
                )
        }
        query_list.append(append_query)

        return query_list

    def get_sandbox_tablenames(self):
        pass

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


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.parse_args()

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(
            ARGS.project_id,
            ARGS.dataset_id,
            ARGS.sandbox_dataset_id,
            [(GenerateResearchDeviceIds,)],
        )
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(
            ARGS.project_id,
            ARGS.dataset_id,
            ARGS.sandbox_dataset_id,
            [(GenerateResearchDeviceIds,)],
        )
