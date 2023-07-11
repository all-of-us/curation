"""
Generic clean up rule to ensure each mapping table contains only the records for
domain tables existing after the dataset has been fully cleaned.

Original Issue: DC-715

The intent is to ensure the mapping table continues to represent a true record of the
cleaned domain table by sandboxing the mapping table records and rows dropped
when the records of the row references have been dropped by a cleaning rule.
"""

import logging

from common import PIPELINE_TABLES, FITBIT_TABLES, JINJA_ENV
from gcloud.bq import BigQueryClient
import constants.cdr_cleaner.clean_cdr as cdr_consts

from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule

LOGGER = logging.getLogger(__name__)

DEVICE_ID = 'device_id'
DEVICE = 'device'

DEID_FITBIT_DEVICE_ID = JINJA_ENV.from_string("""
UPDATE `{{project_id}}.{{dataset_id}}.{{device}}` AS d
SET d.{{device_id}} = sub.research_device_id
FROM `{{project_id}}.{{pipeline_tables}}.wearables_device_id_masking` AS wdim
WHERE d.person_id = wdim.person_id
AND d.{{device_id}} = wdim.{{device_id}}
AND wearables_type = 'fitbit'
""")


ISSUE_NUMBERS = ['DC-3254'] #* LOCATE_


def get_mapping_tables():
    """
    Returns list of mapping tables in fields path

    Uses json table defintion files to identify mapping tables and create
    a list of extension tables.

    :returns: a list of mapping and extension tables based on mapping
        table names
    """
    return {table for table in PIPELINE_TABLES}


class DeidFitbitDeviceId(BaseCleaningRule):
    """
    Ensures each domain mapping table only contains records for domain tables
    that exist after the dataset has been fully cleaned.
    """

    def __init__(self,
                 project_id,
                 dataset_id):
        """
        Initialize the class with proper info.

        Set the issue numbers, description and affected datasets.  As other
        tickets may affect this SQL, append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """
        desc = ("""Every person_id/device_id pair should be given a unique id """
                """that will be stable across CDR versions""")

        self.project_id = project_id
        self.dataset_id = dataset_id

        super().__init__(description=desc,
                         issue_numbers=ISSUE_NUMBERS,
                         project_id=project_id,
                         dataset_id=dataset_id,
                         affected_datasets=(
                         cdr_consts.REGISTERED_TIER_DEID,
                         cdr_consts.CONTROLLED_TIER_DEID,
                         ))



        self.client = BigQueryClient(project_id=project_id)

    def setup_rule(self, client):
        """
        Function to run any data upload options before executing a query.

        Should also be used to setup the class with any calls required to
        instantiate the class properly.
        """
        raise NotImplementedError('Not Required.')

    def get_query_specs():

        return [{ cdr_consts.QUERY: DEID_FITBIT_DEVICE_ID.render(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            pipeline_tables=PIPELINE_TABLES,
            device=DEVICE,
            device_id=DEVICE_ID,),
            }]

    def setup_validation(self, client):
        """
        Run required steps for validation setup

        This abstract method was added to the base class after this rule was authored.
        This rule needs to implement logic to setup validation on cleaning rules that
        will be updating or deleting the values.
        Until done no issue exists for this yet.
        """
        raise NotImplementedError('Not Required.')

    def validate_rule(self, client):
        """
        Validates the cleaning rule which deletes or updates the data from the tables

        This abstract method was added to the base class after this rule was authored.
        This rule needs to implement logic to run validation on cleaning rules that will
        be updating or deleting the values.
        Until done no issue exists for this yet.
        """
        raise NotImplementedError('Not Required.')

    def run_rule(self):
        update_query = DEID_FITBIT.render(project_id=self.project_id,
                                          dataset_id=self.dataset_id,
                                          pipeline_tables=PIPELINE_TABLES)

        self.client.query(update_query)

        # 



if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.parse_args()

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(ARGS.project_id,
                                                 ARGS.dataset_id,
                                                 [(DeidFitbitDeviceId,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   [(DeidFitbitDeviceId,)])
