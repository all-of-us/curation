"""
deidentify 'device_id' in RT and CT

Original Issue: DC-3262

The new fitbit device table contains a device_id field that has a privacy requirement associated
with it. Every person_id/device_id pair should be given a unique id that will be stable across CDR
versions.

A lookup table will have been created in pipeline_tables. DC-3262: Create the `
pipeline_tables.wearables_device_id_masking` table

A cleaning rule will have been created to keep the lookup table updated at each run of the CDR.
DC-3229: Create a CR to update 'device_id_mapping' in pipeline_tables

"""
import os
import logging

from common import PIPELINE_TABLES, JINJA_ENV
import constants.cdr_cleaner.clean_cdr as cdr_consts

from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule

LOGGER = logging.getLogger(__name__)

DEVICE_ID = 'device_id'

DEID_FITBIT_DEVICE_ID = JINJA_ENV.from_string("""
UPDATE `{{project_id}}.{{dataset_id}}.device` AS d
SET d.{{device_id}} = wdim.research_device_id
FROM `{{project_id}}.{{pipeline_tables}}.wearables_device_id_masking` AS wdim
WHERE d.person_id = wdim.person_id
AND d.{{device_id}} = wdim.{{device_id}}
AND wearable_type = 'fitbit'
""")

ISSUE_NUMBERS = ['DC-3254']


class DeidFitbitDeviceId(BaseCleaningRule):
    """
    Every person_id/device_id pair should be given a unique ID
    """

    def __init__(self, project_id, dataset_id, sandbox_dataset_id):
        """
        Initialize the class with proper info.

        Set the issue numbers, description and affected datasets.  As other
        tickets may affect this SQL, append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """

        desc = """Updates reasearch_device_id with the device_id in the device table."""

        super().__init__(description=desc,
                         issue_numbers=ISSUE_NUMBERS,
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         affected_datasets=[
                             cdr_consts.REGISTERED_TIER_DEID,
                             cdr_consts.CONTROLLED_TIER_DEID,
                         ])

    def setup_rule(self, client):
        """
        Function to run any data upload options before executing a query.

        Should also be used to setup the class with any calls required to
        instantiate the class properly.
        """
        pass

    def get_sandbox_tablenames(self):
        pass

    def get_query_specs(self):

        return [{
            cdr_consts.QUERY:
                DEID_FITBIT_DEVICE_ID.render(
                    project_id=self.project_id,
                    dataset_id=self.dataset_id,
                    pipeline_tables=PIPELINE_TABLES,
                    device_id=DEVICE_ID,
                ),
        }]

    def setup_validation(self, client):
        """
        Run required steps for validation setup

        This abstract method was added to the base class after this rule was authored.
        This rule needs to implement logic to setup validation on cleaning rules that
        will be updating or deleting the values.
        Until done no issue exists for this yet.
        """
        pass

    def validate_rule(self, client):
        """
        Validates the cleaning rule which deletes or updates the data from the tables

        This abstract method was added to the base class after this rule was authored.
        This rule needs to implement logic to run validation on cleaning rules that will
        be updating or deleting the values.
        Until done no issue exists for this yet.
        """
        pass

    def run_rule(self):
        pass


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.default_parse_args()

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(ARGS.project_id,
                                                 ARGS.dataset_id,
                                                 ARGS.sandbox_dataset_id,
                                                 [(DeidFitbitDeviceId,)])

        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(DeidFitbitDeviceId,)])
