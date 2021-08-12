"""
Ticket: DC-564
This cleaning rule is meant to run on RDR datasets
This rule updates old Questions and Answers with the corresponding new ones.
"""
import logging

import constants.cdr_cleaner.clean_cdr as cdr_consts
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule
from common import JINJA_ENV, OBSERVATION

LOGGER = logging.getLogger(__name__)

ISSUE_NUMBERS = ['DC564', 'DC844']

SANDBOX_QUERY = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project_id}}.{{sandbox_dataset_id}}.{{sandbox_tablename}}` AS (
SELECT *
FROM `{{project_id}}.{{dataset_id}}.observation`
WHERE observation_source_concept_id IN (43529632, 43529637, 43529636)
AND value_source_concept_id IN (43529091, 43529094, 702787)
)
""")

UPDATE_FAMILY_HISTORY_QUERY = JINJA_ENV.from_string("""
UPDATE `{{project_id}}.{{dataset_id}}.observation`
SET
observation_source_concept_id = CASE
    WHEN (observation_source_concept_id = 43529632 AND value_source_concept_id = 43529091) THEN 43529655
    WHEN (observation_source_concept_id = 43529637 AND value_source_concept_id = 43529094) THEN 43529660
    WHEN (observation_source_concept_id = 43529636 AND value_source_concept_id = 702787) THEN 43529659
END,
value_source_concept_id = CASE
    WHEN (observation_source_concept_id = 43529632 AND value_source_concept_id = 43529091) THEN 43529090
    WHEN (observation_source_concept_id = 43529637 AND value_source_concept_id = 43529094) THEN 43529093
    WHEN (observation_source_concept_id = 43529636 AND value_source_concept_id = 702787) THEN 43529088
END
WHERE observation_source_concept_id IN (43529632, 43529637, 43529636)
AND value_source_concept_id IN (43529091, 43529094, 702787)
""")


class UpdateFamilyHistoryCodes(BaseCleaningRule):
    """
    Update family history answer codes that are using known old answer codes.
    """

    def __init__(self,
                 project_id,
                 dataset_id,
                 sandbox_dataset_id,
                 table_namer=None):
        """
        Initialize the class with runtime and reporting information.

        Set the issue numbers, description and affected datasets. As other tickets may affect
        this SQL, append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """
        desc = (
            f'In the 2019 CDR, 3 questions in Family Medical History survey '
            f'were found to be using invalid answer codes.  This rule updates '
            f'the invalid answer codes to valid answer codes for only these '
            f'3 question codes.')

        super().__init__(issue_numbers=ISSUE_NUMBERS,
                         description=desc,
                         affected_datasets=[cdr_consts.RDR],
                         affected_tables=[OBSERVATION],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         table_namer=table_namer)
        print(f'init table namer is:  `{self.table_namer}`')

    def get_query_specs(self):
        """
        Create queries for updating family history questions and answers

        :return: list of query dicts
        """
        sandbox_query = {
            cdr_consts.QUERY:
                SANDBOX_QUERY.render(
                    project_id=self.project_id,
                    dataset_id=self.dataset_id,
                    sandbox_dataset_id=self.sandbox_dataset_id,
                    sandbox_tablename=self.sandbox_table_for(OBSERVATION))
        }

        print(f'table namer is:  `{self.table_namer}`')
        update_query = {
            cdr_consts.QUERY:
                UPDATE_FAMILY_HISTORY_QUERY.render(dataset_id=self.dataset_id,
                                                   project_id=self.project_id)
        }

        print(sandbox_query.get('query'))
        print('\n\n')
        print(update_query.get('query'))

        return [sandbox_query, update_query]

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
        return [self.sandbox_table_for(OBSERVATION)]


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine
    from utils import pipeline_logging

    ARGS = parser.parse_args()
    # only stream to console if running as the main script
    pipeline_logging.configure(add_console_handler=True)

    if ARGS.list_queries:
        query_list = clean_engine.get_query_list(ARGS.project_id,
                                                 ARGS.dataset_id,
                                                 ARGS.sandbox_dataset_id,
                                                 [(UpdateFamilyHistoryCodes,)],
                                                 table_namer='rdr')
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.clean_dataset(ARGS.project_id,
                                   ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(UpdateFamilyHistoryCodes,)],
                                   table_namer='rdr')
