"""
Removing irrelevant observation records from the RDR dataset.

Original Issue:  DC-529

The intent is to remove PPI records from the observation table in the RDR
export where observation_source_concept_id in (43530490, 43528818, 43530333).
The records for removal should be archived in the dataset sandbox.

Subsequent Issue: DC-702

Include the observation_source_concept_id 903079 to the list

"""
# Python Imports
import logging

# Third party imports
from jinja2 import Template

# Project imports
from common import OBSERVATION
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule
from constants.bq_utils import WRITE_TRUNCATE
from constants.cdr_cleaner import clean_cdr as cdr_consts

LOGGER = logging.getLogger(__name__)

OBS_SRC_CONCEPTS = '43530490,43528818,43530333,903079'

ISSUE_NUMBERS = ['DC-529', 'DC-520', 'DC-702', 'DC-1619']

# Save rows that will be dropped to a sandboxed dataset.
DROP_SELECTION_QUERY = """
CREATE OR REPLACE TABLE `{{project}}.{{sandbox}}.{{drop_table}}` AS
SELECT *
FROM `{{project}}.{{dataset}}.observation`
WHERE observation_source_concept_id IN ({{obs_concepts}})
"""

DROP_SELECTION_QUERY_TMPL = Template(DROP_SELECTION_QUERY)

DROP_QUERY = """
DELETE FROM `{{project}}.{{dataset}}.observation` AS o
WHERE observation_source_concept_id IN ({{obs_concepts}})
"""

DROP_QUERY_TMPL = Template(DROP_QUERY)


class ObservationSourceConceptIDRowSuppression(BaseCleaningRule):
    """
    Suppress rows by values in the observation_source_concept_id field.
    """

    def __init__(self, project_id, dataset_id, sandbox_dataset_id):
        """
        Initialize the class with proper info.

        Set the issue numbers, description and affected datasets.  As other
        tickets may affect this SQL, append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """
        desc = (f'Remove records from the rdr dataset where '
                f'observation_source_concept_id in ({OBS_SRC_CONCEPTS})')
        super().__init__(issue_numbers=ISSUE_NUMBERS,
                         description=desc,
                         affected_datasets=[cdr_consts.RDR],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         affected_tables=[OBSERVATION],
                         run_for_synthetic=True)

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
        return [self.sandbox_table_for(table) for table in self.affected_tables]


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.parse_args()

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(
            ARGS.project_id, ARGS.dataset_id, ARGS.sandbox_dataset_id,
            [(ObservationSourceConceptIDRowSuppression,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(
            ARGS.project_id, ARGS.dataset_id, ARGS.sandbox_dataset_id,
            [(ObservationSourceConceptIDRowSuppression,)])
