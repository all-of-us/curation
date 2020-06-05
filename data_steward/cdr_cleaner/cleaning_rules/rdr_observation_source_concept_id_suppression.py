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

ISSUE_NUMBERS = ['DC-529', 'DC-520', 'DC-702']

# Save rows that will be dropped to a sandboxed dataset.
DROP_SELECTION_QUERY = """
CREATE OR REPLACE TABLE `{{project}}.{{sandbox}}.{{drop_table}}` AS
SELECT *
FROM `{{project}}.{{dataset}}.observation`
WHERE observation_source_concept_id IN ({{obs_concepts}})
"""

DROP_SELECTION_QUERY_TMPL = Template(DROP_SELECTION_QUERY)

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
                         affected_tables=[OBSERVATION])

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
                    project=self.get_project_id(),
                    dataset=self.get_dataset_id(),
                    sandbox=self.get_sandbox_dataset_id(),
                    drop_table=self.get_sandbox_tablenames()[0],
                    obs_concepts=OBS_SRC_CONCEPTS),
        }

        drop_rows_query = {
            cdr_consts.QUERY:
                DROP_QUERY_TMPL.render(project=self.get_project_id(),
                                       dataset=self.get_dataset_id(),
                                       obs_concepts=OBS_SRC_CONCEPTS),
            cdr_consts.DESTINATION_TABLE:
                OBSERVATION,
            cdr_consts.DESTINATION_DATASET:
                self.get_dataset_id(),
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
        issue_numbers = self.get_issue_numbers()
        primary_issue = issue_numbers[0].replace(
            '-', '_').lower() if issue_numbers else 'UNKNOWN_ISSUE'

        sandbox_table_name = f"{primary_issue}_{OBSERVATION}"
        return [sandbox_table_name]


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.parse_args()
    clean_engine.add_console_logging(ARGS.console_log)
    rdr_cleaner = ObservationSourceConceptIDRowSuppression(
        ARGS.project_id, ARGS.dataset_id, ARGS.sandbox_dataset_id)
    query_list = rdr_cleaner.get_query_specs()

    if ARGS.list_queries:
        rdr_cleaner.log_queries()
    else:
        clean_engine.clean_dataset(ARGS.project_id, query_list)
