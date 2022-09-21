"""
sandbox and drop participants from the person table if they do not have records in any of the
AoU required OMOP tables except person.

required tables:
condition_occurrence, death, device_exposure, drug_exposure, measurement, note,
observation, procedure_occurrence, specimen, visit_occurrence, visit_detail

"""
import logging

# Project imports
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule
from common import JINJA_ENV, PERSON
from constants.cdr_cleaner import clean_cdr as cdr_consts
from utils import pipeline_logging

LOGGER = logging.getLogger(__name__)

ISSUE_NUMBERS = ['DC-2481']

SANDBOX_ORPHANED_PIDS = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE
  `{{project}}.{{sandbox_dataset}}.{{intermediary_table}}` AS
SELECT
  DISTINCT person_id
FROM
  `{{project}}.{{dataset}}.{{person_table}}` p
LEFT JOIN (SELECT DISTINCT person_id FROM `{{project}}.{{dataset}}.condition_occurrence`) co
USING (person_id)
LEFT JOIN (SELECT DISTINCT person_id FROM `{{project}}.{{dataset}}.death`) d
USING (person_id)
LEFT JOIN (SELECT DISTINCT person_id FROM `{{project}}.{{dataset}}.device_exposure`) dee
USING (person_id)
LEFT JOIN (SELECT DISTINCT person_id FROM `{{project}}.{{dataset}}.drug_exposure`) dre
USING (person_id)
LEFT JOIN (SELECT DISTINCT person_id FROM `{{project}}.{{dataset}}.measurement`) m
USING (person_id)
LEFT JOIN (SELECT DISTINCT person_id FROM `{{project}}.{{dataset}}.note`) n
USING (person_id)
LEFT JOIN (SELECT DISTINCT person_id FROM `{{project}}.{{dataset}}.observation`) o
USING (person_id)
LEFT JOIN (SELECT DISTINCT person_id FROM `{{project}}.{{dataset}}.procedure_occurrence`) po
USING (person_id)
LEFT JOIN (SELECT DISTINCT person_id FROM `{{project}}.{{dataset}}.specimen`) s
USING (person_id)
LEFT JOIN (SELECT DISTINCT person_id FROM `{{project}}.{{dataset}}.visit_occurrence`) vo
USING (person_id)
LEFT JOIN (SELECT DISTINCT person_id FROM `{{project}}.{{dataset}}.visit_detail`) vd
USING (person_id)
where co.person_id is NULL AND d.person_id is NULL AND dee.person_id is NULL AND dre.person_id is NULL 
AND m.person_id is NULL AND n.person_id is NULL AND o.person_id is NULL AND po.person_id is NULL
AND s.person_id is NULL AND vo.person_id is NULL AND vd.person_id is NULL
""")

REMOVE_ORPHANED_PIDS = JINJA_ENV.from_string("""
DELETE FROM
  `{{project}}.{{dataset}}.{{person_table}}`
WHERE
person_id
IN ( SELECT
    person_id
    FROM `{{project}}.{{sandbox_dataset}}.{{intermediary_table}}` )
""")


class DropOrphanedPIDS(BaseCleaningRule):
    """
    sandbox and drop participants from the person table if they do not have records in any of the
    AoU required OMOP tables except person
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
            'Removes all pids from person table if they do not have records in any of the'
            'AoU required OMOP tables.')
        super().__init__(issue_numbers=ISSUE_NUMBERS,
                         description=desc,
                         affected_datasets=[
                             cdr_consts.COMBINED,
                             cdr_consts.CONTROLLED_TIER_DEID,
                             cdr_consts.REGISTERED_TIER_DEID
                         ],
                         affected_tables=[PERSON],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         table_namer=table_namer,
                         run_for_synthetic=True)

    def get_query_specs(self):
        """
        Return a list of dictionary query specifications.

        :return:  A list of dictionaries. Each dictionary contains a single query
            and a specification for how to execute that query. The specifications
            are optional but the query is required.
        """

        sandbox_orphaned_rows = {
            cdr_consts.QUERY:
                SANDBOX_ORPHANED_PIDS.render(
                    project=self.project_id,
                    dataset=self.dataset_id,
                    sandbox_dataset=self.sandbox_dataset_id,
                    person_table=PERSON,
                    intermediary_table=self.get_sandbox_tablenames()[0])
        }

        delete_orphaned_rows = {
            cdr_consts.QUERY:
                REMOVE_ORPHANED_PIDS.render(
                    project=self.project_id,
                    dataset=self.dataset_id,
                    sandbox_dataset=self.sandbox_dataset_id,
                    person_table=PERSON,
                    intermediary_table=self.get_sandbox_tablenames()[0])
        }

        return [sandbox_orphaned_rows, delete_orphaned_rows]

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
        sandbox_table = self.sandbox_table_for(self.affected_tables[0])
        return [sandbox_table]


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    pipeline_logging.configure()
    ARGS = parser.parse_args()

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(ARGS.project_id,
                                                 ARGS.dataset_id,
                                                 ARGS.sandbox_dataset_id,
                                                 [(DropOrphanedPIDS,)],
                                                 ARGS.table_namer)
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(DropOrphanedPIDS,)], ARGS.table_namer)
