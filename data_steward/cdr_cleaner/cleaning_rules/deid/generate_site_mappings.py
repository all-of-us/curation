"""
Rule to create non-deterministic site ids for ehr sites.
"""
# Python Imports
import logging
import random

# Project imports
import bq_utils
from constants.bq_utils import LOOKUP_TABLES_DATASET_ID, HPO_SITE_ID_MAPPINGS_TABLE_ID
from constants.cdr_cleaner import clean_cdr as cdr_consts
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule
from common import JINJA_ENV

LOGGER = logging.getLogger(__name__)

ISSUE_NUMBERS = ['DC-1351']

SITE_TABLE_ID = '_site_mappings'
EHR_SITE_PREFIX = 'EHR site '
RDR = 'rdr'
PPI_PM = 'PPI/PM'

SITE_MAPPINGS_QUERY = JINJA_ENV.from_string("""
CREATE TABLE IF NOT EXISTS
  `{{project_id}}.{{dataset_id}}.{{table_id}}` (hpo_id STRING OPTIONS(description="The hpo_id of the hpo site/RDR."),
    src_id STRING OPTIONS(description="The masked id of the hpo site/RDR.")) AS (
    WITH random_ids AS
(
  SELECT
    CONCAT({{site_prefix}}, new_id) AS hpo_id,
    ROW_NUMBER() OVER(ORDER BY GENERATE_UUID()) AS row_id
  FROM
    UNNEST(GENERATE_ARRAY(100, 999)) AS new_id
)
SELECT
  r.hpo_id,
  LOWER(m.hpo_id) as src_id
FROM
(
  SELECT
    m.*,
    ROW_NUMBER() OVER(ORDER BY GENERATE_UUID()) AS row_id
  FROM `{{project_id}}.{{lookup_tabels_dataset}}.{{hpo_site_id_mappings_table}}` AS m
  WHERE m.HPO_ID IS NOT NULL AND m.HPO_ID != ''
) AS m
JOIN random_ids AS r
USING (row_id)
union all
select {{ppi_pm}} as hpo_id, {{rdr}} as src_id
)
""")


class GenerateSiteMappings(BaseCleaningRule):
    """
    Generates non-deterministic site_ids for hpo sites.
    """

    def __init__(self, project_id, dataset_id, sandbox_dataset_id):
        """
        Initialize the class with proper info.

        Set the issue numbers, description and affected datasets.  As other
        tickets may affect this SQL, append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """
        desc = f'Change PIDs to RIDs in specified tables'
        super().__init__(issue_numbers=ISSUE_NUMBERS,
                         description=desc,
                         affected_datasets=[cdr_consts.CONTROLLED_TIER_DEID],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         affected_tables=[])

    def get_query_specs(self):
        """
        Return a list of dictionary query specifications.

        :return:  A list of dictionaries.  Each dictionary contains a
            single query and a specification for how to execute that query.
            The specifications are optional but the query is required.
        """
        query = dict()
        query[cdr_consts.QUERY] = SITE_MAPPINGS_QUERY.render(
            project_id=self.project_id,
            dataset_id=self.sandbox_dataset_id,
            table_id=SITE_TABLE_ID,
            site_prefix=EHR_SITE_PREFIX,
            lookup_tabels_dataset=LOOKUP_TABLES_DATASET_ID,
            hpo_site_id_mappings_table=HPO_SITE_ID_MAPPINGS_TABLE_ID,
            ppi_pm=PPI_PM,
            rdr=RDR)
        return [query]

    def get_sandbox_tablenames(self):
        return []

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

    ARGS = parser.default_parse_args()

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(ARGS.project_id,
                                                 ARGS.dataset_id,
                                                 ARGS.sandbox_dataset_id,
                                                 [(GenerateSiteMappings,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(GenerateSiteMappings,)])
