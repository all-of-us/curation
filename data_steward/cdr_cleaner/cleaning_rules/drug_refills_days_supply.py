"""
Ensure drug refills < 10 and days_supply < 180. This rule is leveraging the Achilles rule 24 and 25.

Original Issues: DC-403, DC-815
"""
# Python imports
import logging

# Project imports
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule
from constants.cdr_cleaner import clean_cdr as cdr_consts
from common import DRUG_EXPOSURE, JINJA_ENV
from utils import pipeline_logging

LOGGER = logging.getLogger(__name__)

MAX_DAYS_SUPPLY = 180
MAX_REFILLS = 10

SANDBOX_QUERY = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project_id}}.{{sandbox_dataset_id}}.{{sandbox_table_id}}` AS (
    SELECT * FROM `{{project_id}}.{{dataset_id}}.drug_exposure`
    WHERE {{max_days_supply}} < days_supply
    OR {{max_refills}} < refills
)
""")

DELETE_QUERY = JINJA_ENV.from_string("""
DELETE FROM `{{project_id}}.{{dataset_id}}.drug_exposure` 
WHERE drug_exposure_id IN (
    SELECT drug_exposure_id 
    FROM `{{project_id}}.{{sandbox_dataset_id}}.{{sandbox_table_id}}`
)
""")


class DrugRefillsDaysSupply(BaseCleaningRule):

    def __init__(self,
                 project_id,
                 dataset_id,
                 sandbox_dataset_id,
                 table_namer=None):
        """
        Initialize the class with proper information.
        """
        desc = ('Ensure drug refills < 10 and days_supply < 180.')
        super().__init__(issue_numbers=['DC403', 'DC815'],
                         description=desc,
                         affected_datasets=[cdr_consts.COMBINED],
                         affected_tables=DRUG_EXPOSURE,
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         table_namer=table_namer)

    def setup_rule(self, client):
        """
        Responsible for grabbing and storing deactivated participant data.
        :param client: a BiQueryClient passed to store the data
        """
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

    def get_sandbox_tablenames(self):
        return [
            self.sandbox_table_for(affected_table)
            for affected_table in self.affected_tables
        ]

    def get_query_specs(self):
        """
        Return a list of dictionary query specifications.
        :return:  A list of dictionaries. Each dictionary contains a single query
            and a specification for how to execute that query. The specifications
            are optional but the query is required.
        """
        sandbox_queries, delete_queries = [], []

        sandbox_queries.append({
            cdr_consts.QUERY:
                SANDBOX_QUERY.render(
                    project_id=self.project_id,
                    sandbox_dataset_id=self.sandbox_dataset_id,
                    sandbox_table_id=self.sandbox_table_for(DRUG_EXPOSURE),
                    dataset_id=self.dataset_id,
                    max_days_supply=MAX_DAYS_SUPPLY,
                    max_refills=MAX_REFILLS)
        })

        delete_queries.append({
            cdr_consts.QUERY:
                DELETE_QUERY.render(
                    project_id=self.project_id,
                    sandbox_dataset_id=self.sandbox_dataset_id,
                    sandbox_table_id=self.sandbox_table_for(DRUG_EXPOSURE),
                    dataset_id=self.dataset_id)
        })

        return sandbox_queries + delete_queries


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    pipeline_logging.configure(level=logging.DEBUG, add_console_handler=True)

    ARGS = parser.parse_args()

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(ARGS.project_id,
                                                 ARGS.dataset_id,
                                                 ARGS.sandbox_dataset_id,
                                                 [(DrugRefillsDaysSupply,)])

        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(DrugRefillsDaysSupply,)])
