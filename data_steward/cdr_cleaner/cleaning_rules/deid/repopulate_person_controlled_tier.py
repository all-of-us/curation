import logging

from common import JINJA_ENV, PERSON
import constants.cdr_cleaner.clean_cdr as cdr_consts
from cdr_cleaner.cleaning_rules.deid.repopulate_person_using_observation import \
    AbstractRepopulatePerson

LOGGER = logging.getLogger(__name__)

JIRA_ISSUE_NUMBERS = ['DC-1439']


class RepopulatePersonControlledTier(AbstractRepopulatePerson):

    def __init__(self, project_id, dataset_id, sandbox_dataset_id):
        """
        Initialize the class with proper information.

        Set the issue numbers, description and affected datasets. As other tickets may affect
        this SQL, append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """
        desc = (
            'Returns a parsed query to repopulate the person table using observation.'
        )

        super().__init__(
            issue_numbers=JIRA_ISSUE_NUMBERS,
            description=desc,
            affected_datasets=[cdr_consts.CONTROLLED_TIER_DEID_BASE],
            project_id=project_id,
            dataset_id=dataset_id,
            sandbox_dataset_id=sandbox_dataset_id,
            affected_tables=[PERSON])

    def setup_rule(self, client, *args, **keyword_args):
        pass

    def setup_validation(self, client, *args, **keyword_args):
        pass

    def get_gender_query(self, gender_sandbox_table) -> dict:
        pass

    def get_sex_at_birth_query(self, sex_at_birth_sandbox_table) -> dict:
        pass

    def get_race_query(self, race_sandbox_table) -> dict:
        pass

    def get_ethnicity_query(self, ethnicity_sandbox_table) -> dict:
        pass

    def get_birth_info_query(self, birth_info_sandbox_table) -> dict:
        pass

    def validate_rule(self, client):
        pass


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.parse_args()

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(
            ARGS.project_id, ARGS.dataset_id, ARGS.sandbox_dataset_id,
            [(RepopulatePersonControlledTier,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(RepopulatePersonControlledTier,)])
