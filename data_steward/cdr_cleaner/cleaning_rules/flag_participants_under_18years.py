"""
Record every participant who was under 18 at consent in a lookup table. Removal happens at the tier deid stages (DL-2418).

Original Issues: DL2416
"""

# Python imports
import logging

# Project imports
import common
import constants.cdr_cleaner.clean_cdr as cdr_consts
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule

LOGGER = logging.getLogger(__name__)

PARTICIPANTS_UNDER_18_AT_CONSENT_QUERY = common.JINJA_ENV.from_string("""
    CREATE OR REPLACE TABLE `{{project}}.{{sandbox_dataset}}.{{under18_participant_lookup_table}}` AS (
    SELECT
        person_id,
        CAST(MIN(age_at_consent) AS INT64) AS age_at_consent,
        IF(MIN(age_at_consent) <= 6, '0-6', '7-17') AS age_band
    FROM (
        SELECT
        person_id,
        {{pipeline_tables}}.calculate_age(observation_date, EXTRACT(DATE FROM birth_datetime)) AS age_at_consent
        FROM `{{project}}.{{dataset}}.observation`
        JOIN `{{project}}.{{dataset}}.person` USING (person_id)
        WHERE (observation_source_concept_id = 1585482 OR observation_concept_id = 1585482)
        AND birth_datetime IS NOT NULL
    )
    WHERE age_at_consent < 18
    GROUP BY person_id
    )
""")


class FlagParticipantsUnder18Years(BaseCleaningRule):
    """
    Record every participant under 18 years old at consent, with age, in the
    _under18_participants lookup. Flags only; deletes nothing. Removal happens
    at the tier deid stages (DL-2418).
    """

    def __init__(self, project_id, dataset_id, sandbox_dataset_id):
        """
        Initialize the class with proper information.

        Set the issue numbers, description and affected datasets. As other tickets may affect
        this SQL, append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """

        desc = (
            "All EHR data associated with a participant who was younger than 18 years old at consent "
            "is flagged so that the downstream logic can handle them accordingly."
        )

        super().__init__(
            issue_numbers=["DL2416"],
            description=desc,
            affected_datasets=[cdr_consts.RDR],
            affected_tables=[],
            project_id=project_id,
            dataset_id=dataset_id,
            sandbox_dataset_id=sandbox_dataset_id,
        )

    def get_query_specs(self):
        return [{
            cdr_consts.QUERY:
                PARTICIPANTS_UNDER_18_AT_CONSENT_QUERY.render(
                    project=self.project_id,
                    dataset=self.dataset_id,
                    sandbox_dataset=self.sandbox_dataset_id,
                    pipeline_tables=common.PIPELINE_TABLES,
                    under18_participant_lookup_table=common.
                    UNDER18_PARTICIPANTS_LOOKUP_TABLE,
                )
        }]

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
        """
        Returns an iterable of sandbox table names
        """
        return [common.UNDER18_PARTICIPANTS_LOOKUP_TABLE]


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ext_parser = parser.get_argument_parser()
    ARGS = ext_parser.parse_args()

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(
            ARGS.project_id,
            ARGS.dataset_id,
            ARGS.sandbox_dataset_id,
            [(FlagParticipantsUnder18Years,)],
        )
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(
            ARGS.project_id,
            ARGS.dataset_id,
            ARGS.sandbox_dataset_id,
            [(FlagParticipantsUnder18Years,)],
        )
