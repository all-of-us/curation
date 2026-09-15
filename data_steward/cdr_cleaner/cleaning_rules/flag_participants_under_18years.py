"""
Record every participant who was under 18 at consent in a lookup table. Removal happens at the tier deid stages (DL-2418).

Age at consent is derived from the participant's consent record in survey_conduct,
matched on survey_source_value so the adult and the pediatric consent instruments
both resolve. The concept this rule used to key on, 1585482
(ExtraConsent_TodaysDate), is asked only by the adult ConsentPII instrument, so it
resolves nothing for pediatric participants and every rule reading the lookup ran
against an empty cohort without erroring.

Original Issues: DL2416, DL2501
"""

# Python imports
import logging

# Project imports
import common
import constants.cdr_cleaner.clean_cdr as cdr_consts
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule

LOGGER = logging.getLogger(__name__)

# Source values of the instruments that record a consent decision, lowercase.
# 'consentpii' is written 'ConsentPII' in the adult export and 'consentpii_0to6'
# is the pediatric guardian permission instrument. Adding a further permission
# instrument is a one line change here and needs no edit to any query.
CONSENT_SURVEY_SOURCE_VALUES = ['consentpii', 'consentpii_0to6']

# Shared by every query below so the three cannot disagree about who is
# consented. Rendered inside a CREATE TABLE AS, so it opens the statement.
CONSENT_DATES_CTE = """
WITH consent AS (
    SELECT
        person_id,
        MIN(survey_end_date) AS consent_date
    FROM `{{project}}.{{dataset}}.survey_conduct`
    -- Keyed on the source value alone. survey_concept_id is 0 on every row of --
    -- the pediatric export, and survey_source_concept_id there carries RDR --
    -- internal questionnaire ids that collide with unrelated OMOP concepts --
    -- (consentpii_0to6 is 10263, an ICD10CM injury code), so either numeric --
    -- predicate is incomplete for pediatrics and wrong in meaning. --
    -- LOWER(): the two instruments disagree on case. --
    -- survey_end_date: survey_start_date is NULL on every row of both exports. --
    -- The date floor drops the 0001-01-01 sentinel, which computes an --
    -- implausible age. --
    WHERE LOWER(survey_source_value) IN ('{{ consent_survey_source_values|join("', '") }}')
        AND survey_end_date > DATE '1900-01-01'
    GROUP BY person_id
)
"""

PARTICIPANTS_UNDER_18_AT_CONSENT_QUERY = common.JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project}}.{{sandbox_dataset}}.{{under18_participant_lookup_table}}` AS (
""" + CONSENT_DATES_CTE + """
SELECT
    person_id,
    CAST(age_at_consent AS INT64) AS age_at_consent,
    IF(age_at_consent <= 6, '0-6', '7-17') AS age_band
FROM (
    SELECT
        person_id,
        {{pipeline_tables}}.calculate_age(c.consent_date, EXTRACT(DATE FROM p.birth_datetime)) AS age_at_consent
    FROM consent AS c
    JOIN `{{project}}.{{dataset}}.person` AS p USING (person_id)
    WHERE p.birth_datetime IS NOT NULL
)
WHERE age_at_consent < 18
)
""")

PARTICIPANTS_WITHOUT_CONSENT_DATE_QUERY = common.JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project}}.{{sandbox_dataset}}.{{unresolved_consent_date_lookup_table}}` AS (
""" + CONSENT_DATES_CTE + """
SELECT
    person_id
FROM `{{project}}.{{dataset}}.person`
WHERE birth_datetime IS NOT NULL
    AND person_id NOT IN (SELECT person_id FROM consent)
)
""")

CONSENT_RESOLUTION_COUNTS_QUERY = common.JINJA_ENV.from_string(
    CONSENT_DATES_CTE + """
, resolved AS (
    SELECT
        {{pipeline_tables}}.calculate_age(c.consent_date, EXTRACT(DATE FROM p.birth_datetime)) AS age_at_consent
    FROM consent AS c
    JOIN `{{project}}.{{dataset}}.person` AS p USING (person_id)
    WHERE p.birth_datetime IS NOT NULL
)
-- calculate_age raises on a NULL date, so the unresolved participants are --
-- counted by subtraction rather than by a left join into the UDF. --
SELECT
    (
        SELECT COUNT(*)
        FROM `{{project}}.{{dataset}}.person`
        WHERE birth_datetime IS NOT NULL
    ) AS participants_evaluated,
    (SELECT COUNT(*) FROM resolved) AS consent_dates_resolved,
    (SELECT COUNTIF(age_at_consent < 18 AND age_at_consent <= 6) FROM resolved) AS flagged_0_6,
    (SELECT COUNTIF(age_at_consent < 18 AND age_at_consent > 6) FROM resolved) AS flagged_7_17
""")


class FlagParticipantsUnder18Years(BaseCleaningRule):
    """
    Record every participant under 18 years old at consent, with age and age
    band, in the _under18_participants lookup, and every participant whose
    consent date does not resolve in the _unresolved_consent_date_participants
    lookup. Flags only; deletes nothing. Removal happens at the tier deid
    stages (DL-2418).
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
            issue_numbers=["DL2416", "DL2501"],
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
                    consent_survey_source_values=CONSENT_SURVEY_SOURCE_VALUES,
                    under18_participant_lookup_table=common.
                    UNDER18_PARTICIPANTS_LOOKUP_TABLE,
                )
        }, {
            cdr_consts.QUERY:
                PARTICIPANTS_WITHOUT_CONSENT_DATE_QUERY.render(
                    project=self.project_id,
                    dataset=self.dataset_id,
                    sandbox_dataset=self.sandbox_dataset_id,
                    consent_survey_source_values=CONSENT_SURVEY_SOURCE_VALUES,
                    unresolved_consent_date_lookup_table=common.
                    UNRESOLVED_CONSENT_DATE_LOOKUP_TABLE,
                )
        }]

    def setup_rule(self, client):
        """
        Report how the consent date resolves across the input dataset, so a run
        that found no under 18 participants is distinguishable from a run that
        resolved nobody.

        The counts are taken here, against the input, rather than from the
        lookups after they are written, because the engine calls setup_rule and
        then the query specs and never calls validate_rule. They describe the
        same rows the queries below are about to write.
        """
        counts_query = CONSENT_RESOLUTION_COUNTS_QUERY.render(
            project=self.project_id,
            dataset=self.dataset_id,
            pipeline_tables=common.PIPELINE_TABLES,
            consent_survey_source_values=CONSENT_SURVEY_SOURCE_VALUES,
        )
        for row in client.query(counts_query).result():
            LOGGER.info(
                f"{self.__class__.__name__}: "
                f"{row.participants_evaluated} participants with a birth date evaluated, "
                f"{row.consent_dates_resolved} consent dates resolved, "
                f"{row.participants_evaluated - row.consent_dates_resolved} unresolved, "
                f"{row.flagged_0_6} flagged '0-6', {row.flagged_7_17} flagged '7-17'."
            )

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
        return [
            common.UNDER18_PARTICIPANTS_LOOKUP_TABLE,
            common.UNRESOLVED_CONSENT_DATE_LOOKUP_TABLE
        ]


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
