"""
This cleaning rule uses a list of participants to remove all participant data from combined. 
"""

# Python imports
import logging

# Project imports
from cdr_cleaner.cleaning_rules.sandbox_and_remove_pids import SandboxAndRemovePids, JINJA_ENV, PERSON_TABLE_QUERY, AOU_DEATH, CDM_TABLES
from common import (FACT_RELATIONSHIP, PERSON_DOMAIN_CONCEPT_ID,
                    PEDIATRIC_GUARDIAN_LINKS_LOOKUP_TABLE,
                    UNDER18_PARTICIPANTS_LOOKUP_TABLE)
from constants.cdr_cleaner import clean_cdr as cdr_consts
from gcloud.bq import BigQueryClient

LOGGER = logging.getLogger(__name__)

ISSUE_NUMBERS = ['DC3442', 'DL2486']

# The band of `_under18_participants` that is the CT+ pediatric cohort.
PEDIATRIC_AGE_BAND = '0-6'

# Every person to person `fact_relationship` row links a child to the guardian
# who enrolled them; nothing else writes such rows today. `relationship_concept_id`
# only describes that guardian (Parent, Guardian 40567635, 0 if skipped), so it is
# not filtered on: a concept filter cannot tell a guardian from another relative,
# and would drop the Guardian and skipped cases.
#
# Each row is classified against `_under18_participants`:
#   pair            one side 0-6, the other absent from the lookup (18+)
#   outside_cohort  one side 7-17, neither side 0-6; 7-17 is removed everywhere
#   unresolved      anything else, such as a child missing from the lookup
# An unresolved row stops the run, since a child missing from the lookup would
# keep their data after their guardian withdraws.
# TODO(DL-2482): confirm every person to person row RDR delivers is a guardian
# link.
LINKAGE_ROWS_CTES = """
    linkage_rows AS (
        SELECT
            fr.fact_id_1 AS person_id_1,
            fr.fact_id_2 AS person_id_2,
            c1.age_band AS band_1,
            c2.age_band AS band_2
        FROM `{{project_id}}.{{dataset_id}}.{{fact_relationship}}` fr
        LEFT JOIN `{{project_id}}.{{sandbox_dataset_id}}.{{under18_table}}` c1
            ON c1.person_id = fr.fact_id_1
        LEFT JOIN `{{project_id}}.{{sandbox_dataset_id}}.{{under18_table}}` c2
            ON c2.person_id = fr.fact_id_2
        WHERE fr.domain_concept_id_1 = {{person_domain_concept_id}}
          AND fr.domain_concept_id_2 = {{person_domain_concept_id}}
    ),
    classified AS (
        SELECT
            *,
            CASE
                WHEN band_1 = '{{pediatric_age_band}}' AND band_2 IS NULL
                    THEN 'pair'
                WHEN band_2 = '{{pediatric_age_band}}' AND band_1 IS NULL
                    THEN 'pair'
                WHEN '{{pediatric_age_band}}' IN (band_1, band_2)
                    THEN 'unresolved'
                WHEN band_1 IS NULL AND band_2 IS NULL
                    THEN 'unresolved'
                ELSE 'outside_cohort'
            END AS link_class
        FROM linkage_rows
    )
"""

# Resolved here because the fitbit stage, where the deactivation rule also reads
# the pairs, carries neither table. Rows arrive in both directions, so the age
# band, not the column order, picks the child.
PEDIATRIC_GUARDIAN_LINKS_QUERY = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project_id}}.{{sandbox_dataset_id}}.{{links_table}}` AS (
    WITH""" + LINKAGE_ROWS_CTES + """
    SELECT DISTINCT
        IF(band_1 = '{{pediatric_age_band}}', person_id_2, person_id_1)
            AS adult_person_id,
        IF(band_1 = '{{pediatric_age_band}}', person_id_1, person_id_2)
            AS pediatric_person_id
    FROM classified
    WHERE link_class = 'pair'
)
""")

# Counts for the log, plus the unresolved rows that stop the run.
PEDIATRIC_LINK_COUNTS_QUERY = JINJA_ENV.from_string("""
WITH""" + LINKAGE_ROWS_CTES + """
SELECT
    (SELECT COUNT(*)
     FROM `{{project_id}}.{{sandbox_dataset_id}}.{{under18_table}}`
     WHERE age_band = '{{pediatric_age_band}}') AS pediatric_cohort,
    (SELECT COUNT(*)
     FROM `{{project_id}}.{{sandbox_dataset_id}}.{{links_table}}`) AS pairs,
    (SELECT COUNT(DISTINCT pediatric_person_id)
     FROM `{{project_id}}.{{sandbox_dataset_id}}.{{links_table}}`) AS linked_children,
    (SELECT COUNT(*)
     FROM classified
     WHERE link_class = 'unresolved') AS unresolved_rows
""")

# Query template to copy withdrawn_dups_table from rdr dataset to combined sandbox dataset
COPY_WITHDRAWN_DUPS_TABLE_TEMPLATE = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE 
    `{{project_id}}.{{sandbox_dataset_id}}.{{withdrawn_dups_table}}` AS 
    (
        SELECT
            person_id,
            hpo_id
            src_id,
            consent_for_study_enrollment_authored,
            withdrawal_status
        FROM
            `{{project_id}}.{{dataset_id}}.{{withdrawn_dups_table}}`                                       
    )
""")


class SandboxAndRemoveWithdrawnPids(SandboxAndRemovePids):
    """
    Removes all participant data using a list of participants.
    """

    def __init__(self, project_id, dataset_id, sandbox_dataset_id,
                 withdrawn_dups_table):
        """
        Initialize the class with proper information.

        Set the issue numbers, description and affected datasets. As other tickets may affect
        this SQL, append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """

        self.withdrawn_dups_table = withdrawn_dups_table

        desc = 'Sandbox and remove participant data from a list of participants.'

        super().__init__(issue_numbers=ISSUE_NUMBERS,
                         description=desc,
                         affected_datasets=[cdr_consts.RDR],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         affected_tables=[])

    def setup_rule(self, client: BigQueryClient, ehr_only: bool = False):
        """
        Get list of tables that have a person_id column, excluding mapping tables
        :param ehr_only: For Combined dataset, True if removing only EHR records. False if removing both RDR and EHR records.
        """

        person_table_query = PERSON_TABLE_QUERY.render(project=self.project_id,
                                                       dataset=self.dataset_id,
                                                       ehr_only=ehr_only)
        person_tables = client.query(person_table_query).result()

        self.affected_tables = [
            table.get('table_name')
            for table in person_tables
            if table.get('table_name') in CDM_TABLES + [AOU_DEATH]
        ]

        # Copy withdrawn_dups_table from rdr dataset to combined sandbox dataset
        copy_withdrawn_dups_table_query = COPY_WITHDRAWN_DUPS_TABLE_TEMPLATE.render(
            project_id=self.project_id,
            sandbox_dataset_id=self.sandbox_dataset_id,
            dataset_id=self.dataset_id,
            withdrawn_dups_table=self.withdrawn_dups_table)

        client.query(copy_withdrawn_dups_table_query).result()

        self.derive_pediatric_guardian_links(client)

    def derive_pediatric_guardian_links(self, client: BigQueryClient):
        """
        Write the adult to pediatric pairs, log the counts, and stop the run on
        any unresolved linkage row.

        Runs in `setup_rule` so the counts can be checked before any removal. The
        removal never touches `fact_relationship`, which has no `person_id`.

        :raises RuntimeError: if any person domain linkage row is unresolved
        """
        client.query(
            PEDIATRIC_GUARDIAN_LINKS_QUERY.render(
                project_id=self.project_id,
                dataset_id=self.dataset_id,
                sandbox_dataset_id=self.sandbox_dataset_id,
                links_table=PEDIATRIC_GUARDIAN_LINKS_LOOKUP_TABLE,
                under18_table=UNDER18_PARTICIPANTS_LOOKUP_TABLE,
                fact_relationship=FACT_RELATIONSHIP,
                person_domain_concept_id=PERSON_DOMAIN_CONCEPT_ID,
                pediatric_age_band=PEDIATRIC_AGE_BAND)).result()

        counts = list(
            client.query(
                PEDIATRIC_LINK_COUNTS_QUERY.render(
                    project_id=self.project_id,
                    dataset_id=self.dataset_id,
                    sandbox_dataset_id=self.sandbox_dataset_id,
                    links_table=PEDIATRIC_GUARDIAN_LINKS_LOOKUP_TABLE,
                    under18_table=UNDER18_PARTICIPANTS_LOOKUP_TABLE,
                    fact_relationship=FACT_RELATIONSHIP,
                    person_domain_concept_id=PERSON_DOMAIN_CONCEPT_ID,
                    pediatric_age_band=PEDIATRIC_AGE_BAND)).result())[0]

        LOGGER.info(
            f"`{UNDER18_PARTICIPANTS_LOOKUP_TABLE}` holds "
            f"{counts.pediatric_cohort} participants in the "
            f"'{PEDIATRIC_AGE_BAND}' band. Derived {counts.pairs} adult to "
            f"pediatric pairs covering {counts.linked_children} of them into "
            f"`{PEDIATRIC_GUARDIAN_LINKS_LOOKUP_TABLE}`.")

        if counts.unresolved_rows:
            raise RuntimeError(
                f"{counts.unresolved_rows} person domain `{FACT_RELATIONSHIP}` "
                f"rows do not resolve to one adult and one participant in the "
                f"'{PEDIATRIC_AGE_BAND}' band of "
                f"`{UNDER18_PARTICIPANTS_LOOKUP_TABLE}`. A linked child missing "
                f"from that lookup would keep their data after their guardian "
                f"withdraws, so the run stops. The usual cause is an age at "
                f"consent that `FlagParticipantsUnder18Years` did not derive.")

        if not counts.pediatric_cohort:
            LOGGER.warning(
                f"The '{PEDIATRIC_AGE_BAND}' band of "
                f"`{UNDER18_PARTICIPANTS_LOOKUP_TABLE}` is empty and no "
                f"linkage rows name a child, so the pediatric cascade was not "
                f"evaluated. This is not the same as a passing run.")
        elif not counts.pairs:
            LOGGER.warning(
                f"No adult to pediatric pairs were derived from "
                f"`{FACT_RELATIONSHIP}` despite a non-empty "
                f"'{PEDIATRIC_AGE_BAND}' band, so the cascade was not "
                f"evaluated. Expected until the pediatric export carries the "
                f"person domain linkage rows.")

    def get_query_specs(self) -> list:
        sandbox_records_queries = self.get_sandbox_queries(
            lookup_table=self.withdrawn_dups_table)
        remove_pids_queries = self.get_remove_pids_queries(
            lookup_table=self.withdrawn_dups_table)

        return sandbox_records_queries + remove_pids_queries

    def get_sandbox_tablenames(self):
        """
        generates sandbox table names
        """
        return [
            self.sandbox_table_for(table) for table in self.affected_tables
        ] + [PEDIATRIC_GUARDIAN_LINKS_LOOKUP_TABLE]

    def setup_validation(self, client):
        """
        Run required steps for validation setup.
        """
        raise NotImplementedError("Please fix me.")

    def validate_rule(self, client):
        """
        Validates the cleaning rule which deletes or updates the data from the tables.
        """
        raise NotImplementedError("Please fix me.")


def parse_args():
    """
    This function expands the default argument list defined in cdr_cleaner.args_parser
    :return: an expanded argument list object
    """

    import cdr_cleaner.args_parser as parser

    additional_arguments = [{
        parser.SHORT_ARGUMENT: '-l',
        parser.LONG_ARGUMENT: '--withdrawn_dups_table',
        parser.ACTION: 'store',
        parser.DEST: 'withdrawn_dups_table',
        parser.HELP: 'withdrawn_dups_table',
        parser.REQUIRED: True
    }]
    args = parser.default_parse_args(additional_arguments)
    return args


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.parse_args()

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(
            ARGS.project_id,
            ARGS.dataset_id,
            ARGS.sandbox_dataset_id, [(SandboxAndRemoveWithdrawnPids,)],
            withdrawn_dups_table=ARGS.withdrawn_dups_table)
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(
            ARGS.project_id,
            ARGS.dataset_id,
            ARGS.sandbox_dataset_id, [(SandboxAndRemoveWithdrawnPids,)],
            withdrawn_dups_table=ARGS.withdrawn_dups_table)
