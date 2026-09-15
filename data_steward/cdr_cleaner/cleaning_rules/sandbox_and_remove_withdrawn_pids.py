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

# Concepts that mark a person to person row as a family relationship. This is a
# membership filter, never a direction signal: which side is the child is decided
# by the age band, not by the concept. Both orientations are listed because
# `fact_relationship` rows are emitted in both directions, and because the two
# sources that specify these ids disagree. The workbook gives Grandparent 4244389
# and Sibling 4292398 where the PRD gives Natural grandparent 4301632 and Natural
# sibling 4218412, and that choice is open against DL-2482.
#
# THIS FILTER IS DELIBERATELY TOO WIDE AND IS NOT A FINISHED RULE. The release
# requirements cascade from the adult who consented for the child, and nothing
# here identifies that adult. Any relative absent from the pediatric cohort
# lookup qualifies, so an adult cousin's withdrawal deactivates a linked 0-6
# participant. Trimming data is the safe direction to be wrong in, and the real
# filter cannot exist until the emitting rule says which edge is the consent
# link. 4053608 is the widest edge by a distance: it resolves as `Blood
# relative` in the vocabulary, not as a parent concept, so on its own it admits
# almost any relative. It is listed only because both source lists name it as
# the value for Parent.
# TODO(DL-2482): replace this list with the consent-link edge that rule emits,
# and import it rather than restating it here. Narrowing it is the point; this
# is a placeholder, not a set to preserve.
FAMILY_RELATIONSHIP_CONCEPT_IDS = [
    # Adult described from the child's side.
    4053608,  # `Blood relative` in the vocabulary; both sources use it for Parent
    4301632,  # Natural Grandparent
    4244389,  # Grandparent
    4292398,  # Sibling
    # Child described from the adult's side.
    4326600,  # Natural Child
    4311425,  # Grandchild
    4032151,  # Legal Child
    # Symmetric, so they appear from either side.
    4218412,  # Natural Sibling
    44783070,  # Second Degree Blood Relative
    4206333,  # Cousin
]

# Resolve the adult-child pairs while `fact_relationship` and the pediatric
# cohort lookup are both in reach. The deactivation rule consumes this at the
# combined and fitbit stages, and the fitbit dataset carries neither table.
#
# Orientation-agnostic on purpose. Reading the pair direction out of the
# relationship concept would make the whole cascade depend on DL-2482 emitting
# the direction this rule guesses, and a mismatch would leave the lookup empty
# with nothing failing anywhere. The age band already says which side is the
# child, so both orientations are considered and the concept list is only a
# family-relationship filter.
PEDIATRIC_GUARDIAN_LINKS_QUERY = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project_id}}.{{sandbox_dataset_id}}.{{links_table}}` AS (
    WITH cohort AS (
        SELECT person_id, age_band
        FROM `{{project_id}}.{{sandbox_dataset_id}}.{{under18_table}}`
    ),
    person_pairs AS (
        SELECT
            fr.fact_id_1 AS left_person_id,
            fr.fact_id_2 AS right_person_id,
            fr.relationship_concept_id
        FROM `{{project_id}}.{{dataset_id}}.{{fact_relationship}}` fr
        WHERE fr.domain_concept_id_1 = {{person_domain_concept_id}}
          AND fr.domain_concept_id_2 = {{person_domain_concept_id}}
        UNION ALL
        SELECT
            fr.fact_id_2,
            fr.fact_id_1,
            fr.relationship_concept_id
        FROM `{{project_id}}.{{dataset_id}}.{{fact_relationship}}` fr
        WHERE fr.domain_concept_id_1 = {{person_domain_concept_id}}
          AND fr.domain_concept_id_2 = {{person_domain_concept_id}}
    )
    SELECT DISTINCT
        pp.left_person_id AS adult_person_id,
        pp.right_person_id AS pediatric_person_id
    FROM person_pairs pp
    JOIN cohort AS peds
        ON peds.person_id = pp.right_person_id
        AND peds.age_band = '{{pediatric_age_band}}'
    LEFT JOIN cohort AS adult_side
        ON adult_side.person_id = pp.left_person_id
    WHERE pp.relationship_concept_id IN ({{family_relationship_concept_ids|join(', ')}})
      AND pp.left_person_id != pp.right_person_id
      /* The adult side must be absent from the under-18 lookup entirely, not
         merely outside the 0-6 band. A 7-17 participant cannot consent for a
         child, and several of these concepts are symmetric, so a 17 year old
         sibling would otherwise be admitted as the guardian and their
         withdrawal would cascade onto the child. Anyone 18+ never enters the
         lookup, so absence from it is the test. */
      AND adult_side.person_id IS NULL
)
""")

# Counts logged at run time so an empty cohort is visible in the log rather than
# only inferable from a zero further downstream.
PEDIATRIC_LINK_COUNTS_QUERY = JINJA_ENV.from_string("""
SELECT
    (SELECT COUNT(*)
     FROM `{{project_id}}.{{sandbox_dataset_id}}.{{under18_table}}`
     WHERE age_band = '{{pediatric_age_band}}') AS pediatric_cohort,
    (SELECT COUNT(*)
     FROM `{{project_id}}.{{sandbox_dataset_id}}.{{links_table}}`) AS pairs,
    (SELECT COUNT(DISTINCT pediatric_person_id)
     FROM `{{project_id}}.{{sandbox_dataset_id}}.{{links_table}}`) AS linked_children
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
        Build the adult to pediatric pairs and report what was found.

        This runs in `setup_rule` rather than as a query spec so the counts can be
        logged: the engine calls `setup_rule` before `get_query_specs`, and the
        pair count does not exist until the table has been written. The removal
        queries that follow never touch `fact_relationship`, which carries no
        `person_id` column, so the pairs are unaffected by ordering either way.
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
                pediatric_age_band=PEDIATRIC_AGE_BAND,
                family_relationship_concept_ids=FAMILY_RELATIONSHIP_CONCEPT_IDS)
        ).result()

        counts = list(
            client.query(
                PEDIATRIC_LINK_COUNTS_QUERY.render(
                    project_id=self.project_id,
                    sandbox_dataset_id=self.sandbox_dataset_id,
                    links_table=PEDIATRIC_GUARDIAN_LINKS_LOOKUP_TABLE,
                    under18_table=UNDER18_PARTICIPANTS_LOOKUP_TABLE,
                    pediatric_age_band=PEDIATRIC_AGE_BAND)).result())[0]

        LOGGER.info(
            f"`{UNDER18_PARTICIPANTS_LOOKUP_TABLE}` holds "
            f"{counts.pediatric_cohort} participants in the "
            f"'{PEDIATRIC_AGE_BAND}' band. Derived {counts.pairs} adult to "
            f"pediatric pairs covering {counts.linked_children} of them into "
            f"`{PEDIATRIC_GUARDIAN_LINKS_LOOKUP_TABLE}`.")

        if not counts.pediatric_cohort:
            LOGGER.warning(
                f"The '{PEDIATRIC_AGE_BAND}' band of "
                f"`{UNDER18_PARTICIPANTS_LOOKUP_TABLE}` is empty, so no "
                f"pediatric lifecycle cascade can apply downstream. On a "
                f"pediatric input this is an upstream age derivation failure.")
        elif not counts.pairs:
            LOGGER.warning(
                f"No adult to pediatric pairs were derived from "
                f"`{FACT_RELATIONSHIP}` despite a non-empty "
                f"'{PEDIATRIC_AGE_BAND}' band. Expected until the rule that "
                f"emits the person domain linkage rows lands.")

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
