"""
Emit observation records on the linked adult's person_id for the twelve Pediatric
Basics items collected about the parent or guardian.

The items are reassessments of the adult and are already mapped to standard concepts
for the adult surveys. The PRD requires observation records on the adult's person_id so
researchers can read them as observations about the adult. Its verb is create, so the
pediatric participant's own row is left in place.

One thing this deliberately does not collapse: an adult linked to several pediatric
participants gets one record per child per item. Each comes from a separate survey
completion and carries its own questionnaire_response_id, so the rows stay
distinguishable. Follows the COPE precedent for the same question re-asked.

Original Issues: DL2483
"""

# Python imports
import logging

# Project imports
import constants.cdr_cleaner.clean_cdr as cdr_consts
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule, query_spec_list
from common import (FACT_RELATIONSHIP, JINJA_ENV, MAPPING_PREFIX, OBSERVATION,
                    PERSON_DOMAIN_CONCEPT_ID, SURVEY_CONDUCT)
from resources import fields_for

LOGGER = logging.getLogger(__name__)

# Identified by survey_source_value, not survey_concept_id: no pediatric concept exists
# in the vocabulary yet, so survey_concept_id arrives as 0 here, as it does for
# HPOSitePairing and ProgramUpdate.
#
# Matched with LOWER(), so these must stay lowercase. survey_source_value mixes
# conventions across surveys ('TheBasics' beside 'sdoh'), and raw_rdr_export_qc.py
# matches it with (?i) for the same reason.
PEDIATRIC_BASICS_SURVEY_SOURCE_VALUES = ['ped_basics']

# Relationship concepts that can appear on a pediatric-to-adult row, the inverse
# direction of the pairs DL-2482 emits.
#
# TODO(DL-2482): confirm against the ids that rule emits. It is not built yet, and the
# relationship proposal lists no inverse for the Other and Prefer-not-to-answer answers.
#
# This narrows the candidates but cannot identify the adult on its own: Natural Sibling,
# Second Degree Blood Relative and Cousin are symmetric, so a sibling-to-sibling row
# carries the same concept as a child-to-adult row. Excluding counterparts who are
# themselves pediatric is the primary guard.
ADULT_LINK_RELATIONSHIP_CONCEPT_IDS = [
    4326600,  # Natural Child
    4311425,  # Grandchild
    4218412,  # Natural Sibling
    44783070,  # Second Degree Blood Relative
    4206333,  # Cousin
    4032151,  # Legal Child
]

# The twelve Pediatric Basics items that are about the parent or guardian. The
# numbering is the survey's own and is non-contiguous: the omitted numbers are items
# about the child.
#
# mapped_concept_id is what the pediatric survey mapping assigns to the question.
# standard_concept_id is the concept the export actually emits for that item, which is
# not always the vocabulary's 'Maps to' target. MaritalStatus_CurrentMaritalStatus is
# the case in point: 1332833 is a PPI Question concept that is standard and maps to
# itself, but the export uses LOINC 3046344 on 733,908 rows of 2025q4r5_rdr. Taking the
# vocabulary answer there would put every emitted record out of step with every adult
# row for the same item. Checked against the export 2026-09-17: eleven items matched the
# vocabulary answer, marital status did not.
#
# standard_concept_id is what the emitted record carries, so it tracks the export rather
# than the vocabulary. See EMIT_QUERY.
GUARDIAN_ABOUT_ITEMS = [
    {
        'item': 4,
        'field': 'educationlevel_highestgrade',
        'mapped_concept_id': 1585940,
        'standard_concept_id': 40771091
    },
    {
        'item': 5,
        'field': 'maritalstatus_currentmaritalstatus',
        'mapped_concept_id': 1332833,
        'standard_concept_id': 3046344
    },
    {
        'item': 6,
        'field': 'livingsituation_howmanypeople',
        'mapped_concept_id': 1585889,
        'standard_concept_id': 1585889
    },
    {
        'item': 7,
        'field': 'livingsituation_peopleunder18',
        'mapped_concept_id': 1585890,
        'standard_concept_id': 1585890
    },
    {
        'item': 13,
        'field': 'employment_employmentstatus',
        'mapped_concept_id': 1585952,
        'standard_concept_id': 40771090
    },
    {
        'item': 14,
        'field': 'income_annualincome',
        'mapped_concept_id': 1585375,
        'standard_concept_id': 46235933
    },
    {
        'item': 15,
        'field': 'homeown_currenthomeown',
        'mapped_concept_id': 1585370,
        'standard_concept_id': 1585370
    },
    {
        'item': 16,
        'field': 'livingsituation_currentliving',
        'mapped_concept_id': 1585402,
        'standard_concept_id': 3051968
    },
    {
        'item': 17,
        'field': 'livingsituation_livingsituationfreetext',
        'mapped_concept_id': 1585878,
        'standard_concept_id': 1585878
    },
    {
        'item': 18,
        'field': 'livingsituation_howmanylivingyears',
        'mapped_concept_id': 1585879,
        'standard_concept_id': 1585879
    },
    {
        'item': 19,
        'field': 'hvs_1',
        'mapped_concept_id': 40192517,
        'standard_concept_id': 36304041
    },
    {
        'item': 20,
        'field': 'hvs_2',
        'mapped_concept_id': 40192426,
        'standard_concept_id': 36306143
    },
]

UNRESOLVED_TABLE = 'unresolved_linkage'

# Shared by EMIT_QUERY and UNRESOLVED_QUERY so the two cannot drift: whatever the first
# does not emit, the second must record.
SOURCE_CTES = """
items AS (
    SELECT * FROM UNNEST([
    {%- for i in items %}
        STRUCT({{i.item}} AS item,
               '{{i.field}}' AS field,
               {{i.mapped_concept_id}} AS mapped_concept_id,
               {{i.standard_concept_id}} AS standard_concept_id){{ "," if not loop.last }}
    {%- endfor %}
    ])
),
pediatric_surveys AS (
    SELECT survey_conduct_id, person_id
    FROM `{{project}}.{{dataset}}.{{survey_conduct}}`
    WHERE LOWER(survey_source_value) IN (
        {%- for v in survey_source_values %}'{{v}}'{{ ", " if not loop.last }}{% endfor %})
),
pediatric_responses AS (
    SELECT o.*
    FROM `{{project}}.{{dataset}}.{{obs}}` o
    JOIN pediatric_surveys ps
      ON ps.survey_conduct_id = o.questionnaire_response_id
),
matched_responses AS (
    /* Source value first, concept id only as a fallback: a pediatric row can arrive
       with observation_source_concept_id = 0 while carrying the PPI code, and matching
       on either with an OR would emit two records when the columns disagree. Each join
       matches at most one item, since field and mapped_concept_id are both unique. */
    SELECT
        r.*,
        COALESCE(by_value.field, by_concept.field) AS matched_field,
        COALESCE(by_value.standard_concept_id,
                 by_concept.standard_concept_id) AS standard_concept_id
    FROM pediatric_responses r
    LEFT JOIN items by_value
      ON by_value.field = LOWER(r.observation_source_value)
    LEFT JOIN items by_concept
      ON by_concept.mapped_concept_id = r.observation_source_concept_id
),
guardian_about_responses AS (
    SELECT * FROM matched_responses WHERE matched_field IS NOT NULL
),
person_pairs AS (
    /* The pairs are emitted in both directions, so reading only side 1 already gives
       each participant its counterpart once. */
    SELECT
        fr.fact_id_1 AS pediatric_person_id,
        fr.fact_id_2 AS counterpart_person_id,
        fr.fact_id_2 IN (SELECT person_id FROM pediatric_surveys)
          AS counterpart_is_pediatric,
        fr.relationship_concept_id IN (
            {%- for c in adult_link_relationship_concept_ids %}{{c}}{{ ", " if not loop.last }}{% endfor %})
          AS relationship_is_adult_link
    FROM (
        SELECT fact_id_1, fact_id_2, relationship_concept_id
        FROM `{{project}}.{{dataset}}.{{fact_relationship}}`
        WHERE domain_concept_id_1 = {{person_domain_concept_id}}
          AND domain_concept_id_2 = {{person_domain_concept_id}}
    ) fr
),
linkage AS (
    /* One row per pediatric participant with any person domain pair. Disqualified
       counterparts are classified, not filtered out, so the unresolved lookup can name
       which of the four ways a participant failed to resolve. ANY_VALUE ignores NULLs,
       returning the single qualifying counterpart when n_adults = 1. */
    SELECT
        pediatric_person_id,
        COUNT(*) AS n_pairs,
        COUNTIF(counterpart_is_pediatric) AS n_pediatric_counterparts,
        COUNT(DISTINCT IF(NOT counterpart_is_pediatric AND relationship_is_adult_link,
                          counterpart_person_id, NULL)) AS n_adults,
        ANY_VALUE(IF(NOT counterpart_is_pediatric AND relationship_is_adult_link,
                     counterpart_person_id, NULL)) AS adult_person_id
    FROM person_pairs
    GROUP BY pediatric_person_id
)
"""

EMIT_QUERY = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project}}.{{sandbox_dataset}}.{{sandbox_table}}` AS (
WITH
""" + SOURCE_CTES + """
SELECT
    ROW_NUMBER() OVER (ORDER BY r.person_id, r.observation_id)
      + (SELECT MAX(observation_id) FROM `{{project}}.{{dataset}}.{{obs}}`)
      AS observation_id,
    l.adult_person_id AS person_id,
    /* The declared concept for the item, not the source row's observation_concept_id.
       All twelve were checked against what the export emits on 2026-09-17 and all
       twelve match, so this is the value the source row carries, stated once. The list
       is therefore authoritative and has to be re-checked against the export whenever
       the vocabulary moves: that check is what caught marital status declared as
       1332833 while the data carried 3046344. */
    r.standard_concept_id AS observation_concept_id,
    r.observation_date,
    r.observation_datetime,
    r.observation_type_concept_id,
    r.value_as_number,
    r.value_as_string,
    r.value_as_concept_id,
    r.qualifier_concept_id,
    r.unit_concept_id,
    r.provider_id,
    r.visit_occurrence_id,
    r.visit_detail_id,
    r.observation_source_value,
    r.observation_source_concept_id,
    r.unit_source_value,
    r.qualifier_source_value,
    r.value_source_concept_id,
    r.value_source_value,
    /* Resolves the emitted record to the pediatric survey's survey_conduct row, which
       distinguishes it from the adult's own answer to the same question and is the key
       the duplicate-response exclusion reads. */
    r.questionnaire_response_id,
    m.src_id
FROM guardian_about_responses r
JOIN linkage l
  ON l.pediatric_person_id = r.person_id
 AND l.n_adults = 1
LEFT JOIN `{{project}}.{{dataset}}.{{mapping_obs}}` m
  ON m.observation_id = r.observation_id
)
""")

UNRESOLVED_QUERY = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project}}.{{sandbox_dataset}}.{{unresolved_table}}` AS (
WITH
""" + SOURCE_CTES + """
SELECT
    r.observation_id,
    r.person_id,
    r.questionnaire_response_id,
    r.observation_source_value,
    CASE
      WHEN l.pediatric_person_id IS NULL
        THEN 'no person domain linkage row for this participant'
      WHEN l.n_adults = 0 AND l.n_pediatric_counterparts = l.n_pairs
        THEN 'every linked counterpart is a pediatric participant'
      WHEN l.n_adults = 0
        THEN 'no counterpart carries an adult link relationship'
      ELSE 'linked to more than one adult'
    END AS unresolved_reason
FROM guardian_about_responses r
LEFT JOIN linkage l
  ON l.pediatric_person_id = r.person_id
WHERE l.pediatric_person_id IS NULL
   OR l.n_adults != 1
)
""")

INSERT_QUERY = JINJA_ENV.from_string("""
INSERT INTO `{{project}}.{{dataset}}.{{obs}}`
({{obs_fields}})
SELECT {{obs_fields}}
FROM `{{project}}.{{sandbox_dataset}}.{{sandbox_table}}`
""")

APPEND_MAPPING_QUERY = JINJA_ENV.from_string("""
INSERT INTO `{{project}}.{{dataset}}.{{mapping_obs}}`
(observation_id, src_id)
SELECT observation_id, src_id
FROM `{{project}}.{{sandbox_dataset}}.{{sandbox_table}}`
""")


class RoutePediatricAdultObservations(BaseCleaningRule):
    """
    Emit observation records on the linked adult's person_id for the twelve
    guardian-about Pediatric Basics items, leaving the pediatric participant's own
    rows in place.
    """

    def __init__(self,
                 project_id,
                 dataset_id,
                 sandbox_dataset_id,
                 table_namer=None):
        """
        Initialize the class with proper information.

        Set the issue numbers, description and affected datasets. As other tickets may
        affect this SQL, append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """
        desc = (
            'Emits observation records on the linked adult person_id for the twelve '
            'Pediatric Basics items collected about the parent or guardian, so they '
            'can be read as observations about the adult. The pediatric '
            "participant's own responses are left in place. Responses whose adult "
            'linkage cannot be resolved are recorded in a lookup table and left '
            'unchanged.')

        super().__init__(issue_numbers=['DL2483'],
                         description=desc,
                         affected_datasets=[cdr_consts.RDR],
                         affected_tables=[OBSERVATION],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         table_namer=table_namer)

    def _source_params(self):
        """Parameters shared by the emit and unresolved queries."""
        return {
            'project':
                self.project_id,
            'dataset':
                self.dataset_id,
            'obs':
                OBSERVATION,
            'survey_conduct':
                SURVEY_CONDUCT,
            'fact_relationship':
                FACT_RELATIONSHIP,
            'items':
                GUARDIAN_ABOUT_ITEMS,
            'survey_source_values':
                PEDIATRIC_BASICS_SURVEY_SOURCE_VALUES,
            'person_domain_concept_id':
                PERSON_DOMAIN_CONCEPT_ID,
            'adult_link_relationship_concept_ids':
                ADULT_LINK_RELATIONSHIP_CONCEPT_IDS,
        }

    def setup_rule(self, client):
        pass

    def get_sandbox_tablenames(self):
        return [
            self.sandbox_table_for(OBSERVATION),
            self.sandbox_table_for(UNRESOLVED_TABLE)
        ]

    def get_query_specs(self) -> query_spec_list:
        sandbox_table = self.sandbox_table_for(OBSERVATION)
        mapping_obs = MAPPING_PREFIX + OBSERVATION

        emit = EMIT_QUERY.render(sandbox_dataset=self.sandbox_dataset_id,
                                 sandbox_table=sandbox_table,
                                 mapping_obs=mapping_obs,
                                 **self._source_params())

        unresolved = UNRESOLVED_QUERY.render(
            sandbox_dataset=self.sandbox_dataset_id,
            unresolved_table=self.sandbox_table_for(UNRESOLVED_TABLE),
            **self._source_params())

        insert = INSERT_QUERY.render(
            project=self.project_id,
            dataset=self.dataset_id,
            obs=OBSERVATION,
            sandbox_dataset=self.sandbox_dataset_id,
            sandbox_table=sandbox_table,
            obs_fields=', '.join(
                field['name'] for field in fields_for(OBSERVATION)))

        append_mapping = APPEND_MAPPING_QUERY.render(
            project=self.project_id,
            dataset=self.dataset_id,
            mapping_obs=mapping_obs,
            sandbox_dataset=self.sandbox_dataset_id,
            sandbox_table=sandbox_table)

        return [{
            cdr_consts.QUERY: emit
        }, {
            cdr_consts.QUERY: unresolved
        }, {
            cdr_consts.QUERY: insert
        }, {
            cdr_consts.QUERY: append_mapping
        }]

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

    ARGS = parser.parse_args()

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(
            ARGS.project_id, ARGS.dataset_id, ARGS.sandbox_dataset_id,
            [(RoutePediatricAdultObservations,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(RoutePediatricAdultObservations,)])
