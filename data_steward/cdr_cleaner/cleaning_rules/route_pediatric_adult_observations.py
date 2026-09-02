"""
Emit observation records on the linked adult's person_id for the guardian-about
Pediatric Basics survey items.

The Pediatric Basics survey collects twelve items about the parent or guardian rather
than the child: education, marital status, living situation, employment, income, home
ownership, and two housing vitality items. They are reassessments of the adult and are
already mapped to standard concepts for the adult surveys. The Pediatrics CT+ PRD
requires that they produce observation records on the adult's person_id, so researchers
can read them as observations about the adult.

The pediatric participant's own row is left in place. The PRD's verb is create, and
under the relationship extension table it states that observation "Preserves the
original participant-provided responses", so preservation is the default here.

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

# The survey is identified by survey_source_value rather than survey_concept_id. No
# pediatric concept exists in the vocabulary yet, so survey_conduct.survey_concept_id
# arrives as 0 for these responses, exactly as it does today for HPOSitePairing and
# ProgramUpdate. survey_source_value carries the instrument name either way, so this
# predicate keeps working unchanged once a concept is minted.
PEDIATRIC_BASICS_SURVEY_SOURCE_VALUES = ['ped_basics']

# The twelve Pediatric Basics items that are about the parent or guardian. The
# numbering is the survey's own and is non-contiguous: the omitted numbers are items
# about the child.
#
# mapped_concept_id is what the pediatric survey mapping assigns to the question.
# standard_concept_id is what it resolves to through 'Maps to'. Six of the twelve are
# non-standard PPI Question concepts whose target is a different standard Observation
# concept, which is why this carries two columns rather than one. All twelve were
# re-verified against the vocabulary: every mapped concept exists, every target is
# standard and domain Observation, and the six standard ones map to themselves.
#
# standard_concept_id is a fallback, not an override. See EMIT_QUERY.
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
        'standard_concept_id': 1332833
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
    WHERE survey_source_value IN (
        {%- for v in survey_source_values %}'{{v}}'{{ ", " if not loop.last }}{% endfor %})
),
guardian_about_responses AS (
    /* Matched on either the source value or the source concept id. A pediatric row
       can arrive with observation_source_concept_id = 0 while carrying the PPI code,
       so keying on the concept id alone would silently skip it. The survey filter is
       applied first, so this cannot reach an adult Basics row carrying the same
       code. */
    SELECT o.*, i.standard_concept_id
    FROM `{{project}}.{{dataset}}.{{obs}}` o
    JOIN pediatric_surveys ps
      ON ps.survey_conduct_id = o.questionnaire_response_id
    JOIN items i
      ON LOWER(o.observation_source_value) = i.field
      OR o.observation_source_concept_id = i.mapped_concept_id
),
linkage AS (
    /* One row per pediatric participant. The pairs are emitted in both directions,
       so reading only side 1 already gives each participant its counterpart once.
       Excluding counterparts who are themselves pediatric keeps a sibling pair from
       being read as an adult link. n_adults is carried so an ambiguous participant
       can be recorded rather than fanned out into one record per candidate adult. */
    SELECT
        fr.fact_id_1 AS pediatric_person_id,
        ANY_VALUE(fr.fact_id_2) AS adult_person_id,
        COUNT(DISTINCT fr.fact_id_2) AS n_adults
    FROM `{{project}}.{{dataset}}.{{fact_relationship}}` fr
    WHERE fr.domain_concept_id_1 = {{person_domain_concept_id}}
      AND fr.domain_concept_id_2 = {{person_domain_concept_id}}
      AND fr.fact_id_2 NOT IN (SELECT person_id FROM pediatric_surveys)
    GROUP BY fr.fact_id_1
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
    /* The export already resolves observation_concept_id, including the cases where
       a refusal carries 903102 PMI instead of the item's own standard concept.
       Copying it keeps that shape; the declared standard concept only fills in when
       the export left the row unmapped, which pediatric rows have been observed
       doing. */
    IF(r.observation_concept_id = 0, r.standard_concept_id, r.observation_concept_id)
      AS observation_concept_id,
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
    /* Carried so the emitted record resolves to the pediatric survey's
       survey_conduct row. That is what makes it distinguishable from the adult's own
       answer to the same question, and it is the key the duplicate-response
       exclusion reads. */
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
    IF(l.pediatric_person_id IS NULL,
       'no person domain linkage row for this participant',
       'linked to more than one adult') AS unresolved_reason
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
            'project': self.project_id,
            'dataset': self.dataset_id,
            'obs': OBSERVATION,
            'survey_conduct': SURVEY_CONDUCT,
            'fact_relationship': FACT_RELATIONSHIP,
            'items': GUARDIAN_ABOUT_ITEMS,
            'survey_source_values': PEDIATRIC_BASICS_SURVEY_SOURCE_VALUES,
            'person_domain_concept_id': PERSON_DOMAIN_CONCEPT_ID,
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
