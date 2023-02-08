"""Repopulate the person table using the PPI responses stored in the observation table for
gender, race, and ethnicity. For multi-select questions such as gender and race,
PNA and other responses are mutually exclusive so we don't need to worry about the cases where a
participant submits multiple responses and one of which is PNA. In addition, the Birth related
fields in the person table are generalized as well.  Below is a high-level summary of how we
populate each type of demographics information.

Race: since race and ethnicity are grouped under the same question  "what is your
race/ethnicity?" when querying the race responses only, we need to exclude ethnicity response,
this can be done by value_source_concept_id != 1586147. This makes the assumption that all
responses except for value_source_concept_id != 1586147 are valid race responses.

Then we translate the PPI race concepts manually to the standard OMOP race concepts. The reason
we need to do this is that PPI race concepts are in the Answer class whereas the OMOP race
concepts are in the race class such mappings do not exist in concept_relationship. In case of
multiple race responses, we replace the multiple responses with a generalized concept 2000000008
(WhatRaceEthnicity_GeneralizedMultPopulations).

Ethnicity: "Hispanic or Latino" is one of the responses in "what is your race/ethnicity?" question.
Participants can only indicate their ethnicity to be "Hispanic or Latino" but doesn't have the
option to indicate they are "Not Hispanic or Latino". We manually map the PPI response 1586147 to
the standard OMOP concept 38003563. For those participants who didn't check this option,
non-Hispanic options are expanded as done in the registered tier repopulation.

Gender: the standard OMOP gender concepts are very limiting and the PPI gender concepts are used
instead, so there is no manual mapping for gender. In case of multiple gender responses,
we replace the multiple responses with a generalized concept 2000000002 (
GenderIdentity_GeneralizedDiffGender)

birth information:
    - null out month_of_birth and day_of_birth fields.
    - year_of_birth remains the same
    - birth_datetime: defaults to June 15, year_of_birth 00:00:00

Per ticket DC-1584, The sex_at_birth_concept_id, sex_at_ birth_source_concept_id, and sex_at_birth_source_value columns
were defined and set in multiple repopulate person scripts. This was redundant and caused unwanted schema changes for
the person table.  With the implementation of DC-1514 and DC-1570 these columns are to be removed from all
repopulate_person_* files.
"""

# system imports
import logging
from typing import NamedTuple, Union, List

from enum import Enum

# project imports
from common import JINJA_ENV, PERSON
import constants.cdr_cleaner.clean_cdr as cdr_consts
from constants import bq_utils as bq_consts
from cdr_cleaner.cleaning_rules.deid.repopulate_person_using_observation import \
    AbstractRepopulatePerson, ConceptTranslation

LOGGER = logging.getLogger(__name__)

JIRA_ISSUE_NUMBERS = ['DC1439', 'DC1446', 'DC1584', 'DC2273']

# Gender question concept id
GENDER_IDENTITY_CONCEPT_ID = 1585838
# Generalized gender identity concept id
GENERALIZED_GENDER_IDENTITY_CONCEPT_ID = 2000000002
GENERALIZED_GENDER_IDENTITY_SOURCE_VALUE = 'GenderIdentity_GeneralizedDiffGender'

# Race question and response concepts
RACE_CONCEPT_ID = 1586140
GENERALIZED_RACE_CONCEPT_ID = 2000000008
GENERALIZED_RACE_SOURCE_VALUE = 'WhatRaceEthnicity_GeneralizedMultPopulations'

# Hispanic or Latino response concept id
HISPANIC_LATINO_CONCEPT_ID = 1586147
HISPANIC_LATINO_CONCEPT_SOURCE_VALUE = 'WhatRaceEthnicity_Hispanic'
HISPANIC_LATINO_STANDARD_CONCEPT_ID = 38003563
HISPANIC_LATINO_STANDARD_SOURCE_VALUE = "Hispanic"
# Prefer not to answer
PNA_CONCEPT_ID = 903079
PNA_CONCEPT_SOURCE_VALUE = 'PMI_PreferNotToAnswer'

# None of these
NONE_OF_THESE_CONCEPT_ID = 1586148
NONE_OF_THESE_CONCEPT_SOURCE_VALUE = 'WhatRaceEthnicity_RaceEthnicityNoneOfThese'

# Non hispanic
NON_HISPANIC_LATINO_CONCEPT_ID = 38003564
NON_HISPANIC_LATINO_CONCEPT_SOURCE_VALUE = "Not Hispanic"

# OMOP non matching concept id
NO_MATCHING_CONCEPT_ID = 0
NO_MATCHING_SOURCE_VALUE = 'No matching concept'

# Skip concept
SKIP_CONCEPT_ID = 903096
SKIP_CONCEPT_SOURCE_VALUE = "PMI_Skip"

# Observation fields
OBSERVATION_SOURCE_CONCEPT_ID = 'observation_source_concept_id'
VALUE_SOURCE_CONCEPT_ID = 'value_source_concept_id'

AOU_NONE_INDICATED_CONCEPT_ID = 2100000001
AOU_NONE_INDICATED_SOURCE_VALUE = 'AoUDRC_NoneIndicated'


class JoinOperator(Enum):
    EQUAL = '='
    NOT_EQUAL = '!='
    IN = 'IN'


class JoinExpression(NamedTuple):
    field_name: str
    join_operator: JoinOperator
    value: Union[str, int]


MULTIPLE_RESPONSES_GENERALIZATION_QUERY_TEMPLATE = JINJA_ENV.from_string("""
WITH ppi_response AS
(
    SELECT DISTINCT
        p.person_id,
        COALESCE(o.value_as_concept_id, {{default_answer_concept_id}}) AS {{prefix}}_concept_id,
        COALESCE(o.value_source_concept_id, {{default_answer_concept_id}}) AS {{prefix}}_source_concept_id,
        COALESCE(c.concept_code, '{{default_answer_source_value}}') AS {{prefix}}_source_value,
        o.observation_datetime
    FROM `{{project}}.{{dataset}}.person` AS p
    LEFT JOIN `{{project}}.{{dataset}}.observation` AS o
        ON p.person_id = o.person_id
    {% for join_expression in join_expressions %}
            AND o.{{join_expression.field_name}} {{join_expression.join_operator.value}} {{join_expression.value}}
    {% endfor %}
    LEFT JOIN `{{project}}.{{dataset}}.concept` AS c
        ON o.value_source_concept_id = concept_id
),
multiple_ppi_responses AS
(
SELECT
    g.person_id,
    CAST(COUNT(g.{{prefix}}_concept_id) > 1 AS INT64) AS is_generalized,
    ARRAY_AGG(g.{{prefix}}_concept_id ORDER BY g.observation_datetime DESC) AS {{prefix}}_concept_ids,
    ARRAY_AGG(g.{{prefix}}_source_concept_id ORDER BY g.observation_datetime DESC) AS {{prefix}}_source_concept_ids,
    ARRAY_AGG(g.{{prefix}}_source_value ORDER BY g.observation_datetime DESC) AS {{prefix}}_source_values
FROM ppi_response AS g
GROUP BY g.person_id
)

SELECT
    mr.*
    {% if translate_source_concepts is not none and translate_source_concepts|length > 0 -%}{{'\t'}}
    REPLACE(
        CASE {{prefix}}_source_concept_id
        {% for translate_source_concept in translate_source_concepts %}
            WHEN {{translate_source_concept.concept_id}} THEN {{translate_source_concept.translated_concept_id}}
            {%- if translate_source_concept.comment is not none %} /*{{translate_source_concept.comment}}*/ {% endif %}{{'\n'}}
        {%- endfor %}
            ELSE {{prefix}}_concept_id
        END AS {{prefix}}_concept_id
    )
    {% endif %}
FROM
(
    SELECT
        mr.*,
        IF(mr.is_generalized = 1, {{generalized_concept}}, mr.{{prefix}}_concept_ids[OFFSET(0)]) AS {{prefix}}_concept_id,
        IF(mr.is_generalized = 1, {{generalized_concept}}, mr.{{prefix}}_source_concept_ids[OFFSET(0)]) AS {{prefix}}_source_concept_id,
        IF(mr.is_generalized = 1, '{{generalized_concept_source_value}}', mr.{{prefix}}_source_values[OFFSET(0)]) AS {{prefix}}_source_value
    FROM multiple_ppi_responses AS mr
) mr
""")

SINGLE_RESPONSE_QUERY_TEMPLATE = JINJA_ENV.from_string("""
SELECT DISTINCT
    o.* EXCEPT (rank_order)
    {% if translate_source_concepts is not none and translate_source_concepts|length > 0 -%}{{'\t'}}
    REPLACE(
        CASE {{prefix}}_source_concept_id
        {% for translate_source_concept in translate_source_concepts %}
            WHEN {{translate_source_concept.concept_id}} THEN {{translate_source_concept.translated_concept_id}}
            {%- if translate_source_concept.comment is not none %} /*{{translate_source_concept.comment}}*/{%- endif %}{{'\n'}}
        {%- endfor %}
            ELSE {{prefix}}_concept_id
        END AS {{prefix}}_concept_id
    )
    {% endif %}
FROM
(
    SELECT
        p.person_id,
        COALESCE(o.value_as_concept_id, {{default_answer_concept_id}}) AS {{prefix}}_concept_id,
        COALESCE(o.value_source_concept_id, {{default_answer_concept_id}}) AS {{prefix}}_source_concept_id,
        COALESCE(c.concept_code, '{{default_answer_source_value}}') AS {{prefix}}_source_value,
        DENSE_RANK() OVER(PARTITION BY p.person_id ORDER BY o.observation_datetime DESC, o.observation_id DESC) AS rank_order
    FROM `{{project}}.{{dataset}}.person` AS p
    LEFT JOIN `{{project}}.{{dataset}}.observation` AS o
        ON p.person_id = o.person_id
    {% for join_expression in join_expressions %}
            AND o.{{join_expression.field_name}} {{join_expression.join_operator.value}} {{join_expression.value}}
    {% endfor %}
    LEFT JOIN `{{project}}.{{dataset}}.concept` AS c
        ON value_source_concept_id = concept_id
) AS o
WHERE o.rank_order = 1
""")

ETHNICITY_QUERY_TEMPLATE = JINJA_ENV.from_string("""
SELECT DISTINCT
    o.*
    EXCEPT(rank_order)
    REPLACE(
        CASE ethnicity_source_concept_id
            WHEN {{hispanic_latino_concept_id}} THEN {{hispanic_latino_standard_concept_id}}
            ELSE ethnicity_concept_id
        END AS ethnicity_concept_id,
        CASE ethnicity_source_concept_id
            WHEN {{hispanic_latino_concept_id}} THEN {{hispanic_latino_standard_concept_id}}
            ELSE ethnicity_source_concept_id
        END AS ethnicity_source_concept_id
    )
FROM
(
    SELECT
        p.person_id,
        IF (
            ethnicity_ob.value_as_concept_id IS NULL,
            CASE
                WHEN race_ob.value_source_concept_id = {{no_matching_concept_id}} THEN {{no_matching_concept_id}}
                WHEN race_ob.value_source_concept_id IS NULL THEN {{no_matching_concept_id}}
                WHEN race_ob.value_source_concept_id = {{pna_concept_id}} THEN {{pna_concept_id}}
                WHEN race_ob.value_source_concept_id = {{skip_concept_id}} THEN {{skip_concept_id}}
                WHEN race_ob.value_source_concept_id = {{none_of_these_concept_id}} THEN {{none_of_these_concept_id}}
            ELSE {{non_hispanic_latino_concept_id}} END,
            {{hispanic_latino_concept_id}}
        ) AS ethnicity_concept_id,
        IF (
            ethnicity_ob.value_as_concept_id IS NULL,
            CASE
                WHEN race_ob.value_source_concept_id = {{no_matching_concept_id}} THEN {{no_matching_concept_id}}
                WHEN race_ob.value_source_concept_id IS NULL THEN {{no_matching_concept_id}}
                WHEN race_ob.value_source_concept_id = {{pna_concept_id}} THEN {{pna_concept_id}}
                WHEN race_ob.value_source_concept_id = {{skip_concept_id}} THEN {{skip_concept_id}}
                WHEN race_ob.value_source_concept_id = {{none_of_these_concept_id}} THEN {{none_of_these_concept_id}}
            ELSE {{non_hispanic_latino_concept_id}} END,
            ethnicity_ob.value_source_concept_id
        ) AS ethnicity_source_concept_id,
        IF (
            ethnicity_ob.value_as_concept_id IS NULL,
            CASE
                WHEN race_ob.value_source_concept_id = {{no_matching_concept_id}} THEN "{{no_matching_source_value}}"
                WHEN race_ob.value_source_concept_id IS NULL THEN "{{no_matching_source_value}}"
                WHEN race_ob.value_source_concept_id = {{pna_concept_id}} THEN "{{pna_concept_source_value}}"
                WHEN race_ob.value_source_concept_id = {{skip_concept_id}} THEN "{{skip_concept_source_value}}"
                WHEN race_ob.value_source_concept_id = {{none_of_these_concept_id}} THEN "{{none_of_these_concept_source_value}}"
            ELSE "{{non_hispanic_latino_concept_source_value}}" END,
            "{{hispanic_latino_standard_source_value}}"
        ) AS ethnicity_source_value,
        DENSE_RANK() OVER(PARTITION BY p.person_id ORDER BY ethnicity_ob.observation_datetime DESC, ethnicity_ob.observation_id DESC) AS rank_order
    FROM `{{project}}.{{dataset}}.person` AS p
    LEFT JOIN `{{project}}.{{dataset}}.observation` AS ethnicity_ob
    ON p.person_id = ethnicity_ob.person_id
    {% for join_expression in ethnicity_join_expressions %}
    AND ethnicity_ob.{{join_expression.field_name}} {{join_expression.join_operator.value}} {{join_expression.value}}
    {% endfor %}
    LEFT JOIN `{{project}}.{{dataset}}.observation` AS race_ob
    ON p.person_id = race_ob.person_id
    {% for join_expression in race_join_expressions %}
    AND race_ob.{{join_expression.field_name}} {{join_expression.join_operator.value}} {{join_expression.value}}
    {% endfor %}
) AS o
WHERE o.rank_order = 1
""")

BIRTH_INFO_QUERY_TEMPLATE = JINJA_ENV.from_string("""
SELECT DISTINCT
    p.person_id,
    p.year_of_birth,
    CAST(CONCAT(p.year_of_birth, '-06-15') AS TIMESTAMP) AS birth_datetime,
    NULL AS month_of_birth,
    NULL AS day_of_birth
FROM `{{project}}.{{dataset}}.person` AS p
""")


class RepopulatePersonControlledTier(AbstractRepopulatePerson):

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
            'Returns a parsed query to repopulate the person table using observation.'
        )

        super().__init__(
            issue_numbers=JIRA_ISSUE_NUMBERS,
            description=desc,
            affected_datasets=[cdr_consts.CONTROLLED_TIER_DEID_BASE],
            project_id=project_id,
            dataset_id=dataset_id,
            sandbox_dataset_id=sandbox_dataset_id,
            affected_tables=[PERSON],
            table_namer=table_namer)

    def setup_rule(self, client, *args, **keyword_args):
        pass

    def setup_validation(self, client, *args, **keyword_args):
        pass

    def validate_rule(self, client):
        pass

    def get_gender_query(self, gender_sandbox_table) -> dict:
        gender_join_expressions = [
            JoinExpression(field_name=OBSERVATION_SOURCE_CONCEPT_ID,
                           join_operator=JoinOperator.EQUAL,
                           value=GENDER_IDENTITY_CONCEPT_ID)
        ]

        gender_query = MULTIPLE_RESPONSES_GENERALIZATION_QUERY_TEMPLATE.render(
            project=self.project_id,
            dataset=self.dataset_id,
            prefix=self.GENDER,
            generalized_concept=GENERALIZED_GENDER_IDENTITY_CONCEPT_ID,
            generalized_concept_source_value=
            GENERALIZED_GENDER_IDENTITY_SOURCE_VALUE,
            join_expressions=gender_join_expressions,
            default_answer_concept_id=NO_MATCHING_CONCEPT_ID,
            default_answer_source_value=NO_MATCHING_SOURCE_VALUE,
            translate_source_concepts=self.get_gender_manual_translation())

        return {
            cdr_consts.QUERY: gender_query,
            cdr_consts.DESTINATION_DATASET: self.sandbox_dataset_id,
            cdr_consts.DISPOSITION: bq_consts.WRITE_TRUNCATE,
            cdr_consts.DESTINATION_TABLE: gender_sandbox_table
        }

    def get_race_query(self, race_sandbox_table) -> dict:
        race_join_expressions = [
            JoinExpression(field_name=OBSERVATION_SOURCE_CONCEPT_ID,
                           join_operator=JoinOperator.EQUAL,
                           value=RACE_CONCEPT_ID),
            JoinExpression(field_name=VALUE_SOURCE_CONCEPT_ID,
                           join_operator=JoinOperator.NOT_EQUAL,
                           value=HISPANIC_LATINO_CONCEPT_ID)
        ]

        race_query = MULTIPLE_RESPONSES_GENERALIZATION_QUERY_TEMPLATE.render(
            project=self.project_id,
            dataset=self.dataset_id,
            prefix=self.RACE,
            generalized_concept=GENERALIZED_RACE_CONCEPT_ID,
            generalized_concept_source_value=GENERALIZED_RACE_SOURCE_VALUE,
            join_expressions=race_join_expressions,
            default_answer_concept_id=AOU_NONE_INDICATED_CONCEPT_ID,
            default_answer_source_value=AOU_NONE_INDICATED_SOURCE_VALUE,
            translate_source_concepts=self.get_race_manual_translation())

        return {
            cdr_consts.QUERY: race_query,
            cdr_consts.DESTINATION_DATASET: self.sandbox_dataset_id,
            cdr_consts.DISPOSITION: bq_consts.WRITE_TRUNCATE,
            cdr_consts.DESTINATION_TABLE: race_sandbox_table
        }

    def get_ethnicity_query(self, ethnicity_sandbox_table) -> dict:
        ethnicity_join_expressions = [
            JoinExpression(field_name=OBSERVATION_SOURCE_CONCEPT_ID,
                           join_operator=JoinOperator.EQUAL,
                           value=RACE_CONCEPT_ID),
            JoinExpression(field_name=VALUE_SOURCE_CONCEPT_ID,
                           join_operator=JoinOperator.EQUAL,
                           value=HISPANIC_LATINO_CONCEPT_ID)
        ]

        race_join_expressions = [
            JoinExpression(field_name=OBSERVATION_SOURCE_CONCEPT_ID,
                           join_operator=JoinOperator.EQUAL,
                           value=RACE_CONCEPT_ID),
            JoinExpression(field_name=VALUE_SOURCE_CONCEPT_ID,
                           join_operator=JoinOperator.NOT_EQUAL,
                           value=HISPANIC_LATINO_CONCEPT_ID)
        ]

        ethnicity_query = ETHNICITY_QUERY_TEMPLATE.render(
            project=self.project_id,
            dataset=self.dataset_id,
            ethnicity_join_expressions=ethnicity_join_expressions,
            race_join_expressions=race_join_expressions,
            no_matching_concept_id=NO_MATCHING_CONCEPT_ID,
            no_matching_source_value=NO_MATCHING_SOURCE_VALUE,
            skip_concept_id=SKIP_CONCEPT_ID,
            skip_concept_source_value=SKIP_CONCEPT_SOURCE_VALUE,
            pna_concept_id=PNA_CONCEPT_ID,
            pna_concept_source_value=PNA_CONCEPT_SOURCE_VALUE,
            none_of_these_concept_id=NONE_OF_THESE_CONCEPT_ID,
            none_of_these_concept_source_value=
            NONE_OF_THESE_CONCEPT_SOURCE_VALUE,
            non_hispanic_latino_concept_id=NON_HISPANIC_LATINO_CONCEPT_ID,
            non_hispanic_latino_concept_source_value=
            NON_HISPANIC_LATINO_CONCEPT_SOURCE_VALUE,
            hispanic_latino_concept_id=HISPANIC_LATINO_CONCEPT_ID,
            hispanic_latino_standard_concept_id=
            HISPANIC_LATINO_STANDARD_CONCEPT_ID,
            hispanic_latino_standard_source_value=
            HISPANIC_LATINO_STANDARD_SOURCE_VALUE,
            translate_source_concepts=self.get_ethnicity_manual_translation())

        return {
            cdr_consts.QUERY: ethnicity_query,
            cdr_consts.DESTINATION_DATASET: self.sandbox_dataset_id,
            cdr_consts.DISPOSITION: bq_consts.WRITE_TRUNCATE,
            cdr_consts.DESTINATION_TABLE: ethnicity_sandbox_table
        }

    def get_birth_info_query(self, birth_info_sandbox_table) -> dict:
        birth_info_query = BIRTH_INFO_QUERY_TEMPLATE.render(
            project=self.project_id,
            dataset=self.dataset_id,
        )

        return {
            cdr_consts.QUERY: birth_info_query,
            cdr_consts.DESTINATION_DATASET: self.sandbox_dataset_id,
            cdr_consts.DISPOSITION: bq_consts.WRITE_TRUNCATE,
            cdr_consts.DESTINATION_TABLE: birth_info_sandbox_table
        }

    def get_gender_manual_translation(self) -> List[ConceptTranslation]:
        pass

    def get_race_manual_translation(self) -> List[ConceptTranslation]:
        """
        The manual mapping of PPI concepts to the standard OMOP race concepts.
        Find all the  answers of the PPI race questions here
        https://athena.ohdsi.org/search-terms/terms/1586140.
        Find all the standard OMOP race concepts here
        https://athena.ohdsi.org/search-terms/terms?conceptClass=Race.

        :return:
        """
        return [
            ConceptTranslation(concept_id=1586142,
                               translated_concept_id=8515,
                               comment='asian'),
            ConceptTranslation(concept_id=1586143,
                               translated_concept_id=8516,
                               comment='black/aa'),
            ConceptTranslation(concept_id=1586146,
                               translated_concept_id=8527,
                               comment='white'),
            ConceptTranslation(concept_id=1586141,
                               translated_concept_id=8657,
                               comment='AIAN'),
            ConceptTranslation(concept_id=1586145,
                               translated_concept_id=8557,
                               comment='NHPI'),
            ConceptTranslation(concept_id=1586144,
                               translated_concept_id=38003615,
                               comment='MENA')
        ]

    def get_ethnicity_manual_translation(self) -> List[ConceptTranslation]:
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
