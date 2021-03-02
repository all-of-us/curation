# system imports
import logging
from typing import NamedTuple, Union

from enum import Enum

# project imports
from common import JINJA_ENV, PERSON
import constants.cdr_cleaner.clean_cdr as cdr_consts
from constants import bq_utils as bq_consts
from cdr_cleaner.cleaning_rules.deid.repopulate_person_using_observation import \
    AbstractRepopulatePerson

LOGGER = logging.getLogger(__name__)

JIRA_ISSUE_NUMBERS = ['DC1439']

# Gender question concept id
GENDER_IDENTITY_CONCEPT_ID = 1585838
# Generalized gender identity concept id
GENERALIZED_GENDER_IDENTITY_CONCEPT_ID = 2000000002
GENERALIZED_GENDER_IDENTITY_SOURCE_VALUE = 'GenderIdentity_GeneralizedDiffGender'

# Sex at birth question concept id
SEX_AT_BIRTH_CONCEPT_ID = 1585845

# Race question and response concepts
RACE_CONCEPT_ID = 1586140
RACE_RESPONSE_CONCEPT_IDS = [
    1586141, 1586142, 1586143, 1586144, 1586145, 1586146
]
GENERALIZED_RACE_CONCEPT_ID = 2000000008
GENERALIZED_RACE_SOURCE_VALUE = 'WhatRaceEthnicity_GeneralizedMultPopulations'
# Hispanic or Latino response concept id
HISPANIC_LATINO_CONCEPT_ID = 1586147
HISPANIC_LATINO_CONCEPT_SOURCE_VALUE = 'WhatRaceEthnicity_Hispanic'

# Aou Non Indicated concept_id and the corresponding source value
AOU_NONE_INDICATED_CONCEPT_ID = 2100000001
AOU_NONE_INDICATED_SOURCE_VALUE = 'AoUDRC_NoneIndicated'

# OMOP non matching concept id
NO_MATCHING_CONCEPT_ID = 0
NO_MATCHING_SOURCE_VALUE = 'No matching concept'

# Observation fields
OBSERVATION_SOURCE_CONCEPT_ID = 'observation_source_concept_id'
VALUE_SOURCE_CONCEPT_ID = 'value_source_concept_id'


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
    mr.*,
    IF(mr.is_generalized = 1, {{generalized_concept}}, mr.{{prefix}}_concept_ids[OFFSET(0)]) AS {{prefix}}_concept_id,
    IF(mr.is_generalized = 1, {{generalized_concept}}, mr.{{prefix}}_source_concept_ids[OFFSET(0)]) AS {{prefix}}_source_concept_id,
    IF(mr.is_generalized = 1, '{{generalized_concept_source_value}}', mr.{{prefix}}_source_values[OFFSET(0)]) AS {{prefix}}_source_value
FROM multiple_ppi_responses AS mr
""")

SINGLE_RESPONSE_QUERY_TEMPLATE = JINJA_ENV.from_string("""
SELECT DISTINCT
    o.* EXCEPT (rank_order)
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
            default_answer_source_value=NO_MATCHING_SOURCE_VALUE)

        return {
            cdr_consts.QUERY: gender_query,
            cdr_consts.DESTINATION_DATASET: self.sandbox_dataset_id,
            cdr_consts.DISPOSITION: bq_consts.WRITE_TRUNCATE,
            cdr_consts.DESTINATION_TABLE: gender_sandbox_table
        }

    def get_sex_at_birth_query(self, sex_at_birth_sandbox_table) -> dict:
        sex_at_birth_join_expressions = [
            JoinExpression(field_name=OBSERVATION_SOURCE_CONCEPT_ID,
                           join_operator=JoinOperator.EQUAL,
                           value=SEX_AT_BIRTH_CONCEPT_ID)
        ]

        sex_at_birth_query = SINGLE_RESPONSE_QUERY_TEMPLATE.render(
            project=self.project_id,
            dataset=self.dataset_id,
            prefix=self.SEX_AT_BIRTH,
            join_expressions=sex_at_birth_join_expressions,
            default_answer_concept_id=NO_MATCHING_CONCEPT_ID,
            default_answer_source_value=NO_MATCHING_SOURCE_VALUE)

        return {
            cdr_consts.QUERY: sex_at_birth_query,
            cdr_consts.DESTINATION_DATASET: self.sandbox_dataset_id,
            cdr_consts.DISPOSITION: bq_consts.WRITE_TRUNCATE,
            cdr_consts.DESTINATION_TABLE: sex_at_birth_sandbox_table
        }

    def get_race_query(self, race_sandbox_table) -> dict:
        race_join_expressions = [
            JoinExpression(field_name=OBSERVATION_SOURCE_CONCEPT_ID,
                           join_operator=JoinOperator.EQUAL,
                           value=RACE_CONCEPT_ID),
            JoinExpression(field_name=VALUE_SOURCE_CONCEPT_ID,
                           join_operator=JoinOperator.NOT_EQUAL,
                           value=HISPANIC_LATINO_CONCEPT_ID),
            JoinExpression(
                field_name=VALUE_SOURCE_CONCEPT_ID,
                join_operator=JoinOperator.IN,
                value=f'({",".join(map(str, RACE_RESPONSE_CONCEPT_IDS))})'),
        ]

        race_query = MULTIPLE_RESPONSES_GENERALIZATION_QUERY_TEMPLATE.render(
            project=self.project_id,
            dataset=self.dataset_id,
            prefix=self.RACE,
            generalized_concept=GENERALIZED_RACE_CONCEPT_ID,
            generalized_concept_source_value=GENERALIZED_RACE_SOURCE_VALUE,
            join_expressions=race_join_expressions,
            default_answer_concept_id=NO_MATCHING_CONCEPT_ID,
            default_answer_source_value=NO_MATCHING_SOURCE_VALUE)

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

        ethnicity_query = SINGLE_RESPONSE_QUERY_TEMPLATE.render(
            project=self.project_id,
            dataset=self.dataset_id,
            prefix=self.ETHNICITY,
            join_expressions=ethnicity_join_expressions,
            default_answer_concept_id=AOU_NONE_INDICATED_CONCEPT_ID,
            default_answer_source_value=AOU_NONE_INDICATED_SOURCE_VALUE)

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
