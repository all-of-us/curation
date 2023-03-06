"""
An abstract survey backfill class that expects an instantiating class to
provide a list of observation_source_concept_ids to use for backfilling.
If a participant has answered at least one of these questions, but has not
answered them all, this rule is responsible for filling in the missing
responses with skip codes.  

This abstract class does most of the backfill work with customization provided
by extending classes. It expects extending classes to provide the list of
concepts that will be used to determine backfill eligibility.  
It also allows extending classes to define criteria that may be specific to a
sub-group (e.g. we only backfill menstruation questions for female participants).

Original issue: DC-3096
"""
# Python imports
import logging
from typing import Dict, List

# Project imports
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule, query_spec_list
from constants.cdr_cleaner.clean_cdr import QUERY
from common import JINJA_ENV, OBSERVATION, PERSON
from resources import fields_for

LOGGER = logging.getLogger(__name__)

BACKFILL_QUERY = JINJA_ENV.from_string("""
INSERT INTO `{{project}}.{{dataset}}.{{obs}}`
({{obs_fields}})
WITH person_who_answered_survey AS (
    SELECT
        person_id,
        MAX(observation_date) AS observation_date,
        MAX(observation_datetime) AS observation_datetime,
    FROM `{{project}}.{{dataset}}.{{obs}}`
    WHERE observation_source_concept_id IN ({{backfill_concepts}})
    GROUP BY person_id
),
backfill_survey AS (
    SELECT DISTINCT 
        observation_concept_id,
        observation_source_concept_id,
        observation_source_value,
        observation_type_concept_id
    FROM `{{project}}.{{dataset}}.{{obs}}`
    WHERE observation_source_concept_id IN ({{backfill_concepts}})
),
{% if additional_backfill_conditions -%}
person_info_for_additional_condition AS (
    SELECT * FROM `{{project}}.{{dataset}}.{{pers}}`
    WHERE person_id IN (
        SELECT person_id FROM person_who_answered_survey
    )
),
{% endif %}
missing_survey AS (
    SELECT 
        pwas.person_id,
        bs.observation_concept_id,
        pwas.observation_date,
        pwas.observation_datetime,
        bs.observation_type_concept_id,
        bs.observation_source_value,
        bs.observation_source_concept_id
    FROM person_who_answered_survey pwas
    CROSS JOIN backfill_survey bs
    {% if additional_backfill_conditions -%}
    JOIN person_info_for_additional_condition pi
    ON pwas.person_id = pi.person_id
    {% endif %}
    WHERE NOT EXISTS (
        SELECT 1 FROM `{{project}}.{{dataset}}.{{obs}}` o
        WHERE pwas.person_id = o.person_id
        AND bs.observation_source_concept_id = o.observation_source_concept_id
    )
    {% if additional_backfill_conditions -%}
    AND {% for concept in additional_backfill_conditions %}
        (
            observation_source_concept_id != {{concept}}
            OR (
                observation_source_concept_id = {{concept}}
                AND {{additional_backfill_conditions[concept]}}
            )
        )
    {% if not loop.last -%} AND {% endif %}
    {% endfor %}
    {% endif %}
)
SELECT
    ROW_NUMBER() OVER(
        ORDER BY person_id, observation_source_concept_id
    ) + (
        SELECT MAX(observation_id) 
        FROM `{{project}}.{{dataset}}.{{obs}}`
    ) AS observation_id,
    person_id,
    observation_concept_id,
    observation_date,
    observation_datetime,
    observation_type_concept_id,
    NULL AS value_as_number,
    CAST(NULL AS STRING) AS value_as_string,
    903096 AS value_as_concept_id,
    0 AS qualifier_concept_id,
    0 AS unit_concept_id,
    NULL AS provider_id,
    NULL AS visit_occurrence_id,
    NULL AS visit_detail_id,
    observation_source_value,
    observation_source_concept_id,
    CAST(NULL AS STRING) AS unit_source_value,
    CAST(NULL AS STRING) AS qualifier_source_value,
    903096 AS value_source_concept_id,
    'PMI_Skip' AS value_source_value, 
    NULL AS questionnaire_response_id 
FROM missing_survey
""")


class AbstractBackfillSurveyRecords(BaseCleaningRule):
    """
    Abstract class for backfill cleaning rules.
    """

    def __init__(self,
                 project_id,
                 dataset_id,
                 sandbox_dataset_id,
                 issue_numbers,
                 description,
                 affected_datasets,
                 affected_tables,
                 backfill_concepts: List[int],
                 additional_backfill_conditions: Dict[int, str] = {},
                 table_namer=None):
        """
        Args that are unique to this abstract class:
        - backfill_concepts (mandatory)
            List of the concept IDs that need backfilling.            
        - additional_backfill_conditions (optional)
            Dict of the additional condition for backfilling.
            Key: Concept ID that the additional condition is applied to.
            Value: Condition written using BigQuery operators('=', '!=', '<', etc)
                   for the concept ID.
            Example:
                An example for concept ID = 1585784 if it needs to be
                backfilled only when the partiticipant's
                gender_concept_id is 8532:
                '''        
                additional_backfill_conditions = {
                    1585784: 'gender_concept_id = 8532',
                }
                '''
        """

        super().__init__(issue_numbers=issue_numbers,
                         description=description,
                         affected_datasets=affected_datasets,
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         affected_tables=affected_tables,
                         table_namer=table_namer)

        self.backfill_concepts = backfill_concepts
        self.additional_backfill_conditions = additional_backfill_conditions

    def setup_rule(self, client):
        pass

    def get_sandbox_tablenames(self):
        # No sandbox table exists for this CR because it runs only an INSERT statement.
        return []

    def get_query_specs(self) -> query_spec_list:
        query = BACKFILL_QUERY.render(
            project=self.project_id,
            dataset=self.dataset_id,
            obs=OBSERVATION,
            pers=PERSON,
            obs_fields=', '.join(
                field['name'] for field in fields_for(OBSERVATION)),
            backfill_concepts=", ".join(
                [str(concept_id) for concept_id in self.backfill_concepts]),
            additional_backfill_conditions=self.additional_backfill_conditions)

        return [{QUERY: query}]
