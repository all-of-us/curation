"""
For all answers for the survey question (43528428) and given pids,
    1. Mark answers as invalid for all participants
    2. Use the second survey (1384450) to generate valid answers for a subset of pids who took the second survey
"""
import logging

from common import JINJA_ENV, OBSERVATION, RDR
from constants.cdr_cleaner import clean_cdr as cdr_consts
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule, query_spec_list

LOGGER = logging.getLogger(__name__)

ORIGINAL_OBSERVATION_SOURCE_CONCEPT_ID = 43528428
HCAU_OBSERVATION_SOURCE_CONCEPT_ID = 1384450

ISSUE_NUMBERS = ['dc826']

INSURANCE_LOOKUP = 'insurance_lookup'
NEW_INSURANCE_ROWS = 'new_insurance_rows'
PIDS_LOOKUP_TABLE = 'health_insurance_pids'
PIPELINE_TABLES = 'pipeline_tables'

INSURANCE_LOOKUP_FIELDS = [{
    "type": "string",
    "name": "answer_for_obs_src_c_id_43528428",
    "mode": "nullable",
    "description": "Answers for observation_source_concept_id = 43528428"
}, {
    "type": "string",
    "name": "basics_value_source_value",
    "mode": "nullable",
    "description": "value_source_value field for the basics survey answer"
}, {
    "type": "integer",
    "name": "basics_value_source_concept_id",
    "mode": "nullable",
    "description": "value_source_concept_id for the basics survey answer"
}, {
    "type": "string",
    "name": "hcau_value_source_value",
    "mode": "nullable",
    "description": "value_source_value field for the HCAU survey answer"
}, {
    "type": "integer",
    "name": "hcau_value_source_concept_id",
    "mode": "nullable",
    "description": "value_source_concept_id for the HCAU survey answer"
}]

SANDBOX_CREATE_QUERY = JINJA_ENV.from_string("""
CREATE TABLE `{{project_id}}.{{sandbox_dataset_id}}.{{new_insurance_rows}}`
AS
SELECT
  *
FROM
(SELECT 
  ob.observation_id, 
  ob.person_id, 
  ob.observation_concept_id, 
  ob.observation_date, 
  ob.observation_datetime, 
  ob.observation_type_concept_id, 
  ob.value_as_number, 
  new_value_source_value AS value_as_string, 
  new_value_as_concept_id AS value_as_concept_id, 
  ob.qualifier_concept_id, 
  ob.unit_concept_id, 
  ob.provider_id, 
  ob.visit_occurrence_id, 
  'HealthInsurance_InsuranceTypeUpdate' AS observation_source_value, 
  {{ORIGINAL_OBSERVATION_SOURCE_CONCEPT_ID}} AS observation_source_concept_id, 
  ob.unit_source_value, 
  ob.qualifier_source_value, 
  new_value_source_concept_id AS value_source_concept_id, 
  new_value_source_value AS value_source_value, 
  ob.questionnaire_response_id
FROM `{{project_id}}.{{combined_dataset_id}}.observation` ob
JOIN (
  SELECT DISTINCT
    hcau_value_source_concept_id,
    source_c.concept_code AS new_value_source_value, 
    source_c.concept_id AS new_value_source_concept_id, 
    FIRST_VALUE(standard_c.concept_id) OVER (PARTITION BY source_c.concept_id ORDER BY c_r.relationship_id DESC)
    AS new_value_as_concept_id
  FROM 
    `{{project_id}}.{{sandbox_dataset_id}}.{{insurance_lookup}}`
  JOIN `{{project_id}}.{{combined_dataset_id}}.concept` source_c ON (basics_value_source_concept_id=source_c.concept_id)
  JOIN `{{project_id}}.{{combined_dataset_id}}.concept_relationship` c_r ON (source_c.concept_id=c_r.concept_id_1)
  JOIN `{{project_id}}.{{combined_dataset_id}}.concept` standard_c ON (standard_c.concept_id=c_r.concept_id_2)
  WHERE source_c.vocabulary_id='PPI' 
  AND c_r.relationship_id LIKE 'Maps to%'  --prefers the 'maps to value', but will take 'maps to' if necessary --
)
ON ob.value_source_concept_id = hcau_value_source_concept_id 
WHERE observation_source_concept_id IN ({{HCAU_OBSERVATION_SOURCE_CONCEPT_ID}})
AND person_id IN (SELECT person_id FROM
     `{{project_id}}.{{pipeline_tables}}.{{pids_lookup_table}}`)
)
""")

UPDATE_INVALID_QUERY = JINJA_ENV.from_string("""
UPDATE 
 `{{project_id}}.{{combined_dataset_id}}.observation` ob
SET
  ob.value_as_concept_id = 46237613,
  ob.value_as_string = 'Invalid',
  ob.value_source_concept_id = 46237613,
  ob.value_source_value = 'Invalid'
WHERE ob.observation_source_concept_id IN ({{ORIGINAL_OBSERVATION_SOURCE_CONCEPT_ID}})
AND ob.person_id IN (
    SELECT person_id FROM
     `{{project_id}}.{{pipeline_tables}}.{{pids_lookup_table}}`
     )
""")

DELETE_ORIGINAL_FOR_HCAU_PARTICIPANTS = JINJA_ENV.from_string("""
DELETE
FROM `{{project_id}}.{{combined_dataset_id}}.observation` ob
WHERE ob.observation_source_concept_id IN ({{ORIGINAL_OBSERVATION_SOURCE_CONCEPT_ID}})
AND ob.person_id IN 
(SELECT DISTINCT
  person_id
FROM `{{project_id}}.{{sandbox_dataset_id}}.{{new_insurance_rows}}`)
""")

INSERT_ANSWERS_FOR_HCAU_PARTICIPANTS = JINJA_ENV.from_string("""
INSERT INTO `{{project_id}}.{{combined_dataset_id}}.observation`
  (observation_id,
  person_id,
  observation_concept_id,
  observation_date,
  observation_datetime,
  observation_type_concept_id,
  value_as_number,
  value_as_string,
  value_as_concept_id,
  qualifier_concept_id,
  unit_concept_id,
  provider_id,
  visit_occurrence_id,
  observation_source_value,
  observation_source_concept_id,
  unit_source_value,
  qualifier_source_value,
  value_source_concept_id,
  value_source_value,
  questionnaire_response_id)
SELECT
  *
FROM `{{project_id}}.{{sandbox_dataset_id}}.{{new_insurance_rows}}`
""")


class MapHealthInsuranceResponses(BaseCleaningRule):

    def __init__(self,
                 project_id,
                 dataset_id,
                 sandbox_dataset_id,
                 table_namer=None):
        """
        Initialize the class.
        Set the issue numbers, description and affected datasets. As other tickets may affect
        this SQL, append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """
        desc = (
            'For observation_concept_id 43528428 and the given subset of pids -'
            'Marks answers as invalid for all participants and Use the second survey (1384450) '
            'to generate valid answers for the given participants')

        super().__init__(issue_numbers=ISSUE_NUMBERS,
                         description=desc,
                         affected_tables=[OBSERVATION],
                         affected_datasets=[RDR],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         table_namer=table_namer)

    def setup_rule(self, client, *args, **keyword_args):
        """
        Run required steps for validation setup

        Method to run to setup validation on cleaning rules that will be updating or deleting the values.
        For example:
        if your class updates all the datetime fields you should be implementing the
        logic to get the initial list of values which adhere to a condition we are looking for.

        if your class deletes a subset of rows in the tables you should be implementing
        the logic to get the row counts of the tables prior to applying cleaning rule

        """
        raise NotImplementedError("Please fix me.")

    def get_query_specs(self, *args, **keyword_args) -> query_spec_list:
        queries = []

        sandbox_query = dict()
        sandbox_query[cdr_consts.QUERY] = SANDBOX_CREATE_QUERY.render(
            project_id=self.project_id,
            combined_dataset_id=self.dataset_id,
            sandbox_dataset_id=self.sandbox_dataset_id,
            new_insurance_rows=NEW_INSURANCE_ROWS,
            insurance_lookup=INSURANCE_LOOKUP,
            ORIGINAL_OBSERVATION_SOURCE_CONCEPT_ID=
            ORIGINAL_OBSERVATION_SOURCE_CONCEPT_ID,
            HCAU_OBSERVATION_SOURCE_CONCEPT_ID=
            HCAU_OBSERVATION_SOURCE_CONCEPT_ID,
            pipeline_tables=PIPELINE_TABLES,
            pids_lookup_table=PIDS_LOOKUP_TABLE)
        queries.append(sandbox_query)

        invalidate_query = dict()
        invalidate_query[cdr_consts.QUERY] = UPDATE_INVALID_QUERY.render(
            project_id=self.project_id,
            combined_dataset_id=self.dataset_id,
            ORIGINAL_OBSERVATION_SOURCE_CONCEPT_ID=
            ORIGINAL_OBSERVATION_SOURCE_CONCEPT_ID,
            pipeline_tables=PIPELINE_TABLES,
            pids_lookup_table=PIDS_LOOKUP_TABLE)
        queries.append(invalidate_query)

        delete_query = dict()
        delete_query[
            cdr_consts.QUERY] = DELETE_ORIGINAL_FOR_HCAU_PARTICIPANTS.render(
                project_id=self.project_id,
                combined_dataset_id=self.dataset_id,
                sandbox_dataset_id=self.sandbox_dataset_id,
                new_insurance_rows=NEW_INSURANCE_ROWS,
                ORIGINAL_OBSERVATION_SOURCE_CONCEPT_ID=
                ORIGINAL_OBSERVATION_SOURCE_CONCEPT_ID)
        queries.append(delete_query)

        insert_query = dict()
        insert_query[
            cdr_consts.QUERY] = INSERT_ANSWERS_FOR_HCAU_PARTICIPANTS.render(
                project_id=self.project_id,
                combined_dataset_id=self.dataset_id,
                sandbox_dataset_id=self.sandbox_dataset_id,
                new_insurance_rows=NEW_INSURANCE_ROWS)
        queries.append(insert_query)

        return queries

    def get_sandbox_tablenames(self):
        """
        generates sandbox table names
        """
        raise NotImplementedError("Please fix me.")

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

    ARGS = parser.default_parse_args()

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(
            ARGS.project_id, ARGS.dataset_id, ARGS.sandbox_dataset_id,
            [(MapHealthInsuranceResponses,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(MapHealthInsuranceResponses,)])
