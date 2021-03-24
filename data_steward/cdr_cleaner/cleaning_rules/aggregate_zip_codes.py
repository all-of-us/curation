"""
To further obfuscate participant identity, some generalized zip codes will be aggregated together.
The PII zip code and state will be transformed to a neighboring zip code/state pair for 
those zip codes with low population density.
It is expected that this lookup table will be static and will remain unchanged. 
It is based on US population, and not on participant address metrics.

Original Issues: DC-1379, DC-1504
"""

# Python imports
import logging

# Project imports
import constants.bq_utils as bq_consts
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule
from cdr_cleaner.cleaning_rules.generalize_zip_codes import GeneralizeZipCodes
from cdr_cleaner.cleaning_rules.deid.string_fields_suppression import StringFieldsSuppression
from constants.cdr_cleaner import clean_cdr as cdr_consts
from common import JINJA_ENV, OBSERVATION, PIPELINE_TABLES
from utils import pipeline_logging

LOGGER = logging.getLogger(__name__)
PII_STATE_VOCAB = 'pii_state_vocab'
ZIP_CODE_AGGREGATION_MAP = 'zip_code_aggregation_map'
ZIP_CODES_AND_STATES_TO_MODIFY = '_zip_codes_and_states_to_modify'

SANDBOX_QUERY = JINJA_ENV.from_string("""
WITH modified_zip_codes_and_states AS (
    SELECT DISTINCT
        zip_code_obs.person_id, zip_code_obs.observation_id zip_code_observation_id,
        map.zip_code_3, map.transformed_zip_code_3,
        state_obs.observation_id state_observation_id, 
        map.state, map.transformed_state
    FROM `{{project_id}}.{{dataset_id}}.{{obs_table}}` zip_code_obs
    JOIN `{{project_id}}.{{pipeline_tables_dataset}}.{{zip_code_aggregation_map}}` map
        ON map.zip_code_3 = zip_code_obs.value_as_string
    JOIN `{{project_id}}.{{pipeline_tables_dataset}}.{{pii_state_vocab}}` state_vocab
        ON state_vocab.postal_code = map.state
    LEFT JOIN `{{project_id}}.{{dataset_id}}.{{obs_table}}` state_obs
        ON state_obs.person_id = zip_code_obs.person_id
            AND state_obs.observation_source_concept_id = 1585249
            AND state_vocab.concept_id = state_obs.value_source_concept_id
    WHERE zip_code_obs.observation_source_concept_id = 1585250    
),
unique_zip_code_transforms AS (
    SELECT DISTINCT
        person_id, zip_code_observation_id,
        zip_code_3, transformed_zip_code_3
    FROM modified_zip_codes_and_states mzc 
),
unique_state_transforms AS (
    SELECT DISTINCT
        person_id, state_observation_id, 
        state, transformed_state
    FROM modified_zip_codes_and_states mzc
)
SELECT
    obs.*
FROM `{{project_id}}.{{dataset_id}}.{{obs_table}}` obs
WHERE obs.observation_id IN (SELECT zip_code_observation_id FROM unique_zip_code_transforms)
    OR obs.observation_id IN (SELECT state_observation_id FROM unique_state_transforms)
""")

MODIFY_ZIP_CODES_AND_STATES_QUERY = JINJA_ENV.from_string("""
WITH modified_zip_codes_and_states AS (
    SELECT DISTINCT
        zip_code_obs.person_id, zip_code_obs.observation_id zip_code_observation_id,
        map.zip_code_3, map.transformed_zip_code_3,
        state_obs.observation_id state_observation_id, 
        map.state, map.transformed_state
    FROM `{{project_id}}.{{dataset_id}}.{{obs_table}}` zip_code_obs
    JOIN `{{project_id}}.{{pipeline_tables_dataset}}.{{zip_code_aggregation_map}}` map
        ON map.zip_code_3 = zip_code_obs.value_as_string
    JOIN `{{project_id}}.{{pipeline_tables_dataset}}.{{pii_state_vocab}}` state_vocab
        ON state_vocab.postal_code = map.state
    LEFT JOIN `{{project_id}}.{{dataset_id}}.{{obs_table}}` state_obs
        ON state_obs.person_id = zip_code_obs.person_id
            AND state_obs.observation_source_concept_id = 1585249
            AND state_vocab.concept_id = state_obs.value_source_concept_id
    WHERE zip_code_obs.observation_source_concept_id = 1585250    
),
unique_zip_code_transforms AS (
    SELECT DISTINCT
        person_id, zip_code_observation_id,
        zip_code_3, transformed_zip_code_3
    FROM modified_zip_codes_and_states mzc 
),
unique_state_transforms AS (
    SELECT DISTINCT
        person_id, state_observation_id, 
        state, transformed_state
    FROM modified_zip_codes_and_states mzc
)
SELECT
    obs.*
FROM `{{project_id}}.{{dataset_id}}.{{obs_table}}` obs
LEFT JOIN unique_zip_code_transforms t1
    ON t1.zip_code_observation_id = obs.observation_id
LEFT JOIN unique_state_transforms t2
    ON t2.state_observation_id = obs.observation_id
WHERE t1.zip_code_observation_id IS NULL 
    AND t2.state_observation_id IS NULL
UNION ALL
SELECT
    obs.* REPLACE (
        COALESCE(mzc.transformed_zip_code_3, obs.value_as_string) AS value_as_string
    )
FROM `{{project_id}}.{{dataset_id}}.{{obs_table}}` obs
JOIN unique_zip_code_transforms mzc
    ON mzc.zip_code_observation_id = obs.observation_id
        AND mzc.person_id = obs.person_id
UNION ALL
SELECT
    obs.* REPLACE (
        COALESCE(state_vocab.concept_id, obs.value_source_concept_id) AS value_source_concept_id,
        COALESCE(state_vocab.concept_id, obs.value_as_concept_id) AS value_as_concept_id
    )
FROM `{{project_id}}.{{dataset_id}}.{{obs_table}}` obs
JOIN unique_state_transforms mzc
    ON mzc.state_observation_id = obs.observation_id
        AND mzc.person_id = obs.person_id
LEFT JOIN `{{project_id}}.{{pipeline_tables_dataset}}.{{pii_state_vocab}}` state_vocab
    ON state_vocab.postal_code = mzc.transformed_state
""")


class AggregateZipCodes(BaseCleaningRule):

    def __init__(self, project_id, dataset_id, sandbox_dataset_id):
        """
        Initialize the class with proper information.

        Set the issue numbers, description and affected datasets. As other tickets may affect
        this SQL, append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """
        desc = 'Aggregates generalized zip codes based on first digits.'
        super().__init__(
            issue_numbers=['DC1379'],
            description=desc,
            affected_datasets=[cdr_consts.CONTROLLED_TIER_DEID],
            affected_tables=[OBSERVATION],
            project_id=project_id,
            dataset_id=dataset_id,
            sandbox_dataset_id=sandbox_dataset_id,
            depends_on=[GeneralizeZipCodes, StringFieldsSuppression])
        # Identifiable information may exist if StringFieldsSuppression rule fails

    def get_query_specs(self, *args, **keyword_args):
        """
        Interface to return a list of query dictionaries.

        :returns:  a list of query dictionaries.  Each dictionary specifies
            the query to execute and how to execute.  The dictionaries are
            stored in list order and returned in list order to maintain
            an ordering.
        """

        sandbox_query = dict()
        sandbox_query[cdr_consts.QUERY] = SANDBOX_QUERY.render(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            obs_table=OBSERVATION,
            pii_state_vocab=PII_STATE_VOCAB,
            zip_code_aggregation_map=ZIP_CODE_AGGREGATION_MAP,
            pipeline_tables_dataset=PIPELINE_TABLES)
        sandbox_query[cdr_consts.DESTINATION_TABLE] = self.sandbox_table_for(
            OBSERVATION)
        sandbox_query[cdr_consts.DESTINATION_DATASET] = self.sandbox_dataset_id
        sandbox_query[cdr_consts.DISPOSITION] = bq_consts.WRITE_TRUNCATE

        modify_zip_codes_and_states_query = dict()
        modify_zip_codes_and_states_query[
            cdr_consts.QUERY] = MODIFY_ZIP_CODES_AND_STATES_QUERY.render(
                project_id=self.project_id,
                dataset_id=self.dataset_id,
                obs_table=OBSERVATION,
                pii_state_vocab=PII_STATE_VOCAB,
                zip_code_aggregation_map=ZIP_CODE_AGGREGATION_MAP,
                pipeline_tables_dataset=PIPELINE_TABLES)
        modify_zip_codes_and_states_query[
            cdr_consts.DESTINATION_TABLE] = OBSERVATION
        modify_zip_codes_and_states_query[
            cdr_consts.DESTINATION_DATASET] = self.dataset_id
        modify_zip_codes_and_states_query[
            cdr_consts.DISPOSITION] = bq_consts.WRITE_TRUNCATE

        return [sandbox_query, modify_zip_codes_and_states_query]

    def get_sandbox_tablenames(self):
        raise NotImplementedError("Please fix me.")

    def validate_rule(self, client, *args, **keyword_args):
        """
        Validates the cleaning rule which deletes or updates the data from the tables

        Method to run validation on cleaning rules that will be updating the values.
        For example:
        if your class updates all the datetime fields you should be implementing the
        validation that checks if the date time values that needs to be updated no
        longer exists in the table.

        if your class deletes a subset of rows in the tables you should be implementing
        the validation that checks if the count of final final row counts + deleted rows
        should equals to initial row counts of the affected tables.

        Raises RunTimeError if the validation fails.
        """

        raise NotImplementedError("Please fix me.")

    def setup_validation(self, client, *args, **keyword_args):
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

    def setup_rule(self, client, *args, **keyword_args):
        """
        Load required resources prior to executing cleaning rule queries.

        Method to run data upload options before executing the first cleaning
        rule of a class.  For example, if your class requires loading a static
        table, that load operation should be defined here.  It SHOULD NOT BE
        defined as part of get_query_specs().
        """
        pass


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.parse_args()
    pipeline_logging.configure(level=logging.DEBUG, add_console_handler=True)

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(ARGS.project_id,
                                                 ARGS.dataset_id,
                                                 ARGS.sandbox_dataset_id,
                                                 [(AggregateZipCodes,)])

        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(AggregateZipCodes,)])
