# coding=utf-8
"""
Update survey Questions and Answers not mapped to OMOP concepts.

There are several survey questions and answers that are not getting properly mapped from the RDR into the CDR.
There are two sources for this error:
    > Odysseus attempted to introduce some “short codes” that were not implemented in the RDR.
    > There were some concepts that were invalidated and changed around.
To address this, we implemented a fix that maps from the source_values present in the data to the “real”
concept_ids in the OMOP vocabulary. This includes leveraging the supplemental old_map_short_codes.CSV where,
Odyseus provided the short codes, as well as a brief review of other outstanding unmapped PPI codes.
"""
import logging

import bq_utils
import constants.cdr_cleaner.clean_cdr as cdr_consts

LOGGER = logging.getLogger(__name__)

OLD_MAP_SHORT_CODES_TABLE = 'old_map_short_codes'

OLD_MAP_SHORT_CODES_TABLE_FIELDS = [{
    "type": "string",
    "name": "type",
    "mode": "required",
    "description": "determines if it is a question or an answer"
}, {
    "type": "string",
    "name": "pmi_code",
    "mode": "required",
    "description": "Concept code which is available is concept table."
}, {
    "type": "string",
    "name": "short_code",
    "mode": "required",
    "description": "short codee used by Odysseus"
}]

UPDATE_QUESTIONS_MAP_QUERY = """
    UPDATE
        `{project}.{dataset}.observation` obs
    SET
        observation_concept_id=new_observation_concept_id,
        observation_source_concept_id=new_observation_source_concept_id
    FROM
        (SELECT
        DISTINCT short_pmi_code AS observation_source_value,
        source_c.concept_id AS new_observation_source_concept_id,
        FIRST_VALUE(standard_c.concept_id) OVER (PARTITION BY source_c.concept_id ORDER BY c_r.relationship_id DESC ) AS new_observation_concept_id
        FROM (
            SELECT
            SUBSTR(pmi_code,1,50) AS short_pmi_code,
            short_code
            FROM
                `{project}.{sandbox}.{old_map}`
            WHERE
                type='Question' )
        LEFT JOIN
            `{project}.{dataset}.concept` source_c
        ON
            (short_code=concept_code)
        JOIN
            `{project}.{dataset}.concept_relationship` c_r  
        ON
            (source_c.concept_id=c_r.concept_id_1)
        JOIN
            `{project}.{dataset}.concept` standard_c
        ON
            (standard_c.concept_id=c_r.concept_id_2)
        WHERE
            source_c.vocabulary_id='PPI'
            AND c_r.relationship_id LIKE 'Maps to%'
        ) map
    WHERE
        map.observation_source_value=obs.observation_source_value"""

UPDATE_ANSWERS_MAP_QUERY = """
    UPDATE
        `{project}.{dataset}.observation` obs
    SET
        value_as_concept_id=new_value_as_concept_id,
        value_source_concept_id=new_value_source_concept_id
    FROM (
        SELECT
        DISTINCT short_pmi_code AS value_source_value,
        source_c.concept_id AS new_value_source_concept_id,
        FIRST_VALUE(standard_c.concept_id) OVER (PARTITION BY source_c.concept_id ORDER BY c_r.relationship_id DESC ) AS new_value_as_concept_id
        FROM (
            SELECT
            SUBSTR(pmi_code,1,50) AS short_pmi_code,
            short_code
            FROM
                `{project}.{sandbox}.{old_map}`
            WHERE
                type='Answer' )
        LEFT JOIN
            `{project}.{dataset}.concept` source_c
        ON
            (short_code=concept_code)
        JOIN
            `{project}.{dataset}.concept_relationship` c_r
        ON
            (source_c.concept_id=c_r.concept_id_1)
        JOIN
            `{project}.{dataset}.concept` standard_c
        ON
            (standard_c.concept_id=c_r.concept_id_2)
        WHERE
            source_c.vocabulary_id='PPI'
        AND c_r.relationship_id LIKE 'Maps to%') map
    WHERE
        map.value_source_value=obs.value_source_value"""


def get_update_questions_answers_not_mapped_to_omop(project_id, dataset_id,
                                                    sandbox_dataset_id):
    """

    This function gets the queries required to update the questions and answers that were unmapped to OMOP concepts

    :param project_id: Name of the project
    :param dataset_id: Name of the dataset where the queries should be run
    :param sandbox_dataset_id: Name of the sandbox dataset
    :return:
    """

    bq_utils.load_table_from_csv(project_id=project_id,
                                 dataset_id=sandbox_dataset_id,
                                 table_name=OLD_MAP_SHORT_CODES_TABLE,
                                 fields=OLD_MAP_SHORT_CODES_TABLE_FIELDS)

    queries_list = []

    # Update concept_ids to questions using OLD_MAP_SHORT_CODES_TABLE.
    query = dict()
    query[cdr_consts.QUERY] = UPDATE_QUESTIONS_MAP_QUERY.format(
        dataset=dataset_id,
        project=project_id,
        old_map=OLD_MAP_SHORT_CODES_TABLE,
        sandbox=sandbox_dataset_id)
    queries_list.append(query)

    # Update concept_ids to answers using OLD_MAP_SHORT_CODES_TABLE.
    query = dict()
    query[cdr_consts.QUERY] = UPDATE_ANSWERS_MAP_QUERY.format(
        dataset=dataset_id,
        project=project_id,
        old_map=OLD_MAP_SHORT_CODES_TABLE,
        sandbox=sandbox_dataset_id)
    queries_list.append(query)

    return queries_list


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.parse_args()

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(
            ARGS.project_id, ARGS.dataset_id, ARGS.sandbox_dataset_id,
            [(get_update_questions_answers_not_mapped_to_omop,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(
            ARGS.project_id, ARGS.dataset_id, ARGS.sandbox_dataset_id,
            [(get_update_questions_answers_not_mapped_to_omop,)])
