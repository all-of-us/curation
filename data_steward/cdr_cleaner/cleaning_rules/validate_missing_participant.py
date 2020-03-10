import logging
import bq_utils
import resources
import oauth2client
import googleapiclient
from validation.participants import readers
from cdr_cleaner.cleaning_rules import sandbox_and_remove_pids as remove_pids
from constants.validation.participants.identity_match import (PERSON_ID_FIELD,
                                                              FIRST_NAME_FIELD,
                                                              LAST_NAME_FIELD,
                                                              BIRTH_DATE_FIELD)
from constants.validation.participants.writers import ALGORITHM_FIELD
from common import PARTICIPANT_MATCH

IDENTITY_MATCH = 'identity_match'

LOGGER = logging.getLogger(__name__)

TICKET_NUMBER = 'DC-468'

NUM_OF_MISSING_KEY_FIELDS = 2
NUM_OF_MISSING_ALL_FIELDS = 4

KEY_FIELDS = [FIRST_NAME_FIELD, LAST_NAME_FIELD, BIRTH_DATE_FIELD]

IDENTITY_MATCH_EXCLUDED_FIELD = [PERSON_ID_FIELD, ALGORITHM_FIELD]

CAST_MISSING_COLUMN = "CAST({column} <> 'match' AS int64)"

CRITERION_COLUMN_TEMPLATE = "({column_expr}) >= {num_of_missing}"

SELECT_NON_MATCH_PARTICIPANTS_QUERY = """
WITH non_match_participants AS
(
SELECT 
    *,
    ({criterion_one_expr}) AS criterion_one,
    ({criterion_two_expr}) AS criterion_two
FROM `{project_id}.{validation_dataset_id}.{identity_match_table}`
)

SELECT
    person_id
FROM non_match_participants
WHERE criterion_one IS TRUE OR criterion_two IS TRUE
"""


def exist_participant_match(ehr_dataset_id, hpo_id):
    """
    This function checks if the hpo has submitted the participant_match data 
    
    :param ehr_dataset_id: 
    :param hpo_id: 
    :return: 
    """
    return bq_utils.table_exists(
        bq_utils.get_table_id(hpo_id, PARTICIPANT_MATCH), ehr_dataset_id)


def get_missing_criterion(field_names):
    """
    This function generates a bigquery column expression for missing criteria
    
    :param field_names: a list of field names for counting `missing`s 
    :return: 
    """
    joined_column_expr = ' + '.join([
        CAST_MISSING_COLUMN.format(column=field_name)
        for field_name in field_names
    ])
    return joined_column_expr


def get_list_non_match_participants(project_id, validation_dataset_id, hpo_id):
    """
    This function retrieves a list of non-match participants
    
    :param project_id: 
    :param validation_dataset_id:
    :param hpo_id: 
    :return: 
    """

    # get the the hpo specific <hpo_id>_identity_match
    identity_match_table = bq_utils.get_table_id(hpo_id, IDENTITY_MATCH)

    non_match_participants_query = get_non_match_participant_query(
        project_id, validation_dataset_id, identity_match_table)

    try:
        LOGGER.info(
            'Identifying non-match participants in {dataset_id}.{identity_match_table}'
            .format(dataset_id=validation_dataset_id,
                    identity_match_table=identity_match_table))

        results = bq_utils.query(q=non_match_participants_query)

    except (oauth2client.client.HttpAccessTokenRefreshError,
            googleapiclient.errors.HttpError) as exp:

        LOGGER.exception('Could not execute the query \n{query}'.format(
            query=non_match_participants_query))
        raise exp

    # wait for job to finish
    query_job_id = results['jobReference']['jobId']
    incomplete_jobs = bq_utils.wait_on_jobs([query_job_id])
    if incomplete_jobs:
        raise bq_utils.BigQueryJobWaitError(incomplete_jobs)

    # return the person_ids only
    return [row[PERSON_ID_FIELD] for row in bq_utils.response2rows(results)]


def get_non_match_participant_query(project_id, validation_dataset_id,
                                    identity_match_table):
    """
    This function generates the query for identifying non_match participants query flagged by the DRC match algorithm
    
    :param project_id: 
    :param validation_dataset_id: 
    :param identity_match_table: 
    :return: 
    """

    # if any of the two of first_name, last_name and birthday are missing, this is a non-match
    num_of_missing_key_fields = CRITERION_COLUMN_TEMPLATE.format(
        column_expr=get_missing_criterion(KEY_FIELDS),
        num_of_missing=NUM_OF_MISSING_KEY_FIELDS)

    identity_match_fields = [
        field['name']
        for field in resources.fields_for(IDENTITY_MATCH)
        if field['name'] not in IDENTITY_MATCH_EXCLUDED_FIELD
    ]
    # if the total number of missings is equal to and bigger than 4, this is a non-match
    num_of_missing_all_fields = CRITERION_COLUMN_TEMPLATE.format(
        column_expr=get_missing_criterion(identity_match_fields),
        num_of_missing=NUM_OF_MISSING_ALL_FIELDS)
    # instantiate the query for identifying the non-match participants in the validation_dataset
    select_non_match_participants_query = SELECT_NON_MATCH_PARTICIPANTS_QUERY.format(
        project_id=project_id,
        validation_dataset_id=validation_dataset_id,
        identity_match_table=identity_match_table,
        criterion_one_expr=num_of_missing_key_fields,
        criterion_two_expr=num_of_missing_all_fields)

    return select_non_match_participants_query


def delete_records_for_non_matching_participants(project_id,
                                                 combined_dataset_id,
                                                 ehr_dataset_id,
                                                 validation_dataset_id):
    """
    This function generates the queries that delete participants and their corresponding data points, for which the 
    participant_match data is missing and DRC matching algorithm flags it as a no match 
    
    :param project_id: 
    :param combined_dataset_id: 
    :param ehr_dataset_id: 
    :param validation_dataset_id:

    :return: 
    """

    non_matching_person_ids = []

    # Retrieving all hpo_ids
    for hpo_id in readers.get_hpo_site_names():
        if not exist_participant_match(ehr_dataset_id, hpo_id):
            LOGGER.info(
                'The hpo site {hpo_id} is missing the participant_match data'.
                format(hpo_id=hpo_id))

            non_matching_person_ids.extend(
                get_list_non_match_participants(project_id,
                                                validation_dataset_id, hpo_id))
        else:
            LOGGER.info(
                'The hpo site {hpo_id} submitted the participant_match data'.
                format(hpo_id=hpo_id))

    queries = []

    if non_matching_person_ids:
        LOGGER.info(
            'Participants: {person_ids} and their data will be dropped from {combined_dataset_id}'
            .format(person_ids=non_matching_person_ids,
                    combined_dataset_id=combined_dataset_id))

        queries.append(
            remove_pids.get_sandbox_queries(project_id, combined_dataset_id,
                                            non_matching_person_ids,
                                            TICKET_NUMBER))
        queries.extend(
            remove_pids.get_remove_pids_queries(project_id, combined_dataset_id,
                                                non_matching_person_ids))

    return queries


def parse_args():
    """
    This function expands the default argument list defined in cdr_cleaner.args_parser
    :return: an expanded argument list object
    """

    import cdr_cleaner.args_parser as parser

    additional_arguments = [{
        parser.SHORT_ARGUMENT: '-a',
        parser.LONG_ARGUMENT: '--sandbox_dataset_id',
        parser.ACTION: 'store',
        parser.DEST: 'sandbox_dataset_id',
        parser.HELP: 'sandbox_dataset_id',
        parser.REQUIRED: True
    }, {
        parser.SHORT_ARGUMENT: '-e',
        parser.LONG_ARGUMENT: '--ehr_dataset_id',
        parser.ACTION: 'store',
        parser.DEST: 'ehr_dataset_id',
        parser.HELP: 'ehr_dataset_id',
        parser.REQUIRED: True
    }, {
        parser.SHORT_ARGUMENT: '-v',
        parser.LONG_ARGUMENT: '--validation_dataset_id',
        parser.ACTION: 'store',
        parser.DEST: 'validation_dataset_id',
        parser.HELP: 'validation_dataset_id',
        parser.REQUIRED: True
    }]
    args = parser.default_parse_args(additional_arguments)
    return args


if __name__ == '__main__':
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parse_args()
    # Uncomment this line if testing locally
    clean_engine.add_console_logging(ARGS.console_log)
    query_list = delete_records_for_non_matching_participants(
        ARGS.project_id, ARGS.dataset_id, ARGS.ehr_dataset_id,
        ARGS.validation_dataset_id)
    clean_engine.clean_dataset(ARGS.project_id, query_list)
