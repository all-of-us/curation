import logging
import common
import bq_utils
import resources
import oauth2client
import googleapiclient
from validation.participants import readers
from constants.cdr_cleaner import clean_cdr as cdr_consts
from cdr_cleaner.cleaning_rules import drop_rows_for_missing_persons as missing_persons
from constants.validation.participants.identity_match import (PERSON_ID_FIELD,
                                                              FIRST_NAME_FIELD,
                                                              LAST_NAME_FIELD,
                                                              BIRTH_DATE_FIELD)
from constants.validation.participants.writers import ALGORITHM_FIELD
from common import PARTICIPANT_MATCH

LOGGER = logging.getLogger(__name__)

DELETE_PERSON_IDS_QUERY = """
DELETE
FROM
  `{project_id}.{dataset_id}.person`
WHERE
  person_id IN 
  (
    {person_ids} 
  )
"""

CAST_MISSING_COLUMN = """
CAST({column} = 'missing' AS int64)
"""

SELECT_NON_MATCH_PARTICIPANTS_QUERY = """
WITH non_match_participants AS
(
SELECT 
    *,
    ({match_criterion_one}) AS criterion_one,
    ({match_criterion_two}) AS criterion_two
FROM {project_id}.{dataset_id}.{participant_match}
)

SELECT
    person_id
FROM non_match_participants
WHERE criterion_one IS TRUE OR criterion_two IS TRUE
"""

CRITERION_COLUMN_TEMPLATE = """
({column_expr}) >= {num_of_missing})
"""

PARTICIPANT_MATCH_EXCLUDED_FIELD = [PERSON_ID_FIELD, ALGORITHM_FIELD]


def exist_participant_match(ehr_dataset_id, hpo_id):
    """
    This function checks if the hpo has submitted the participant_match data 
    
    :param ehr_dataset_id: 
    :param hpo_id: 
    :return: 
    """
    return bq_utils.table_exists(
        bq_utils.get_table_id(hpo_id, common.PARTICIPANT_MATCH), ehr_dataset_id)


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

    # if any of the two of first_name, last_name and birthday are missing, this is a non-match
    criterion_one_expr = CRITERION_COLUMN_TEMPLATE.format(
        column_expr=get_missing_criterion(
            [FIRST_NAME_FIELD, LAST_NAME_FIELD, BIRTH_DATE_FIELD]),
        num_of_missing=2)

    participant_match_fields = [
        field['name']
        for field in resources.fields_for(
            bq_utils.get_table_id(hpo_id, PARTICIPANT_MATCH))
        if field['name'] not in PARTICIPANT_MATCH_EXCLUDED_FIELD
    ]

    # if the total number of missings is equal to and bigger than 4, this is a non-match
    criterion_two_expr = CRITERION_COLUMN_TEMPLATE.format(
        column_expr=participant_match_fields, num_of_missing=4)

    # get the the hpo specific <hpo_id>_participant_match
    participant_match_table = bq_utils.get_table_id(hpo_id, PARTICIPANT_MATCH)

    # instantiate the query for identifying the non-match participants in the validation_dataset
    select_non_match_participants_query = SELECT_NON_MATCH_PARTICIPANTS_QUERY.format(
        project_id=project_id,
        validation_dataset_id=validation_dataset_id,
        participant_match=participant_match_table,
        criterion_one_expr=criterion_one_expr,
        criterion_two_expr=criterion_two_expr)

    try:

        LOGGER.info(
            'Identifying non-match participants in {dataset_id}.{participant_match_table}'
            .format(dataset_id=validation_dataset_id,
                    participant_match_table=participant_match_table))

        results = bq_utils.query(q=select_non_match_participants_query)

    except (oauth2client.client.HttpAccessTokenRefreshError,
            googleapiclient.errors.HttpError) as exp:

        LOGGER.exception('Could not execute the query \n{query}'.format(
            query=select_non_match_participants_query))

    # wait for job to finish
    query_job_id = results['jobReference']['jopartitionBybId']
    incomplete_jobs = bq_utils.wait_on_jobs([query_job_id])
    if incomplete_jobs:
        raise bq_utils.BigQueryJobWaitError(incomplete_jobs)

    # return the person_ids only
    return [row[PERSON_ID_FIELD] for row in bq_utils.response2rows(results)]


def get_delete_persons_query(project_id, combined_dataset_id, person_ids):
    """
    This function generates a query to delete a list of person_ids from the person table in the combined dataset
    
    :param project_id: 
    :param combined_dataset_id: 
    :param person_ids: 
    :return: 
    """

    delete_query = dict()
    delete_query[cdr_consts.QUERY] = DELETE_PERSON_IDS_QUERY.format(
        project_id=project_id,
        dataset_id=combined_dataset_id,
        person_ids=','.join(person_ids))
    delete_query[cdr_consts.BATCH] = True

    return delete_query


def delete_records_for_non_matching_participants(project_id, ehr_dataset_id,
                                                 validation_dataset_id,
                                                 combined_dataset_id):
    """
    This function generates the queries that delete participants and their corresponding data points, for which the 
    participant_match data is missing and DRC matching algorithm flags it as a no match 
    
    :param project_id: 
    :param ehr_dataset_id: 
    :param combined_dataset_id: 
    :param validation_dataset_id:

    :return: 
    """

    non_matching_person_ids = []

    # Retrieving all hpo_ids
    for hpo_id in readers.get_hpo_site_names():
        if not exist_participant_match(project_id, ehr_dataset_id, hpo_id):
            LOGGER.log(
                'The hpo site {hpo_id} is missing the participant_match data'.
                format(hpo_id=hpo_id))

            non_matching_person_ids.extend(
                get_list_non_match_participants(project_id,
                                                validation_dataset_id, hpo_id))
        else:
            LOGGER.log(
                'The hpo site {hpo_id} submitted the participant_match data'.
                format(hpo_id=hpo_id))

    queries = []

    if non_matching_person_ids:
        LOGGER.log(
            'Participants: {person_ids} and their data will be dropped from {combined_dataset_id}'
            .format(person_ids=non_matching_person_ids,
                    combined_dataset_id=combined_dataset_id))

        queries.extend(
            get_delete_persons_query(project_id, combined_dataset_id,
                                     non_matching_person_ids))
        queries.extend(
            missing_persons.get_queries(project_id, combined_dataset_id))

    return queries
