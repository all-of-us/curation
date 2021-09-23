"""
Utility to fetch and store deactivated participants information.

Original Issues: DC-1213, DC-1042, DC-971, DC-972

The intent of the get_deactivated_participants function is to call and store deactivated participants information by
    leveraging the RDR Participant Summary API. Deactivated participant information stored is `participantId`,
    `suspensionStatus`, and `suspensionTime`
The intent of the get_participant_information function is to retrieve the information needed for participant
    validation for a single site based on the site name (hpo_id). The information necessary for this request as defined
    in the ticket (DC-1213) as well as the Participant Summary Field List
    (https://all-of-us-raw-data-repository.readthedocs.io/en/latest/api_workflows/field_reference/participant_summary_field_list.html)
    are `participantId`, `firstName`, `middleName`, `lastName`, `streetAddress`, `streetAddress2`, `city`, `state`,
    `zipCode`, `phoneNumber`, `email`, `dateOfBirth`, `sex`
"""
# Python imports
import re
import logging
from typing import List, Dict
import requests

# Third party imports
import pandas
import google.auth.transport.requests as req
from google.auth import default
from google.cloud.bigquery.schema import SchemaField
from google.cloud.bigquery import LoadJobConfig

# Project imports
from utils import auth
from utils.bq import get_client, get_table_schema

LOGGER = logging.getLogger(__name__)

# These fields are coming in from RDR with their naming convention and will be converted
# to the Curation naming convention in the `get_site_participant_information` function
FIELDS_OF_INTEREST_FOR_VALIDATION = [
    'participantId', 'firstName', 'middleName', 'lastName', 'streetAddress',
    'streetAddress2', 'city', 'state', 'zipCode', 'phoneNumber', 'email',
    'dateOfBirth', 'sex'
]


def get_access_token():
    """
    Obtains GCP Bearer token

    :return: returns the access_token
    """

    scopes = [
        'https://www.googleapis.com/auth/cloud-platform', 'email', 'profile'
    ]

    credentials, _ = default()
    credentials = auth.delegated_credentials(credentials, scopes=scopes)

    request = req.Request()
    credentials.refresh(request)
    access_token = credentials.token

    return access_token


def get_participant_data(api_project_id: str, params: Dict) -> List[Dict]:
    """
    Fetches participant data via ParticipantSummary API

    :param api_project_id: RDR project id when PS API rests
    :param params: the fields and their values

    :return: list of data fetched from the ParticipantSummary API
    """
    # Base /ParticipantSummary endpoint to fetch information about the participant
    url = f'https://{api_project_id}.appspot.com/rdr/v1/ParticipantSummary'

    done = False
    participant_data = []

    token = get_access_token()

    headers = {
        'content-type': 'application/json',
        'Authorization': f'Bearer {token}'
    }

    session = requests.Session()

    while not done:
        resp = session.get(url, headers=headers, params=params)
        if not resp or resp.status_code != 200:
            LOGGER.warning(f'Error: API request failed because {resp}')
        else:
            r_json = resp.json()
            participant_data += r_json.get('entry', {})
            if 'link' in r_json:
                link_obj = r_json.get('link')
                link_url = link_obj[0].get('url')
                params['_token'] = link_url[link_url.find('_token') +
                                            len('_token') + 1:]
            else:
                done = True

    return participant_data


def process_api_data_to_df(data: List[Dict], columns: List[str],
                           column_map: Dict) -> pandas.DataFrame:
    """
    Converts data retrieved from PS API to curation table formatted df

    :param data: data retrieved from PS API
    :param columns: columns of interest
    :param column_map: columns to be renamed as {old_name: new_name, ..}
    :return: dataframe with supplied columns and renamed as per column_map
    """
    participant_records = []

    # loop over participant summary records, insert participant data in same order as deactivated_participant_cols
    for full_participant_record in data:
        resource = full_participant_record.get('resource', {})
        # loop over fields that exist in both resource_dict and columns and save key-value pairs
        limited_participant_record = {
            col: resource[col] for col in resource.keys() & columns
        }
        participant_records.append(limited_participant_record)

    df = pandas.DataFrame.from_records(participant_records, columns=columns)

    # Transforms participantId to an integer string
    df['participantId'] = df['participantId'].apply(participant_id_to_int)

    # Rename columns to be consistent with curation naming convention
    bq_columns = {
        k: '_'.join(re.split('(?=[A-Z])', k)).lower() for k in columns
    }
    df.rename(bq_columns, axis='columns', inplace=True)
    df.rename(column_map, axis='columns', inplace=True)
    return df


def get_deactivated_participants(api_project_id: str,
                                 columns: List[str]) -> pandas.DataFrame:
    """
    Fetches all deactivated participants via API if suspensionStatus = 'NO_CONTACT'
    and stores all the deactivated participants in a BigQuery dataset table
    :param api_project_id: The RDR project that contains participant summary data
    :param columns: columns to be pushed to a table in BigQuery in the form of a list of strings
    :return: returns dataframe of deactivated participants
    """

    # Parameter checks
    if not isinstance(api_project_id, str):
        raise RuntimeError(f'Please specify the RDR project')

    if not isinstance(columns, list):
        raise RuntimeError(
            'Please provide a list of columns to be pushed to BigQuery table')

    # Make request to get API version. This is the current RDR version for reference
    # See https://github.com/all-of-us/raw-data-repository/blob/master/opsdataAPI.md for documentation of this api.
    params = {'_sort': 'lastModified', 'suspensionStatus': 'NO_CONTACT'}

    participant_data = get_participant_data(api_project_id, params=params)

    column_map = {
        'participant_id': 'person_id',
        'suspension_time': 'deactivated_date'
    }

    df = process_api_data_to_df(participant_data, columns, column_map)

    return df


def get_site_participant_information(project_id: str, hpo_id: str):
    """
    Fetches the necessary participant information for a particular site.
    :param project_id: The RDR project hosting the API
    :param hpo_id: awardee name of the site
    :return: a dataframe of participant information
    :raises: RuntimeError if the project_id and hpo_id are not strings
    :raises: TimeoutError if response takes longer than 10 minutes
    """
    # Parameter checks
    if not isinstance(project_id, str):
        raise RuntimeError(f'Please specify the RDR project')

    if not isinstance(hpo_id, str):
        raise RuntimeError(f'Please provide an hpo_id')

    # Make request to get API version. This is the current RDR version for reference see
    # see https://github.com/all-of-us/raw-data-repository/blob/master/opsdataAPI.md for documentation of this API.
    # consentForElectronicHealthRecords=SUBMITTED -- ensures only consenting participants are returned via the API
    #   regardless if there is EHR data uploaded for that participant
    # suspensionStatus=NOT_SUSPENDED and withdrawalStatus=NOT_WITHDRAWN -- ensures only active participants returned
    #   via the API
    params = {
        'awardee': f'{hpo_id}',
        'suspensionStatus': 'NOT_SUSPENDED',
        'consentForElectronicHealthRecords': 'SUBMITTED',
        'withdrawalStatus': 'NOT_WITHDRAWN',
        '_sort': 'participantId',
        '_count': '1000'
    }

    participant_data = get_participant_data(project_id, params=params)

    column_map = {'participant_id': 'person_id'}

    df = process_api_data_to_df(participant_data,
                                FIELDS_OF_INTEREST_FOR_VALIDATION, column_map)

    return df


def get_org_participant_information(project_id: str,
                                    org_id: str) -> pandas.DataFrame:
    """
    Fetches the necessary participant information for a particular organization.

    :param project_id: The RDR project hosting the API
    :param org_id: organization name of the site

    :return: a dataframe of participant information
    :raises: RuntimeError if the project_id and hpo_id are not strings
    :raises: TimeoutError if response takes longer than 10 minutes
    """
    # Parameter checks
    if not isinstance(project_id, str):
        raise RuntimeError(f'Please specify the RDR project')

    if not isinstance(org_id, str):
        raise RuntimeError(f'Please provide an org_id')

    # Make request to get API version. This is the current RDR version for reference see
    # see https://github.com/all-of-us/raw-data-repository/blob/master/opsdataAPI.md for documentation of this API.
    # consentForElectronicHealthRecords=SUBMITTED -- ensures only consenting participants are returned via the API
    #   regardless if there is EHR data uploaded for that participant
    # suspensionStatus=NOT_SUSPENDED and withdrawalStatus=NOT_WITHDRAWN -- ensures only active participants returned
    #   via the API
    params = {
        'organization': f'{org_id}',
        'suspensionStatus': 'NOT_SUSPENDED',
        'consentForElectronicHealthRecords': 'SUBMITTED',
        'withdrawalStatus': 'NOT_WITHDRAWN',
        '_sort': 'participantId',
        '_count': '1000'
    }

    participant_data = get_participant_data(project_id, params=params)

    column_map = {'participant_id': 'person_id'}

    df = process_api_data_to_df(participant_data,
                                FIELDS_OF_INTEREST_FOR_VALIDATION, column_map)

    return df


def participant_id_to_int(participant_id):
    """
    Transforms the participantId received from RDR ParticipantSummary API from an
    alphanumeric string to an integer string.

    :param participant_id: the RDR internal unique ID of a participant
    :return: returns the participantId as integer data type
    """

    return int(participant_id[1:])


def set_dataframe_date_fields(df: pandas.DataFrame,
                              schema: List[SchemaField]) -> pandas.DataFrame:
    """Convert dataframe fields from string to datetime for BQ schema fields
        with type in ['DATE', 'DATETIME', 'TIMESTAMP'].

    :param df: A dataframe
    :param schema: A list of schema fields
    :return: A modified dataframe with date fields converted to type datetime
    """
    df = df.copy()
    for schema_field in schema:
        field_name = schema_field.name
        if schema_field.field_type.upper() in (
                'DATE', 'DATETIME', 'TIMESTAMP') and field_name in df.columns:
            df[field_name] = pandas.to_datetime(df[field_name], errors='coerce')

    return df


def store_participant_data(df, project_id, destination_table, schema=None):
    """
    Stores the fetched participant data in a BigQuery dataset. If the
    table doesn't exist, it will create that table. If the table does
    exist, it will append the data onto that designated table.

    :param df: pandas dataframe created to hold participant data fetched from ParticipantSummary API
    :param project_id: identifies the project
    :param destination_table: name of the table to be written in the form of dataset.tablename
    :param schema: a list of SchemaField objects corresponding to the destination table

    :return: returns a dataset with the participant data
    """

    # Parameter check
    if not isinstance(project_id, str):
        raise RuntimeError(
            f'Please specify the project in which to create the tables')

    client = get_client(project_id)
    if not schema:
        schema = get_table_schema(destination_table.split('.')[-1])

    # Dataframe data fields must be of type datetime
    df = set_dataframe_date_fields(df, schema)

    load_job_config = LoadJobConfig(schema=schema)
    job = client.load_table_from_dataframe(df,
                                           destination_table,
                                           job_config=load_job_config)
    job.result()

    return job.job_id
