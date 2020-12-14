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
    are `participant_id`, `firstName`, `middleName`, `lastName`, `streetAddress`, `streetAddress2`, `city`, `state`,
    `zipCode`, `phoneNumber`, `email`, `dateOfBirth`, `sex`
"""

# Third party imports
import re
import signal
import pandas
import requests
import pandas_gbq
import google.auth.transport.requests as req
from google.auth import default

# Project imports
from utils import auth
from resources import fields_for


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


def get_participant_data(url, headers):
    """
    Fetches participant data via ParticipantSummary API with a retrieval limit of 3 minutes

    :param url: the /ParticipantSummary endpoint to fetch information about the participant
    :param headers: the metadata associated with the API request and response

    :return: list of data fetched from the ParticipantSummary API
    """

    done = False
    participant_data = []
    original_url = url
    next_url = url

    while not done:
        resp = requests.get(next_url, headers=headers)
        if not resp or resp.status_code != 200:
            print(f'Error: API request failed because {resp}')
        else:
            r_json = resp.json()
            participant_data += r_json.get('entry', {})
            if 'link' in r_json:
                link_obj = r_json.get('link')
                link_url = link_obj[0].get('url')
                next_url = original_url + '&' + link_url[link_url.find('_token'
                                                                      ):]
                print(next_url)
            else:
                done = True

    return participant_data


def get_deactivated_participants(project_id, dataset_id, tablename, columns):
    """
    Fetches all deactivated participants via API if suspensionStatus = 'NO_CONTACT'
    and stores all the deactivated participants in a BigQuery dataset table

    :param project_id: The RDR project that contains participant summary data
    :param dataset_id: The dataset name
    :param tablename: The name of the table to house the deactivated participant data
    :param columns: columns to be pushed to a table in BigQuery in the form of a list of strings

    :return: returns dataframe of deactivated participants
    """

    # Parameter checks
    if not isinstance(project_id, str):
        raise RuntimeError(f'Please specify the RDR project')

    if not isinstance(dataset_id, str):
        raise RuntimeError(f'Please provide a dataset_id')

    if not isinstance(tablename, str):
        raise RuntimeError(
            f'Please provide a tablename to house deactivated participant data')

    if not isinstance(columns, list):
        raise RuntimeError(
            'Please provide a list of columns to be pushed to BigQuery table')

    token = get_access_token()

    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer {0}'.format(token)
    }

    # Make request to get API version. This is the current RDR version for reference
    # See https://github.com/all-of-us/raw-data-repository/blob/master/opsdataAPI.md for documentation of this api.
    url = 'https://{0}.appspot.com/rdr/v1/ParticipantSummary?_sort=lastModified&suspensionStatus={1}'.format(
        project_id, 'NO_CONTACT')

    participant_data = get_participant_data(url, headers)

    deactivated_participants_cols = columns

    deactivated_participants = []
    # loop over participant summary records, insert participant data in same order as deactivated_participant_cols
    for entry in participant_data:
        item = []
        for col in deactivated_participants_cols:
            for key, val in entry.get('resource', {}).items():
                if col == key:
                    item.append(val)
        deactivated_participants.append(item)

    df = pandas.DataFrame(deactivated_participants,
                          columns=deactivated_participants_cols)

    # Converts column `suspensionTime` from string to timestamp
    if 'suspensionTime' in deactivated_participants_cols:
        df['suspensionTime'] = pandas.to_datetime(df['suspensionTime'])
        df['suspensionTime'] = df['suspensionTime'].dt.date

    # Transforms participantId to an integer string
    df['participantId'] = df['participantId'].apply(participant_id_to_int)

    # Rename columns to be consistent with the curation software
    bq_columns = [
        '_'.join(re.split('(?=[A-Z])', k)).lower()
        for k in deactivated_participants_cols
    ]
    bq_columns = [
        'person_id' if k == 'participant_id' else k for k in bq_columns
    ]
    bq_columns = [
        'deactivated_date' if k == 'suspension_time' else k for k in bq_columns
    ]
    column_map = {
        k: v for k, v in zip(deactivated_participants_cols, bq_columns)
    }

    df = df.rename(columns=column_map)

    return df


def get_site_participant_information(project_id, hpo_id):
    """
    Fetches the necessary participant information for a particular site.

    :param project_id: The RDR project that contains participant summary data
    :param hpo_id: awardee name of the site

    :return: returns dataframe of participant information
    """
    # Parameter checks
    if not isinstance(project_id, str):
        raise RuntimeError(f'Please specify the RDR project')

    if not isinstance(hpo_id, str):
        raise RuntimeError(f'Please provide an hpo_id')

    token = get_access_token()

    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer {0}'.format(token)
    }

    # Make request to get API version. This is the current RDR version for reference see
    # see https://github.com/all-of-us/raw-data-repository/blob/master/opsdataAPI.md for documentation of this API.
    url = 'https://{0}.appspot.com/rdr/v1/ParticipantSummary?awardee={1}&_sort=participantId&_count=1000'.format(
        project_id, hpo_id)

    participant_data = get_participant_data(url, headers)

    # Columns of interest for participants of a desired site
    participant_information_cols = FIELDS_OF_INTEREST_FOR_VALIDATION

    participant_information = []

    # Loop over participant summary records, insert participant data in the same order as participant_information_cols
    for entry in participant_data:
        item = []
        for col in participant_information_cols:
            for key, val in entry.get('resource', {}).items():
                if col == key:
                    item.append(val)
        participant_information.append(item)

    df = pandas.DataFrame(participant_information, columns=participant_information_cols)

    # Transforms participantId to an integer string
    df['participantId'] = df['participantId'].apply(participant_id_to_int)

    # Rename columns to be consistent with the curation software
    bq_columns = [
        '_'.join(re.split('(?=[A-Z])', k)).lower()
        for k in participant_information_cols
    ]
    bq_columns = [
        'person_id' if k == 'participant_id' else k for k in bq_columns
    ]
    column_map = {
        k: v for k, v in zip(participant_information_cols, bq_columns)
    }

    df = df.rename(columns=column_map)

    return df


def participant_id_to_int(participant_id):
    """
    Transforms the participantId received from RDR ParticipantSummary API from an
    alphanumeric string to an integer string.

    :param participant_id: the RDR internal unique ID of a participant
    :return: returns the participantId as integer data type
    """

    return int(participant_id[1:])


def store_participant_data(df, project_id, destination_table):
    """
    Stores the fetched participant data in a BigQuery dataset. If the
    table doesn't exist, it will create that table. If the table does
    exist, it will append the data onto that designated table.

    :param df: pandas dataframe created to hold participant data fetched from ParticipantSummary API
    :param project_id: identifies the project
    :param destination_table: name of the table to be written in the form of dataset.tablename

    :return: returns a dataset with the participant data
    """

    # Parameter check
    if not isinstance(project_id, str):
        raise RuntimeError(
            f'Please specify the project in which to create the tables')

    table_schema = fields_for(destination_table.split('.')[-1])

    return pandas_gbq.to_gbq(df,
                             destination_table,
                             project_id,
                             if_exists="replace",
                             table_schema=table_schema)


if __name__ == '__main__':
    # columns = ['participantId', 'suspensionStatus', 'suspensionTime']
    # tablename = '_deactivated_participants'
    # get_deactivated_participants('all-of-us-rdr-prod', 'cdr-20201008-20201201', tablename, columns)
    get_site_participant_information('all-of-us-rdr-prod', 'PITT')


