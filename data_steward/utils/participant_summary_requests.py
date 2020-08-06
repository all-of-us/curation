"""
Utility to fetch and store deactivated participants information.

The intent of this module is to call and store deactivated participants information by leveraging the RDR Participant
    Summary API. Deactivated participant information stored is `participantID`, `suspensionStatus`, and `suspensionTime`.
"""

# Third party imports
from google.auth import default
import google.auth.transport.requests as req
import requests
import pandas
import pandas_gbq

# Project imports
from utils import auth


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
    Fetches participant data via ParticipantSummary API

    :param url: the /ParticipantSummary endpoint to fetch information about the participant
    :param headers: the metadata associated with the API request and response

    :return: list of data fetched from the ParticipantSummary API
    """

    done = False
    participant_data = []

    while not done:
        resp = requests.get(url, headers=headers)
        if not resp or resp.status_code != 200:
            print(f'Error: API request failed because {resp}')
        else:
            r_json = resp.json()
            participant_data += r_json.get('entry', {})
            if 'link' in r_json:
                link_obj = r_json.get('link')
                url = link_obj[0].get('url')
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
    if not project_id or not isinstance(project_id, str):
        raise RuntimeError(f'Please specify the RDR project')

    if not dataset_id or not isinstance(dataset_id, str):
        raise RuntimeError(f'Please provide a dataset_id')

    if not tablename or not isinstance(tablename, str):
        raise RuntimeError(
            f'Please provide a tablename to house deactivated participant data')

    if not columns or not isinstance(columns, list):
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
    # loop over participant summary records, insert participant data in same order as good_cols.
    for entry in participant_data:
        item = []
        for col in deactivated_participants_cols:
            for key, val in entry.get('resource', {}).items():
                if col == key:
                    item.append(val)
        deactivated_participants.append(item)

    df = pandas.DataFrame(deactivated_participants,
                          columns=deactivated_participants_cols)

    # To store dataframe in a BQ dataset table
    destination_table = dataset_id + '.' + tablename

    dataset = store_participant_data(df, project_id, destination_table)

    return dataset


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
    if not project_id or not isinstance(project_id, str):
        raise RuntimeError(
            f'Please specify the project in which to create the tables')

    stored_dataset = pandas_gbq.to_gbq(df,
                                       destination_table,
                                       project_id,
                                       if_exists="append")

    return stored_dataset
