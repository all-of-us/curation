"""
Utility to fetch and store deactivated participants information.

The intent of this module is to call and store deactivated participants information by leveraging the RDR Participant
    Summary API. Deactivated participant information stored is `participantID`, `suspensionStatus`, and `suspensionTime`.
"""

# Python imports
import os

# Third party imports
import google.auth
import google.auth.transport.requests
import requests
import pandas
from google.oauth2 import service_account


def get_access_token(sa_key):
    """
    Obtains GCP Bearer token

    :param sa_key: path that points to your cdr-ops SA key

    :return: returns the access_token
    """

    scopes = [
        'https://www.googleapis.com/auth/cloud-platform', 'email', 'profile'
    ]

    service_account_file = sa_key

    credentials = service_account.Credentials.from_service_account_file(
        filename=service_account_file, scopes=scopes)

    auth_req = google.auth.transport.requests.Request()
    credentials.refresh(auth_req)

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


def get_deactivated_participants(project_id, sa_key, columns):
    """
    Fetches all deactivated participants via API if suspensionStatus = 'NO_CONTACT'

    :param project_id: THE RDR project that contains participant summary data
    :param sa_key: path that points to your cdr-ops SA key
    :param columns: columns to be pushed to a table in BigQuery

    :return: returns dataframe of deactivated participants
    """

    token = get_access_token(sa_key)

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
            for key, val in entry['resource'].items():
                if col == key:
                    item.append(val)
        deactivated_participants.append(item)

    df = pandas.DataFrame(deactivated_participants,
                          columns=deactivated_participants_cols)

    return df
