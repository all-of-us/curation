"""
Utility to fetch and store deactivated participants information.

Original Issues: DC-797, DC-971 (sub-task), DC-972 (sub-task)

The intent of this module is to call and store deactivated participants information by leveraging the RDR Participant
    Summary API. Deactivated participant information stored is `participantID`, `suspensionStatus`, and `suspensionTime`.
"""

# Python imports
import requests
import pandas
import os

# Third party imports
from google.oauth2 import service_account
from google.cloud import storage


def get_access_token(project_id):
    """
    Obtains GCR access token

    :param project_id: The project that will be used to
    :return: returns the access_token
    """
    scopes = ['https://www.googleapis.com/auth/sqlservice.admin']
    service_account_file = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')

    credentials = service_account.Credentials.from_service_account_file(
        service_account_file, scopes=scopes)
    try:
        list(storage.Client(project=project_id, credentials=credentials).bucket('random_bucket').list_blobs())
    except:
        """This will fail because no bucket exists with the name 'random buck' in this project.
           This is fine because the library attempted to use the credentials and in doing so
           generated the access token which is needed."""
        pass

    access_token = credentials.token

    if access_token.startswith('ya'):
        return access_token
    else:
        raise RuntimeError('Authentication Token Error')


def get_deactivated_participants(project_id):
    """
    Gets all deactivated participants via API if suspensionStatus = 'NO_CONTACT'

    :param project_id: The project that will contain the created table.

    :return: returns list of deactivated_participants
    """
    token = get_access_token(project_id)
    print(token)

    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer {0}'.format(token)
    }

    print(headers)

    # Make request to get API version. This is the current RDR version for reference
    # See https://github.com/all-of-us/raw-data-repository/blob/master/opsdataAPI.md for documentation of this api.
    url = 'https://{0}.appspot.com/rdr/v1/ParticipantSummary?_sort=lastModified&suspensionStatus={1}'.format(
        project_id, 'NO_CONTACT')
    print(url)
    not_done = True
    data = []

    while not_done:
        resp = requests.get(url, headers=headers)
        if not resp or resp.status_code != 200:
            print('Error: api request failed.\n\n{0}.'.format(
                resp.text if resp else 'Unknown error.'))
        else:
            r_json = resp.json()
            data += r_json['entry']
            if 'link' in r_json:
                url = r_json['link'][0]['url']
            else:
                not_done = False

    deactivated_participants_cols = ['participantId', 'suspensionStatus', 'suspensionTime']  # any other relevant columns we want to push to a table in BQ

    deactivated_participants = []
    # loop over participant summary records, insert participant data in same order as good_cols.
    for entry in data:
        item = []
        for col in deactivated_participants_cols:
            for key, val in entry['resource'].items():
                if col == key:
                    item.append(val)
        deactivated_participants.append(item)

    df = pandas.DataFrame(deactivated_participants, columns=deactivated_participants_cols)
    print(df)
