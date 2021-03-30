"""
Background

The Genomics program requires stable research IDs (RIDs). This is a script that will
add only pid/rid mappings for participants that don't currently exist in the 
priamry pid_rid_mapping table. 

The regisered tier deid module contained the logic to generate a _deid_map table
containing person_id, research_id, and date_shift.  The date shift can be created
here as it was created there.

These records will be appended to the pipeline_tables.pid_rid_mapping table in BigQuery.
There cannot be duplicate mappings.
"""
# Python imports
import argparse
from datetime import datetime, date

# Third party imports
from google.cloud import bigquery
import pandas as pd
import numpy as np
import pandas_gbq

# Project imports
from common import JINJA_ENV
from utils import auth, bq, pipeline_logging

SCOPES = [
    'https://www.googleapis.com/auth/bigquery',
    'https://www.googleapis.com/auth/devstorage.read_write',
]

GET_NEW_MAPPINGS = JINJA_ENV.from_string("""
SELECT person_id, research_id
FROM `{{rdr_table.project}}.{{rdr_table.dataset_id}}.{{rdr_table.table_id}}`
WHERE person_id not in (
  SELECT person_id
  FROM `{{primary.project}}.{{primary.dataset_id}}.{{primary.table_id}}`)
-- This is just to make sure we don't duplicate either person_id OR research_id --
AND research_id not in (
  SELECT research_id
  FROM `{{primary.project}}.{{primary.dataset_id}}.{{primary.table_id}}`)
""")


def milliseconds_since_epoch():
    """
    Helper method to get the number of milliseconds from the epoch to now
    :return:  an integer number of milliseconds
    """
    return int(
        (datetime.utcnow() - datetime(1970, 1, 1)).total_seconds() * 1000)


def get_participants(api_project_id, existing_pids):
    """
    Method to hit the participant summary API based on cutoff dates and max age. Filters out participants that already
    exist in the pipeline_tables._deid_map table.

    Loops through the results to double check the API returned the correct participants.

    :param api_project_id: project_id to send to API call
    :param existing_pids: list of pids that already exist in mapping table
    :return: dataframe with single column person_id from participant summary API, which needs RIDS created for
    """

    # create datetimes from cutoff dates and datetimes to bin API call requests
    bin_1_gt_datetime = datetime.strptime('2019-08-31T23:59:59',
                                          '%Y-%m-%dT%H:%M:%S')
    bin_1_lt_datetime = datetime.strptime('2020-08-02T00:00:00',
                                          '%Y-%m-%dT%H:%M:%S')
    bin_2_datetime = datetime.strptime('2019-01-01T00:00:00',
                                       '%Y-%m-%dT%H:%M:%S')
    bin_3_gt_datetime = datetime.strptime('2018-12-31T23:59:59',
                                          '%Y-%m-%dT%H:%M:%S')
    bin_3_lt_datetime = datetime.strptime('2019-09-01T00:00:00',
                                          '%Y-%m-%dT%H:%M:%S')

    # Make request to get API version. This is the current RDR version for reference
    # See https://github.com/all-of-us/raw-data-repository/blob/master/opsdataAPI.md for documentation of this api.
    request_url_cutoff_participants = "https://{0}.appspot.com/rdr/v1/ParticipantSummary?_sort=" \
                                      "consentForStudyEnrollmentAuthored&withdrawalStatus={1}" \
                                      "&consentForStudyEnrollmentAuthored=gt{2}&consentForStudyEnrollmentAuthored=" \
                                      "lt{3}".format(api_project_id, 'NOT_WITHDRAWN', bin_1_gt_datetime,
                                                     bin_1_lt_datetime)
    request_url_max_age_participants_1 = "https://{0}.appspot.com/rdr/v1/ParticipantSummary?_sort=" \
                                         "consentForStudyEnrollmentAuthored&withdrawalStatus={1}" \
                                         "&consentForStudyEnrollmentAuthored=lt{2}".format(api_project_id,
                                                                                           'NOT_WITHDRAWN',
                                                                                           bin_2_datetime)
    request_url_max_age_participants_2 = "https://{0}.appspot.com/rdr/v1/ParticipantSummary?_sort=" \
                                         "consentForStudyEnrollmentAuthored&withdrawalStatus={1}" \
                                         "&consentForStudyEnrollmentAuthored=gt{2}&consentForStudyEnrollmentAuthored=" \
                                         "lt{3}".format(api_project_id, 'NOT_WITHDRAWN', bin_3_gt_datetime,
                                                        bin_3_lt_datetime)

    list_url_requests = [
        request_url_cutoff_participants, request_url_max_age_participants_1,
        request_url_max_age_participants_2
    ]
    participant_data = []

    # loop through urls and create new tokens each request to avoid API timing out
    for url in list_url_requests:
        token = get_access_token()
        headers = {
            'content-type': 'application/json',
            'Authorization': 'Bearer {0}'.format(token)
        }
        participant_data = participant_data + get_participant_data(url, headers)

    participants = pd.DataFrame(columns=['person_id'])
    # Loop through participant_data to retrieve only person_id
    for participant in participant_data:
        participant = participant['resource']
        participant_id = int(participant['participantId'].replace('P', ''))

        # remove pids that already exist in mapping table
        if participant_id not in existing_pids:
            # turn string into datetime to compare
            participants = participants.append({'person_id': participant_id},
                                               ignore_index=True)

    return participants.drop_duplicates()


def store_to_primary_mapping_table(fq_rdr_mapping_table,
                                   client=None,
                                   run_as=None):
    """
    Method to generate mapping table rows to append to existing mapping table: pipeline_tables._deid_map table
    Retrieves participants from the participant summary API based on specific parameters. Only retrieving participants
    who were submitted during the cutoff date range with no age limit and also retrieving participants outside of the
    cutoff date range above the max age.

    Creates the research_ids and date shift based on previous logic used in DEID. Queries the current
    pipeline_tables._deid_map table to retrieve current research_ids to ensure all research_ids are unique.

    """
    project, dataset, table = fq_rdr_mapping_table.split('.')

    print(
        f'Using: project -> {project}\tdataset -> {dataset}\ttable -> {table}')
    #LOGGER.info(f'Using: project -> {project}\tdataset -> {dataset}\ttable -> {table}')
    if not client and not run_as:
        print('Run cannot proceed without proper credentials')
        #LOGGER.error('Run cannot proceed without proper credentials')

    # set up an impersonated client if one is not provided
    if not client:
        print('Using impersonation credentials.')
        #LOGGER.info('Using impersonation credentials.')
        # get credentials and create client
        impersonation_creds = auth.get_impersonation_credentials(run_as, SCOPES)

        client = bq.get_client(project, credentials=impersonation_creds)

    # rdr table ref
    dataset_ref = bigquery.DatasetReference(project, dataset)
    rdr_table = bigquery.TableReference(dataset_ref, table)

    # pipeline table ref
    pipeline_ref = bigquery.DatasetReference(project, 'pipeline_tables')
    primary_mapping_table = bigquery.TableReference(pipeline_ref,
                                                    'pid_rid_mapping')

    new_mappings = client.query(
        GET_NEW_MAPPINGS.render(rdr_table=rdr_table,
                                primary=primary_mapping_table)).to_dataframe()

    print(new_mappings)
    import sys
    sys.exit(2)

    # retrieve pids in mapping table
    existing_pids = client.query(
        GET_PIDS_IN_EXISITNG_MAPPING.render(
            project=project_id, dataset=dataset,
            table=mapping_table)).to_dataframe()['person_id'].values.tolist()

    person_table = get_participants(api_project_id, existing_pids)

    # retrieve existing rids, to not reuse
    existing_rids = client.query(
        GET_EXISTING_RIDS.render(
            project=project_id, dataset=dataset,
            table=mapping_table)).to_dataframe()['research_id'].values.tolist()

    lower_bound = 1000000
    max_day_shift = 365

    # create the deid_map table.  set upper and lower bounds of the research_id array
    records = person_table.shape[0]
    upper_bound = lower_bound + (10 * records)
    map_table = pd.DataFrame({"person_id": person_table['person_id'].tolist()})

    # generate random research_ids, using numpy setdiff1d to exclude existing rids
    research_id_array = np.random.choice(np.setdiff1d(
        np.arange(lower_bound, upper_bound), existing_rids),
                                         records,
                                         replace=False)

    # throw in some extra, non-deterministic shuffling
    for _ in range(milliseconds_since_epoch() % 5):
        np.random.shuffle(research_id_array)
    map_table['research_id'] = research_id_array

    # generate date shift values
    shift_array = np.random.choice(np.arange(1, max_day_shift), records)

    # throw in some extra, non-deterministic shuffling
    for _ in range(milliseconds_since_epoch() % 5):
        np.random.shuffle(shift_array)
    map_table['shift'] = shift_array

    # write this to bigquery
    pandas_gbq.to_gbq(map_table,
                      dataset + '.' + mapping_table,
                      project_id,
                      if_exists='append')


def check_table_name(name_str):
    name_parts = name_str.split('.')
    if len(name_parts) != 3:
        raise ValueError(f'A fully qualified table name must be of the form '
                         f'<project_id>.<dataset_id>.<table_name> .  You '
                         f'provided {name_str}')

    return name_str


def check_email_address(address_str):
    if '@' not in address_str:
        raise ValueError(f'An email address must be specified.  '
                         f'You supplied {address_str}')

    return address_str


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Add new mappings to our primary pid/rid mapping table.',
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        '-r',
        '--fq_rdr_mapping',
        action='store',
        dest='rdr_mapping',
        help=('The fully qualified rdr mapping table name.  '
              'The project_id will be extracted from this table name.'),
        type=check_table_name,
        required=True)
    parser.add_argument(
        '-i',
        '--run_as',
        action='store',
        dest='run_as',
        help=('The email address of the service account to impersonate.'),
        type=check_email_address)
    args = parser.parse_args()

    store_to_primary_mapping_table(args.rdr_mapping, run_as=args.run_as)
