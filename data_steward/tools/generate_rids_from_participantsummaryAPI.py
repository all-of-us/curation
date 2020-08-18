"""
Background

The Genomics program requires stable research IDs (RIDs). This is a one off script that will generate RIDs for
participants who joined the AoU program after the last data cutoff date and current participants over the max cutoff age.

The deid module currently contains the logic to generate a lookup table _deid_map containing person_id, research_id,
date_shift. In order to ensure that new PID-RID mappings conform to existing privacy methods, the logic currently in
the deid module will be reused in the development of a script which generates the new mappings.

These records will be appended to the pipeline_tables.pid_rid_mapping table in BigQuery. Ensure there are no duplicates

The current participants should be obtained using the Participant Summary API.
The data cutoff data for the Oct 2020 CDR is 8/1/2020.

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
from utils.participant_summary_requests import get_access_token, get_participant_data
from utils.bq import JINJA_ENV

GET_EXISTING_RIDS = JINJA_ENV.from_string("""
SELECT DISTINCT research_id FROM `{{project}}.{{dataset}}.{{table}}`
""")

GET_PIDS_IN_EXISITNG_MAPPING = JINJA_ENV.from_string("""
SELECT DISTINCT person_id FROM `{{project}}.{{dataset}}.{{table}}`
""")


def calculate_max_age_date(max_age):
    """
    Helper method to get the current date to pass to the participant summary API based on the max_age
    :param max_age: max age of participants
    :return: date x years ago based on max age
    """
    today = date.today()
    year = today.year - int(max_age)
    return date(year, today.month, today.day)


def milliseconds_since_epoch():
    """
    Helper method to get the number of milliseconds from the epoch to now
    :return:  an integer number of milliseconds
    """
    return int(
        (datetime.utcnow() - datetime(1970, 1, 1)).total_seconds() * 1000)


def get_participants(start_date, end_date, max_age, existing_pids):
    """
    Method to hit the participant summary API based on cutoff dates and max age. Filters out participants that already
    exist in the pipeline_tables.pid_rid_mapping table.

    Loops through the results to double check the API returned the correct participants.

    :param start_date: start date of cutoff date range
    :param end_date: end date of cutoff date range
    :param max_age: max age of participants that were initially excluded, that now need to be backfilled
    :param existing_pids: list of pids that already exist in mapping table
    :return: dataframe with single column person_id from participant summary API, which needs RIDS created for
    """
    token = get_access_token()
    headers = {
        'content-type': 'application/json',
        'Authorization': 'Bearer {0}'.format(token)
    }

    # get max age date to pass as parameter to Participant Summary API
    max_age_date = calculate_max_age_date(max_age)

    # create datetime from cutoff_date string
    end_date_obj = datetime.strptime(end_date, '%Y-%m-%dT%H:%M:%S')
    start_date_obj = datetime.strptime(start_date, '%Y-%m-%dT%H:%M:%S')

    # Make request to get API version. This is the current RDR version for reference
    # See https://github.com/all-of-us/raw-data-repository/blob/master/opsdataAPI.md for documentation of this api.
    url_cutoff_participants = "https://{0}.appspot.com/rdr/v1/ParticipantSummary?_sort=lastModified" \
                              "&withdrawalStatus={1}&consentForStudyEnrollmentAuthored=gt{2}" \
                              "&consentForStudyEnrollmentAuthored=lt{3}".format('all-of-us-rdr-prod', 'NOT_WITHDRAWN',
                                                                                start_date_obj, end_date_obj)
    url_max_age_participants_1 = "https://{0}.appspot.com/rdr/v1/ParticipantSummary?_sort=lastModified" \
                                 "&withdrawalStatus={1}&consentForStudyEnrollmentAuthored=lt{2}" \
                                 "&dateOfBirth=gt{3}".format('all-of-us-rdr-prod', 'NOT_WITHDRAWN', start_date_obj,
                                                             max_age_date)
    url_max_age_participants_2 = "https://{0}.appspot.com/rdr/v1/ParticipantSummary?_sort=lastModified" \
                                 "&withdrawalStatus={1}&consentForStudyEnrollmentAuthored=gt{2}" \
                                 "&dateOfBirth=gt{3}".format('all-of-us-rdr-prod', 'NOT_WITHDRAWN', end_date_obj,
                                                             max_age_date)

    participant_data = get_participant_data(
        url_cutoff_participants, headers) + get_participant_data(
            url_max_age_participants_1, headers) + get_participant_data(
                url_max_age_participants_2, headers)

    participants = pd.DataFrame(columns=['person_id'])
    # Loop through participant_data to retrieve only person_id
    for participant in participant_data:
        participant = participant['resource']
        participant_id = int(participant['participantId'].replace('P', ''))

        # remove pids that already exist in mapping table
        if participant_id not in existing_pids:
            # turn string into datetime to compare
            enrollment_date = datetime.strptime(
                participant['consentForStudyEnrollmentAuthored'],
                "%Y-%m-%dT%H:%M:%S")
            # verify API returned only participants between start and cutoff dates
            # and above max age if outside cutoff date range, then append person_id to DF
            if start_date_obj < enrollment_date < end_date_obj:
                participants = participants.append(
                    {'person_id': participant_id}, ignore_index=True)
            elif datetime.strptime(participant['dateOfBirth'],
                                   '%Y-%m-%d').date() > max_age_date:
                participants = participants.append(
                    {'person_id': participant_id}, ignore_index=True)
            else:
                continue

    return participants.drop_duplicates()


def generate_mapping_table_rows(project_id, start_date, end_date, max_age,
                                dataset, mapping_table):
    """
    Method to generate mapping table rows to append to existing mapping table: pipeline_tables.pid_rid_mapping table
    Retrieves participants from the participant summary API based on specific parameters. Only retrieving participants
    who were submitted during the cutoff date range with no age limit and also retrieving participants outside of the
    cutoff date range above the max age.

    Creates the research_ids and date shift based on previous logic used in DEID. Queries the current
    pipeline_tables.pid_rid_mapping table to retrieve current research_ids to ensure all research_ids are unique.

    :param project_id: bq project ID where the mapping table resides
    :param start_date: start date of cutoff date range
    :param end_date: end date of cutoff date range
    :param max_age: max age of participants that were initially excluded, that now need to be backfilled
    :param dataset: bq dataset where the mapping table resides
    :param mapping_table: name of mapping table
    """
    client = bigquery.Client(project=project_id)

    # retrieve pids in mapping table
    existing_pids = client.query(
        GET_PIDS_IN_EXISITNG_MAPPING.render(
            project=project_id, dataset=dataset,
            table=mapping_table)).to_dataframe()['person_id'].values.tolist()

    person_table = get_participants(start_date, end_date, max_age,
                                    existing_pids)

    # retrieve existing rids, to not resuse
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

    # write this to bigquery.
    # pandas_gbq.to_gbq(map_table,
    #                   dataset + '.' + mapping_table,
    #                   project_id,
    #                   if_exists='append')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=
        'Creates research IDs for participants obtained from the Participant Summary API',
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('-p',
                        '--project_id',
                        action='store',
                        dest='project_id',
                        help='Identifies the project',
                        required=True)
    parser.add_argument(
        '-s',
        '--start_date',
        action='store',
        dest='start_date',
        help=
        'Start date of cutoff date range, to only retrieve participants after said date. Formatted in datetime '
        'YYYY-MM-DDTHH:MM:SS',
        required=True)
    parser.add_argument(
        '-e',
        '--end_date',
        action='store',
        dest='end_date',
        help=
        'End date of cutoff date range, to only retrieve participants before said date. Formatted in datetime '
        'YYYY-MM-DDTHH:MM:SS',
        required=True)
    parser.add_argument(
        '-a',
        '--max_age',
        action='store',
        dest='max_age',
        help='Max age of participants, which need to be backfilled eg: 89',
        required=True)
    parser.add_argument('-d',
                        '--dataset',
                        action='store',
                        dest='dataset',
                        help='dataset where the mapping table is',
                        required=True)
    parser.add_argument('-m',
                        '--mapping_table',
                        action='store',
                        dest='mapping_table',
                        help='Table that the PIDS/RIDS will be appended to',
                        required=True)
    args = parser.parse_args()

    generate_mapping_table_rows(args.project_id, args.start_date, args.end_date,
                                args.max_age, args.dataset, args.mapping_table)
