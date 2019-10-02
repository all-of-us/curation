#!/usr/bin/env python
# coding: utf-8
# # REQUIREMENTS
#
# Remove data for questions participants should not have received (based on improper branching logic). For some questions responses should only appear depending on how their parent question was answered. A bug in the PTSC causes some responses to appear for such child questions where they should not. The solution is to remove these responses.
#
# This notebook accepts a set of CSV files where:
# 1. column headers indicate the observation_source_value of observation rows to delete
# 1. cells represent person_id of observation rows to delete
#
# For example, if person_id 123456789 appears in the column with heading "Insurance_InsuranceType," this will remove all rows in the observation table having person_id 123456789 and observation_source_value "Insurance_InsuranceType".

import pandas as pd
import os
from bq import query


PROJECT_ID = os.environ.get('APPLICATION_ID')
SANDBOX_DATASET_ID = '' # dataset where intermediary tables are stored
TARGET_DATASET_ID = ''  # dataset where records must be deleted
COMBINED = ''           # dataset containing the deid_map table
DEID = 'deid' in TARGET_DATASET_ID                # if True map research IDs to participant IDs

# +
OBS_COUNT_FMT = """SELECT COUNT(1) n FROM `{PROJECT_ID}.{TARGET_DATASET_ID}.observation` 
WHERE observation_source_value = '{OBSERVATION_SOURCE_VALUE}'
AND person_id IN ({PERSON_IDS})
"""

DEID_OBS_COUNT_FMT = """SELECT COUNT(1) n FROM `{PROJECT_ID}.{TARGET_DATASET_ID}.observation` o
JOIN `{PROJECT_ID}.{COMBINED}.deid_map` d ON o.person_id = d.research_id
WHERE observation_source_value = '{OBSERVATION_SOURCE_VALUE}'
AND d.person_id IN ({PERSON_IDS})
"""

OBS_QUERY_FMT = """SELECT o.* FROM `{PROJECT_ID}.{TARGET_DATASET_ID}.observation` o
WHERE o.observation_source_value = '{OBSERVATION_SOURCE_VALUE}'
AND d.person_id IN ({PERSON_IDS})
"""

DEID_OBS_QUERY_FMT = """SELECT o.* FROM `{PROJECT_ID}.{TARGET_DATASET_ID}.observation` o
JOIN `{PROJECT_ID}.{COMBINED}.deid_map` d ON o.person_id = d.research_id
WHERE observation_source_value = '{OBSERVATION_SOURCE_VALUE}'
AND d.person_id IN ({PERSON_IDS})
"""

def csv_file_updates(csv_file):
    """
    Summarize the deletes associated with a CSV file
    
    :param csv_file: path to a file where each column is a list of pids and the header is an observation_source_value
    :return: dictionary with keys file_name, observation_source_value, num_pids, num_rows, q
    """
    
    if not os.path.exists(csv_file):
        raise IOError('File "%s" not found' % csv_file)
    obs_count_fmt = OBS_COUNT_FMT
    obs_query_fmt = OBS_QUERY_FMT
    if DEID:
        obs_count_fmt = DEID_OBS_COUNT_FMT
        obs_query_fmt = DEID_OBS_QUERY_FMT
    file_name = os.path.basename(csv_file)
    csv_df = pd.read_csv(csv_file)
    cols = list(csv_df.columns.to_native_types())
    results = list()
    for col in cols:
        person_ids = csv_df[col].dropna().apply(str).to_list()
        q = obs_count_fmt.format(PROJECT_ID=PROJECT_ID, 
                                 TARGET_DATASET_ID=TARGET_DATASET_ID, 
                                 COMBINED=COMBINED,
                                 OBSERVATION_SOURCE_VALUE=col,
                                 PERSON_IDS=', '.join(person_ids))
        num_rows_result = query(q)
        q = obs_query_fmt.format(PROJECT_ID=PROJECT_ID, 
                                 TARGET_DATASET_ID=TARGET_DATASET_ID, 
                                 COMBINED=COMBINED,
                                 OBSERVATION_SOURCE_VALUE=col,
                                 PERSON_IDS=', '.join(person_ids))
        num_rows = num_rows_result.iloc[0]['n']
        result = dict(file_name=file_name,
                      observation_source_value=col, 
                      q=q, 
                      num_pids=len(person_ids), 
                      num_rows=num_rows)
        results.append(result)
    return results

def updates_from_csv_files(csv_files):
    all_updates = list()
    for csv_file in csv_files:
        updates = csv_file_updates(csv_file)
        all_updates += updates
    return all_updates


# -

def print_updates(updates):
    for update in updates:
        print(update['file_name'])
        print(update['observation_source_value'])
        print('Number of person_ids in CSV file : %s' % update['num_pids'])
        print('Number of rows that would be removed from %s.observation: %s' % (TARGET_DATASET_ID, update['num_rows']))


def temp_table_query(updates):
    """
    Get the query used to generate a temp table to store records to be deleted
    """
    subqueries = []
    for update in updates:
        subquery = ' (' + update['q'] + ') '
        subqueries.append(subquery)
    union_all_query = '\nUNION ALL\n'.join(subqueries)
    return union_all_query


# TODO use spreadsheets as external data sources in queries
csv_files = ['AC67-REMOVE DATA-CHILD QUESTIONS-PERSONAL MEDICAL.csv',
            'AC67-REMOVE DATA-CHILD QUESTIONS-OVERALL HEALTH.csv',
            'AC67-REMOVE DATA-CHILD QUESTIONS-LIFESTYLE.csv',
            'AC67-REMOVE DATA-CHILD QUESTIONS-HCAU.csv']
updates = updates_from_csv_files(csv_files)
