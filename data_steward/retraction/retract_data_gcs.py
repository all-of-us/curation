#!/usr/bin/env python
"""
This script retracts rows for specified pids from the site's submissions located in the archive
The pids must be specified via a pid table containing a person_id and research_id
The pid table must be located in the sandbox_dataset
The schema for the pid table is located in retract_data_bq.py as PID_TABLE_FIELDS
If the submission folder is set to 'all_folders', all the submissions from the site will be considered for retraction
If a submission folder is specified, only that folder will be considered for retraction
"""
import os
from io import BytesIO
import argparse
import logging

from google.cloud import storage, bigquery

import common
from utils import pipeline_logging, gcs

EXTRACT_PIDS_QUERY = """
SELECT person_id
FROM `{project_id}.{sandbox_dataset_id}.{pid_table_id}`
"""

PID_IN_COL1 = [common.PERSON, common.DEATH] + common.PII_TABLES
PID_IN_COL2 = [
    common.VISIT_OCCURRENCE, common.CONDITION_OCCURRENCE, common.DRUG_EXPOSURE,
    common.MEASUREMENT, common.PROCEDURE_OCCURRENCE, common.OBSERVATION,
    common.DEVICE_EXPOSURE, common.SPECIMEN, common.NOTE
]


def run_gcs_retraction(project_id,
                       sandbox_dataset_id,
                       pid_table_id,
                       hpo_id,
                       folder,
                       force_flag,
                       bucket=None,
                       site_bucket=None):
    """
    Retract from a folder/folders in a GCS bucket all records associated with a pid

    :param project_id: project contaning the sandbox dataset
    :param sandbox_dataset_id: dataset containing the pid_table
    :param pid_table_id: table containing the person_ids whose data needs to be retracted
    :param hpo_id: hpo_id of the site to run retraction on
    :param folder: the site's submission folder; if set to 'all_folders', retract from all folders by the site
        if set to 'none', skip retraction from bucket folders
    :param force_flag: if False then prompt for each file
    :param bucket: DRC bucket maintained by curation
    :param site_bucket: Site's bucket name
    """

    # extract the pids
    pids = extract_pids_from_table(project_id, sandbox_dataset_id, pid_table_id)

    if not bucket:
        bucket = os.environ.get('DRC_BUCKET_NAME')
    gcs_client = storage.Client(project_id)
    logging.info(f'Retracting from bucket {bucket}')

    if hpo_id == 'none':
        logging.info('"RETRACTION_HPO_ID" set to "none", skipping retraction')
        full_bucket_path = ''
        folder_prefixes = []
    else:
        if not site_bucket:
            site_bucket = os.environ.get(f'BUCKET_NAME_{hpo_id.upper()}')
        full_bucket_path = bucket + '/' + hpo_id + '/' + site_bucket
        prefix = f'{hpo_id}/{site_bucket}/'
        # retract from latest folders first
        folder_prefixes = gcs.list_sub_prefixes(gcs_client, bucket, prefix)
        folder_prefixes.sort(reverse=True)

    if folder == 'all_folders':
        to_process_folder_list = folder_prefixes
    elif folder == 'none':
        logging.info(
            '"RETRACTION_SUBMISSION_FOLDER" set to "none", skipping retraction')
        to_process_folder_list = []
    else:
        folder_path = full_bucket_path + '/' + folder if folder[
            -1] == '/' else full_bucket_path + '/' + folder + '/'

        if folder_path in folder_prefixes:
            to_process_folder_list = [folder_path]
        else:
            logging.info(
                f'Folder {folder} does not exist in {full_bucket_path}. Exiting'
            )
            return

    logging.info("Retracting data from the following folders:")
    logging.info([
        bucket + '/' + folder_prefix for folder_prefix in to_process_folder_list
    ])

    for folder_prefix in to_process_folder_list:
        logging.info(f'Processing gs://{bucket}/{folder_prefix}')
        # separate cdm from the unknown (unexpected) files
        bucket_item_objs = gcs_client.list_blobs(bucket,
                                                 prefix=folder_prefix,
                                                 delimiter='/')
        folder_items = [blob.name for blob in bucket_item_objs]
        found_files = []
        file_names = [item.split('/')[-1] for item in folder_items]
        for item in file_names:
            # Only retract from CDM or PII files containing PIDs
            item = item.lower()
            table_name = item.split('.')[0]
            if table_name in PID_IN_COL1 + PID_IN_COL2:
                found_files.append(item)

        logging.info('Found the following files to retract data from:')
        logging.info([
            bucket + '/' + folder_prefix + file_name
            for file_name in found_files
        ])

        logging.info("Proceed?")
        if force_flag:
            logging.info(
                f"Attempting to force retract for folder {folder_prefix} in bucket {bucket}"
            )
            response = "Y"
        else:
            # Make sure user types Y to proceed
            response = get_response()
        if response == "Y":
            retract(gcs_client, pids, bucket, found_files, folder_prefix,
                    force_flag)
            logging.info(
                f"Retraction completed for folder {bucket}/{folder_prefix}")
        elif response.lower() == "n":
            logging.info(f"Skipping folder {folder_prefix}")
    logging.info("Retraction from GCS complete")
    return


def retract(gcs_client, pids, bucket, found_files, folder_prefix, force_flag):
    """
    Retract from a folder in a GCS bucket all records associated with a pid
    pid table must follow schema described in retract_data_bq.PID_TABLE_FIELDS and must reside in sandbox_dataset_id
    This function removes lines from all files containing person_ids if they exist in pid_table_id
    Throws SyntaxError/TypeError/ValueError if non-ints are found

    :param gcs_client: google cloud storage client
    :param pids: person_ids to retract
    :param bucket: bucket containing records to retract
    :param found_files: files found in the current folder
    :param folder_prefix: current folder being processed
    :param force_flag: if False then prompt for each file
    """
    for file_name in found_files:
        table_name = file_name.split(".")[0]
        lines_removed = 0
        file_gcs_path = f'{bucket}/{folder_prefix}{file_name}'
        if force_flag:
            logging.info(f"Downloading file in path {file_gcs_path}")
            response = "Y"
        else:
            # Make sure user types Y to proceed
            logging.info(
                f"Are you sure you want to retract rows for person_ids {pids} from path {file_gcs_path}?"
            )
            response = get_response()
        if response == "Y":
            # Output and input file content initialization
            retracted_file_string = BytesIO()
            gcs_bucket = gcs_client.bucket(bucket)
            blob = gcs_bucket.blob(folder_prefix + file_name)
            input_file_lines = blob.download_as_string().split(b'\n')
            if len(input_file_lines) < 2:
                continue
            input_header = input_file_lines[0]
            input_contents = input_file_lines[1:]
            retracted_file_string.write(input_header + b'\n')
            logging.info(
                f"Checking for person_ids {pids} in path {file_gcs_path}")

            # Check if file has person_id in first or second column
            for input_line in input_contents:
                input_line = input_line.strip()
                # ensure line is not empty
                if input_line:
                    cols = input_line.split(b',')
                    # ensure at least two columns exist
                    if len(cols) > 1:
                        col_1 = cols[0].replace(b'"', b'')
                        col_2 = cols[1].replace(b'"', b'')
                        # skip if non-integer is encountered and keep the line as is
                        try:
                            if ((table_name in PID_IN_COL1 and
                                 int(col_1) in pids) or
                                (table_name in PID_IN_COL2 and
                                 int(col_2) in pids)):
                                # do not write back this line since it contains a pid to retract
                                # increment removed lines counter
                                lines_removed += 1
                            else:
                                # pid not found, retain this line
                                retracted_file_string.write(input_line + b'\n')
                        except ValueError:
                            # write back non-num lines
                            retracted_file_string.write(input_line + b'\n')
                    else:
                        # write back ill-formed lines. Note: These lines do not make it into BigQuery
                        retracted_file_string.write(input_line + b'\n')

            # Write result back to bucket
            if lines_removed > 0:
                logging.info(
                    f"{lines_removed} rows retracted from {file_gcs_path}")
                logging.info(f"Uploading to overwrite...")
                blob.upload_from_file(retracted_file_string,
                                      rewind=True,
                                      content_type='text/csv')
                logging.info(f"Retraction successful for file {file_gcs_path}")
            else:
                logging.info(
                    f"Not updating file {file_gcs_path} since pids {pids} not found"
                )
        elif response.lower() == "n":
            logging.info(f"Skipping file {file_gcs_path}")
    return


# Make sure user types Y to proceed
def get_response():
    prompt_text = 'Please press Y/n\n'
    response = input(prompt_text)
    while response not in ('Y', 'n', 'N'):
        response = input(prompt_text)
    return response


def extract_pids_from_table(project_id, sandbox_dataset_id, pid_table_id):
    """
    Extracts person_ids from table in BQ in the form of a set of integers

    :param project_id: project containing the sandbox dataset with pid table
    :param sandbox_dataset_id: dataset containing the pid table
    :param pid_table_id: identifies the table containing the person_ids to retract
    :return: set of integer pids
    """
    q = EXTRACT_PIDS_QUERY.format(project_id=project_id,
                                  sandbox_dataset_id=sandbox_dataset_id,
                                  pid_table_id=pid_table_id)
    client = bigquery.Client(project_id)
    job = client.query(q)
    pids = job.result().to_dataframe()['person_id'].to_list()
    return pids


if __name__ == '__main__':
    pipeline_logging.configure(logging.DEBUG, add_console_handler=True)

    parser = argparse.ArgumentParser(
        description=
        'Performs retraction on bucket files for site to retract data for, '
        'determined by hpo_id. Uses project_id, sandbox_dataset_id and '
        'pid_table_id to determine the pids to retract data for. '
        'Folder name is optional. Will retract from all folders for the site '
        'if unspecified. Force flag overrides prompts for each folder.',
        formatter_class=argparse.RawDescriptionHelpFormatter)

    parser.add_argument(
        '-p',
        '--project_id',
        action='store',
        dest='project_id',
        help='Identifies the project containing the sandbox dataset',
        required=True)
    parser.add_argument('-s',
                        '--sandbox_dataset_id',
                        action='store',
                        dest='sandbox_dataset_id',
                        help='Identifies the dataset containing the pid table',
                        required=True)
    parser.add_argument(
        '-t',
        '--pid_table_id',
        action='store',
        dest='pid_table_id',
        help='Identifies the table containing the person_ids for retraction',
        required=True)
    parser.add_argument('-i',
                        '--hpo_id',
                        action='store',
                        dest='hpo_id',
                        help='Identifies the site to retract data from',
                        required=True)
    parser.add_argument(
        '-n',
        '--folder_name',
        action='store',
        dest='folder_name',
        help='Name of the folder to retract from'
        'If set to "none", skips retraction'
        'If set to "all_folders", retracts from all folders by the site',
        required=True)
    parser.add_argument(
        '-f',
        '--force_flag',
        dest='force_flag',
        action='store_true',
        help='Optional. Indicates pids must be retracted without user prompts',
        required=False)

    args = parser.parse_args()

    run_gcs_retraction(args.project_id, args.sandbox_dataset_id,
                       args.pid_table_id, args.hpo_id, args.folder_name,
                       args.force_flag)
