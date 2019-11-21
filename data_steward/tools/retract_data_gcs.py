#!/usr/bin/env python
from io import StringIO
import argparse
import logging

import bq_utils
import common
import gcs_utils
import resources

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('Data retraction from buckets logger')
# logger.setLevel(logging.INFO)

EXTRACT_PIDS_QUERY = """
SELECT person_id
FROM `{project_id}.{sandbox_dataset_id}.{pid_table_id}`
"""

PID_IN_COL1 = [common.PERSON, common.DEATH] + common.PII_TABLES
PID_IN_COL2 = [common.VISIT_OCCURRENCE, common.CONDITION_OCCURRENCE, common.DRUG_EXPOSURE, common.MEASUREMENT,
               common.PROCEDURE_OCCURRENCE, common.OBSERVATION, common.DEVICE_EXPOSURE, common.SPECIMEN, common.NOTE]


def run_retraction(project_id, sandbox_dataset_id, pid_table_id, hpo_id, folder, force_flag):
    """
    Retract from a folder/folders in a GCS bucket all records associated with a pid

    :param project_id: project contaning the sandbox dataset
    :param sandbox_dataset_id: dataset containing the pid_table
    :param pid_table_id: table containing the person_ids whose data needs to be retracted
    :param hpo_id: hpo_id of the site to run retraction on
    :param folder: the site's submission folder; if unspecified, retract from all folders
    :param force_flag: if False then prompt for each file
    :return: metadata for each object updated in order to retract as a list of lists
    """

    # extract the pids
    pids = extract_pids_from_table(project_id, sandbox_dataset_id, pid_table_id)

    bucket = gcs_utils.get_drc_bucket()
    logger.info('Retracting from bucket %s' % bucket)

    site_bucket = gcs_utils.get_hpo_bucket(hpo_id)
    full_bucket_path = bucket+'/'+hpo_id+'/'+site_bucket
    folder_prefixes = gcs_utils.list_bucket_prefixes(full_bucket_path)

    result_dict = {}
    if folder is None:
        to_process_folder_list = folder_prefixes
    else:
        folder_path = full_bucket_path+'/'+folder if folder[-1] == '/' else full_bucket_path+'/'+folder+'/'

        if folder_path in folder_prefixes:
            to_process_folder_list = [folder_path]
        else:
            logger.info('Folder %s does not exist in %s. Exiting' % (folder, full_bucket_path))
            return result_dict

    logger.info("Retracting data from the following folders:")
    logger.info([bucket+'/'+folder_prefix for folder_prefix in to_process_folder_list])

    for folder_prefix in to_process_folder_list:
        logger.info('Processing gs://%s/%s' % (bucket, folder_prefix))
        # separate cdm from the unknown (unexpected) files
        bucket_items = gcs_utils.list_bucket_dir(bucket+'/'+folder_prefix[:-1])
        found_files = []
        folder_items = [item['name'].split('/')[-1]
                        for item in bucket_items
                        if item['name'].startswith(folder_prefix)]
        for item in folder_items:
            # Only retract from CDM or PII files
            item = item.lower()
            if item in resources.CDM_FILES or item in common.PII_FILES:
                found_files.append(item)

        logger.info('Found the following files to retract data from:')
        logger.info([bucket + '/' + folder_prefix + file_name for file_name in found_files])

        logger.info("Proceed?")
        if force_flag:
            logger.info("Attempting to force retract for folder %s in bucket %s" % (folder_prefix, bucket))
            response = "Y"
        else:
            # Make sure user types Y to proceed
            response = get_response()
        if response == "Y":
            folder_upload_output = retract(pids, bucket, found_files, folder_prefix, force_flag)
            result_dict[folder_prefix] = folder_upload_output
            logger.info("Retraction successful for folder %s/%s " % (bucket, folder_prefix))
        elif response.lower() == "n":
            logger.info("Skipping folder %s" % folder_prefix)
    logger.info("Retraction from GCS complete")
    return result_dict


def retract(pids, bucket, found_files, folder_prefix, force_flag):
    """
    Retract from a folder in a GCS bucket all records associated with a pid
    pid table must follow schema described in retract_data_bq.PID_TABLE_FIELDS and must reside in sandbox_dataset_id
    This function removes lines from all files containing person_ids if they exist in pid_table_id
    Throws SyntaxError/TypeError/ValueError if non-ints are found

    :param pids: person_ids to retract
    :param bucket: bucket containing records to retract
    :param found_files: files found in the current folder
    :param folder_prefix: current folder being processed
    :param force_flag: if False then prompt for each file
    :return: metadata for each object updated in order to retract
    """
    result_list = []
    for file_name in found_files:
        table_name = file_name.split(".")[0]
        lines_removed = 0
        file_gcs_path = '%s/%s%s' % (bucket, folder_prefix, file_name)
        if force_flag:
            logger.info("Attempting to force retract for person_ids %s in path %s/%s%s"
                         % (pids, bucket, folder_prefix, file_name))
            response = "Y"
        else:
            # Make sure user types Y to proceed
            logger.info("Are you sure you want to retract rows for person_ids %s from path %s/%s%s?"
                         % (pids, bucket, folder_prefix, file_name))
            response = get_response()
        if response == "Y":
            # Output and input file content initialization
            retracted_file_string = StringIO()
            input_file_string = gcs_utils.get_object(bucket, folder_prefix + file_name)
            input_file_lines = input_file_string.split('\n')
            input_header = input_file_lines[0]
            input_contents = input_file_lines[1:]
            retracted_file_string.write(input_header + '\n')
            logger.info("Checking for person_ids %s in path %s" % (pids, file_gcs_path))

            # Check if file has person_id in first or second column
            for input_line in input_contents:
                if input_line != '':
                    cols = input_line.split(",")
                    col_1 = cols[0]
                    col_2 = cols[1]
                    if (table_name in PID_IN_COL1 and get_integer(col_1) in pids) or \
                            (table_name in PID_IN_COL2 and get_integer(col_2) in pids):
                        lines_removed += 1
                    else:
                        retracted_file_string.write(input_line + '\n')

            # Write result back to bucket
            if lines_removed > 0:
                logger.info("%d rows retracted from %s, overwriting..." % (lines_removed, file_gcs_path))
                upload_result = gcs_utils.upload_object(bucket, folder_prefix + file_name, retracted_file_string)
                result_list.append(upload_result)
                logger.info("Retraction successful for file %s" % file_gcs_path)
            else:
                logger.info("Not updating file %s since pids %s not found" % (file_gcs_path, pids))
        elif response.lower() == "n":
            logger.info("Skipping file %s" % file_gcs_path)
    return result_list


# Make sure user types Y to proceed
def get_response():
    prompt_text = 'Please press Y/n\n'
    response = input(prompt_text)
    while response not in ('Y', 'n', 'N'):
        response = input(prompt_text)
    return response


def get_integer(num_str):
    """
    Converts an integer in string form to integer form
    Throws SyntaxError/TypeError/ValueError if input string is not an integer and terminates

    :param num_str: an integer in string form
    :return: integer form of num_str
    """
    return int(num_str)


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
    r = bq_utils.query(q)
    rows = bq_utils.response2rows(r)
    pids = set()
    for row in rows:
        pids.add(get_integer(row['person_id']))
    return pids


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Performs retraction on bucket files for site to retract data for, '
                                                 'determined by hpo_id. Uses project_id, sandbox_dataset_id and '
                                                 'pid_table_id to determine the pids to retract data for. '
                                                 'Folder name is optional. Will retract from all folders for the site '
                                                 'if unspecified. Force flag overrides prompts for each folder.',
                                     formatter_class=argparse.RawDescriptionHelpFormatter)

    parser.add_argument('-p', '--project_id',
                        action='store', dest='project_id',
                        help='Identifies the project to retract data from',
                        required=True)
    parser.add_argument('-s', '--sandbox_dataset_id',
                        action='store', dest='sandbox_dataset_id',
                        help='Identifies the dataset containing the pid table',
                        required=True)
    parser.add_argument('-t', '--pid_table_id',
                        action='store', dest='pid_table_id',
                        help='Identifies the table containing the person_ids for retraction',
                        required=True)
    parser.add_argument('-i', '--hpo_id',
                        action='store', dest='hpo_id',
                        help='Identifies the site to retract data from',
                        required=True)
    parser.add_argument('-n', '--folder_name',
                        action='store', dest='folder_name',
                        help='Optional. Path of the folder to retract from',
                        required=False)
    parser.add_argument('-f', '--force_flag', dest='force_flag', action='store_true',
                        help='Optional. Indicates pids must be force retracted',
                        required=False)

    args = parser.parse_args()

    # result is mainly for debugging file uploads
    result = run_retraction(args.project_id, args.sandbox_dataset_id, args.pid_table_id, args.hpo_id,
                            args.folder_name, args.force_flag)
