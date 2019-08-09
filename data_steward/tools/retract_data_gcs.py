#!/usr/bin/env python
import StringIO
import argparse
import ast
import logging

import common
import gcs_utils
import retract_data_bq
from validation import main as val

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('Data retraction from buckets logger')
logger.setLevel(logging.DEBUG)

# TODO Accommodate should new PII or CDM files be added
PID_IN_COL1 = [common.PERSON] + common.PII_TABLES
PID_IN_COL2 = [common.VISIT_OCCURRENCE, common.CONDITION_OCCURRENCE, common.DRUG_EXPOSURE, common.MEASUREMENT,
               common.PROCEDURE_OCCURRENCE, common.OBSERVATION, common.DEVICE_EXPOSURE, common.SPECIMEN, common.NOTE]


def run_retraction(pids, bucket, folder, force_flag):
    """
    Retract from a folder/folders in a GCS bucket all records associated with a pid

    :param pids: person_ids to retract
    :param bucket: bucket containing records to retract
    :param folder: folder in the bucket; if unspecified, retract from all folders
    :param force_flag: if False then prompt for each file
    :return: metadata for each object updated in order to retract as a list of lists
    """
    logger.debug('Retracting from bucket %s' % bucket)
    bucket_items = val.list_bucket(bucket)

    if folder is not None:
        if folder[-1] != '/':
            folder = folder + '/'

    # Get list of folders in the bucket
    folder_list = set([item['name'].split('/')[0] + '/' 
                       for item in bucket_items 
                       if len(item['name'].split('/')) > 1])
    result_dict = {}

    if folder is None:
        to_process_folder_list = list(folder_list)
    else:
        if folder in folder_list:
            to_process_folder_list = [folder]
        else:
            logger.debug('Folder %s does not exist in bucket %s. Exiting' % (folder, bucket))
            return result_dict

    logger.debug("Retracting data from the following folders:")
    for folder_item in to_process_folder_list:
        logger.debug(folder_item)

    for folder_prefix in to_process_folder_list:
        logger.debug('Processing gs://%s/%s' % (bucket, folder_prefix))
        # separate cdm from the unknown (unexpected) files
        found_files = []
        folder_items = [item['name'].split('/')[1] for item in bucket_items if item['name'].startswith(folder_prefix)]
        for item in folder_items:
            # Only retract from CDM or PII files
            if val._is_cdm_file(item) or val._is_pii_file(item):
                found_files.append(item)

        logger.debug('Found the following files to retract data from:')
        for file_name in found_files:
            logger.debug(bucket + '/' + folder_prefix + file_name)

        logger.debug("Proceed?")
        response = get_response()
        if response == "Y":
            folder_upload_output = retract(pids, bucket, found_files, folder_prefix, force_flag)
            result_dict[folder_prefix] = folder_upload_output
        elif response.lower() == "n":
            logger.debug("Skipping folder %s" % folder_prefix)
    return result_dict


def retract(pids, bucket, found_files, folder_prefix, force_flag):
    """
    Retract from a folder in a GCS bucket all records associated with a pid

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
        if force_flag:
            logger.debug("force retracting rows for person_ids %s from path %s/%s%s" % (pids, bucket, folder_prefix, file_name))
            response = "Y"
        else:
            # Make sure user types Y to proceed
            logger.debug("Are you sure you want to retract rows for person_ids %s from path %s/%s%s?"
                         % (pids, bucket, folder_prefix, file_name))
            response = get_response()
        if response == "Y":
            # Output and input file content initialization
            retracted_file_string = StringIO.StringIO()
            input_file_string = gcs_utils.get_object(bucket, folder_prefix + file_name)
            input_contents = input_file_string.split('\n')
            modified_flag = False

            # Check if file has person_id in first or second column
            for input_line in input_contents:
                if input_line != '':
                    if (table_name in PID_IN_COL1 and get_integer(input_line.split(",")[0]) in pids) or \
                            (table_name in PID_IN_COL2 and get_integer(input_line.split(",")[1]) in pids):
                        lines_removed += 1
                    else:
                        retracted_file_string.write(input_line + '\n')
                        modified_flag = True
            # Write result back to bucket
            if modified_flag:
                logger.debug("Retracted %d rows from %s/%s%s" % (lines_removed, bucket, folder_prefix, file_name))
                logger.debug("Overwriting file %s/%s%s" % (bucket, folder_prefix, file_name))
                upload_result = gcs_utils.upload_object(bucket, folder_prefix + file_name, retracted_file_string)
                result_list.append(upload_result)
            else:
                logger.debug("Skipping file %s/%s%s since pids %s not found" % (bucket, folder_prefix, file_name, pids))
        elif response.lower() == "n":
            logger.debug("Ignoring file %s" % file_name)
    return result_list


# Make sure user types Y to proceed
def get_response():
    prompt_text = 'Please press Y/n\n'
    response = raw_input(prompt_text)
    while response not in ('Y', 'n', 'N'):
        response = raw_input(prompt_text)
    return response


def get_integer(num_str):
    try:
        num = int(ast.literal_eval(str(num_str)))
        if isinstance(num, int):
            return num
    except ValueError:
        return None


if __name__ == '__main__':
    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter)

    parser.add_argument('-p', '--pid_file',
                        action='store', dest='pid_file',
                        help='Text file containing the pids on separate lines',
                        required=True)
    parser.add_argument('-b', '--bucket',
                        action='store', dest='bucket',
                        help='Identifies the bucket to retract data from',
                        required=True)
    parser.add_argument('-p', '--folder_path',
                        action='store', dest='folder_path',
                        help='Path of the folder to retract from')
    parser.add_argument('-f', '--force_flag', dest='force_flag', action='store_true',
                        help='Indicates pids must be force retracted')

    args = parser.parse_args()
    pids = retract_data_bq.extract_pids_from_file(args.pid_file)
    # result is mainly for debugging file uploads
    result = run_retraction(pids, args.bucket, args.folder_path, args.force_flag)
