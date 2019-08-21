#!/usr/bin/env python
import StringIO
import argparse
import ast
import logging

import common
import gcs_utils
import resources
import retract_data_bq

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('Data retraction from buckets logger')
# logger.setLevel(logging.DEBUG)

PID_IN_COL1 = [common.PERSON, common.DEATH] + common.PII_TABLES
PID_IN_COL2 = [common.VISIT_OCCURRENCE, common.CONDITION_OCCURRENCE, common.DRUG_EXPOSURE, common.MEASUREMENT,
               common.PROCEDURE_OCCURRENCE, common.OBSERVATION, common.DEVICE_EXPOSURE, common.SPECIMEN, common.NOTE]


def run_retraction(pids, bucket, hpo_id, site_bucket, folder, force_flag):
    """
    Retract from a folder/folders in a GCS bucket all records associated with a pid

    :param pids: person_ids to retract
    :param bucket: bucket containing records to retract
    :param hpo_id: hpo_id of the site to run retraction on
    :param site_bucket: bucket name associated with the site
    :param folder: the site's submission folder; if unspecified, retract from all folders
    :param force_flag: if False then prompt for each file
    :return: metadata for each object updated in order to retract as a list of lists
    """
    logger.debug('Retracting from bucket %s' % bucket)
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
            logger.debug('Folder %s does not exist in %s. Exiting' % (folder, full_bucket_path))
            return result_dict

    logger.debug("Retracting data from the following folders:")
    logger.debug([bucket+'/'+folder_prefix for folder_prefix in to_process_folder_list])

    for folder_prefix in to_process_folder_list:
        logger.debug('Processing gs://%s/%s' % (bucket, folder_prefix))
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

        logger.debug('Found the following files to retract data from:')
        logger.debug([bucket + '/' + folder_prefix + file_name for file_name in found_files])

        logger.debug("Proceed?")
        if force_flag:
            logger.debug("Attempting to force retract for folder %s in bucket %s" % (folder_prefix, bucket))
            response = "Y"
        else:
            # Make sure user types Y to proceed
            response = get_response()
        if response == "Y":
            folder_upload_output = retract(pids, bucket, found_files, folder_prefix, force_flag)
            result_dict[folder_prefix] = folder_upload_output
            logger.debug("Retraction successful for folder %s/%s " % (bucket, folder_prefix))
        elif response.lower() == "n":
            logger.debug("Skipping folder %s" % folder_prefix)
    logger.debug("Retraction from GCS complete")
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
            logger.debug("Attempting to force retract for person_ids %s in path %s/%s%s"
                         % (pids, bucket, folder_prefix, file_name))
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

            logger.debug("Checking for person_ids %s in path %s/%s%s"
                         % (pids, bucket, folder_prefix, file_name))

            # Check if file has person_id in first or second column
            for input_line in input_contents:
                if input_line != '':
                    if (table_name in PID_IN_COL1 and get_integer(input_line.split(",")[0]) in pids) or \
                            (table_name in PID_IN_COL2 and get_integer(input_line.split(",")[1]) in pids):
                        lines_removed += 1
                        modified_flag = True
                    else:
                        retracted_file_string.write(input_line + '\n')

            # Write result back to bucket
            if modified_flag:
                logger.debug("Retracted %d rows from %s/%s%s" % (lines_removed, bucket, folder_prefix, file_name))
                logger.debug("Overwriting file %s/%s%s" % (bucket, folder_prefix, file_name))
                upload_result = gcs_utils.upload_object(bucket, folder_prefix + file_name, retracted_file_string)
                result_list.append(upload_result)
                logger.debug("Retraction successful for file %s/%s%s " % (bucket, folder_prefix, file_name))
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
    except (SyntaxError, TypeError, ValueError):
        return None


if __name__ == '__main__':
    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter)

    parser.add_argument('-i', '--pid_file',
                        action='store', dest='pid_file',
                        help='Text file containing the pids on separate lines',
                        required=True)
    parser.add_argument('-b', '--bucket',
                        action='store', dest='bucket',
                        help='Identifies the bucket to retract data from',
                        required=True)
    parser.add_argument('-s', '--hpo_id',
                        action='store', dest='hpo_id',
                        help='Identifies the site to retract data from',
                        required=True)
    parser.add_argument('-a', '--hpo_bucket',
                        action='store', dest='hpo_bucket',
                        help='Identifies the site bucket to retract data from',
                        required=True)
    parser.add_argument('-n', '--folder_name',
                        action='store', dest='folder_name',
                        help='Path of the folder to retract from')
    parser.add_argument('-f', '--force_flag', dest='force_flag', action='store_true',
                        help='Indicates pids must be force retracted')

    args = parser.parse_args()
    pids = retract_data_bq.extract_pids_from_file(args.pid_file)
    # result is mainly for debugging file uploads
    result = run_retraction(pids, args.bucket, args.hpo_id, args.hpo_bucket, args.folder_name, args.force_flag)
