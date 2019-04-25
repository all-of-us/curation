#!/usr/bin/env python
import StringIO
import argparse
import ast

import common
import gcs_utils
from validation import main as val

# TODO Accommodate should new PII or CDM files be added
PID_IN_COL1 = ["person.csv"] + common.PII_TABLES
PID_IN_COL2 = ["visit_occurrence.csv", "condition_occurrence.csv", "drug_exposure.csv", "measurement.csv",
               "procedure_occurrence.csv", "observation.csv", "device_exposure.csv", "specimen.csv", "note.csv"]


def run_retraction(pid, bucket, folder, force):
    """
    Retract from a folder/folders in a GCS bucket all records associated with a pid

    :param pid: person_id
    :param bucket: bucket containing records to retract
    :param folder: folder in the bucket; if unspecified, retract from all folders
    :param force: if False then prompt for each file
    :return: metadata for each object updated in order to retract as a list of lists
    """
    print('Retracting from bucket %s' % bucket)
    bucket_items = val.list_bucket(bucket)

    if folder is not None:
        if folder[-1] != '/':
            folder = folder + '/'

    # Get list of folders in the bucket
    folder_list = set([item['name'].split('/')[0] + '/' for item in bucket_items if len(item['name'].split('/')) > 1])
    result_dict = {}

    if folder is None:
        to_process_folder_list = list(folder_list)
    else:
        if folder in folder_list:
            to_process_folder_list = [folder]
        else:
            print('Folder %s does not exist in bucket %s. Exiting' % (folder, bucket))
            return result_dict

    print("Retracting data from the following folders:")
    for folder_item in to_process_folder_list:
        print(folder_item)

    for folder_prefix in to_process_folder_list:
        print('Processing gs://%s/%s' % (bucket, folder_prefix))
        # separate cdm from the unknown (unexpected) files
        found_files = []
        folder_items = [item['name'].split('/')[1] for item in bucket_items if item['name'].startswith(folder_prefix)]
        for item in folder_items:
            # Only retract from CDM or PII files
            if val._is_cdm_file(item) or val._is_pii_file(item):
                found_files.append(item)

        print('Found the following files to retract data from:')
        for file_name in found_files:
            print(bucket + '/' + folder_prefix + file_name)

        print("Proceed?")
        response = get_response()
        if response == "Y":
            folder_upload_output = retract(pid, bucket, found_files, folder_prefix, force)
            result_dict[folder_prefix] = folder_upload_output
        elif response.lower() == "n":
            print("Skipping folder %s" % folder_prefix)
    return result_dict


def retract(pid, bucket, found_files, folder_prefix, force):
    """
    Retract from a folder in a GCS bucket all records associated with a pid

    :param pid: person_id
    :param bucket: bucket containing records to retract
    :param found_files: files found in the current folder
    :param folder_prefix: current folder being processed
    :param force: if False then prompt for each file
    :return: metadata for each object updated in order to retract
    """
    result_list = []
    for file_name in found_files:
        if force:
            print("Force retracting rows for person_id %s from path %s/%s%s" % (pid, bucket, folder_prefix, file_name))
            response = "Y"
        else:
            # Make sure user types Y to proceed
            print("Are you sure you want to retract rows for person_id %s from path %s/%s%s?"
                  % (pid, bucket, folder_prefix, file_name))
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
                    if (file_name in PID_IN_COL1 and get_integer(input_line.split(",")[0]) != pid) or \
                            (file_name in PID_IN_COL2 and get_integer(input_line.split(",")[1]) != pid):
                        retracted_file_string.write(input_line + '\n')
                    else:
                        modified_flag = True
            # TODO: return number of lines removed, message if no file in the folder was updated
            # Write result back to bucket
            if modified_flag:
                print("Overwriting file %s/%s%s" % (bucket, folder_prefix, file_name))
                upload_result = gcs_utils.upload_object(bucket, folder_prefix + file_name, retracted_file_string)
                result_list.append(upload_result)
            else:
                print("Skipping file %s/%s%s since pid %s not found" % (bucket, folder_prefix, file_name, pid))
        elif response.lower() == "n":
            print("Ignoring file %s" % file_name)
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
    parser = argparse.ArgumentParser()

    parser.add_argument('-f', action='store_true', help='Force retraction', required=False)
    parser.add_argument('-i', '--pid', help='Person ID', required=True)
    parser.add_argument('-b', '--bucket', help='Bucket name', required=True)
    parser.add_argument('-p', '--folder_path', help='Folder path', required=False)

    args = vars(parser.parse_args())
    # result is mainly for debugging file uploads
    result = run_retraction(get_integer(args['pid']), args['bucket'], args['folder_path'], args['f'])
