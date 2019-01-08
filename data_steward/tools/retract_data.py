#!/usr/bin/env python
import StringIO
from validation import main as val
import argparse

import gcs_utils


PID_IN_COL1 = ["person.csv","pii_name.csv","pii_email.csv","pii_phone_number.csv","pii_address.csv","pii_mrn.csv"]
PID_IN_COL2 = ["visit_occurrence.csv","condition_occurrence.csv","drug_exposure.csv","measurement.csv",
               "procedure_occurrence.csv","observation.csv","device_exposure.csv","specimen.csv","note.csv"]


def retract_from_bucket(pid, bucket, folder_path=None, force=False):
    """
    data retraction end point
    """
    if folder_path is not None:
        if folder_path[-1] != '/':
            folder_path = folder_path+'/'
    result = run_retraction(pid, bucket, folder_path, force)
    return result


def run_retraction(pid, bucket, folder, force):
    """
    runs retraction for a single hpo_id
    """
    print('Retracting from bucket %s' % bucket)
    bucket_items = val.list_bucket(bucket)

    # Get list of folders in the bucket
    folder_list = set([item['name'].split('/')[0] + '/' for item in bucket_items if len(item['name'].split('/')) > 1])
    result = []

    if folder is None:
        to_process_folder_list = list(folder_list)
        print("Retracting data from the following folders:")
        for folder_item in to_process_folder_list:
            print(folder_item)
    else:
        if folder in folder_list:
            to_process_folder_list = [folder]
            print("Retracting data from the following folders:")
            print(folder)
        else:
            print('Folder %s does not exist in bucket %s. Exiting' % (folder, bucket))
            return result

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
            print(bucket+'/'+folder_prefix+file_name)

        print("Proceed?")
        response = get_response()
        if response == "Y":
            result = retract(pid, bucket, found_files, folder_prefix, force)
        elif response.lower() == "n":
            print("Ignoring folder %s" % folder_prefix)
            continue

    return result


def retract(pid, bucket, found_files, folder_prefix, force):
    result = []
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

            # Check if file has person_id in first or second column
            for input_line in input_contents:
                if input_line != '':
                    if (file_name in PID_IN_COL1 and get_integer(input_line.split(",")[0]) != pid) or \
                       (file_name in PID_IN_COL2 and get_integer(input_line.split(",")[1]) != pid):
                        retracted_file_string.write(input_line + '\n')
            # Write result back to bucket
            result.append(gcs_utils.upload_object(bucket, folder_prefix + file_name, retracted_file_string))
        elif response.lower() == "n":
            print("Ignoring file %s" % file_name)
            continue
    return result


# Make sure user types Y to proceed
def get_response():
    response = raw_input("Please press Y/n\n")
    while response not in ("Y", "y", "n", "N"):
        response = raw_input("Please press Y/n\n")
    if response == "y":
        while response not in ("Y", "n", "N"):
            response = raw_input("Please press Y\n")
    return response


def get_integer(num_str):
    try:
        num = int(eval(str(num_str)))
        if type(num) == int:
            return num
    except:
        return None


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('-f', action='store_true', help='Force retraction', required=False)
    parser.add_argument('-i', '--pid', help='Person ID', required=True)
    parser.add_argument('-b', '--bucket', help='Bucket name', required=True)
    parser.add_argument('-p', '--folder_path', help='Folder path', required=False)

    args = vars(parser.parse_args())
    # result is mainly for debugging file uploads
    result = retract_from_bucket(get_integer(args['pid']), args['bucket'], args['folder_path'], args['f'])

