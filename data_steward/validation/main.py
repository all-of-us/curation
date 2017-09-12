#!/usr/bin/env python
import logging
from flask import Flask
import os

import api_util
import common
import gcs_utils
import bq_utils
import StringIO

import time

RESULT_CSV = 'result.csv'
WARNINGS_CSV = 'warnings.csv'
UNKNOWN_FILE = 'Unknown file'

PREFIX = '/data_steward/v1/'
app = Flask(__name__)


class DataError(RuntimeError):
    """Bad sample data during import.

  Args:
    msg: Passed through to superclass.
    external: If True, this error should be reported to external partners (HPO). Externally
        reported DataErrors are only reported if HPO recipients are in the config.
  """

    def __init__(self, msg, external=False):
        super(DataError, self).__init__(msg)
        self.external = external


@api_util.auth_required_cron
def validate_hpo_files(hpo_id):
    logging.info('Validating hpo_id %s' % hpo_id)
    bucket = gcs_utils.get_hpo_bucket(hpo_id)
    bucket_items = gcs_utils.list_bucket(bucket)

    # separate cdm from the unknown (unexpected) files
    found_cdm_files = []
    unknown_files = []
    for bucket_item in bucket_items:
        if _is_cdm_file(bucket_item):
            found_cdm_files.append(bucket_item)
        else:
            unknown_files.append(bucket_item)

    # (filename, found) for each expected cdm file
    result = [
        (cdm_file, 1 if cdm_file in map(lambda f: f['name'], found_cdm_files) else 0) for cdm_file in common.CDM_FILES
    ]

    # (filename, found, parsed, loaded) for each expected cdm file
    cdm_file_status_map = {}
    for cdm_file in common.CDM_FILES:
        if cdm_file in map(lambda f: f['name'], found_cdm_files) :
            # This is where the parse_check would go.
            # don't run the rest of this is csv not parseable
            
            # run a bq_load jb
            result = bq_utils.load_table_from_bucket('chci',
                                                     cdm_file.split('.')[0])

            load_job_id = result['jobReference']['jobId']
            job_running = True

            wait_count = 100
            while job_running :

                load_job_resource=bq_utils.get_job_details(job_id=load_job_id)
                
                if load_job_resource['status']['state'] == 'DONE':
                    job_running = False
                    if load_job_resource['status'].get('errorResult',None) is None : 
                        cdm_file_status_map[cdm_file] = {'found':1,'parsed':1,'loaded':1} 
                    else:
                        logging.info("file {} has errors {}".format(cdm_file, 
                                                                    load_job_resource['status']['errors'])
                                    )
                        # If not parseable, should'nt get here.
                        cdm_file_status_map[cdm_file] = {'found':1,'parsed':1,'loaded':0} 

                wait_count = wait_count - 1
                if wait_count == 0: 
                    cdm_file_status_map[cdm_file] = {'found':1,'parsed':0,'loaded':0} 
                    break

                time.sleep(1)
        else:
            cdm_file_status_map[cdm_file] = {'found':0,'parsed':0,'loaded':0} 
            
    # (filename, message) for each unknown file
    warnings = [
        (unknown_file['name'], UNKNOWN_FILE) for unknown_file in unknown_files
    ]

    # the following loop is used instead of python list-comprehension but the
    # problem is tha cdm_file order gets garbled because of python dict
    result = []
    for cdm_file in common.CDM_FILES:
        status = cdm_file_status_map[cdm_file]
        result = result + [(cdm_file,status['found'],status['parsed'],status['loaded'])]

    # output to GCS
    _save_result_in_gcs(bucket, RESULT_CSV, result)

    if len(warnings) > 0:
        _save_warnings_in_gcs(bucket, WARNINGS_CSV, warnings)
    return '{"report-generator-status": "started"}'


def _is_cdm_file(gcs_file_stat):
    return gcs_file_stat['name'].lower() in common.CDM_FILES


def _save_warnings_in_gcs(bucket, name, warnings):
    """
    Save the warnings in GCS
    :param bucket: bucket to save to
    :param name: name of the file (object) to save to in GCS
    :param warnings: list of tuples (<file_name>, <message>)
    :return:
    """
    f = StringIO.StringIO()
    f.write('"file_name","message"\n')
    for (file_name, message) in warnings:
        line = '"%(file_name)s","%(message)s"\n' % locals()
        f.write(line)
    f.seek(0)
    result = gcs_utils.upload_object(bucket, name, f)
    f.close()
    return result


def _save_result_in_gcs(bucket, name, cdm_file_results):
    """
    Save the validation results in GCS
    :param bucket: bucket to save to
    :param name: name of the file (object) to save to in GCS
    :param cdm_file_results: list of tuples (<cdm_file_name>, <found>)
    :return:
    """
    f = StringIO.StringIO()
    f.write('"cdm_file_name","found","parsed","loaded"\n')
    for (cdm_file_name, found, parsed, loaded) in cdm_file_results:
        line = '"%(cdm_file_name)s","%(found)s","%(parsed)s","%(loaded)s"\n' % locals()
        f.write(line)
    f.seek(0)
    result = gcs_utils.upload_object(bucket, name, f)
    f.close()
    return result


app.add_url_rule(
    PREFIX + 'ValidateHpoFiles/<string:hpo_id>',
    endpoint='validate_hpo_files',
    view_func=validate_hpo_files,
    methods=['GET'])
