#!/usr/bin/env python
import StringIO
import logging
import time
import os
import json

from flask import Flask

import api_util
import bq_utils
import common
import gcs_utils
from common import RESULT_CSV, WARNINGS_CSV, ERRORS_CSV
import resources

import achilles
import achilles_heel
import export

UNKNOWN_FILE = 'Unknown file'
BQ_LOAD_DELAY_SECONDS = 10

PREFIX = '/data_steward/v1/'
app = Flask(__name__)


def check_results_for_include_list(hpo_id):
    result_file = gcs_utils.get_object(gcs_utils.get_hpo_bucket(hpo_id), common.RESULT_CSV)

    result_file = StringIO.StringIO(result_file)
    result_items = resources._csv_file_to_list(result_file)

    count = 0
    for item in result_items:
        if item['cdm_file_name'] in common.INCLUDE_FILES:
            if item['loaded'] != '1':
                return False
            count = count + 1
    if count < 6:
        return False
    return True


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


def run_export(hpo_id):

    # TODO : add check for required tables
    hpo_bucket = gcs_utils.get_hpo_bucket(hpo_id)
    for export_name in ['achillesheel', 'person', 'datadensity']:
        sql_path = os.path.join(export.EXPORT_PATH, export_name)
        result = export.export_from_path(sql_path, hpo_id)
        content = json.dumps(result)
        fp = StringIO.StringIO(content)
        gcs_utils.upload_object(hpo_bucket, export_name + '.json', fp)

    return '{"export-status": "Done"}'


@api_util.auth_required_cron
def run_achilles(hpo_id):
    """checks for full results and run achilles/heel

    :hpo_id: hpo on which to run achilles
    :returns: success or not

    """

    # for cdm_table in common.CDM_TABLES:
    #     # cdm_file_name = os.path.join(test_util.FIVE_PERSONS_PATH, cdm_table + '.csv')
    #     if cdm_table not in common.INCLUDE_LIST:
    #         fp = StringIO.StringIO('dummy\n')
    #         gcs_utils.upload_object(gcs_utils.get_hpo_bucket(hpo_id), cdm_table + '.csv', fp)
    #         bq_utils.load_cdm_csv(hpo_id, cdm_table)

    run_achilles_flag = check_results_for_include_list(hpo_id)

    if run_achilles_flag:
        achilles.create_tables(hpo_id, True)
        achilles.load_analyses(hpo_id)
        achilles.run_analyses(hpo_id=hpo_id)

        time.sleep(1)

        achilles_heel.create_tables(hpo_id, True)
        achilles_heel.run_heel(hpo_id=hpo_id)

    return '{"achilles-run-status": "started"}'


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
            if bucket_item['name'].lower() in common.IGNORE_LIST + common.CDM_FILES:
                continue
            unknown_files.append(bucket_item)

    errors = []
    # (filename, found, parsed, loaded) for each expected cdm file
    cdm_file_result_map = {}
    for cdm_file in map(lambda f: f['name'], found_cdm_files):
        # create a job to load table
        cdm_file_name = cdm_file.split('.')[0]
        load_results = bq_utils.load_cdm_csv(hpo_id, cdm_file_name)
        load_job_id = load_results['jobReference']['jobId']

        time.sleep(BQ_LOAD_DELAY_SECONDS)
        job_resource = bq_utils.get_job_details(job_id=load_job_id)
        job_status = job_resource['status']

        if job_status['state'] == 'DONE':
            if 'errorResult' in job_status:
                # logging.info("file {} has errors  {}".format'.format(item['message'](cdm_file, job_status['errors']))
                error_messages = ['{}'.format(item['message'], item['location']) for item in job_status['errors']]
                errors.append((cdm_file, ' || '.join(error_messages)))
                cdm_file_result_map[cdm_file] = {'found': 1, 'parsed': 0, 'loaded': 0}
            else:
                cdm_file_result_map[cdm_file] = {'found': 1, 'parsed': 1, 'loaded': 1}
        else:
            cdm_file_result_map[cdm_file] = {'found': 1, 'parsed': 0, 'loaded': 0}
            logging.info("Wait timeout exceeded before load job with id '%s' was done" % load_job_id)
            # print "Wait timeout exceeded before load job with id '%s' was done" % load_job_id

    # (filename, message) for each unknown file
    warnings = [
        (unknown_file['name'], UNKNOWN_FILE) for unknown_file in unknown_files
    ]

    # TODO consider the order files are validated
    load_results = []
    cdm_files_ordered = [cdm_file for cdm_file in common.INCLUDE_FILES]
    for cdm_file in common.CDM_FILES:
        if cdm_file in common.INCLUDE_FILES:
            continue
        cdm_files_ordered.append(cdm_file)

    for cdm_file in cdm_files_ordered:
        if cdm_file in cdm_file_result_map:
            load_result = cdm_file_result_map[cdm_file]
            load_results.append((cdm_file, load_result['found'], load_result['parsed'], load_result['loaded']))
        else:
            if cdm_file in common.INCLUDE_FILES:
                load_results.append((cdm_file, 0, 0, 0))

    # output to GCS
    _save_result_in_gcs(bucket, RESULT_CSV, load_results)
    _save_warnings_in_gcs(bucket, WARNINGS_CSV, warnings)
    _save_errors_in_gcs(bucket, ERRORS_CSV, errors)

    return '{"report-generator-status": "started"}'


def _is_cdm_file(gcs_file_stat):
    return gcs_file_stat['name'].lower() in common.CDM_FILES


def _save_errors_in_gcs(bucket, name, errors):
    """Save errors.csv into hpo bucket

    :bucket:  bucket to save in
    :name: file_name to save to
    :errors: list of errors of form (file_name, errors)
    :returns: result of upload operation. not being used for now.

    """
    f = StringIO.StringIO()
    f.write('"file_name","errors"\n')
    for (file_name, message) in errors:
        line = '"%(file_name)s","%(message)s"\n' % locals()
        f.write(line)
    f.seek(0)
    result = gcs_utils.upload_object(bucket, name, f)
    f.close()
    return result


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


app.add_url_rule(
    PREFIX + 'RunAchilles/<string:hpo_id>',
    endpoint='run_achilles',
    view_func=run_achilles,
    methods=['GET'])


app.add_url_rule(
    PREFIX + 'RunExport/<string:hpo_id>',
    endpoint='export_json_for_achilles',
    view_func=run_export,
    methods=['GET'])
