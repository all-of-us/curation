#!/usr/bin/env python
import StringIO
import json
import logging
import os

from flask import Flask

import achilles
import achilles_heel
import api_util
import bq_utils
import common
import export
import gcs_utils
import resources
from common import RESULT_CSV, WARNINGS_CSV, ERRORS_CSV, ACHILLES_EXPORT_PREFIX_STRING, ACHILLES_EXPORT_DATASOURCES_JSON

UNKNOWN_FILE = 'Unknown file'
BQ_LOAD_RETRY_COUNT = 7

PREFIX = '/data_steward/v1/'
app = Flask(__name__)


def all_required_files_loaded(hpo_id):
    result_file = gcs_utils.get_object(gcs_utils.get_hpo_bucket(hpo_id), common.RESULT_CSV)
    result_file = StringIO.StringIO(result_file)
    result_items = resources._csv_file_to_list(result_file)
    for item in result_items:
        if item['cdm_file_name'] in common.REQUIRED_FILES:
            if item['loaded'] != '1':
                return False
    return True


def save_datasources_json(hpo_id):
    hpo_bucket = gcs_utils.get_hpo_bucket(hpo_id)
    datasource = dict(name=hpo_id, folder=hpo_id, cdmVersion=5)
    datasources = dict(datasources=[datasource])
    datasources_fp = StringIO.StringIO(json.dumps(datasources))
    result = gcs_utils.upload_object(hpo_bucket, ACHILLES_EXPORT_DATASOURCES_JSON, datasources_fp)
    return result


def run_export(hpo_id):
    """
    this function also changes the datasources.json file
    """
    results = []
    logging.info('running export for hpo_id %s' % hpo_id)
    # TODO : add check for required tables
    hpo_bucket = gcs_utils.get_hpo_bucket(hpo_id)
    _reports_prefix = ACHILLES_EXPORT_PREFIX_STRING + hpo_id + "/"
    for export_name in common.ALL_REPORTS:
        sql_path = os.path.join(export.EXPORT_PATH, export_name)
        result = export.export_from_path(sql_path, hpo_id)
        content = json.dumps(result)
        fp = StringIO.StringIO(content)
        result = gcs_utils.upload_object(hpo_bucket, _reports_prefix + export_name + '.json', fp)
        results.append(result)

    datasources_json_result = save_datasources_json(hpo_id)
    results.append(datasources_json_result)

    return results


def run_achilles(hpo_id):
    """checks for full results and run achilles/heel

    :hpo_id: hpo on which to run achilles
    :returns:
    """
    logging.info('running achilles for hpo_id %s' % hpo_id)
    achilles.create_tables(hpo_id, True)
    achilles.load_analyses(hpo_id)
    achilles.run_analyses(hpo_id=hpo_id)
    logging.info('running achilles_heel for hpo_id %s' % hpo_id)
    achilles_heel.create_tables(hpo_id, True)
    achilles_heel.run_heel(hpo_id=hpo_id)


@api_util.auth_required_cron
def upload_achilles_files(hpo_id):
    result = _upload_achilles_files(hpo_id)
    return json.dumps(result, sort_keys=True, indent=4, separators=(',', ': '))


def _upload_achilles_files(hpo_id):
    """uploads achilles web files to the corresponding hpo bucket

    :hpo_id: which hpo bucket do these files go into
    :returns:

    """
    results = []
    bucket = gcs_utils.get_hpo_bucket(hpo_id)
    for filename in common.ACHILLES_INDEX_FILES:
        logging.info('uploading achilles file `%s` to bucket `%s`' % (filename, bucket))
        bucket_file_name = filename.split(resources.resource_path + os.sep)[1].strip()
        with open(filename, 'r') as fp:
            upload_result = gcs_utils.upload_object(bucket, bucket_file_name, fp)
            results.append(upload_result)
    return results


@api_util.auth_required_cron
def validate_hpo_files(hpo_id):
    """
    validation end point for individual hpo_ids
    """
    return run_validation(hpo_id)


@api_util.auth_required_cron
def validate_all_hpos():
    """
    validation end point for individual hpo_ids
    """
    for item in resources.hpo_csv():
        hpo_id = item['hpo_id']
        run_validation(hpo_id, True)
    return 'validation done!'


def run_validation(hpo_id, error_ignore_flag=False):
    """
    runs validation for a single hpo_id
    param hpo_id : which hpo_id to run for
    param error_ignore_flag : ignore errors when running for all
    """
    logging.info(' Validating hpo_id %s' % hpo_id)
    bucket = gcs_utils.get_hpo_bucket(hpo_id)
    try:
        bucket_items = gcs_utils.list_bucket(bucket)
    except:
        if error_ignore_flag:
            logging.warning('skipping {}. bucket does not exist.'.format(hpo_id))
            return 'skipping'
        raise RuntimeError('{} does not exist. create bucket before validation for hpo {}'.format(bucket, hpo_id))

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
    results = []
    found_cdm_file_names = map(lambda f: f['name'], found_cdm_files)
    for cdm_file_name in common.CDM_FILES:
        found = parsed = loaded = 0
        cdm_table_name = cdm_file_name.split('.')[0]
        if cdm_file_name in found_cdm_file_names:
            found = 1
            load_results = bq_utils.load_cdm_csv(hpo_id, cdm_table_name)
            load_job_id = load_results['jobReference']['jobId']

            incomplete_jobs = bq_utils.wait_on_jobs([load_job_id], retry_count=BQ_LOAD_RETRY_COUNT)

            if len(incomplete_jobs) == 0:
                job_resource = bq_utils.get_job_details(job_id=load_job_id)
                job_status = job_resource['status']
                if 'errorResult' in job_status:
                    error_messages = ['{}'.format(item['message'], item['location']) for item in job_status['errors']]
                    errors.append((cdm_file_name, ' || '.join(error_messages)))
                else:
                    parsed = loaded = 1
            else:
                logging.info("Wait timeout exceeded before load job with id '%s' was done" % load_job_id)
        else:
            # load empty table
            table_id = bq_utils.get_table_id(hpo_id, cdm_table_name)
            bq_utils.create_standard_table(cdm_table_name, table_id, drop_existing=True)
        if cdm_file_name in common.REQUIRED_FILES or found:
            results.append((cdm_file_name, found, parsed, loaded))

    # (filename, message) for each unknown file
    warnings = [
        (unknown_file['name'], UNKNOWN_FILE) for unknown_file in unknown_files
    ]

    # output to GCS
    _save_result_in_gcs(bucket, RESULT_CSV, results)
    _save_warnings_in_gcs(bucket, WARNINGS_CSV, warnings)
    _save_errors_in_gcs(bucket, ERRORS_CSV, errors)

    if all_required_files_loaded(hpo_id):
        run_achilles(hpo_id)
        run_export(hpo_id)

    logging.info('uploading achilles index files')
    _upload_achilles_files(hpo_id)

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
    PREFIX + 'ValidateAllHpoFiles',
    endpoint='validate_all_hpos',
    view_func=validate_all_hpos,
    methods=['GET'])

app.add_url_rule(
    PREFIX + 'ValidateHpoFiles/<string:hpo_id>',
    endpoint='validate_hpo_files',
    view_func=validate_hpo_files,
    methods=['GET'])

app.add_url_rule(
    PREFIX + 'UploadAchillesFiles/<string:hpo_id>',
    endpoint='upload_achilles_files',
    view_func=upload_achilles_files,
    methods=['GET'])
