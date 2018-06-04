#!/usr/bin/env python
import StringIO
import json
import logging
import os
import datetime

from flask import Flask
from googleapiclient.errors import HttpError

import achilles
import achilles_heel
import api_util
import bq_utils
import common
import export
import gcs_utils
import resources
import ehr_merge
from common import RESULT_CSV, WARNINGS_CSV, ERRORS_CSV, ACHILLES_EXPORT_PREFIX_STRING, ACHILLES_EXPORT_DATASOURCES_JSON

UNKNOWN_FILE = 'Unknown file'
BQ_LOAD_RETRY_COUNT = 7

PREFIX = '/data_steward/v1/'
app = Flask(__name__)


class InternalValidationError(RuntimeError):
    """Raised when an internal error occurs during validation"""

    def __init__(self, msg):
        super(InternalValidationError, self).__init__(msg)


class BucketDoesNotExistError(RuntimeError):
    """Raised when a configured bucket does not exist"""

    def __init__(self, msg, bucket):
        super(BucketDoesNotExistError, self).__init__(msg)
        self.bucket = bucket


def all_required_files_loaded(hpo_id, folder_prefix):
    result_file = gcs_utils.get_object(gcs_utils.get_hpo_bucket(hpo_id), folder_prefix + common.RESULT_CSV)
    result_file = StringIO.StringIO(result_file)
    result_items = resources._csv_file_to_list(result_file)
    for item in result_items:
        if item['cdm_file_name'] in common.REQUIRED_FILES:
            if item['loaded'] != '1':
                return False
    return True


def save_datasources_json(hpo_id=None, folder_prefix="", target_bucket=None):
    """
    Generate and save datasources.json (from curation report) in a GCS bucket

    :param hpo_id: the ID of the HPO that report should go to
    :param folder_prefix: relative path in GCS to save to (without 'gs://')
    :param target_bucket: GCS bucket to save to. If not supplied, uses the bucket assigned to hpo_id.
    :return:
    """
    if hpo_id is None:
        if target_bucket is None:
            raise RuntimeError('Cannot save datasources.json if neither hpo_id or target_bucket are specified.')
        hpo_id = 'default'
    else:
        if target_bucket is None:
            target_bucket = gcs_utils.get_hpo_bucket(hpo_id)

    datasource = dict(name=hpo_id, folder=hpo_id, cdmVersion=5)
    datasources = dict(datasources=[datasource])
    datasources_fp = StringIO.StringIO(json.dumps(datasources))
    result = gcs_utils.upload_object(target_bucket, folder_prefix + ACHILLES_EXPORT_DATASOURCES_JSON, datasources_fp)
    return result


def run_export(hpo_id=None, folder_prefix="", target_bucket=None):
    """
    this function also changes the datasources.json file
    """
    results = []
    if hpo_id is None and target_bucket is None:
        raise RuntimeError('either hpo_id or target_bucket should be specified')

    if target_bucket is not None:
        hpo_bucket = target_bucket
        logging.info('running export to bucket %s' % target_bucket)
    else:
        hpo_bucket = gcs_utils.get_hpo_bucket(hpo_id)
        logging.info('running export for hpo_id %s' % hpo_id)

    if hpo_id is None:
        _reports_prefix = ACHILLES_EXPORT_PREFIX_STRING + 'default' + "/"
    else:
        _reports_prefix = ACHILLES_EXPORT_PREFIX_STRING + hpo_id + "/"


    # TODO : add check for required tables
    for export_name in common.ALL_REPORTS:
        sql_path = os.path.join(export.EXPORT_PATH, export_name)
        result = export.export_from_path(sql_path, hpo_id)
        content = json.dumps(result)
        fp = StringIO.StringIO(content)
        result = gcs_utils.upload_object(hpo_bucket, folder_prefix + _reports_prefix + export_name + '.json', fp)
        results.append(result)
    datasources_json_result = save_datasources_json(hpo_id=hpo_id, folder_prefix=folder_prefix, target_bucket=hpo_bucket)
    results.append(datasources_json_result)

    return results


def run_achilles(hpo_id=None):
    """checks for full results and run achilles/heel

    :hpo_id: hpo on which to run achilles
    :returns:
    """
    if hpo_id is not None:
        logging.info('running achilles for hpo_id %s' % hpo_id)
    achilles.create_tables(hpo_id, True)
    achilles.load_analyses(hpo_id)
    achilles.run_analyses(hpo_id=hpo_id)
    if hpo_id is not None:
        logging.info('running achilles_heel for hpo_id %s' % hpo_id)
    achilles_heel.create_tables(hpo_id, True)
    achilles_heel.run_heel(hpo_id=hpo_id)


@api_util.auth_required_cron
def upload_achilles_files(hpo_id):
    result = _upload_achilles_files(hpo_id, "")
    return json.dumps(result, sort_keys=True, indent=4, separators=(',', ': '))


def _upload_achilles_files(hpo_id=None, folder_prefix='', target_bucket=None):
    """uploads achilles web files to the corresponding hpo bucket

    :hpo_id: which hpo bucket do these files go into
    :returns:

    """
    results = []
    if target_bucket is not None:
        bucket = target_bucket
    else:
        if hpo_id is None:
            raise RuntimeError('either hpo_id or target_bucket must be specified')
        bucket = gcs_utils.get_hpo_bucket(hpo_id)

    for filename in common.ACHILLES_INDEX_FILES:
        logging.debug('Uploading achilles file `%s` to bucket `%s`' % (filename, bucket))
        bucket_file_name = filename.split(resources.resource_path + os.sep)[1].strip()
        with open(filename, 'r') as fp:
            upload_result = gcs_utils.upload_object(bucket, folder_prefix + bucket_file_name, fp)
            results.append(upload_result)
    return results


@api_util.auth_required_cron
def validate_hpo_files(hpo_id):
    """
    validation end point for individual hpo_ids
    """
    run_validation(hpo_id, force_run=True)
    return 'validation done!'


@api_util.auth_required_cron
def validate_all_hpos():
    """
    validation end point for individual hpo_ids
    """
    for item in resources.hpo_csv():
        hpo_id = item['hpo_id']
        try:
            run_validation(hpo_id)
        except BucketDoesNotExistError as bucket_error:
            bucket = bucket_error.bucket
            logging.warn('Bucket `{bucket}` configured for hpo_id `hpo_id` does not exist'.format(bucket=bucket,
                                                                                                  hpo_id=hpo_id))
    return 'validation done!'


def list_bucket(bucket):
    try:
        return gcs_utils.list_bucket(bucket)
    except HttpError as err:
        if err.resp.status == 404:
            raise BucketDoesNotExistError('Failed to list objects in bucket', bucket)
        raise
    except Exception:
        raise


def run_validation(hpo_id, force_run=False):
    """
    runs validation for a single hpo_id

    :param hpo_id: which hpo_id to run for
    :param force_run: if True, process the latest submission whether or not it has already been processed before
    :raises
    BucketDoesNotExistError:
      Raised when a configured bucket does not exist
    InternalValidationError:
      Raised when an internal error is encountered during validation
    """
    logging.info(' Validating hpo_id %s' % hpo_id)
    bucket = gcs_utils.get_hpo_bucket(hpo_id)
    bucket_items = list_bucket(bucket)
    to_process_folder_list = _get_to_process_list(bucket, bucket_items, force_run)

    for folder_prefix in to_process_folder_list:
        logging.info('Processing gs://%s/%s' % (bucket, folder_prefix))
        # separate cdm from the unknown (unexpected) files
        found_cdm_files = []
        unknown_files = []
        folder_items = [item['name'].split('/')[1] for item in bucket_items if item['name'].startswith(folder_prefix)]
        for item in folder_items:
            if _is_cdm_file(item):
                found_cdm_files.append(item)
            else:
                if item in common.IGNORE_LIST + common.CDM_FILES:
                    continue
                unknown_files.append(item)

        errors = []
        results = []
        found_cdm_file_names = found_cdm_files

        # Create all tables first to simplify downstream processes
        # (e.g. ehr_union doesn't have to check if tables exist)
        for cdm_file_name in common.CDM_FILES:
            cdm_table_name = cdm_file_name.split('.')[0]
            table_id = bq_utils.get_table_id(hpo_id, cdm_table_name)
            bq_utils.create_standard_table(cdm_table_name, table_id, drop_existing=True)

        for cdm_file_name in common.CDM_FILES:
            logging.info('Validating file `{file_name}`'.format(file_name=cdm_file_name))
            found = parsed = loaded = 0
            cdm_table_name = cdm_file_name.split('.')[0]

            if cdm_file_name in found_cdm_file_names:
                found = 1
                load_results = bq_utils.load_cdm_csv(hpo_id, cdm_table_name, folder_prefix)
                load_job_id = load_results['jobReference']['jobId']
                incomplete_jobs = bq_utils.wait_on_jobs([load_job_id])

                if len(incomplete_jobs) == 0:
                    job_resource = bq_utils.get_job_details(job_id=load_job_id)
                    job_status = job_resource['status']
                    if 'errorResult' in job_status:
                        # These are issues (which we report back) as opposed to internal errors
                        issues = [item['message'] for item in job_status['errors']]
                        errors.append((cdm_file_name, ' || '.join(issues)))
                        logging.info(
                            'Issues found in gs://{bucket}/{folder_prefix}/{cdm_file_name}'.format(
                                bucket=bucket, folder_prefix=folder_prefix, cdm_file_name=cdm_file_name)
                        )
                        for issue in issues:
                            logging.info(issue)
                    else:
                        # Processed ok
                        parsed = loaded = 1
                else:
                    # Incomplete jobs are internal unrecoverable errors.
                    # Aborting the process allows for this submission to be validated when system recovers.
                    message_fmt = 'Loading hpo_id `%s` table `%s` failed because job id `%s` did not complete.'
                    message = message_fmt % (hpo_id, cdm_table_name, load_job_id)
                    message += ' Aborting processing `gs://%s/%s`.' % (bucket, folder_prefix)
                    logging.error(message)
                    raise InternalValidationError(message)

            if cdm_file_name in common.REQUIRED_FILES or found:
                results.append((cdm_file_name, found, parsed, loaded))

        # (filename, message) for each unknown file
        warnings = [
            (unknown_file, UNKNOWN_FILE) for unknown_file in unknown_files
        ]

        # output to GCS
        _save_result_in_gcs(bucket, folder_prefix + RESULT_CSV, results)
        _save_warnings_in_gcs(bucket, folder_prefix + WARNINGS_CSV, warnings)
        _save_errors_in_gcs(bucket, folder_prefix + ERRORS_CSV, errors)

        if all_required_files_loaded(hpo_id, folder_prefix=folder_prefix):
            run_achilles(hpo_id)
            run_export(hpo_id=hpo_id, folder_prefix=folder_prefix)

        logging.info('Uploading achilles index files to `gs://%s/%s`.' % (bucket, folder_prefix))
        _upload_achilles_files(hpo_id, folder_prefix)

        now_datetime_string = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
        logging.info('Processing complete. Saving timestamp %s to `gs://%s/%s`.' %
                     (bucket, now_datetime_string, folder_prefix + common.PROCESSED_TXT))
        _write_string_to_file(bucket, folder_prefix + common.PROCESSED_TXT, now_datetime_string)


def _validation_done(bucket, folder):
    if gcs_utils.get_metadata(bucket=bucket, name=folder + common.PROCESSED_TXT) is not None:
        return True
    return False


def _get_to_process_list(bucket, bucket_items, force_process=False):
    """returns a set of folders to process as part of validation

    :bucket: bucket to look into
    :param force_process: if True return most recent folder whether or not it has been processed already
    :returns: list of folder prefix strings of form "<folder_name>/"

    """
    # files in root are ignored here
    all_folder_list = set([item['name'].split('/')[0] + '/' for item in bucket_items
                           if len(item['name'].split('/')) > 1])

    def basename(gcs_object_metadata):
        """returns name of file inside folder

        :gcs_object_metadata: metadata as returned by list bucket
        :returns: name without folder name

        """
        name = gcs_object_metadata['name']
        if len(name.split('/')) > 1:
            return '/'.join(name.split('/')[1:])

    def updated_datetime_object(gcs_object_metadata):
        """returns update datetime

        :gcs_object_metadata: metadata as returned by list bucket
        :returns: datetime object

        """
        return datetime.datetime.strptime(gcs_object_metadata['updated'], '%Y-%m-%dT%H:%M:%S.%fZ')

    folder_datetime_list = []
    folders_with_submitted_files = []
    for folder_name in all_folder_list:
        # this is not in a try/except block because this follows a bucket read which is in a try/except
        folder_bucket_items = [item for item in bucket_items if item['name'].startswith(folder_name)]
        submitted_bucket_items = [item for item in folder_bucket_items if basename(item) not in common.IGNORE_LIST]
        if len(submitted_bucket_items) > 0:
            folders_with_submitted_files.append(folder_name)
            latest_datetime = max([updated_datetime_object(item) for item in submitted_bucket_items])
            folder_datetime_list.append(latest_datetime)

    if len(folder_datetime_list) > 0:
        latest_datetime_index = folder_datetime_list.index(max(folder_datetime_list))
        to_process_folder = folders_with_submitted_files[latest_datetime_index]
        if force_process:
            return [to_process_folder]
        else:
            processed = _validation_done(bucket, to_process_folder)
            if not processed:
                return [to_process_folder]
    return []


def _is_cdm_file(gcs_file_name):
    return gcs_file_name.lower() in common.CDM_FILES


@api_util.auth_required_cron
def copy_files(hpo_id):
    """copies over files from hpo bucket to drc bucket

    :hpo_id: hpo from which to copy

    """
    hpo_bucket = gcs_utils.get_hpo_bucket(hpo_id)
    drc_private_bucket = gcs_utils.get_drc_bucket()

    bucket_items = gcs_utils.list_bucket(hpo_bucket)

    prefix = hpo_id + '/' + hpo_bucket + '/'

    for item in bucket_items:
        item_name = item['name']
        gcs_utils.copy_object(source_bucket=hpo_bucket,
                              source_object_id=item_name,
                              destination_bucket=drc_private_bucket,
                              destination_object_id=prefix + item_name)

    return '{"copy-status": "done"}'


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


def _write_string_to_file(bucket, name, string):
    """
    Save the validation results in GCS
    :param bucket: bucket to save to
    :param name: name of the file (object) to save to in GCS
    :param cdm_file_results: list of tuples (<cdm_file_name>, <found>)
    :return:
    """
    f = StringIO.StringIO()
    f.write(string)
    f.seek(0)
    result = gcs_utils.upload_object(bucket, name, f)
    f.close()
    return result


@api_util.auth_required_cron
def merge_ehr():
    hpo_id = 'unioned_ehr'
    app_id = bq_utils.app_identity.get_application_id()
    dataset_id = bq_utils.get_dataset_id()
    ehr_merge.merge(dataset_id=dataset_id, project_id=app_id)

    run_achilles(hpo_id)
    now_date_string = datetime.datetime.now().strftime('%Y_%m_%d')
    folder_prefix = 'unioned_ehr_' + now_date_string + '/'
    run_export(hpo_id=hpo_id, folder_prefix=folder_prefix)
    logging.info('uploading achilles index files')
    _upload_achilles_files(hpo_id, folder_prefix)

    return 'merge-and-achilles-done'


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


app.add_url_rule(
    PREFIX + 'CopyFiles/<string:hpo_id>',
    endpoint='copy_files',
    view_func=copy_files,
    methods=['GET'])

app.add_url_rule(
    PREFIX + 'MergeEHR',
    endpoint='merge_ehr',
    view_func=merge_ehr,
    methods=['GET'])
