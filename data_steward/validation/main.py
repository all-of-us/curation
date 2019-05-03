#!/usr/bin/env python
import StringIO
import datetime
import json
import logging
import os

from flask import Flask
from google.appengine.api.app_identity import app_identity
from googleapiclient.errors import HttpError

import api_util
import bq_utils
import common
import common_sql
import gcs_utils
import resources
import validation.achilles as achilles
import validation.achilles_heel as achilles_heel
import validation.ehr_union as ehr_union
import validation.export as export
from common import ACHILLES_EXPORT_PREFIX_STRING, ACHILLES_EXPORT_DATASOURCES_JSON

UNKNOWN_FILE = 'Unknown file'
BQ_LOAD_RETRY_COUNT = 7

PREFIX = '/data_steward/v1/'
app = Flask(__name__)

RESULT_FILE_HEADERS = ["File Name", "Found", "Parsed", "Loaded"]
ERROR_FILE_HEADERS = ["File Name", "Message"]
RESULT_FAIL_CODE = '&#x2718'
RESULT_PASS_CODE = '&#x2714'
RESULT_FAIL_COLOR = 'red'
RESULT_PASS_COLOR = 'green'


class InternalValidationError(RuntimeError):
    """Raised when an internal error occurs during validation"""

    def __init__(self, msg):
        super(InternalValidationError, self).__init__(msg)


class BucketDoesNotExistError(RuntimeError):
    """Raised when a configured bucket does not exist"""

    def __init__(self, msg, bucket):
        super(BucketDoesNotExistError, self).__init__(msg)
        self.bucket = bucket


def all_required_files_loaded(result_items):
    for (file_name, _, _, loaded) in result_items:
        if file_name in common.REQUIRED_FILES:
            if loaded != 1:
                return False
    return True


def save_datasources_json(hpo_id=None, folder_prefix="", target_bucket=None):
    """
    Generate and save datasources.json (from curation report) in a GCS bucket

    :param hpo_id: the ID of the HPO that report should go to
    :param folder_prefix: relative path in GCS to save to (without 'gs://')
    :param target_bucket: GCS bucket to save to. If not supplied, uses the
        bucket assigned to hpo_id.
    :return:
    """
    if hpo_id is None:
        if target_bucket is None:
            raise RuntimeError('Cannot save datasources.json if neither hpo_id '
                               'or target_bucket are specified.')
        hpo_id = 'default'
    else:
        if target_bucket is None:
            target_bucket = gcs_utils.get_hpo_bucket(hpo_id)

    datasource = dict(name=hpo_id, folder=hpo_id, cdmVersion=5)
    datasources = dict(datasources=[datasource])
    datasources_fp = StringIO.StringIO(json.dumps(datasources))
    result = gcs_utils.upload_object(target_bucket,
                                     folder_prefix + ACHILLES_EXPORT_DATASOURCES_JSON,
                                     datasources_fp)
    return result


def run_export(hpo_id=None, folder_prefix="", target_bucket=None):
    """
    Run export queries for an HPO and store JSON payloads in specified folder in (optional) target bucket

    :type hpo_id: ID of the HPO to run export for. This is the data source name in the report.
    :param folder_prefix: Relative base path to store report. empty by default.
    :param target_bucket: Bucket to save report. If None, use bucket associated with hpo_id.
    """
    results = []

    # Using separate var rather than hpo_id here because hpo_id None needed in calls below
    datasource_name = 'default'
    if hpo_id is None:
        if target_bucket is None:
            raise RuntimeError('Cannot export if neither hpo_id or target_bucket is specified.')
    else:
        datasource_name = hpo_id
        if target_bucket is None:
            target_bucket = gcs_utils.get_hpo_bucket(hpo_id)

    logging.info('Exporting %s report to bucket %s', datasource_name, target_bucket)

    # Run export queries and store json payloads in specified folder in the target bucket
    reports_prefix = folder_prefix + ACHILLES_EXPORT_PREFIX_STRING + datasource_name + '/'
    for export_name in common.ALL_REPORTS:
        sql_path = os.path.join(export.EXPORT_PATH, export_name)
        result = export.export_from_path(sql_path, hpo_id)
        content = json.dumps(result)
        fp = StringIO.StringIO(content)
        result = gcs_utils.upload_object(target_bucket, reports_prefix + export_name + '.json', fp)
        results.append(result)
    result = save_datasources_json(hpo_id=hpo_id, folder_prefix=folder_prefix, target_bucket=target_bucket)
    results.append(result)
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
    """
    uploads achilles web files to the corresponding hpo bucket

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
        bucket_file_name = filename.split(resources.resource_path + os.sep)[1].strip().replace('\\', '/')
        with open(filename, 'rb') as fp:
            upload_result = gcs_utils.upload_object(bucket, folder_prefix + bucket_file_name, fp)
            results.append(upload_result)
    return results


@api_util.auth_required_cron
def validate_hpo_files(hpo_id):
    """
    validation end point for individual hpo_ids
    """
    process_hpo(hpo_id, force_run=True)
    return 'validation done!'


@api_util.auth_required_cron
def validate_all_hpos():
    """
    validation end point for individual hpo_ids
    """
    for item in resources.hpo_csv():
        hpo_id = item['hpo_id']
        try:
            process_hpo(hpo_id)
        except BucketDoesNotExistError as bucket_error:
            bucket = bucket_error.bucket
            logging.warn('Bucket `%s` configured for hpo_id `%s` does not exist',
                         bucket, hpo_id)
    return 'validation done!'


def list_bucket(bucket):
    try:
        return gcs_utils.list_bucket(bucket)
    except HttpError as err:
        if err.resp.status == 404:
            raise BucketDoesNotExistError('Failed to list objects in bucket ', bucket)
        raise
    except Exception:
        raise


def validate_submission(hpo_id, bucket, bucket_items, folder_prefix):
    logging.info('Validating %s submission in gs://%s/%s',
                 hpo_id, bucket, folder_prefix)
    # separate cdm from the unknown (unexpected) files
    found_cdm_files = []
    unknown_files = []
    found_pii_files = []
    folder_items = [item['name'][len(folder_prefix):] \
                    for item in bucket_items if item['name'].startswith(folder_prefix)]
    for item in folder_items:
        if _is_cdm_file(item):
            found_cdm_files.append(item)
        elif _is_pii_file(item):
            found_pii_files.append(item)
        else:
            if not (_is_known_file(item) or _is_string_excluded_file(item)):
                unknown_files.append(item)

    errors = []
    results = []

    # Create all tables first to simplify downstream processes
    # (e.g. ehr_union doesn't have to check if tables exist)
    for file_name in common.CDM_FILES + common.PII_FILES:
        table_name = file_name.split('.')[0]
        table_id = bq_utils.get_table_id(hpo_id, table_name)
        bq_utils.create_standard_table(table_name, table_id, drop_existing=True)

    for cdm_file_name in sorted(common.CDM_FILES):
        file_results, file_errors = perform_validation_on_file(cdm_file_name, found_cdm_files, hpo_id,
                                                               folder_prefix, bucket)
        results.extend(file_results)
        errors.extend(file_errors)

    for pii_file_name in sorted(common.PII_FILES):
        file_results, file_errors = perform_validation_on_file(pii_file_name, found_pii_files, hpo_id,
                                                               folder_prefix, bucket)
        results.extend(file_results)
        errors.extend(file_errors)

    # (filename, message) for each unknown file
    warnings = [
        (unknown_file, UNKNOWN_FILE) for unknown_file in unknown_files
    ]
    return dict(results=results, errors=errors, warnings=warnings)


def process_hpo(hpo_id, force_run=False):
    """
    runs validation for a single hpo_id

    :param hpo_id: which hpo_id to run for
    :param force_run: if True, process the latest submission whether or not it
        has already been processed before
    :raises
    BucketDoesNotExistError:
      Raised when a configured bucket does not exist
    InternalValidationError:
      Raised when an internal error is encountered during validation
    """
    logging.info('Processing hpo_id %s', hpo_id)
    bucket = gcs_utils.get_hpo_bucket(hpo_id)
    bucket_items = list_bucket(bucket)
    folder_prefix = _get_submission_folder(bucket, bucket_items, force_run)
    if folder_prefix is None:
        logging.info('No submissions to process in %s bucket %s', hpo_id, bucket)
    else:
        validate_result = validate_submission(hpo_id, bucket, bucket_items, folder_prefix)
        results = validate_result['results']
        errors = validate_result['errors']
        warnings = validate_result['warnings']

        if not all_required_files_loaded(results):
            logging.info('Required files not loaded in %s. Skipping achilles.', folder_prefix)
        else:
            logging.info('Running achilles on %s', folder_prefix)
            run_achilles(hpo_id)
            run_export(hpo_id=hpo_id, folder_prefix=folder_prefix)
            logging.info('Uploading achilles index files to `gs://%s/%s`.', bucket, folder_prefix)
            _upload_achilles_files(hpo_id, folder_prefix)

        # Get heel errors
        heel_errors, heel_header_list = get_heel_errors_in_results_html(hpo_id,
                                                                        common_sql.HEEL_ERROR_QUERY_VALIDATION,
                                                                        common.ACHILLES_HEEL_RESULTS_VALIDATION)

        # Get Drug check counts into results.html
        drug_checks, drug_header_list = get_drug_checks_in_results_html(hpo_id,
                                                                        common_sql.DRUG_CHECKS_QUERY_VALIDATION,
                                                                        common.DRUG_CHECK_TABLE_VALIDATION)

        _save_results_html_in_gcs(hpo_id, bucket, folder_prefix + common.RESULTS_HTML, results, errors, warnings,
                                  heel_errors, heel_header_list, drug_checks, drug_header_list)

        now_datetime_string = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
        logging.info('Processing complete. Saving timestamp %s to `gs://%s/%s`.',
                     bucket, now_datetime_string, folder_prefix + common.PROCESSED_TXT)
        _write_string_to_file(bucket, folder_prefix + common.PROCESSED_TXT, now_datetime_string)


def get_heel_errors_in_results_html(hpo_id, query_string, table_id):
    heel_result = get_query_result(hpo_id, query_string, table_id)
    heel_errors = _convert_query_result_to_list_of_rows(heel_result)
    if not heel_result:
        heel_header_list = ['Record Count', 'Heel Error', 'Analysis ID', 'Rule ID']
    else:
        heel_header_list = _get_query_result_header(heel_result)
    return heel_errors, heel_header_list


def get_drug_checks_in_results_html(hpo_id, query_string, table_id):
    drug_check_result = get_query_result(hpo_id, query_string, table_id)
    drug_checks = _convert_query_result_to_list_of_rows(drug_check_result)
    if not drug_checks:
        drug_checks_header_list = ['Counts by Drug class',
                                   'Drug Class Concept Name',
                                   'Drug Class',
                                   'Percentage',
                                   'Drug Class Concept ID']
    else:
        drug_checks_header_list = _get_query_result_header(drug_check_result)
    return drug_checks, drug_checks_header_list


def get_query_result(hpo_id, query_string, table_id, app_id=None, dataset_id=None, ):
    """
    :param hpo_id: the name of the hpo_id for which validation is being done
    :param table_id: Name of the table running analysis on
    :param query_string: variable name of the query string stored in the common_sql
    :param app_id: name of the big query application id
    :param dataset_id: name of the big query dataset id
    :return: returns dictionary of rows
    """
    if app_id is None:
        app_id = app_identity.get_application_id()
    if dataset_id is None:
        dataset_id = bq_utils.get_dataset_id()
    table_name = '{hpo_name}{results_table}'.format(hpo_name=hpo_id,
                                                    results_table=table_id)
    result = None
    if bq_utils.table_exists(table_name):
        query = query_string.format(application=app_id, dataset=dataset_id, table_id=table_name)
        if query:
            # Found achilles_heel_results table(s), run the query
            response = bq_utils.query(query)
            result = bq_utils.response2rows(response)
    if result is None:
        result = []
    return result


def _convert_query_result_to_list_of_rows(list_of_dicts):
    result_list = list()
    if list_of_dicts is []:
        return result_list.append(tuple())
    else:
        for dict_item in list_of_dicts:
            dict_values = tuple(dict_item.values())
            result_list.append(dict_values)
        return result_list


def _get_query_result_header(list_of_dicts):
    header_list = list(list_of_dicts[0].keys())
    header_list = [header.replace('_', ' ') for header in header_list]
    return header_list


def perform_validation_on_file(file_name, found_file_names, hpo_id, folder_prefix, bucket):
    errors = []
    results = []
    logging.info('Validating file `%s`', file_name)
    found = parsed = loaded = 0
    table_name = file_name.split('.')[0]

    if file_name in found_file_names:
        found = 1
        load_results = bq_utils.load_from_csv(hpo_id, table_name, folder_prefix)
        load_job_id = load_results['jobReference']['jobId']
        incomplete_jobs = bq_utils.wait_on_jobs([load_job_id])

        if incomplete_jobs == []:
            job_resource = bq_utils.get_job_details(job_id=load_job_id)
            job_status = job_resource['status']
            if 'errorResult' in job_status:
                # These are issues (which we report back) as opposed to internal errors
                issues = [item['message'] for item in job_status['errors']]
                errors.append((file_name, ' || '.join(issues)))
                logging.info('Issues found in gs://%s/%s/%s',
                             bucket, folder_prefix, file_name)
                for issue in issues:
                    logging.info(issue)
            else:
                # Processed ok
                parsed = loaded = 1
        else:
            # Incomplete jobs are internal unrecoverable errors.
            # Aborting the process allows for this submission to be validated when system recovers.
            message_fmt = 'Loading hpo_id `%s` table `%s` failed because job id `%s` did not complete.'
            message = message_fmt % (hpo_id, table_name, load_job_id)
            message += ' Aborting processing `gs://%s/%s`.' % (bucket, folder_prefix)
            logging.error(message)
            raise InternalValidationError(message)

    if file_name in common.SUBMISSION_FILES:
        results.append((file_name, found, parsed, loaded))

    return results, errors


def _validation_done(bucket, folder):
    if gcs_utils.get_metadata(bucket=bucket, name=folder + common.PROCESSED_TXT) is not None:
        return True
    return False


def basename(gcs_object_metadata):
    """returns name of file inside folder

    :gcs_object_metadata: metadata as returned by list bucket
    :returns: name without folder name

    """
    name = gcs_object_metadata['name']
    if len(name.split('/')) > 1:
        return '/'.join(name.split('/')[1:])
    return ''


def updated_datetime_object(gcs_object_metadata):
    """returns update datetime

    :gcs_object_metadata: metadata as returned by list bucket
    :returns: datetime object

    """
    return datetime.datetime.strptime(gcs_object_metadata['updated'], '%Y-%m-%dT%H:%M:%S.%fZ')


def list_submitted_bucket_items(folder_bucketitems):
    """
    :param folder_bucketitems: List of Bucket items
    :return: list of files
    """
    files_list = []
    object_retention_days = 30
    today = datetime.datetime.today()
    for file_name in folder_bucketitems:
        if basename(file_name) not in common.IGNORE_LIST:
            # in common.CDM_FILES or is_pii(basename(file_name)):
            created_date = initial_date_time_object(file_name)
            retention_time = datetime.timedelta(days=object_retention_days)
            retention_start_time = datetime.timedelta(days=1)
            age_threshold = created_date + retention_time - retention_start_time
            if age_threshold > today:
                files_list.append(file_name)
    return files_list


def initial_date_time_object(gcs_object_metadata):
    """
    :param gcs_object_metadata: metadata as returned by list bucket
    :return: datetime object
    """
    date_created = datetime.datetime.strptime(gcs_object_metadata['timeCreated'], '%Y-%m-%dT%H:%M:%S.%fZ')
    return date_created


def _get_submission_folder(bucket, bucket_items, force_process=False):
    """
    Get the string name of the most recent submission directory for validation

    Skips directories listed in IGNORE_DIRECTORIES with a case insensitive
    match.

    :param bucket: string bucket name to look into
    :param bucket_items: list of unicode string items in the bucket
    :param force_process: if True return most recently updated directory, even if it has already been processed.
    :returns: a directory prefix string of the form "<directory_name>/" if
        the directory has not been processed, it is not an ignored directory,
        and force_process is False.  a directory prefix string of the form
        "<directory_name>/" if the directory has been processed, it is not an
        ignored directory, and force_process is True.  None if the directory
        has been processed and force_process is False or no submission
        directory exists
    """
    # files in root are ignored here
    all_folder_list = set([item['name'].split('/')[0] + '/' for item in bucket_items
                           if len(item['name'].split('/')) > 1])

    folder_datetime_list = []
    folders_with_submitted_files = []
    for folder_name in all_folder_list:
        # DC-343  special temporary case where we have to deal with a possible
        # directory dumped into the bucket by 'ehr sync' process from RDR
        if folder_name.lower() in common.IGNORE_DIRECTORIES:
            logging.info("Skipping %s directory.  It is not a submission "
                         "directory.", folder_name)
            continue

        # this is not in a try/except block because this follows a bucket read which is in a try/except
        folder_bucket_items = [item for item in bucket_items if item['name'].startswith(folder_name)]
        submitted_bucket_items = list_submitted_bucket_items(folder_bucket_items)

        if submitted_bucket_items and submitted_bucket_items != []:
            folders_with_submitted_files.append(folder_name)
            latest_datetime = max([updated_datetime_object(item) for item in submitted_bucket_items])
            folder_datetime_list.append(latest_datetime)

    if folder_datetime_list and folder_datetime_list != []:
        latest_datetime_index = folder_datetime_list.index(max(folder_datetime_list))
        to_process_folder = folders_with_submitted_files[latest_datetime_index]
        if force_process:
            return to_process_folder
        else:
            processed = _validation_done(bucket, to_process_folder)
            if not processed:
                return to_process_folder
    return None


def _is_cdm_file(gcs_file_name):
    return gcs_file_name.lower() in common.CDM_FILES


def _is_pii_file(gcs_file_name):
    return gcs_file_name.lower() in common.PII_FILES


def _is_known_file(gcs_file_name):
    return gcs_file_name in common.IGNORE_LIST


def _is_string_excluded_file(gcs_file_name):
    return any(gcs_file_name.startswith(prefix) for prefix in common.IGNORE_STRING_LIST)


@api_util.auth_required_cron
def copy_files(hpo_id):
    """copies over files from hpo bucket to drc bucket

    :hpo_id: hpo from which to copy
    :return: json string indicating the job has finished
    """
    hpo_bucket = gcs_utils.get_hpo_bucket(hpo_id)
    drc_private_bucket = gcs_utils.get_drc_bucket()

    bucket_items = list_bucket(hpo_bucket)

    ignored_items = 0
    filtered_bucket_items = []
    for item in bucket_items:
        item_root = item['name'].split('/')[0] + '/'
        if item_root.lower() in common.IGNORE_DIRECTORIES:
            ignored_items += 1
        else:
            filtered_bucket_items.append(item)

    logging.info("Ignoring %d items in %s", ignored_items, hpo_bucket)

    prefix = hpo_id + '/' + hpo_bucket + '/'

    for item in filtered_bucket_items:
        item_name = item['name']
        gcs_utils.copy_object(source_bucket=hpo_bucket,
                              source_object_id=item_name,
                              destination_bucket=drc_private_bucket,
                              destination_object_id=prefix + item_name)

    return '{"copy-status": "done"}'


def _save_results_html_in_gcs(hpo_id, bucket, file_name, results, errors, warnings,
                              heel_errors, heel_error_header,
                              drug_checks, drug_check_header):
    """
    Save the validation results in GCS
    :param hpo_id: name of the hpo_id
    :param bucket: bucket to save to
    :param file_name: name of the file (object) to save to in GCS
    :param results: list of tuples (<cdm_file_name>, <found>, <loaded>, <parsed>)
    :param errors: list of tuples (<cdm_file_name>, <message>)
    :param warnings: list of tuples (<cdm_file_name>, <message>)
    :return:
    """
    results_heading = '{hpo_id} EHR Submission Results'.format(hpo_id=hpo_id.upper())
    html_report_list = []
    with open(resources.html_boilerplate_path) as f:
        for line in f:
            html_report_list.append(line)

    html_report_list.append('\n')
    html_report_list.append(html_tag_wrapper(results_heading, 'h1', 'align="center"'))
    html_report_list.append('\n')
    html_report_list.append(create_html_table(RESULT_FILE_HEADERS, results, "Results"))
    html_report_list.append('\n')
    html_report_list.append(create_html_table(ERROR_FILE_HEADERS, errors, "Errors"))
    html_report_list.append('\n')
    html_report_list.append(create_html_table(ERROR_FILE_HEADERS, warnings, "Warnings"))
    html_report_list.append('\n')
    html_report_list.append(create_html_table(heel_error_header, heel_errors, "Heel Errors"))
    html_report_list.append('\n')
    html_report_list.append(create_html_table(drug_check_header, drug_checks, "Drug Concept Mapping Percentages"))
    html_report_list.append('\n')
    html_report_list.append('</body>\n')
    html_report_list.append('</html>\n')

    f = StringIO.StringIO()
    for line in html_report_list:
        f.write(line)
    f.seek(0)
    result = gcs_utils.upload_object(bucket, file_name, f)
    f.close()
    return result


def create_html_table(headers, table, table_name):
    html_report_list = []
    table_config = 'id="dataframe" style="width:80%" class="center"'
    html_report_list.append(html_tag_wrapper(table_name, 'caption'))
    table_header_row = create_html_row(headers, 'th', 'tr')
    html_report_list.append(html_tag_wrapper(table_header_row, 'thead'))
    results_rows = []
    if table is not None:
        for item in table:
            results_rows.append(create_html_row(item, 'td', 'tr', headers))
    table_body_rows = '\n'.join(results_rows)
    html_report_list.append(html_tag_wrapper(table_body_rows, 'tbody'))
    return html_tag_wrapper('\n'.join(html_report_list), 'table', table_config)


def create_html_row(row_items, item_tag, row_tag, headers=None):
    row_item_list = []
    checkbox_style = 'style="text-align:center; font-size:150%; font-weight:bold; color:{0};"'
    message = "BigQuery generated the following message while processing the files: " + "<br/>"
    for index, row_item in enumerate(row_items):
        if row_item == 1 and headers == RESULT_FILE_HEADERS:
            row_item_list.append(html_tag_wrapper(RESULT_PASS_CODE,
                                                  item_tag,
                                                  checkbox_style.format(RESULT_PASS_COLOR)))
        elif row_item == 0 and headers == RESULT_FILE_HEADERS:
            row_item_list.append(html_tag_wrapper(RESULT_FAIL_CODE,
                                                  item_tag,
                                                  checkbox_style.format(RESULT_FAIL_COLOR)))
        elif index == 1 and headers == ERROR_FILE_HEADERS:
            row_item_list.append(html_tag_wrapper(message + row_item.replace(' || ', '<br/>'), item_tag))
        else:
            row_item_list.append(html_tag_wrapper(row_item, item_tag))
    row_item_string = '\n'.join(row_item_list)
    row_item_string = html_tag_wrapper(row_item_string, row_tag)
    return row_item_string


def html_tag_wrapper(text, tag, message=''):
    if message == '':
        return '<%(tag)s>\n%(text)s\n</%(tag)s>' % locals()

    return '<%(tag)s %(message)s>\n%(text)s\n</%(tag)s>' % locals()


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
def union_ehr():
    hpo_id = 'unioned_ehr'
    app_id = bq_utils.app_identity.get_application_id()
    input_dataset_id = bq_utils.get_dataset_id()
    output_dataset_id = bq_utils.get_unioned_dataset_id()
    ehr_union.main(input_dataset_id, output_dataset_id, app_id)

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
    PREFIX + 'UnionEHR',
    endpoint='union_ehr',
    view_func=union_ehr,
    methods=['GET'])
