#!/usr/bin/env python
"""
This module is responsible for validating EHR submissions.

This module focuses on preserving and validating
submission data.
"""
# Python imports
import datetime
import json
import logging
import os
import re
from io import StringIO

# Third party imports
from flask import Flask
import app_identity
from googleapiclient.errors import HttpError

# Project imports
import api_util
import bq_utils
import cdm
import common
from constants.validation import main as consts
from constants.validation import hpo_report as report_consts
import gcs_utils
import resources
from utils.slack_alerts import post_message
from validation import achilles as achilles
from validation import achilles_heel as achilles_heel
from validation.app_errors import (errors_blueprint, InternalValidationError,
                                   BucketDoesNotExistError)
from validation.metrics import completeness as completeness
from validation.metrics import required_labs as required_labs
from validation import ehr_union as ehr_union
from validation import export as export
from validation.participants import identity_match as matching
from common import ACHILLES_EXPORT_PREFIX_STRING, ACHILLES_EXPORT_DATASOURCES_JSON
from validation import hpo_report
from tools import retract_data_bq, retract_data_gcs
from io import open
from curation_logging.curation_gae_handler import begin_request_logging, end_request_logging, initialize_logging

PREFIX = '/data_steward/v1/'
app = Flask(__name__)

# register application error handlers
app.register_blueprint(errors_blueprint)


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
    datasources_fp = StringIO(json.dumps(datasources))
    result = gcs_utils.upload_object(
        target_bucket, folder_prefix + ACHILLES_EXPORT_DATASOURCES_JSON,
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
            raise RuntimeError(
                'Cannot export if neither hpo_id or target_bucket is specified.'
            )
    else:
        datasource_name = hpo_id
        if target_bucket is None:
            target_bucket = gcs_utils.get_hpo_bucket(hpo_id)

    logging.info('Exporting %s report to bucket %s', datasource_name,
                 target_bucket)

    # Run export queries and store json payloads in specified folder in the target bucket
    reports_prefix = folder_prefix + ACHILLES_EXPORT_PREFIX_STRING + datasource_name + '/'
    for export_name in common.ALL_REPORTS:
        sql_path = os.path.join(export.EXPORT_PATH, export_name)
        result = export.export_from_path(sql_path, hpo_id)
        content = json.dumps(result)
        fp = StringIO(content)
        result = gcs_utils.upload_object(target_bucket,
                                         reports_prefix + export_name + '.json',
                                         fp)
        results.append(result)
    result = save_datasources_json(hpo_id=hpo_id,
                                   folder_prefix=folder_prefix,
                                   target_bucket=target_bucket)
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
            raise RuntimeError(
                'either hpo_id or target_bucket must be specified')
        bucket = gcs_utils.get_hpo_bucket(hpo_id)
    logging.info('Uploading achilles index files to `gs://%s/%s`...', bucket,
                 folder_prefix)
    for filename in resources.ACHILLES_INDEX_FILES:
        logging.info('Uploading achilles file `%s` to bucket `%s`' %
                     (filename, bucket))
        bucket_file_name = filename.split(resources.resource_path +
                                          os.sep)[1].strip().replace('\\', '/')
        with open(filename, 'rb') as fp:
            upload_result = gcs_utils.upload_object(
                bucket, folder_prefix + bucket_file_name, fp)
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
    validation end point for all hpo_ids
    """
    for item in bq_utils.get_hpo_info():
        hpo_id = item['hpo_id']
        process_hpo(hpo_id)
    return 'validation done!'


def list_bucket(bucket):
    try:
        return gcs_utils.list_bucket(bucket)
    except HttpError as err:
        if err.resp.status == 404:
            raise BucketDoesNotExistError('Failed to list objects in bucket ',
                                          bucket)
        raise
    except Exception:
        raise


def categorize_folder_items(folder_items):
    """
    Categorize submission items into three lists: CDM, PII, UNKNOWN

    :param folder_items: list of filenames in a submission folder (name of folder excluded)
    :return: a tuple with three separate lists - (cdm files, pii files, unknown files)
    """
    found_cdm_files = []
    unknown_files = []
    found_pii_files = []
    for item in folder_items:
        if _is_cdm_file(item):
            found_cdm_files.append(item)
        elif _is_pii_file(item):
            found_pii_files.append(item)
        else:
            if not (_is_known_file(item) or _is_string_excluded_file(item)):
                unknown_files.append(item)
    return found_cdm_files, found_pii_files, unknown_files


def validate_submission(hpo_id, bucket, bucket_items, folder_prefix):
    """
    Load submission in BigQuery and summarize outcome

    :param hpo_id:
    :param bucket:
    :param bucket_items:
    :param folder_prefix:
    :return: a dict with keys results, errors, warnings
      results is list of tuples (file_name, found, parsed, loaded)
      errors and warnings are both lists of tuples (file_name, message)
    """
    logging.info('Validating %s submission in gs://%s/%s', hpo_id, bucket,
                 folder_prefix)
    # separate cdm from the unknown (unexpected) files
    folder_items = [item['name'][len(folder_prefix):] \
                    for item in bucket_items if item['name'].startswith(folder_prefix)]
    found_cdm_files, found_pii_files, unknown_files = categorize_folder_items(
        folder_items)

    errors = []
    results = []

    # Create all tables first to simplify downstream processes
    # (e.g. ehr_union doesn't have to check if tables exist)
    for file_name in resources.CDM_FILES + common.PII_FILES:
        table_name = file_name.split('.')[0]
        table_id = bq_utils.get_table_id(hpo_id, table_name)
        bq_utils.create_standard_table(table_name, table_id, drop_existing=True)

    for cdm_file_name in sorted(resources.CDM_FILES):
        file_results, file_errors = perform_validation_on_file(
            cdm_file_name, found_cdm_files, hpo_id, folder_prefix, bucket)
        results.extend(file_results)
        errors.extend(file_errors)

    for pii_file_name in sorted(common.PII_FILES):
        file_results, file_errors = perform_validation_on_file(
            pii_file_name, found_pii_files, hpo_id, folder_prefix, bucket)
        results.extend(file_results)
        errors.extend(file_errors)

    # (filename, message) for each unknown file
    warnings = [
        (unknown_file, common.UNKNOWN_FILE) for unknown_file in unknown_files
    ]
    return dict(results=results, errors=errors, warnings=warnings)


def generate_metrics(hpo_id, bucket, folder_prefix, summary):
    """
    Generate metrics regarding a submission

    :param hpo_id: identifies the HPO site
    :param bucket: name of the bucket with the submission
    :param folder_prefix: folder containing the submission
    :param summary: file summary from validation
     {results: [(file_name, found, parsed, loaded)],
      errors: [(file_name, message)],
      warnings: [(file_name, message)]}
    :return:
    """
    report_data = summary.copy()
    processed_datetime_str = datetime.datetime.now().strftime(
        '%Y-%m-%dT%H:%M:%S')
    error_occurred = False

    # TODO separate query generation, query execution, writing to GCS
    gcs_path = 'gs://%s/%s' % (bucket, folder_prefix)
    report_data[report_consts.HPO_NAME_REPORT_KEY] = get_hpo_name(hpo_id)
    report_data[report_consts.FOLDER_REPORT_KEY] = folder_prefix
    report_data[report_consts.TIMESTAMP_REPORT_KEY] = processed_datetime_str
    results = report_data['results']
    try:
        # TODO modify achilles to run successfully when tables are empty
        # achilles queries will raise exceptions (e.g. division by zero) if files not present
        if all_required_files_loaded(results):
            logging.info('Running achilles on %s.', folder_prefix)
            run_achilles(hpo_id)
            run_export(hpo_id=hpo_id, folder_prefix=folder_prefix)
            logging.info('Uploading achilles index files to `%s`.', gcs_path)
            _upload_achilles_files(hpo_id, folder_prefix)
            heel_error_query = get_heel_error_query(hpo_id)
            report_data[report_consts.HEEL_ERRORS_REPORT_KEY] = query_rows(
                heel_error_query)
        else:
            report_data[
                report_consts.
                SUBMISSION_ERROR_REPORT_KEY] = 'Required files are missing'
            logging.info('Required files are missing in %s. Skipping achilles.',
                         gcs_path)

        # non-unique key metrics
        logging.info('Getting non-unique key stats for %s...' % hpo_id)
        nonunique_metrics_query = get_duplicate_counts_query(hpo_id)
        report_data[
            report_consts.NONUNIQUE_KEY_METRICS_REPORT_KEY] = query_rows(
                nonunique_metrics_query)

        # drug class metrics
        logging.info('Getting drug class for %s...' % hpo_id)
        drug_class_metrics_query = get_drug_class_counts_query(hpo_id)
        report_data[report_consts.DRUG_CLASS_METRICS_REPORT_KEY] = query_rows(
            drug_class_metrics_query)

        # missing PII
        logging.info('Getting missing record stats for %s...' % hpo_id)
        missing_pii_query = get_hpo_missing_pii_query(hpo_id)
        missing_pii_results = query_rows(missing_pii_query)
        report_data[report_consts.MISSING_PII_KEY] = missing_pii_results

        # completeness
        logging.info('Getting completeness stats for %s...' % hpo_id)
        completeness_query = completeness.get_hpo_completeness_query(hpo_id)
        report_data[report_consts.COMPLETENESS_REPORT_KEY] = query_rows(
            completeness_query)

        # lab concept metrics
        logging.info('Getting lab concepts for %s...' % hpo_id)
        lab_concept_metrics_query = required_labs.get_lab_concept_summary_query(
            hpo_id)
        report_data[report_consts.LAB_CONCEPT_METRICS_REPORT_KEY] = query_rows(
            lab_concept_metrics_query)

        logging.info(
            'Processing complete. Saving timestamp %s to `gs://%s/%s`.',
            processed_datetime_str, bucket,
            folder_prefix + common.PROCESSED_TXT)
        _write_string_to_file(bucket, folder_prefix + common.PROCESSED_TXT,
                              processed_datetime_str)

    except HttpError as err:
        # cloud error occurred- log details for troubleshooting
        logging.exception(
            'Failed to generate full report due to the following cloud error:\n\n%s'
            % err.content)
        error_occurred = True

        # re-raise error
        raise err
    finally:
        # report all results collected (attempt even if cloud error occurred)
        report_data[report_consts.ERROR_OCCURRED_REPORT_KEY] = error_occurred
        results_html = hpo_report.render(report_data)
        _write_string_to_file(bucket, folder_prefix + common.RESULTS_HTML,
                              results_html)
    return report_data


def generate_empty_report(hpo_id, bucket, folder_prefix):
    """
    Generate an empty report with a "validation failed" error
    Also write processed.txt to folder to prevent processing in the future

    :param hpo_id: identifies the HPO site
    :param bucket: name of the bucket with the submission
    :param folder_prefix: folder containing the submission
    :return: report_data: dict whose keys are params in resources/templates/hpo_report.html
    """
    report_data = dict()
    processed_datetime_str = datetime.datetime.now().strftime(
        '%Y-%m-%dT%H:%M:%S')
    report_data[report_consts.HPO_NAME_REPORT_KEY] = get_hpo_name(hpo_id)
    report_data[report_consts.FOLDER_REPORT_KEY] = folder_prefix
    report_data[report_consts.TIMESTAMP_REPORT_KEY] = processed_datetime_str
    report_data[
        report_consts.
        SUBMISSION_ERROR_REPORT_KEY] = f'Submission folder name {folder_prefix} does not follow the ' \
                                       f'naming convention {consts.FOLDER_NAMING_CONVENTION}, where vN represents ' \
                                       f'the version number for the day, starting at v1 each day. ' \
                                       f'Please resubmit the files in a new folder with the correct naming convention'
    logging.info(
        'Processing skipped. Reason: Folder %s does not follow naming convention %s. '
        'Saving timestamp %s to `gs://%s/%s`.', folder_prefix,
        consts.FOLDER_NAMING_CONVENTION, processed_datetime_str, bucket,
        folder_prefix + common.PROCESSED_TXT)
    _write_string_to_file(bucket, folder_prefix + common.PROCESSED_TXT,
                          processed_datetime_str)
    results_html = hpo_report.render(report_data)
    _write_string_to_file(bucket, folder_prefix + common.RESULTS_HTML,
                          results_html)
    return report_data


def is_valid_folder_prefix_name(folder_prefix):
    """
    Verifies whether folder name follows naming convention YYYY-MM-DD-vN, where vN is the submission version number,
    starting at v1 every day

    :param folder_prefix: folder containing the submission
    :return: Boolean indicating whether the input folder follows the aforementioned naming convention
    """
    folder_name_format = re.compile(consts.FOLDER_NAME_REGEX)
    if not folder_name_format.match(folder_prefix):
        return False
    try:
        datetime.datetime.strptime(folder_prefix[:10], '%Y-%m-%d')
    except ValueError:
        return False
    return True


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
    try:
        logging.info('Processing hpo_id %s', hpo_id)
        bucket = gcs_utils.get_hpo_bucket(hpo_id)
        bucket_items = list_bucket(bucket)
        folder_prefix = _get_submission_folder(bucket, bucket_items, force_run)
        if folder_prefix is None:
            logging.info('No submissions to process in %s bucket %s', hpo_id,
                         bucket)
        else:
            if is_valid_folder_prefix_name(folder_prefix):
                # perform validation
                summary = validate_submission(hpo_id, bucket, bucket_items,
                                              folder_prefix)
                generate_metrics(hpo_id, bucket, folder_prefix, summary)
            else:
                # do not perform validation. Generate empty report and processed.txt
                generate_empty_report(hpo_id, bucket, folder_prefix)
    except BucketDoesNotExistError as bucket_error:
        bucket = bucket_error.bucket
        logging.warning('Bucket `%s` configured for hpo_id `%s` does not exist',
                        bucket, hpo_id)
    except HttpError as http_error:
        message = 'Failed to process hpo_id `%s` due to the following HTTP error: %s' % (
            hpo_id, http_error.content.decode())
        logging.exception(message)


def get_hpo_name(hpo_id):
    hpo_list_of_dicts = bq_utils.get_hpo_info()
    for hpo_dict in hpo_list_of_dicts:
        if hpo_dict['hpo_id'].lower() == hpo_id.lower():
            return hpo_dict['name']
    raise ValueError('%s is not a valid hpo_id' % hpo_id)


def render_query(query_str, **kwargs):
    project_id = app_identity.get_application_id()
    dataset_id = bq_utils.get_dataset_id()
    return query_str.format(project_id=project_id,
                            dataset_id=dataset_id,
                            **kwargs)


def query_rows(query):
    response = bq_utils.query(query)
    return bq_utils.response2rows(response)


def get_heel_error_query(hpo_id):
    """
    Query to retrieve errors in Achilles Heel for an HPO site

    :param hpo_id: identifies the HPO site
    :return: the query
    """
    table_id = bq_utils.get_table_id(hpo_id, consts.ACHILLES_HEEL_RESULTS_TABLE)
    return render_query(consts.HEEL_ERROR_QUERY_VALIDATION, table_id=table_id)


def get_duplicate_counts_query(hpo_id):
    """
    Query to retrieve count of duplicate primary keys in domain tables for an HPO site

    :param hpo_id: identifies the HPO site
    :return: the query
    """
    sub_queries = []
    all_table_ids = bq_utils.list_all_table_ids()
    for table_name in cdm.tables_to_map():
        table_id = bq_utils.get_table_id(hpo_id, table_name)
        if table_id in all_table_ids:
            sub_query = render_query(consts.DUPLICATE_IDS_SUBQUERY,
                                     table_name=table_name,
                                     table_id=table_id)
            sub_queries.append(sub_query)
    unioned_query = consts.UNION_ALL.join(sub_queries)
    return consts.DUPLICATE_IDS_WRAPPER.format(
        union_of_subqueries=unioned_query)


def get_drug_class_counts_query(hpo_id):
    """
    Query to retrieve counts of drug classes in an HPO site's drug_exposure table

    :param hpo_id: identifies the HPO site
    :return: the query
    """
    table_id = bq_utils.get_table_id(hpo_id, consts.DRUG_CHECK_TABLE)
    return render_query(consts.DRUG_CHECKS_QUERY_VALIDATION, table_id=table_id)


def is_valid_rdr(rdr_dataset_id):
    """
    Verifies whether the rdr_dataset_id follows the rdrYYYYMMDD naming convention

    :param rdr_dataset_id: identifies the rdr dataset
    :return: Boolean indicating if the rdr_dataset_id conforms to rdrYYYYMMDD
    """
    rdr_regex = re.compile(r'rdr\d{8}')
    return re.match(rdr_regex, rdr_dataset_id)


def extract_date_from_rdr_dataset_id(rdr_dataset_id):
    """
    Uses the rdr dataset id (string, rdrYYYYMMDD) to extract the date (string, YYYY-MM-DD format)

    :param rdr_dataset_id: identifies the rdr dataset
    :return: date formatted in string as YYYY-MM-DD
    :raises: ValueError if the rdr_dataset_id does not conform to rdrYYYYMMDD
    """
    # verify input is of the format rdrYYYYMMDD
    if is_valid_rdr(rdr_dataset_id):
        # remove 'rdr' prefix
        rdr_date = rdr_dataset_id[3:]
        # TODO remove dependence on date string in RDR dataset id
        rdr_date = rdr_date[:4] + '-' + rdr_date[4:6] + '-' + rdr_date[6:]
        return rdr_date
    else:
        raise ValueError('%s is not a valid rdr_dataset_id' % rdr_dataset_id)


def get_hpo_missing_pii_query(hpo_id):
    """
    Query to retrieve counts of drug classes in an HPO site's drug_exposure table

    :param hpo_id: identifies the HPO site
    :return: the query
    """
    person_table_id = bq_utils.get_table_id(hpo_id, common.PERSON)
    pii_name_table_id = bq_utils.get_table_id(hpo_id, common.PII_NAME)
    pii_wildcard = bq_utils.get_table_id(hpo_id, common.PII_WILDCARD)
    participant_match_table_id = bq_utils.get_table_id(hpo_id,
                                                       common.PARTICIPANT_MATCH)
    rdr_dataset_id = bq_utils.get_rdr_dataset_id()
    rdr_date = extract_date_from_rdr_dataset_id(rdr_dataset_id)
    ehr_no_rdr_with_date = consts.EHR_NO_RDR.format(date=rdr_date)
    rdr_person_table_id = common.PERSON
    return render_query(
        consts.MISSING_PII_QUERY,
        person_table_id=person_table_id,
        rdr_dataset_id=rdr_dataset_id,
        rdr_person_table_id=rdr_person_table_id,
        ehr_no_pii=consts.EHR_NO_PII,
        ehr_no_rdr=ehr_no_rdr_with_date,
        pii_no_ehr=consts.PII_NO_EHR,
        ehr_no_participant_match=consts.EHR_NO_PARTICIPANT_MATCH,
        pii_name_table_id=pii_name_table_id,
        pii_wildcard=pii_wildcard,
        participant_match_table_id=participant_match_table_id)


def perform_validation_on_file(file_name, found_file_names, hpo_id,
                               folder_prefix, bucket):
    """
    Attempts to load a csv file into BigQuery

    :param file_name: name of the file to validate
    :param found_file_names: files found in the submission folder
    :param hpo_id: identifies the hpo site
    :param folder_prefix: directory containing the submission
    :param bucket: bucket containing the submission
    :return: tuple (results, errors) where
     results is list of tuples (file_name, found, parsed, loaded)
     errors is list of tuples (file_name, message)
    """
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

        if not incomplete_jobs:
            job_resource = bq_utils.get_job_details(job_id=load_job_id)
            job_status = job_resource['status']
            if 'errorResult' in job_status:
                # These are issues (which we report back) as opposed to internal errors
                issues = [item['message'] for item in job_status['errors']]
                errors.append((file_name, ' || '.join(issues)))
                logging.info('Issues found in gs://%s/%s/%s', bucket,
                             folder_prefix, file_name)
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
            message += ' Aborting processing `gs://%s/%s`.' % (bucket,
                                                               folder_prefix)
            logging.error(message)
            raise InternalValidationError(message)

    if file_name in common.SUBMISSION_FILES:
        results.append((file_name, found, parsed, loaded))

    return results, errors


def _validation_done(bucket, folder):
    if gcs_utils.get_metadata(bucket=bucket,
                              name=folder + common.PROCESSED_TXT) is not None:
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
    return datetime.datetime.strptime(gcs_object_metadata['updated'],
                                      '%Y-%m-%dT%H:%M:%S.%fZ')


def list_submitted_bucket_items(folder_bucketitems):
    """
    :param folder_bucketitems: List of Bucket items
    :return: list of files
    """
    files_list = []
    object_retention_days = 30
    today = datetime.datetime.today()
    for file_name in folder_bucketitems:
        if basename(file_name) not in resources.IGNORE_LIST:
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
    date_created = datetime.datetime.strptime(
        gcs_object_metadata['timeCreated'], '%Y-%m-%dT%H:%M:%S.%fZ')
    return date_created


def _get_submission_folder(bucket, bucket_items, force_process=False):
    """
    Get the string name of the most recent submission directory for validation

    Skips directories listed in IGNORE_DIRECTORIES with a case insensitive
    match.

    :param bucket: string bucket name to look into
    :param bucket_items: list of unicode string items in the bucket
    :param force_process: if True return most recently updated directory, even
        if it has already been processed.
    :returns: a directory prefix string of the form "<directory_name>/" if
        the directory has not been processed, it is not an ignored directory,
        and force_process is False.  a directory prefix string of the form
        "<directory_name>/" if the directory has been processed, it is not an
        ignored directory, and force_process is True.  None if the directory
        has been processed and force_process is False or no submission
        directory exists
    """
    # files in root are ignored here
    all_folder_list = set([
        item['name'].split('/')[0] + '/'
        for item in bucket_items
        if len(item['name'].split('/')) > 1
    ])

    folder_datetime_list = []
    folders_with_submitted_files = []
    for folder_name in all_folder_list:
        # DC-343  special temporary case where we have to deal with a possible
        # directory dumped into the bucket by 'ehr sync' process from RDR
        ignore_folder = False
        for exp in common.IGNORE_DIRECTORIES:
            compiled_exp = re.compile(exp)
            if compiled_exp.match(folder_name.lower()):
                logging.info(
                    "Skipping %s directory.  It is not a submission "
                    "directory.", folder_name)
                ignore_folder = True

        if ignore_folder:
            continue

        # this is not in a try/except block because this follows a bucket read which is in a try/except
        folder_bucket_items = [
            item for item in bucket_items
            if item['name'].startswith(folder_name)
        ]
        submitted_bucket_items = list_submitted_bucket_items(
            folder_bucket_items)

        if submitted_bucket_items and submitted_bucket_items != []:
            folders_with_submitted_files.append(folder_name)
            latest_datetime = max([
                updated_datetime_object(item) for item in submitted_bucket_items
            ])
            folder_datetime_list.append(latest_datetime)

    if folder_datetime_list and folder_datetime_list != []:
        latest_datetime_index = folder_datetime_list.index(
            max(folder_datetime_list))
        to_process_folder = folders_with_submitted_files[latest_datetime_index]
        if force_process:
            return to_process_folder
        else:
            processed = _validation_done(bucket, to_process_folder)
            if not processed:
                return to_process_folder
    return None


def _is_cdm_file(gcs_file_name):
    return gcs_file_name.lower() in resources.CDM_FILES


def _is_pii_file(gcs_file_name):
    return gcs_file_name.lower() in common.PII_FILES


def _is_known_file(gcs_file_name):
    return gcs_file_name in resources.IGNORE_LIST


def _is_string_excluded_file(gcs_file_name):
    return any(
        gcs_file_name.startswith(prefix)
        for prefix in common.IGNORE_STRING_LIST)


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


def _write_string_to_file(bucket, name, string):
    """
    Save the validation results in GCS
    :param bucket: bucket to save to
    :param name: name of the file (object) to save to in GCS
    :param string: string to write
    :return:
    """
    f = StringIO()
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


@api_util.auth_required_cron
def run_retraction_cron():
    project_id = bq_utils.app_identity.get_application_id()
    output_project_id = bq_utils.get_output_project_id()
    hpo_id = bq_utils.get_retraction_hpo_id()
    retraction_type = bq_utils.get_retraction_type()
    pid_table_id = bq_utils.get_retraction_pid_table_id()
    sandbox_dataset_id = bq_utils.get_retraction_sandbox_dataset_id()

    # retract from bq
    dataset_ids = bq_utils.get_retraction_dataset_ids()
    logging.info('Dataset id/s to target from env variable: %s' % dataset_ids)
    logging.info('Running retraction on BQ datasets')
    if output_project_id:
        # retract from output dataset
        retract_data_bq.run_bq_retraction(output_project_id, sandbox_dataset_id,
                                          project_id, pid_table_id, hpo_id,
                                          dataset_ids, retraction_type)
    # retract from default dataset
    retract_data_bq.run_bq_retraction(project_id, sandbox_dataset_id,
                                      project_id, pid_table_id, hpo_id,
                                      dataset_ids, retraction_type)
    logging.info('Completed retraction on BQ datasets')

    # retract from gcs
    folder = bq_utils.get_retraction_submission_folder()
    logging.info('Submission folder/s to target from env variable: %s' % folder)
    logging.info('Running retraction from internal bucket folders')
    retract_data_gcs.run_gcs_retraction(project_id,
                                        sandbox_dataset_id,
                                        pid_table_id,
                                        hpo_id,
                                        folder,
                                        force_flag=True)
    logging.info('Completed retraction from internal bucket folders')
    return 'retraction-complete'


@api_util.auth_required_cron
def validate_pii():
    project = bq_utils.app_identity.get_application_id()
    combined_dataset = bq_utils.get_combined_dataset_id()
    ehr_dataset = bq_utils.get_dataset_id()
    dest_dataset = bq_utils.get_validation_results_dataset_id()
    logging.info('Calling match_participants')
    _, errors = matching.match_participants(project, combined_dataset,
                                            ehr_dataset, dest_dataset)

    if errors > 0:
        logging.error("Errors encountered in validation process")

    return consts.VALIDATION_SUCCESS


@api_util.auth_required_cron
def write_drc_pii_validation_file():
    project = bq_utils.app_identity.get_application_id()
    validation_dataset = bq_utils.get_validation_results_dataset_id()
    logging.info('Calling write_results_to_drc_bucket')
    matching.write_results_to_drc_bucket(project, validation_dataset)

    return consts.DRC_VALIDATION_REPORT_SUCCESS


@api_util.auth_required_cron
def write_sites_pii_validation_files():
    project = bq_utils.app_identity.get_application_id()
    validation_dataset = bq_utils.get_validation_results_dataset_id()
    logging.info('Calling write_results_to_site_buckets')
    matching.write_results_to_site_buckets(project, validation_dataset)

    return consts.SITES_VALIDATION_REPORT_SUCCESS


@app.before_first_request
def set_up_logging():
    initialize_logging()


app.add_url_rule(consts.PREFIX + 'ValidateAllHpoFiles',
                 endpoint='validate_all_hpos',
                 view_func=validate_all_hpos,
                 methods=['GET'])

app.add_url_rule(consts.PREFIX + 'ValidateHpoFiles/<string:hpo_id>',
                 endpoint='validate_hpo_files',
                 view_func=validate_hpo_files,
                 methods=['GET'])

app.add_url_rule(consts.PREFIX + 'UploadAchillesFiles/<string:hpo_id>',
                 endpoint='upload_achilles_files',
                 view_func=upload_achilles_files,
                 methods=['GET'])

app.add_url_rule(consts.PREFIX + 'CopyFiles/<string:hpo_id>',
                 endpoint='copy_files',
                 view_func=copy_files,
                 methods=['GET'])

app.add_url_rule(consts.PREFIX + 'UnionEHR',
                 endpoint='union_ehr',
                 view_func=union_ehr,
                 methods=['GET'])

app.add_url_rule(consts.PREFIX + consts.WRITE_DRC_VALIDATION_FILE,
                 endpoint='write_drc_pii_validation_file',
                 view_func=write_drc_pii_validation_file,
                 methods=['GET'])

app.add_url_rule(consts.PREFIX + consts.WRITE_SITE_VALIDATION_FILES,
                 endpoint='write_sites_pii_validation_files',
                 view_func=write_sites_pii_validation_files,
                 methods=['GET'])

app.add_url_rule(consts.PREFIX + consts.PARTICIPANT_VALIDATION,
                 endpoint='validate_pii',
                 view_func=validate_pii,
                 methods=['GET'])

app.add_url_rule(consts.PREFIX + 'RetractPids',
                 endpoint='run_retraction_cron',
                 view_func=run_retraction_cron,
                 methods=['GET'])

app.before_request(
    begin_request_logging)  # Must be first before_request() call.

app.teardown_request(
    end_request_logging
)  # teardown_request to be called regardless if there is an exception thrown
