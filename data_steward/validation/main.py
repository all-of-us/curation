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
from io import StringIO, open

# Third party imports
import dateutil
from flask import Flask
from google.cloud import bigquery
from google.cloud.storage.bucket import Blob
from google.cloud.exceptions import GoogleCloudError
from googleapiclient.errors import HttpError

# Project imports
import api_util
import app_identity
import bq_utils
import cdm
import common
from cdr_cleaner import clean_cdr
from cdr_cleaner.args_parser import add_kwargs_to_args
from constants.cdr_cleaner.clean_cdr import CRON_RETRACTION
import constants.global_variables
from gcloud.bq import BigQueryClient
from gcloud.gcs import StorageClient
import resources
from common import ACHILLES_EXPORT_PREFIX_STRING, ACHILLES_EXPORT_DATASOURCES_JSON
from constants.validation import hpo_report as report_consts
from constants.validation import main as consts
from curation_logging.curation_gae_handler import begin_request_logging, end_request_logging
from retraction import retract_data_bq, retract_data_gcs
from validation import achilles, achilles_heel, ehr_union, export, hpo_report
from validation import email_notification as en
from validation.app_errors import (BucketNotSet, log_traceback,
                                   errors_blueprint, InternalValidationError,
                                   BucketDoesNotExistError)
from validation.metrics import completeness, required_labs
from validation.participants.store_participant_summary_results import fetch_and_store_full_ps_data
from validation.participants.validate import setup_and_validate_participants, get_participant_validation_summary_query

app = Flask(__name__)

# register application error handlers
app.register_blueprint(errors_blueprint)


def all_required_files_loaded(result_items):
    for (file_name, _, _, loaded) in result_items:
        if file_name in common.REQUIRED_FILES:
            if loaded != 1:
                return False
    return True


def save_datasources_json(storage_client,
                          datasource_id=None,
                          folder_prefix="",
                          target_bucket=None):
    """
    Generate and save datasources.json (from curation report) in a GCS bucket

    :param datasource_id: the ID of the HPO aggregate dataset that report should go to
    :param folder_prefix: relative path in GCS to save to (without 'gs://')
    :param target_bucket: GCS bucket to save to. If not supplied, uses the
        bucket assigned to hpo_id.
    :return:
    """
    if datasource_id is None:
        if target_bucket is None:
            raise RuntimeError(
                f"Cannot save datasources.json if neither hpo_id "
                f"nor target_bucket are specified.")
    else:
        if target_bucket is None:
            target_bucket = storage_client.get_hpo_bucket(datasource_id)
        else:
            target_bucket = storage_client.bucket(target_bucket)

    datasource = dict(name=datasource_id, folder=datasource_id, cdmVersion=5)
    datasources = dict(datasources=[datasource])
    datasources_fp = StringIO(json.dumps(datasources))
    blob = target_bucket.blob(
        f'{folder_prefix}{ACHILLES_EXPORT_DATASOURCES_JSON}')
    blob.upload_from_file(datasources_fp)
    result: dict = storage_client.get_blob_metadata(blob)
    return result


def run_export(datasource_id=None, folder_prefix="", target_bucket=None):
    """
    Run export queries for an HPO and store JSON payloads in specified folder in (optional) target bucket

    :type datasource_id: ID of the HPO or aggregate dataset to run export for. This is the data source name in the report.
    :param folder_prefix: Relative base path to store report. empty by default.
    :param target_bucket: Bucket to save report. If None, use bucket associated with hpo_id.
    """
    results = []
    project_id = app_identity.get_application_id()
    storage_client = StorageClient(project_id)
    # Using separate var rather than hpo_id here because hpo_id None needed in calls below
    if datasource_id is None and target_bucket is None:
        raise RuntimeError(
            f"Cannot export if neither hpo_id nor target_bucket is specified.")
    else:
        if target_bucket is None:
            target_bucket = storage_client.get_hpo_bucket(datasource_id)
        else:
            target_bucket = storage_client.bucket(target_bucket)

    logging.info(
        f"Exporting {datasource_id} report to bucket {target_bucket.name}")

    # Run export queries and store json payloads in specified folder in the target bucket
    reports_prefix: str = f'{folder_prefix}{ACHILLES_EXPORT_PREFIX_STRING}{datasource_id}/'
    for export_name in common.ALL_REPORTS:
        sql_path = os.path.join(export.EXPORT_PATH, export_name)
        result = export.export_from_path(sql_path, datasource_id)
        content = json.dumps(result)
        fp = StringIO(content)
        blob = target_bucket.blob(f'{reports_prefix}{export_name}.json')
        blob.upload_from_file(fp)
        result: dict = storage_client.get_blob_metadata(blob)
        results.append(result)
    result = save_datasources_json(storage_client=storage_client,
                                   datasource_id=datasource_id,
                                   folder_prefix=folder_prefix,
                                   target_bucket=target_bucket.name)
    results.append(result)
    return results


def run_achilles(client, hpo_id=None):
    """
    checks for full results and run achilles/heel

    :client: a BigQueryClient
    :hpo_id: hpo on which to run achilles
    :returns:
    """
    if hpo_id is not None:
        logging.info(f"Running achilles for hpo_id '{hpo_id}'")
    achilles.create_tables(hpo_id, True)
    achilles.load_analyses(hpo_id)
    achilles.run_analyses(client, hpo_id=hpo_id)
    if hpo_id is not None:
        logging.info(f"Running achilles_heel for hpo_id '{hpo_id}'")
    achilles_heel.create_tables(hpo_id, True)
    achilles_heel.run_heel(client, hpo_id=hpo_id)


@api_util.auth_required_cron
@log_traceback
def upload_achilles_files(hpo_id):
    result = _upload_achilles_files(hpo_id, "")
    return json.dumps(result, sort_keys=True, indent=4, separators=(',', ': '))


def _upload_achilles_files(hpo_id: str = None,
                           folder_prefix: str = '',
                           target_bucket: str = None) -> list:
    """
    uploads achilles web files to the corresponding hpo bucket

    :hpo_id: which hpo bucket do these files go into
    :returns:
    """
    results = []
    project_id = app_identity.get_application_id()
    storage_client = StorageClient(project_id)

    if not target_bucket:
        if not hpo_id:
            raise RuntimeError(
                f"Either hpo_id or target_bucket must be specified")
        target_bucket = storage_client.get_hpo_bucket(hpo_id)
    else:
        target_bucket = storage_client.bucket(target_bucket)
    logging.info(
        f"Uploading achilles index files to 'gs://{target_bucket.name}/{folder_prefix}'"
    )

    for filename in resources.ACHILLES_INDEX_FILES:
        logging.info(
            f"Uploading achilles file '{filename}' to bucket {target_bucket.name}"
        )
        bucket_file_name = filename.split(resources.resource_files_path +
                                          os.sep)[1].strip().replace('\\', '/')
        with open(filename, 'rb') as fp:
            blob = target_bucket.blob(f'{folder_prefix}{bucket_file_name}')
            blob.upload_from_file(fp)
            upload_result: dict = storage_client.get_blob_metadata(blob)
            results.append(upload_result)
    return results


@api_util.auth_required_cron
@log_traceback
def validate_hpo_files(hpo_id):
    """
    validation end point for individual hpo_ids
    """
    process_hpo(hpo_id, force_run=True)
    return 'validation done!'


@api_util.auth_required_cron
@log_traceback
def validate_all_hpos():
    """
    validation end point for all hpo_ids
    """
    for item in bq_utils.get_hpo_info():
        hpo_id = item['hpo_id']
        process_hpo(hpo_id)
    return 'validation done!'


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


def validate_submission(hpo_id: str, bucket, folder_items: list,
                        folder_prefix: str):
    """
    Load submission in BigQuery and summarize outcome

    :param hpo_id:
    :param bucket:
    :param folder_items:
    :param folder_prefix:
    :return: a dict with keys results, errors, warnings
      results is list of tuples (file_name, found, parsed, loaded)
      errors and warnings are both lists of tuples (file_name, message)
    """
    logging.info(
        f"Validating {hpo_id} submission in gs://{bucket.name}/{folder_prefix}")
    # separate cdm from the unknown (unexpected) files
    found_cdm_files, found_pii_files, unknown_files = categorize_folder_items(
        folder_items)

    errors = []
    results = []

    # Create all tables first to simplify downstream processes
    # (e.g. ehr_union doesn't have to check if tables exist)
    for file_name in resources.CDM_CSV_FILES + common.PII_FILES:
        table_name = file_name.split('.')[0]
        table_id = resources.get_table_id(table_name, hpo_id=hpo_id)
        bq_utils.create_standard_table(table_name, table_id, drop_existing=True)

    for cdm_file_name in sorted(resources.CDM_CSV_FILES):
        file_results, file_errors = perform_validation_on_file(
            cdm_file_name, found_cdm_files, hpo_id, folder_prefix, bucket)
        results.extend(file_results)
        errors.extend(file_errors)

    # TODO use sorted(resources.CDM_JSONL_FILES) in the future
    for cdm_file_name in [f'{common.NOTE}.jsonl']:
        file_results, file_errors = perform_validation_on_file(
            cdm_file_name, found_cdm_files, hpo_id, folder_prefix, bucket)
        # If JSONL file found, remove note.csv and add note.jsonl instead
        if file_results[0][1]:
            results = [
                result for result in results
                if not result[0].startswith(f'{common.NOTE}')
            ]
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


def is_first_validation_run(folder_items):
    return common.RESULTS_HTML not in folder_items and common.PROCESSED_TXT not in folder_items


def generate_metrics(project_id, hpo_id, bucket, folder_prefix, summary):
    """
    Generate metrics regarding a submission

    :param project_id: identifies the project
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
    error_occurred = False

    # TODO separate query generation, query execution, writing to GCS
    gcs_path = f"gs://{bucket}/{folder_prefix}"
    report_data[report_consts.HPO_NAME_REPORT_KEY] = get_hpo_name(hpo_id)
    report_data[report_consts.FOLDER_REPORT_KEY] = folder_prefix
    results = report_data['results']
    try:
        # TODO modify achilles to run successfully when tables are empty
        # achilles queries will raise exceptions (e.g. division by zero) if files not present

        bq_client = BigQueryClient(project_id)

        if all_required_files_loaded(results):
            logging.info(f"Running achilles on {folder_prefix}.")
            run_achilles(bq_client, hpo_id)
            run_export(datasource_id=hpo_id, folder_prefix=folder_prefix)
            logging.info(f"Uploading achilles index files to '{gcs_path}'.")
            _upload_achilles_files(hpo_id, folder_prefix)
            heel_error_query = get_heel_error_query(hpo_id)
            report_data[report_consts.HEEL_ERRORS_REPORT_KEY] = query_rows(
                heel_error_query)
        else:
            report_data[
                report_consts.
                SUBMISSION_ERROR_REPORT_KEY] = "Required files are missing"
            logging.info(
                f"Required files are missing in {gcs_path}. Skipping achilles.")

        # non-unique key metrics
        logging.info(f"Getting non-unique key stats for {hpo_id}")
        nonunique_metrics_query = get_duplicate_counts_query(bq_client, hpo_id)
        report_data[
            report_consts.NONUNIQUE_KEY_METRICS_REPORT_KEY] = query_rows(
                nonunique_metrics_query)

        # drug class metrics
        logging.info(f"Getting drug class for {hpo_id}")
        drug_class_metrics_query = get_drug_class_counts_query(hpo_id)
        report_data[report_consts.DRUG_CLASS_METRICS_REPORT_KEY] = query_rows(
            drug_class_metrics_query)

        # missing PII
        logging.info(f"Getting missing record stats for {hpo_id}")
        missing_pii_query = get_hpo_missing_pii_query(hpo_id)
        missing_pii_results = query_rows(missing_pii_query)
        report_data[report_consts.MISSING_PII_KEY] = missing_pii_results

        # completeness
        logging.info(f"Getting completeness stats for {hpo_id}")
        completeness_query = completeness.get_hpo_completeness_query(hpo_id)
        report_data[report_consts.COMPLETENESS_REPORT_KEY] = query_rows(
            completeness_query)

        # participant validation metrics
        logging.info(f"Ensuring participant validation can be run for {hpo_id}")
        setup_and_validate_participants(bq_client, hpo_id)
        participant_validation_query = get_participant_validation_summary_query(
            hpo_id)
        # TODO add to report_data based on requirements from EHR_OPS

        # lab concept metrics
        logging.info(f"Getting lab concepts for {hpo_id}")
        lab_concept_metrics_query = required_labs.get_lab_concept_summary_query(
            bq_client, hpo_id)
        report_data[report_consts.LAB_CONCEPT_METRICS_REPORT_KEY] = query_rows(
            lab_concept_metrics_query)

        logging.info(f"Processing complete.")
    except HttpError as err:
        # cloud error occurred- log details for troubleshooting
        logging.exception(
            f"Failed to generate full report due to the following cloud error:\n\n{err.content}"
        )
        error_occurred = True
    finally:
        # report all results collected (attempt even if cloud error occurred)
        report_data[report_consts.ERROR_OCCURRED_REPORT_KEY] = error_occurred
    return report_data


def generate_empty_report(hpo_id, folder_prefix):
    """
    Generate an empty report with a "validation failed" error
    Also write processed.txt to folder to prevent processing in the future

    :param hpo_id: identifies the HPO site
    :param folder_prefix: folder containing the submission
    :return: report_data: dict whose keys are params in resource_files/templates/hpo_report.html
    """
    report_data = dict()
    report_data[report_consts.HPO_NAME_REPORT_KEY] = get_hpo_name(hpo_id)
    report_data[report_consts.FOLDER_REPORT_KEY] = folder_prefix
    report_data[report_consts.SUBMISSION_ERROR_REPORT_KEY] = (
        f"Submission folder name {folder_prefix} does not follow the "
        f"naming convention {consts.FOLDER_NAMING_CONVENTION}, where vN represents "
        f"the version number for the day, starting at v1 each day. "
        f"Please resubmit the files in a new folder with the correct naming convention"
    )
    logging.info(
        f"Processing skipped. Reason: Folder {folder_prefix} does not follow "
        f"naming convention {consts.FOLDER_NAMING_CONVENTION}.")
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


def get_eastern_time():
    """
    Return current Eastern Time

    :return: formatted current eastern time as string
    """
    eastern_timezone = dateutil.tz.gettz('America/New_York')
    return datetime.datetime.now(eastern_timezone).strftime(
        consts.DATETIME_FORMAT)


def perform_reporting(hpo_id, report_data, folder_items, bucket, folder_prefix,
                      failed_submission):
    """
    Generate html report, upload to GCS and send email if possible

    :param hpo_id: identifies the hpo site
    :param report_data: dictionary containing items for populating hpo_report.html
    :param folder_items: items in the folder without folder prefix
    :param bucket: bucket containing the folder
    :param folder_prefix: submission folder
    :param failed_submission: Indicates if a submission has failed
    :return:
    """
    processed_time_str = get_eastern_time()
    report_data[report_consts.TIMESTAMP_REPORT_KEY] = processed_time_str
    results_html = hpo_report.render(report_data)

    results_html_path = f'{folder_prefix}{common.RESULTS_HTML}'
    logging.info(f"Saving file {common.RESULTS_HTML} to "
                 f"gs://{bucket.name}/{results_html_path}.")
    results_html_blob = bucket.blob(results_html_path)
    results_html_blob.upload_from_string(results_html)

    processed_txt_path = f'{folder_prefix}{common.PROCESSED_TXT}'
    logging.info(f"Saving timestamp {processed_time_str} to "
                 f"gs://{bucket.name}/{processed_txt_path}.")
    processed_txt_blob = bucket.blob(processed_txt_path)
    processed_txt_blob.upload_from_string(processed_time_str)

    folder_uri = f"gs://{bucket.name}/{folder_prefix}"
    if (folder_items and
            is_first_validation_run(folder_items)) or failed_submission:
        logging.info(f"Attempting to send report via email for {hpo_id}")
        email_msg = en.generate_email_message(hpo_id, results_html, folder_uri,
                                              report_data)
        if email_msg is None:
            logging.info(
                f"Not enough info in contact list to send emails for hpo_id {hpo_id}"
            )
        else:
            result = en.send_email(email_msg)
            if result is None:
                logging.info(
                    'Mandrill error occurred. Please check logs for more details'
                )
            else:
                result_ids = ', '.join(
                    [result_item['_id'] for result_item in result])
                logging.info(
                    f"Sending emails for hpo_id {hpo_id} with Mandrill tracking ids: {result_ids}"
                )
    logging.info(f"Reporting complete")
    return


def get_folder_items(bucket_items, folder_prefix):
    """
    Returns items in bucket which belong to a folder

    :param bucket_items: items in the bucket
    :param folder_prefix: prefix containing the folder name
    :return: list of items in the folder without the folder prefix
    """
    return [
        item['name'][len(folder_prefix):]
        for item in bucket_items
        if item['name'].startswith(folder_prefix)
    ]


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
        logging.info(f"Processing hpo_id {hpo_id}")
        project_id = app_identity.get_application_id()
        storage_client = StorageClient(project_id)
        bucket = storage_client.get_hpo_bucket(hpo_id)
        bucket_items: list = storage_client.get_bucket_items_metadata(bucket)
        folder_prefix = _get_submission_folder(bucket, bucket_items, force_run)
        if not folder_prefix:
            logging.info(
                f"No submissions to process in {hpo_id} bucket {bucket.name}")
        else:
            folder_items = []
            if is_valid_folder_prefix_name(folder_prefix):
                # perform validation
                folder_items = get_folder_items(bucket_items, folder_prefix)
                summary = validate_submission(hpo_id, bucket, folder_items,
                                              folder_prefix)
                report_data = generate_metrics(project_id, hpo_id, bucket,
                                               folder_prefix, summary)
                failed_submission = False
            else:
                # do not perform validation
                report_data = generate_empty_report(hpo_id, folder_prefix)
                failed_submission = True
            perform_reporting(hpo_id, report_data, folder_items, bucket,
                              folder_prefix, failed_submission)
    except BucketNotSet as exc:
        logging.info(f'{exc}')
    except BucketDoesNotExistError as exc:
        logging.warning(f'{exc}')
    except GoogleCloudError as google_cloud_error:
        message = (f"Failed to process hpo_id '{hpo_id}' due to the following "
                   f"HTTP error: {google_cloud_error.message}")
        logging.exception(message)


def get_hpo_name(hpo_id):
    hpo_list_of_dicts = bq_utils.get_hpo_info()
    for hpo_dict in hpo_list_of_dicts:
        if hpo_dict['hpo_id'].lower() == hpo_id.lower():
            return hpo_dict['name']
    raise ValueError(f"{hpo_id} is not a valid hpo_id")


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
    table_id = resources.get_table_id(consts.ACHILLES_HEEL_RESULTS_TABLE,
                                      hpo_id=hpo_id)
    return render_query(consts.HEEL_ERROR_QUERY_VALIDATION, table_id=table_id)


def get_duplicate_counts_query(client, hpo_id):
    """
    Query to retrieve count of duplicate primary keys in domain tables for an HPO site

    :param client: BigQueryClient
    :param hpo_id: identifies the HPO site
    :return: the query
    """
    sub_queries = []
    all_table_ids = [
        table.table_id
        for table in client.list_tables(os.environ.get('BIGQUERY_DATASET_ID'))
    ]
    for table_name in cdm.tables_to_map():
        table_id = resources.get_table_id(table_name, hpo_id=hpo_id)
        if table_id in all_table_ids:
            sub_query = render_query(consts.DUPLICATE_IDS_SUBQUERY,
                                     table_name=table_name,
                                     table_id=table_id,
                                     primary_key=f'{table_name}_id')
            sub_queries.append(sub_query)
    for table_name in common.PII_TABLES + [common.PERSON, common.DEATH]:
        table_id = resources.get_table_id(table_name, hpo_id=hpo_id)
        if table_id in all_table_ids:
            sub_query = render_query(consts.DUPLICATE_IDS_SUBQUERY,
                                     table_name=table_name,
                                     table_id=table_id,
                                     primary_key='person_id')
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
    table_id = resources.get_table_id(consts.DRUG_CHECK_TABLE, hpo_id=hpo_id)
    return render_query(consts.DRUG_CHECKS_QUERY_VALIDATION, table_id=table_id)


def is_valid_rdr(rdr_dataset_id):
    """
    Verifies whether the rdr_dataset_id follows the rdrYYYYMMDD naming convention

    :param rdr_dataset_id: identifies the rdr dataset
    :return: Boolean indicating if the rdr_dataset_id conforms to rdrYYYYMMDD
    """
    rdr_regex = re.compile(r'rdr\d{8}')
    return re.match(rdr_regex, rdr_dataset_id)


def get_hpo_missing_pii_query(hpo_id):
    """
    Query to retrieve counts of drug classes in an HPO site's drug_exposure table

    :param hpo_id: identifies the HPO site
    :return: the query
    """
    person_table_id = resources.get_table_id(common.PERSON, hpo_id=hpo_id)
    pii_name_table_id = resources.get_table_id(common.PII_NAME, hpo_id=hpo_id)
    pii_wildcard = resources.get_table_id(common.PII_WILDCARD, hpo_id=hpo_id)
    participant_match_table_id = resources.get_table_id(
        common.PARTICIPANT_MATCH, hpo_id=hpo_id)

    return render_query(
        consts.MISSING_PII_QUERY,
        person_table_id=person_table_id,
        ehr_no_pii=consts.EHR_NO_PII,
        pii_no_ehr=consts.PII_NO_EHR,
        ehr_no_participant_match=consts.EHR_NO_PARTICIPANT_MATCH,
        pii_name_table_id=pii_name_table_id,
        pii_wildcard=pii_wildcard,
        participant_match_table_id=participant_match_table_id)


def perform_validation_on_file(file_name: str, found_file_names: list,
                               hpo_id: str, folder_prefix, bucket):
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
    found = parsed = loaded = 0
    table_name, extension = file_name.split('.')
    if extension.upper() == 'JSONL':
        logging.info(f"Validating JSONL file '{file_name}' if found")
        if file_name in found_file_names:
            logging.info(f"Found file '{file_name}'")
            found = 1
            app_id: str = app_identity.get_application_id()
            storage_client = StorageClient(app_id)
            hpo_bucket = storage_client.get_hpo_bucket(hpo_id)

            bq_client = BigQueryClient(app_id)

            if table_name not in resources.CDM_TABLES:
                raise ValueError(f'{table_name} is not a valid table to load')

            dataset_id: str = bq_utils.get_dataset_id()

            gcs_object_path: str = (f'gs://{hpo_bucket.name}/'
                                    f'{folder_prefix}'
                                    f'{table_name}.{extension}')
            table_id = resources.get_table_id(table_name, hpo_id)
            fq_table_id = f'{bq_client.project}.{dataset_id}.{table_id}'

            job_config = bigquery.LoadJobConfig(
                schema=bq_client.get_table_schema(table_name),
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            )

            load_job = bq_client.load_table_from_uri(
                gcs_object_path,
                destination=fq_table_id,
                location="US",
                job_config=job_config,
            )

            try:
                # These are issues (which we report back) as opposed to internal errors
                load_job.result()
                # Processed ok
                parsed = loaded = 1
                destination_table = bq_client.get_table(fq_table_id)
                logging.info(f"Loaded {destination_table.num_rows} rows.")
            except GoogleCloudError:
                for e in load_job.errors:
                    errors.append((file_name, e["message"]))
                    logging.info(
                        f"Issues found in gs://{bucket.name}/{folder_prefix}/{file_name}"
                    )

        results.append((file_name, found, parsed, loaded))

        return results, errors

    logging.info(f"Validating file '{file_name}' if found")
    if file_name in found_file_names:
        logging.info(f"Found file '{file_name}'")
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
                logging.info(
                    f"Issues found in gs://{bucket.name}/{folder_prefix}/{file_name}"
                )
                for issue in issues:
                    logging.info(issue)
            else:
                # Processed ok
                parsed = loaded = 1
        else:
            # Incomplete jobs are internal unrecoverable errors.
            # Aborting the process allows for this submission to be validated when system recovers.
            message = (
                f"Loading hpo_id '{hpo_id}' table '{table_name}' failed because "
                f"job id '{load_job_id}' did not complete.\n")
            message += f"Aborting processing 'gs://{bucket.name}/{folder_prefix}'."
            logging.error(message)
            raise InternalValidationError(message)

    if file_name in common.SUBMISSION_FILES:
        results.append((file_name, found, parsed, loaded))

    return results, errors


def _validation_done(bucket, folder):
    return Blob(bucket=bucket, name=f'{folder}{common.PROCESSED_TXT}').exists()


def basename(item_metadata):
    """
    returns name of file inside folder

    :item_metadata: metadata as returned by get bucket times metadata
    :returns: name without folder name

    """
    name = item_metadata['name']
    if len(name.split('/')) > 1:
        return '/'.join(name.split('/')[1:])
    return ''


def _has_all_required_files(table_names):
    return set(common.AOU_REQUIRED).issubset(set(table_names))


def list_submitted_bucket_items(folder_bucketitems):
    """
    :param folder_bucketitems: List of Bucket items
    :return: list of files
    """
    files_list = []
    object_retention_days = 30
    utc_today = datetime.datetime.now(tz=None)

    # If any required file missing, stop submission
    folder_bucketitems_table_names = [
        basename(file_name).split('.')[0] for file_name in folder_bucketitems
    ]

    to_process_items = [
        item for item in folder_bucketitems
        if basename(item) not in resources.IGNORE_LIST
    ]

    if not to_process_items:
        return files_list

    # Process if all required files present
    if _has_all_required_files(folder_bucketitems_table_names):
        logging.info(f"All required files found, processing.")
        return to_process_items

    # Check submission times and validate if > 3 hrs old and < 29 days old
    upper_age_threshold = min(item['timeCreated'] +
                              datetime.timedelta(days=object_retention_days) -
                              datetime.timedelta(days=1)
                              for item in folder_bucketitems
                              if basename(item) not in resources.IGNORE_LIST)
    upper_age_threshold = upper_age_threshold.replace(tzinfo=None)

    lower_age_threshold = max(item['updated'] + datetime.timedelta(hours=3)
                              for item in folder_bucketitems
                              if basename(item) not in resources.IGNORE_LIST)
    lower_age_threshold = lower_age_threshold.replace(tzinfo=None)

    if upper_age_threshold > utc_today:
        if lower_age_threshold < utc_today:
            logging.info(
                f"All required files not found but submission is stale (> 3 hrs), processing."
            )
            return to_process_items
        diff = lower_age_threshold - utc_today
        hrs = (diff.total_seconds() // 3600) + 1
        logging.info(
            f"Delaying processing for hpo_id by 3 hrs (to next cron run) "
            f"since files were recently uploaded. Latest file was uploaded "
            f"less than {hrs} hours ago.")
    else:
        logging.info(
            "Not all required files are found in the folder, and some files "
            "are reaching or already reached the retention period. "
            "Validation will not run on this folder. "
            "Investigation needed for why not all required files are present.")

    return files_list


def _get_submission_folder(bucket, bucket_items, force_process=False):
    """
    Get the string name of the most recent submission directory for validation

    Skips directories listed in IGNORE_DIRECTORIES with a case insensitive
    match.

    :param bucket: Bucket Object to validate on
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
                    f"Skipping {folder_name} directory.  It is not a submission directory."
                )
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

        if submitted_bucket_items:
            folders_with_submitted_files.append(folder_name)
            latest_datetime = max(
                [item['updated'] for item in submitted_bucket_items])
            folder_datetime_list.append(latest_datetime)

    if folder_datetime_list:
        latest_datetime_index = folder_datetime_list.index(
            max(folder_datetime_list))
        to_process_folder = folders_with_submitted_files[latest_datetime_index]
        if force_process:
            return to_process_folder
        processed = _validation_done(bucket, to_process_folder)
        if not processed:
            return to_process_folder
        else:
            logging.info(
                f'Skipping already processed folder {to_process_folder}')
    return None


def _is_cdm_file(gcs_file_name):
    return gcs_file_name.lower(
    ) in resources.CDM_CSV_FILES or gcs_file_name.lower(
    ) in resources.CDM_JSONL_FILES


def _is_pii_file(gcs_file_name):
    return gcs_file_name.lower() in common.PII_FILES


def _is_known_file(gcs_file_name):
    return gcs_file_name in resources.IGNORE_LIST


def _is_string_excluded_file(gcs_file_name):
    return any(
        gcs_file_name.startswith(prefix)
        for prefix in common.IGNORE_STRING_LIST)


def process_hpo_copy(hpo_id):
    """
    copies over files from hpo bucket to drc bucket
    :hpo_id: hpo from which to copy
    """

    project_id = app_identity.get_application_id()
    storage_client = StorageClient(project_id)

    bucket_items: list = []
    try:
        hpo_bucket = storage_client.get_hpo_bucket(hpo_id)
        drc_bucket = storage_client.get_drc_bucket()
        bucket_items: list = storage_client.get_bucket_items_metadata(
            hpo_bucket)
    except BucketNotSet as exc:
        logging.info(f'{exc}')
    except BucketDoesNotExistError as exc:
        logging.warning(f'{exc}')

    ignored_count: int = 0
    for item in bucket_items:
        item_root: str = item['name'].split('/')[0] + '/'
        if item_root.lower() in common.IGNORE_DIRECTORIES:
            ignored_count += 1
        else:
            name: str = item['name']
            full_name: str = f'{hpo_id}/{hpo_bucket.name}/{name}'
            hpo_blob = hpo_bucket.get_blob(name)
            hpo_bucket.copy_blob(hpo_blob, drc_bucket, full_name)

    logging.info(
        f"Ignoring {ignored_count} of {len(bucket_items)} items in bucket "
        f"{storage_client._get_hpo_bucket_id(hpo_id)} for hpo {hpo_id}")


@api_util.auth_required_cron
@log_traceback
def copy_files(hpo_id):
    """
    endpoint to copy files for hpo_id

    :hpo_id: hpo from which to copy
    :return: json string indicating the job has finished
    """
    process_hpo_copy(hpo_id)
    return '{"copy-status": "done"}'


@api_util.auth_required_cron
@log_traceback
def union_ehr():
    hpo_id = 'unioned_ehr'
    app_id = bq_utils.app_identity.get_application_id()
    input_dataset_id = bq_utils.get_dataset_id()
    output_dataset_id = bq_utils.get_unioned_dataset_id()
    bq_client = BigQueryClient(app_id)
    ehr_union.main(input_dataset_id, output_dataset_id, app_id)

    run_achilles(bq_client, hpo_id)
    now_date_string = datetime.datetime.now().strftime('%Y_%m_%d')
    folder_prefix = f'unioned_ehr_{now_date_string}/'
    run_export(datasource_id=hpo_id, folder_prefix=folder_prefix)
    logging.info(f"Uploading achilles index files")
    _upload_achilles_files(hpo_id, folder_prefix)
    return 'merge-and-achilles-done'


@api_util.auth_required_cron
@log_traceback
def run_retraction_cron():
    """
    Run a cron job to mimic the run_retraction.py script
    """
    constants.global_variables.DISABLE_SANDBOX = True
    project_id = bq_utils.app_identity.get_application_id()
    hpo_id = bq_utils.get_retraction_hpo_id()
    retraction_type = bq_utils.get_retraction_type()
    pid_table_id = bq_utils.get_retraction_pid_table_id()
    sandbox_dataset_id = bq_utils.get_retraction_sandbox_dataset_id()

    # Dataset and table containing list of datasets
    datasets_to_retract_dataset = bq_utils.get_retraction_dataset_ids_dataset()
    datasets_to_retract_table = bq_utils.get_retraction_dataset_ids_table()

    # retract from bq
    if not datasets_to_retract_table or not datasets_to_retract_dataset:
        logging.info(
            f"Retraction cannot run without RETRACTION_DATASET_IDS_TABLE and RETRACTION_DATASET_IDS_DATASET"
        )
        return 'retraction-skipped'

    bq_client = BigQueryClient(project_id)
    dataset_query_job = bq_client.query(
        f"SELECT * FROM {project_id}.{datasets_to_retract_dataset}.{datasets_to_retract_table}"
    )
    dataset_ids_result = dataset_query_job.result()
    dataset_ids = dataset_ids_result.to_dataframe()["datasets"].to_list()
    logging.info(f"Dataset id/s to target retrieved from table: {dataset_ids}")
    logging.info(f"Running retraction on BQ datasets")

    # retract from default dataset
    retract_data_bq.run_bq_retraction(project_id,
                                      sandbox_dataset_id,
                                      pid_table_id,
                                      hpo_id,
                                      dataset_ids,
                                      retraction_type,
                                      skip_sandboxing=True,
                                      bq_client=bq_client)
    logging.info(f"Completed retraction on BQ datasets")

    # Run cleaning rules
    for dataset_id in dataset_ids:
        if 'sandbox' in dataset_id.lower():
            continue

        logging.info(f"Running CRs for {dataset_id}...")
        cleaning_args = [
            '-p', project_id, '-d', dataset_id, '-b', sandbox_dataset_id,
            '--data_stage', CRON_RETRACTION, '--run_as',
            f'{project_id}@appspot.gserviceaccount.com'
        ]
        all_cleaning_args = add_kwargs_to_args(cleaning_args, None)
        clean_cdr.main(args=all_cleaning_args)
        logging.info(f"Completed running CRs for {dataset_id}...")

    # retract from gcs
    if retraction_type == 'bucket':
        folder = bq_utils.get_retraction_submission_folder()
        logging.info(
            f"Submission folder/s to target from env variable: {folder}")
        logging.info(f"Running retraction from internal bucket folders")
        retract_data_gcs.run_gcs_retraction(project_id,
                                            sandbox_dataset_id,
                                            pid_table_id,
                                            hpo_id,
                                            folder,
                                            force_flag=True)
        logging.info(f"Completed retraction from internal bucket folders")
    return 'retraction-complete'


@api_util.auth_required_cron
@log_traceback
def validate_pii():
    logging.info(f"Running participant validation on all sites")
    project = bq_utils.app_identity.get_application_id()
    bq_client = BigQueryClient(project)
    for item in bq_utils.get_hpo_info():
        hpo_id = item['hpo_id']
        # Prevent updating udfs for all hpo_sites
        setup_and_validate_participants(bq_client, hpo_id, update_udf=False)

    return consts.VALIDATION_SUCCESS


@api_util.auth_required_cron
@log_traceback
def ps_api_cron():
    project = bq_utils.app_identity.get_application_id()
    bq_client = BigQueryClient(project)
    rdr_project_id = bq_utils.get_rdr_project_id()
    drc_dataset_id = common.DRC_OPS
    logging.info(f"Fetching Participant Summary API data")
    fetch_and_store_full_ps_data(bq_client, project, rdr_project_id,
                                 drc_dataset_id)

    return consts.PS_API_SUCCESS


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

app.add_url_rule(consts.PREFIX + consts.PARTICIPANT_VALIDATION +
                 consts.VALIDATE,
                 endpoint='validate_pii',
                 view_func=validate_pii,
                 methods=['GET'])

app.add_url_rule(consts.PREFIX + 'RetractPids',
                 endpoint='run_retraction_cron',
                 view_func=run_retraction_cron,
                 methods=['GET'])

app.add_url_rule(consts.PREFIX + consts.PARTICIPANT_VALIDATION +
                 consts.FETCH_PS_DATA,
                 endpoint='ps_api_cron',
                 view_func=ps_api_cron,
                 methods=['GET'])

app.before_request(
    begin_request_logging)  # Must be first before_request() call.

app.teardown_request(
    end_request_logging
)  # teardown_request to be called regardless if there is an exception thrown
