import os

import common
import gcs_utils
import resources
from validation import main

FAKE_HPO_ID = 'fake'
VALIDATE_HPO_FILES_URL = main.PREFIX + 'ValidateHpoFiles/' + FAKE_HPO_ID
COPY_HPO_FILES_URL = main.PREFIX + 'CopyFiles/' + FAKE_HPO_ID
TEST_DATA_PATH = os.path.join(resources.base_path, 'test', 'test_data')
EMPTY_VALIDATION_RESULT = os.path.join(TEST_DATA_PATH, 'empty_validation_result.csv')
ALL_FILES_UNPARSEABLE_VALIDATION_RESULT = os.path.join(TEST_DATA_PATH, 'all_files_unparseable_validation_result.csv')
ALL_FILES_UNPARSEABLE_VALIDATION_RESULT_NO_HPO_JSON = os.path.join(TEST_DATA_PATH, 'all_files_unparseable_validation_result_no_hpo.json')
BAD_PERSON_FILE_BQ_LOAD_ERRORS_CSV = os.path.join(TEST_DATA_PATH, 'bq_errors_bad_person.csv')

# Test files for five person sample
FIVE_PERSONS_PATH = os.path.join(TEST_DATA_PATH, 'five_persons')
FIVE_PERSONS_PERSON_CSV = os.path.join(FIVE_PERSONS_PATH, 'person.csv')
FIVE_PERSONS_VISIT_OCCURRENCE_CSV = os.path.join(FIVE_PERSONS_PATH, 'visit_occurrence.csv')
FIVE_PERSONS_CONDITION_OCCURRENCE_CSV = os.path.join(FIVE_PERSONS_PATH, 'condition_occurrence.csv')
FIVE_PERSONS_PROCEDURE_OCCURRENCE_CSV = os.path.join(FIVE_PERSONS_PATH, 'procedure_occurrence.csv')
FIVE_PERSONS_DRUG_EXPOSURE_CSV = os.path.join(FIVE_PERSONS_PATH, 'drug_exposure.csv')
FIVE_PERSONS_MEASUREMENT_CSV = os.path.join(FIVE_PERSONS_PATH, 'measurement.csv')
FIVE_PERSONS_FILES = [FIVE_PERSONS_PERSON_CSV,
                      FIVE_PERSONS_VISIT_OCCURRENCE_CSV,
                      FIVE_PERSONS_CONDITION_OCCURRENCE_CSV,
                      FIVE_PERSONS_PROCEDURE_OCCURRENCE_CSV,
                      FIVE_PERSONS_DRUG_EXPOSURE_CSV,
                      FIVE_PERSONS_MEASUREMENT_CSV]

FIVE_PERSONS_SUCCESS_RESULT_CSV = os.path.join(TEST_DATA_PATH, 'five_persons_success_result.csv')
FIVE_PERSONS_SUCCESS_RESULT_NO_HPO_JSON = os.path.join(TEST_DATA_PATH, 'five_persons_success_result_no_hpo.json')

# OMOP NYC and PITT test data from synpuf
NYC_FIVE_PERSONS_PATH = os.path.join(TEST_DATA_PATH,'nyc_five_person')
PITT_FIVE_PERSONS_PATH = os.path.join(TEST_DATA_PATH,'pitt_five_person')

NYC_FIVE_PERSONS_PERSON_CSV = os.path.join(NYC_FIVE_PERSONS_PATH, 'person.csv')
NYC_FIVE_PERSONS_VISIT_OCCURRENCE_CSV = os.path.join(NYC_FIVE_PERSONS_PATH, 'visit_occurrence.csv')
NYC_FIVE_PERSONS_CONDITION_OCCURRENCE_CSV = os.path.join(NYC_FIVE_PERSONS_PATH, 'condition_occurrence.csv')
NYC_FIVE_PERSONS_PROCEDURE_OCCURRENCE_CSV = os.path.join(NYC_FIVE_PERSONS_PATH, 'procedure_occurrence.csv')
NYC_FIVE_PERSONS_DRUG_EXPOSURE_CSV = os.path.join(NYC_FIVE_PERSONS_PATH, 'drug_exposure.csv')
NYC_FIVE_PERSONS_MEASUREMENT_CSV = os.path.join(NYC_FIVE_PERSONS_PATH, 'measurement.csv')
NYC_FIVE_PERSONS_FILES = [
    NYC_FIVE_PERSONS_PERSON_CSV,
    NYC_FIVE_PERSONS_VISIT_OCCURRENCE_CSV,
    NYC_FIVE_PERSONS_CONDITION_OCCURRENCE_CSV,
    NYC_FIVE_PERSONS_PROCEDURE_OCCURRENCE_CSV,
    NYC_FIVE_PERSONS_DRUG_EXPOSURE_CSV,
    NYC_FIVE_PERSONS_MEASUREMENT_CSV]

PITT_FIVE_PERSONS_PERSON_CSV = os.path.join(PITT_FIVE_PERSONS_PATH, 'person.csv')
PITT_FIVE_PERSONS_VISIT_OCCURRENCE_CSV = os.path.join(PITT_FIVE_PERSONS_PATH, 'visit_occurrence.csv')
PITT_FIVE_PERSONS_CONDITION_OCCURRENCE_CSV = os.path.join(PITT_FIVE_PERSONS_PATH, 'condition_occurrence.csv')
PITT_FIVE_PERSONS_PROCEDURE_OCCURRENCE_CSV = os.path.join(PITT_FIVE_PERSONS_PATH, 'procedure_occurrence.csv')
PITT_FIVE_PERSONS_DRUG_EXPOSURE_CSV = os.path.join(PITT_FIVE_PERSONS_PATH, 'drug_exposure.csv')
PITT_FIVE_PERSONS_MEASUREMENT_CSV = os.path.join(PITT_FIVE_PERSONS_PATH, 'measurement.csv')
PITT_FIVE_PERSONS_FILES = [
    PITT_FIVE_PERSONS_PERSON_CSV,
    PITT_FIVE_PERSONS_VISIT_OCCURRENCE_CSV,
    PITT_FIVE_PERSONS_CONDITION_OCCURRENCE_CSV,
    PITT_FIVE_PERSONS_PROCEDURE_OCCURRENCE_CSV,
    PITT_FIVE_PERSONS_DRUG_EXPOSURE_CSV,
    PITT_FIVE_PERSONS_MEASUREMENT_CSV]

TEST_DATA_EXPORT_PATH = os.path.join(TEST_DATA_PATH, 'export')
TEST_DATA_EXPORT_SYNPUF_PATH = os.path.join(TEST_DATA_EXPORT_PATH, 'synpuf')


def _create_five_persons_success_result():
    """
    Generate the expected result payload for five_persons data set. For internal testing only.
    """
    import csv

    field_names = ['cdm_file_name', 'found', 'parsed', 'loaded']

    expected_result_items = []
    for cdm_file in common.CDM_FILES:
        expected_item = dict(cdm_file_name=cdm_file, found="1", parsed="1", loaded="1")
        expected_result_items.append(expected_item)
    with open(FIVE_PERSONS_SUCCESS_RESULT_CSV, 'w') as f:
        writer = csv.DictWriter(f, field_names, quoting=csv.QUOTE_ALL)
        writer.writeheader()
        writer.writerows(expected_result_items)


def _export_query_response_by_path(p, hpo_id):
    """Utility to create response test payloads"""

    from validation import export
    import bq_utils

    for f in export.list_files_only(p):
        abs_path = os.path.join(p, f)
        with open(abs_path, 'r') as fp:
            sql = fp.read()
            sql = export.render(sql, hpo_id, results_schema=bq_utils.get_dataset_id(), vocab_schema='synpuf_100')
            query_result = bq_utils.query(sql)
            out_file = os.path.join(TEST_DATA_EXPORT_PATH, f.replace('.sql', '_response.json'))
            with open(out_file, 'w') as fp:
                data = dict()
                if 'rows' in query_result:
                    data['rows'] = query_result['rows']
                if 'schema' in query_result:
                    data['schema'] = query_result['schema']
                import json
                json.dump(data, fp, sort_keys=True, indent=4, separators=(',', ': '))


def _export_query_responses():
    from validation import export

    for d in ['datadensity', 'achillesheel', 'person']:
        p = os.path.join(export.EXPORT_PATH, d)
        _export_query_response_by_path(p, FAKE_HPO_ID)


def empty_bucket(bucket):
    bucket_items = gcs_utils.list_bucket(bucket)
    for bucket_item in bucket_items:
        gcs_utils.delete_object(bucket, bucket_item['name'])

import requests


def download_file_from_google_drive(id, destination):
    URL = "https://docs.google.com/uc?export=download"

    session = requests.Session()

    response = session.get(URL, params={'id': id}, stream=True)
    token = get_confirm_token(response)

    if token:
        params = {'id': id, 'confirm': token}
        response = session.get(URL, params=params, stream=True)

    save_response_content(response, destination)


def get_confirm_token(response):
    for key, value in response.cookies.items():
        if key.startswith('download_warning'):
            return value

    return None


def save_response_content(response, destination):
    CHUNK_SIZE = 32768

    with open(destination, "wb") as f:
        for chunk in response.iter_content(CHUNK_SIZE):
            if chunk:  # filter out keep-alive new chunks
                f.write(chunk)


def get_synpuf_results_files():
    files = [('0B8QSHCLE8g4JV1Q4UHFRLXNhM2c', 'achilles_results.csv'),
             ('0B8QSHCLE8g4JeUUxZEh0SS1YNlk', 'achilles_results_dist.csv'),
             ('0B8QSHCLE8g4JQUE1dGJLd1RpWEk', 'achilles_heel_results.csv')]
    for file_id, file_name in files:
        dest_path = os.path.join(TEST_DATA_EXPORT_SYNPUF_PATH, file_name)
        if not os.path.exists(dest_path):
            download_file_from_google_drive(file_id, os.path.join(TEST_DATA_EXPORT_SYNPUF_PATH, file_name))


def read_cloud_file(bucket, name):
    return gcs_utils.get_object(bucket, name)


def write_cloud_str(bucket, name, contents_str):
    import StringIO
    fp = StringIO.StringIO(contents_str)
    return write_cloud_fp(bucket, name, fp)


def write_cloud_file(bucket, f, prefix = ""):
    name = os.path.basename(f)
    with open(f, 'r') as fp:
        return write_cloud_fp(bucket, prefix + name, fp)


def write_cloud_fp(bucket, name, fp):
    return gcs_utils.upload_object(bucket, name, fp)


def populate_achilles(hpo_bucket, include_heel=True):
    from validation import achilles, achilles_heel
    from google.appengine.api import app_identity
    import bq_utils

    app_id = app_identity.get_application_id()

    test_file_name = achilles.ACHILLES_ANALYSIS + '.csv'
    achilles_analysis_file_path = os.path.join(TEST_DATA_EXPORT_PATH, test_file_name)
    schema_path = os.path.join(resources.fields_path, achilles.ACHILLES_ANALYSIS + '.json')
    write_cloud_file(hpo_bucket, achilles_analysis_file_path)
    gcs_path = 'gs://' + hpo_bucket + '/' + test_file_name
    dataset_id = bq_utils.get_dataset_id()
    table_id = bq_utils.get_table_id(FAKE_HPO_ID, achilles.ACHILLES_ANALYSIS)
    bq_utils.load_csv(schema_path, gcs_path, app_id, dataset_id, table_id)

    table_names = [achilles.ACHILLES_RESULTS, achilles.ACHILLES_RESULTS_DIST]
    if include_heel:
        table_names.append(achilles_heel.ACHILLES_HEEL_RESULTS)

    running_jobs = []
    for table_name in table_names:
        schema_file_name = table_name + '.json'
        schema_path = os.path.join(resources.fields_path, schema_file_name)
        test_file_name = table_name + '.csv'
        test_file_path = os.path.join(TEST_DATA_EXPORT_SYNPUF_PATH, table_name + '.csv')
        write_cloud_file(hpo_bucket, test_file_path)
        gcs_path = 'gs://' + hpo_bucket + '/' + test_file_name
        dataset_id = bq_utils.get_dataset_id()
        table_id = bq_utils.get_table_id(FAKE_HPO_ID, table_name)
        load_results = bq_utils.load_csv(schema_path, gcs_path, app_id, dataset_id, table_id)
        running_jobs.append(load_results['jobReference']['jobId'])
    bq_utils.wait_on_jobs(running_jobs)
