import inspect
import os
from io import open

import requests

import bq_utils
import common
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule
from constants.validation import main
import gcs_utils
import resources

FAKE_HPO_ID = 'fake'
VALIDATE_HPO_FILES_URL = main.PREFIX + 'ValidateHpoFiles/' + FAKE_HPO_ID
COPY_HPO_FILES_URL = main.PREFIX + 'CopyFiles/' + FAKE_HPO_ID
BASE_TESTS_PATH = os.path.dirname(
    os.path.abspath(inspect.getfile(inspect.currentframe())))
TEST_DATA_PATH = os.path.join(BASE_TESTS_PATH, 'test_data')
EMPTY_VALIDATION_RESULT = os.path.join(TEST_DATA_PATH,
                                       'empty_validation_result.csv')
ALL_FILES_UNPARSEABLE_VALIDATION_RESULT = os.path.join(
    TEST_DATA_PATH, 'all_files_unparseable_validation_result.csv')
ALL_FILES_UNPARSEABLE_VALIDATION_RESULT_NO_HPO_JSON = os.path.join(
    TEST_DATA_PATH, 'all_files_unparseable_validation_result_no_hpo.json')
EMPTY_ERROR_CSV = os.path.join(TEST_DATA_PATH, 'empty_error.csv')

# Test files for five person sample
FIVE_PERSONS_PATH = os.path.join(TEST_DATA_PATH, 'five_persons')
FIVE_PERSONS_PERSON_CSV = os.path.join(FIVE_PERSONS_PATH, 'person.csv')
FIVE_PERSONS_VISIT_OCCURRENCE_CSV = os.path.join(FIVE_PERSONS_PATH,
                                                 'visit_occurrence.csv')
FIVE_PERSONS_CONDITION_OCCURRENCE_CSV = os.path.join(
    FIVE_PERSONS_PATH, 'condition_occurrence.csv')
FIVE_PERSONS_PROCEDURE_OCCURRENCE_CSV = os.path.join(
    FIVE_PERSONS_PATH, 'procedure_occurrence.csv')
FIVE_PERSONS_DRUG_EXPOSURE_CSV = os.path.join(FIVE_PERSONS_PATH,
                                              'drug_exposure.csv')
FIVE_PERSONS_MEASUREMENT_CSV = os.path.join(FIVE_PERSONS_PATH,
                                            'measurement.csv')
FIVE_PERSON_FACT_RELATIONSHIP_CSV = os.path.join(FIVE_PERSONS_PATH,
                                                 'fact_relationship.csv')
FIVE_PERSONS_PII_NAME_CSV = os.path.join(FIVE_PERSONS_PATH, 'pii_name.csv')
FIVE_PERSONS_PARTICIPANT_MATCH_CSV = os.path.join(FIVE_PERSONS_PATH,
                                                  'participant_match.csv')
FIVE_PERSONS_FILES = [
    FIVE_PERSONS_PERSON_CSV, FIVE_PERSONS_VISIT_OCCURRENCE_CSV,
    FIVE_PERSONS_CONDITION_OCCURRENCE_CSV,
    FIVE_PERSONS_PROCEDURE_OCCURRENCE_CSV, FIVE_PERSONS_DRUG_EXPOSURE_CSV,
    FIVE_PERSONS_MEASUREMENT_CSV, FIVE_PERSONS_PII_NAME_CSV,
    FIVE_PERSONS_PARTICIPANT_MATCH_CSV
]

FIVE_PERSONS_SUCCESS_RESULT_CSV = os.path.join(
    TEST_DATA_PATH, 'five_persons_success_result.csv')
FIVE_PERSONS_SUCCESS_RESULT_NO_HPO_JSON = os.path.join(
    TEST_DATA_PATH, 'five_persons_success_result_no_hpo.json')

# OMOP NYC and PITT test data from synpuf
NYC_FIVE_PERSONS_PATH = os.path.join(TEST_DATA_PATH, 'nyc_five_person')
PITT_FIVE_PERSONS_PATH = os.path.join(TEST_DATA_PATH, 'pitt_five_person')

NYC_FIVE_PERSONS_PERSON_CSV = os.path.join(NYC_FIVE_PERSONS_PATH, 'person.csv')
NYC_FIVE_PERSONS_VISIT_OCCURRENCE_CSV = os.path.join(NYC_FIVE_PERSONS_PATH,
                                                     'visit_occurrence.csv')
NYC_FIVE_PERSONS_CONDITION_OCCURRENCE_CSV = os.path.join(
    NYC_FIVE_PERSONS_PATH, 'condition_occurrence.csv')
NYC_FIVE_PERSONS_PROCEDURE_OCCURRENCE_CSV = os.path.join(
    NYC_FIVE_PERSONS_PATH, 'procedure_occurrence.csv')
NYC_FIVE_PERSONS_DRUG_EXPOSURE_CSV = os.path.join(NYC_FIVE_PERSONS_PATH,
                                                  'drug_exposure.csv')
NYC_FIVE_PERSONS_MEASUREMENT_CSV = os.path.join(NYC_FIVE_PERSONS_PATH,
                                                'measurement.csv')
NYC_FIVE_PERSONS_OBSERVATION_CSV = os.path.join(NYC_FIVE_PERSONS_PATH,
                                                'observation.csv')
NYC_FIVE_PERSONS_FILES = [
    NYC_FIVE_PERSONS_PERSON_CSV, NYC_FIVE_PERSONS_VISIT_OCCURRENCE_CSV,
    NYC_FIVE_PERSONS_CONDITION_OCCURRENCE_CSV,
    NYC_FIVE_PERSONS_PROCEDURE_OCCURRENCE_CSV,
    NYC_FIVE_PERSONS_DRUG_EXPOSURE_CSV, NYC_FIVE_PERSONS_MEASUREMENT_CSV,
    NYC_FIVE_PERSONS_OBSERVATION_CSV
]

PITT_FIVE_PERSONS_PERSON_CSV = os.path.join(PITT_FIVE_PERSONS_PATH,
                                            'person.csv')
PITT_FIVE_PERSONS_VISIT_OCCURRENCE_CSV = os.path.join(PITT_FIVE_PERSONS_PATH,
                                                      'visit_occurrence.csv')
PITT_FIVE_PERSONS_CONDITION_OCCURRENCE_CSV = os.path.join(
    PITT_FIVE_PERSONS_PATH, 'condition_occurrence.csv')
PITT_FIVE_PERSONS_PROCEDURE_OCCURRENCE_CSV = os.path.join(
    PITT_FIVE_PERSONS_PATH, 'procedure_occurrence.csv')
PITT_FIVE_PERSONS_DRUG_EXPOSURE_CSV = os.path.join(PITT_FIVE_PERSONS_PATH,
                                                   'drug_exposure.csv')
PITT_FIVE_PERSONS_MEASUREMENT_CSV = os.path.join(PITT_FIVE_PERSONS_PATH,
                                                 'measurement.csv')
PITT_FIVE_PERSONS_OBSERVATION_CSV = os.path.join(PITT_FIVE_PERSONS_PATH,
                                                 'observation.csv')
PITT_FIVE_PERSONS_FILES = [
    PITT_FIVE_PERSONS_PERSON_CSV, PITT_FIVE_PERSONS_VISIT_OCCURRENCE_CSV,
    PITT_FIVE_PERSONS_CONDITION_OCCURRENCE_CSV,
    PITT_FIVE_PERSONS_PROCEDURE_OCCURRENCE_CSV,
    PITT_FIVE_PERSONS_DRUG_EXPOSURE_CSV, PITT_FIVE_PERSONS_MEASUREMENT_CSV,
    PITT_FIVE_PERSONS_OBSERVATION_CSV
]

RDR_PATH = os.path.join(TEST_DATA_PATH, 'rdr')
RDR_PERSON_PATH = os.path.join(RDR_PATH, 'person.csv')

TEST_DATA_EXPORT_PATH = os.path.join(TEST_DATA_PATH, 'export')
TEST_DATA_EXPORT_SYNPUF_PATH = os.path.join(TEST_DATA_EXPORT_PATH, 'synpuf')
DESCRIPTION = 'description'

PII_NAME_FILE = os.path.join(TEST_DATA_PATH, 'pii_name.csv')
PII_MRN_BAD_PERSON_ID_FILE = os.path.join(TEST_DATA_PATH, 'pii_mrn.csv')
PII_FILE_LOAD_RESULT_CSV = os.path.join(TEST_DATA_PATH,
                                        'pii_file_load_result.csv')

# Removed from repo, generate if required by running the test:
# integration_tests.data_steward.validation.main_test.test_html_report_five_person and place it in the path below
# TODO update html file with results.html generated from synthetic data if needed
FIVE_PERSON_RESULTS_FILE = os.path.join(TEST_DATA_PATH,
                                        'five_person_results.html')
FIVE_PERSON_RESULTS_ACHILLES_ERROR_FILE = os.path.join(
    TEST_DATA_PATH, 'five_person_results_achilles_error.html')

TEST_VOCABULARY_PATH = os.path.join(TEST_DATA_PATH, 'vocabulary')
TEST_VOCABULARY_CONCEPT_CSV = os.path.join(TEST_VOCABULARY_PATH, 'CONCEPT.csv')
TEST_VOCABULARY_VOCABULARY_CSV = os.path.join(TEST_VOCABULARY_PATH,
                                              'VOCABULARY.csv')

TEST_DATA_METRICS_PATH = os.path.join(TEST_DATA_PATH, 'metrics')
TEST_NYC_CU_COLS_CSV = os.path.join(TEST_DATA_METRICS_PATH, 'nyc_cu_cols.csv')
TEST_MEASUREMENT_CSV = os.path.join(TEST_DATA_METRICS_PATH, 'measurement.csv')


def _create_five_persons_success_result():
    """
    Generate the expected result payload for five_persons data set. For internal testing only.
    """
    import csv

    field_names = ['file_name', 'found', 'parsed', 'loaded']

    expected_result_items = []
    for cdm_file in resources.CDM_FILES:
        expected_item = dict(file_name=cdm_file,
                             found="1",
                             parsed="1",
                             loaded="1")
        expected_result_items.append(expected_item)
    with open(FIVE_PERSONS_SUCCESS_RESULT_CSV, 'w') as f:
        writer = csv.DictWriter(f, field_names, quoting=csv.QUOTE_ALL)
        writer.writeheader()
        writer.writerows(expected_result_items)


def _export_query_response_by_path(p, hpo_id):
    """Utility to create response test payloads"""

    from validation import export

    for f in export.list_files_only(p):
        abs_path = os.path.join(p, f)
        with open(abs_path, 'r') as fp:
            sql = fp.read()
            sql = export.render(sql,
                                hpo_id,
                                results_schema=bq_utils.get_dataset_id(),
                                vocab_schema='synpuf_100')
            query_result = bq_utils.query(sql)
            out_file = os.path.join(TEST_DATA_EXPORT_PATH,
                                    f.replace('.sql', '_response.json'))
            with open(out_file, 'w') as fp:
                data = dict()
                if 'rows' in query_result:
                    data['rows'] = query_result['rows']
                if 'schema' in query_result:
                    data['schema'] = query_result['schema']
                import json
                json.dump(data,
                          fp,
                          sort_keys=True,
                          indent=4,
                          separators=(',', ': '))


def _export_query_responses():
    from validation import export

    for d in ['datadensity', 'achillesheel', 'person']:
        p = os.path.join(export.EXPORT_PATH, d)
        _export_query_response_by_path(p, FAKE_HPO_ID)


def empty_bucket(bucket):
    bucket_items = gcs_utils.list_bucket(bucket)
    for bucket_item in bucket_items:
        gcs_utils.delete_object(bucket, bucket_item['name'])


def delete_all_tables(dataset_id):
    """
    Remove all non-vocabulary tables from a dataset

    :param dataset_id: ID of the dataset with the tables to delete
    :return: list of deleted tables
    """

    deleted = []
    table_infos = bq_utils.list_tables(dataset_id)
    table_ids = [table['tableReference']['tableId'] for table in table_infos]
    for table_id in table_ids:
        if table_id not in common.VOCABULARY_TABLES:
            bq_utils.delete_table(table_id, dataset_id)
            deleted.append(table_id)
    return deleted


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
            download_file_from_google_drive(
                file_id, os.path.join(TEST_DATA_EXPORT_SYNPUF_PATH, file_name))


def read_cloud_file(bucket, name):
    return gcs_utils.get_object(bucket, name)


def write_cloud_str(bucket, name, contents_str):
    from io import StringIO
    fp = StringIO(contents_str)
    return write_cloud_fp(bucket, name, fp)


def write_cloud_file(bucket, f, prefix=""):
    name = os.path.basename(f)
    with open(f, 'rb') as fp:
        return write_cloud_fp(bucket, prefix + name, fp)


def write_cloud_fp(bucket, name, fp):
    return gcs_utils.upload_object(bucket, name, fp)


def populate_achilles(hpo_bucket, hpo_id=FAKE_HPO_ID, include_heel=True):
    from validation import achilles, achilles_heel
    import app_identity

    app_id = app_identity.get_application_id()

    test_file_name = achilles.ACHILLES_ANALYSIS + '.csv'
    achilles_analysis_file_path = os.path.join(TEST_DATA_EXPORT_PATH,
                                               test_file_name)
    schema_name = achilles.ACHILLES_ANALYSIS
    write_cloud_file(hpo_bucket, achilles_analysis_file_path)
    gcs_path = 'gs://' + hpo_bucket + '/' + test_file_name
    dataset_id = bq_utils.get_dataset_id()
    table_id = bq_utils.get_table_id(hpo_id, achilles.ACHILLES_ANALYSIS)
    bq_utils.load_csv(schema_name, gcs_path, app_id, dataset_id, table_id)

    table_names = [achilles.ACHILLES_RESULTS, achilles.ACHILLES_RESULTS_DIST]
    if include_heel:
        table_names.append(achilles_heel.ACHILLES_HEEL_RESULTS)

    running_jobs = []
    for table_name in table_names:
        test_file_name = table_name + '.csv'
        test_file_path = os.path.join(TEST_DATA_EXPORT_SYNPUF_PATH,
                                      table_name + '.csv')
        write_cloud_file(hpo_bucket, test_file_path)
        gcs_path = 'gs://' + hpo_bucket + '/' + test_file_name
        dataset_id = bq_utils.get_dataset_id()
        table_id = bq_utils.get_table_id(hpo_id, table_name)
        load_results = bq_utils.load_csv(table_name, gcs_path, app_id,
                                         dataset_id, table_id)
        running_jobs.append(load_results['jobReference']['jobId'])
    bq_utils.wait_on_jobs(running_jobs)


def generate_rdr_files():
    """
    Generate test csv files based on a sample of synthetic RDR data
    :return:
    """
    d = 'rdr_dataset_2018_4_17'
    for table in resources.CDM_TABLES:
        q = 'SELECT * FROM fake_%s WHERE person_id IN (SELECT person_id FROM sample_person_id)' % table
        cmd = 'bq query --dataset_id={d} --format=csv "{q}" > %(table)s.csv'.format(
            d=d, q=q)
        os.system(cmd)


def bash(cmd):
    """
    Run a bash-specific command

    :param cmd: the command to run
    :return: 0 if successful
    :raises
      CalledProcessError: raised when command has a non-zero result

    Note: On Windows, bash and the gcloud SDK binaries (e.g. bq, gsutil) must be in PATH
    """
    import subprocess
    import platform

    bash_cmd = '/bin/bash'
    if platform.system().lower().startswith('windows'):
        # extensions are not inferred
        cmd = cmd.replace('bq ', 'bq.cmd ').replace('gsutil ', 'gsutil.cmd ')
        bash_cmd = 'bash'
    return subprocess.check_call([bash_cmd, '-c', cmd],
                                 stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE)


def command(cmd):
    return os.system(cmd)


def list_files_in(path):
    """
    List the abs paths to files (not dirs) in the supplied path
    :param path:
    :return:
    """
    return [
        os.path.join(path, f)
        for f in os.listdir(path)
        if os.path.isfile(os.path.join(path, f))
    ]


def get_table_summary(dataset_id):
    """
    Get summary of tables in a bq dataset
    :param dataset_id: identifies the dataset
    :return: list of dict with keys: project_id dataset_id table_id creation_time type
    """
    q = '''
        SELECT * FROM {dataset_id}.__TABLES_SUMMARY__
        '''.format(dataset_id=dataset_id)
    response = bq_utils.query(q)
    rows = bq_utils.response2rows(response)
    return rows


def table_count_query(dataset_id, table_id, where=''):
    return '''
      SELECT '{table_id}' AS table_id, COUNT(1) AS n
      FROM {dataset_id}.{table_id} t
      {where}
      '''.format(dataset_id=dataset_id, table_id=table_id, where=where)


def get_table_count_query(dataset_id, table_ids, where):
    queries = []
    for table_id in table_ids:
        if table_id == '_ehr_consent' or 'person_id' in resources.fields_for(
                table_id):
            queries.append(table_count_query(dataset_id, table_id, where))
        else:
            queries.append(table_count_query(dataset_id, table_id, where=''))
    return queries


def get_table_counts(dataset_id, table_ids=None, where=''):
    """
    Evaluate counts for tables in a dataset

    :param dataset_id: dataset with the tables
    :param table_ids: tables to include (all by default)
    :param where: an optional SQL where clause
    :return: a mapping of table_id => count
    """
    if table_ids is None:
        tables = get_table_summary(dataset_id)
        table_ids = set(t['table_id'] for t in tables)
    count_subqueries = get_table_count_query(dataset_id, table_ids, where)
    count_query = '\nUNION ALL\n'.join(count_subqueries)
    response = bq_utils.query(count_query)
    rows = bq_utils.response2rows(response)
    table_counts = dict()
    for row in rows:
        table_id = row['table_id']
        table_counts[table_id] = row['n']
    return table_counts


def normalize_field_payload(field):
    """
    Standardize schema field payload so it is easy to compare in tests
    :param field: a field from a table/query's schema
    :return: the normalized field
    """
    result = field.copy()
    values_to_lower = ['type', 'mode']
    for key in result.keys():
        value = result[key]
        if key in values_to_lower:
            result[key] = value.lower()
    if DESCRIPTION not in field:
        result[DESCRIPTION] = ''
    return result


class FakeRuleClass(BaseCleaningRule):

    def __init__(self, project_id, dataset_id, sandbox_dataset_id):
        super().__init__(issue_numbers=[''],
                         description='',
                         affected_datasets=[],
                         affected_tables=[],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id)

    def get_sandbox_tablenames(self):
        pass

    def setup_rule(self, client, *args, **keyword_args):
        pass

    def setup_validation(self, client, *args, **keyword_args):
        pass

    def get_query_specs(self, *args, **keyword_args):
        pass

    def validate_rule(self, client, *args, **keyword_args):
        pass


def fake_rule_func(project_id, dataset_id, sandbox_dataset_id):
    pass
