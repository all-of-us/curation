# Python imports
import inspect
import os
from typing import Optional

# Third party imports
import googleapiclient.errors
from google.cloud.exceptions import GoogleCloudError
from google.api_core.exceptions import ServiceUnavailable
import requests

# Project imports
import bq_utils
import common
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule
from constants.utils.bq import HPO_ID_BUCKET_NAME_TABLE_ID
from constants.validation import main
import resources

RESOURCES_BUCKET_FMT = '{project_id}-resources'

FAKE_HPO_ID = 'fake'
PITT_HPO_ID = 'pitt'
NYC_HPO_ID = 'nyc'
FAKE_BUCKET_NAME = os.environ.get('BUCKET_NAME_FAKE')
PITT_BUCKET_NAME = os.environ.get('BUCKET_NAME_PITT')
NYC_BUCKET_NAME = os.environ.get('BUCKET_NAME_NYC')
GAE_SERVICE = os.environ.get('GAE_SERVICE', 'default')

LOOKUP_TABLES = [HPO_ID_BUCKET_NAME_TABLE_ID]

VALIDATE_HPO_FILES_URL = f'{main.PREFIX}ValidateHpoFiles/{FAKE_HPO_ID}'
COPY_HPO_FILES_URL = f'{main.PREFIX}CopyFiles/{FAKE_HPO_ID}'
BASE_TESTS_PATH = os.path.dirname(
    os.path.abspath(inspect.getfile(inspect.currentframe())))
TEST_DATA_PATH = os.path.join(BASE_TESTS_PATH, 'test_data')

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
FIVE_PERSONS_PII_NAME_CSV = os.path.join(FIVE_PERSONS_PATH, 'pii_name.csv')
FIVE_PERSONS_PARTICIPANT_MATCH_CSV = os.path.join(FIVE_PERSONS_PATH,
                                                  'participant_match.csv')
FIVE_PERSONS_NOTE_JSONL = os.path.join(FIVE_PERSONS_PATH, 'note.jsonl')
FIVE_PERSONS_FILES = [
    FIVE_PERSONS_PERSON_CSV, FIVE_PERSONS_VISIT_OCCURRENCE_CSV,
    FIVE_PERSONS_CONDITION_OCCURRENCE_CSV,
    FIVE_PERSONS_PROCEDURE_OCCURRENCE_CSV, FIVE_PERSONS_DRUG_EXPOSURE_CSV,
    FIVE_PERSONS_MEASUREMENT_CSV, FIVE_PERSONS_PII_NAME_CSV,
    FIVE_PERSONS_PARTICIPANT_MATCH_CSV, FIVE_PERSONS_NOTE_JSONL
]

FIVE_PERSONS_SUCCESS_RESULT_CSV = os.path.join(
    TEST_DATA_PATH, 'five_persons_success_result.csv')

# OMOP NYC and PITT test data from synpuf
NYC_FIVE_PERSONS_PATH = os.path.join(TEST_DATA_PATH, 'nyc_five_person')
PITT_FIVE_PERSONS_PATH = os.path.join(TEST_DATA_PATH, 'pitt_five_person')

PITT_FIVE_PERSONS_OBSERVATION_CSV = os.path.join(PITT_FIVE_PERSONS_PATH,
                                                 'observation.csv')

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
TEST_VOCABULARY_PATH = os.path.join(TEST_DATA_PATH, 'vocabulary')

TEST_DATA_METRICS_PATH = os.path.join(TEST_DATA_PATH, 'metrics')
TEST_NYC_CU_COLS_CSV = os.path.join(TEST_DATA_METRICS_PATH, 'nyc_cu_cols.csv')


def delete_all_tables(client, dataset_id):
    """
    Remove all non-vocabulary and non-lookup tables from a dataset

    :param client: a BigQueryClient
    :param dataset_id: ID of the dataset with the tables to delete
    :return: list of deleted tables
    """

    deleted = []
    table_infos = client.list_tables(dataset_id)
    table_ids = [table.table_id for table in table_infos]
    for table_id in table_ids:
        if table_id not in common.VOCABULARY_TABLES + LOOKUP_TABLES:
            client.delete_table(f'{dataset_id}.{table_id}')
            deleted.append(table_id)
    return deleted


def populate_achilles(hpo_id=FAKE_HPO_ID, include_heel=True):
    from validation import achilles, achilles_heel
    import app_identity

    app_id = app_identity.get_application_id()
    test_resources_bucket = RESOURCES_BUCKET_FMT.format(project_id=app_id)
    table_names = [
        achilles.ACHILLES_ANALYSIS, achilles.ACHILLES_RESULTS,
        achilles.ACHILLES_RESULTS_DIST
    ]
    if include_heel:
        table_names.append(achilles_heel.ACHILLES_HEEL_RESULTS)

    running_jobs = []
    for table_name in table_names:
        gcs_path = f'gs://{test_resources_bucket}/{table_name}.csv'
        dataset_id = common.BIGQUERY_DATASET_ID
        table_id = resources.get_table_id(table_name, hpo_id=hpo_id)
        load_results = bq_utils.load_csv(table_name, gcs_path, app_id,
                                         dataset_id, table_id)
        running_jobs.append(load_results['jobReference']['jobId'])
    bq_utils.wait_on_jobs(running_jobs)


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


class FakeHTTPResponse(requests.Response):

    def __init__(self,
                 url: str = 'https://127.0.0.1',
                 status_code: int = 200,
                 reason: str = 'OK',
                 content: bytes = b'OK',
                 **kwargs):
        """
        Build yerself a fake response

        :param url: final url of request, if ye want.
        :param status_code: response code
        :param reason: response code reason value
        :param content: response content bytes
        :param **kwargs: anything else you wanna set.
        """
        # init to set up default values
        super().__init__()

        # set a few specific values
        self.url = url
        self.status_code = status_code
        self.reason = reason
        self._content = content

        # loop through any / all others and set them.
        for k, v in kwargs.items():
            if k == 'reason' or k == 'content' or k == 'status_code' or k == 'url':
                continue
            else:
                self.k = v


def mock_google_http_error(status_code: int = 418,
                           content: bytes = b'418: I\'m a teapot',
                           uri: Optional[str] = None,
                           **resp_kwargs) -> googleapiclient.errors.HttpError:
    """
    Creates a mock google api client http error, complete with mock'd http response

    :param status_code: Code to set in mock response
    :param content: Content, as bytes, of mock response
    :param uri: (Optional) URI of mock request
    :param resp_kwargs: Other fields to apply to
    """
    return googleapiclient.errors.HttpError(FakeHTTPResponse(
        status_code=status_code, content=content, uri=uri, **resp_kwargs),
                                            content=content,
                                            uri=uri)


def setup_hpo_id_bucket_name_table(client, dataset_id):
    """
    Sets up `hpo_id_bucket_name` table that `get_hpo_bucket()` looks up.
    Drops the table if exist first, and create it with test lookup data.

    :param client: a BigQueryClient
    :param dataset_id: dataset id where the lookup table is created
    """

    drop_hpo_id_bucket_name_table(client, dataset_id)

    CREATE_LOOKUP_TABLE = common.JINJA_ENV.from_string("""
        CREATE TABLE `{{project_id}}.{{lookup_dataset_id}}.{{hpo_id_bucket_table_id}}`
        (hpo_id STRING, bucket_name STRING, service STRING)
        """)

    create_lookup_table = CREATE_LOOKUP_TABLE.render(
        project_id=client.project,
        lookup_dataset_id=dataset_id,
        hpo_id_bucket_table_id=HPO_ID_BUCKET_NAME_TABLE_ID)

    job = client.query(create_lookup_table)
    job.result()

    INSERT_LOOKUP_TABLE = common.JINJA_ENV.from_string("""
        INSERT INTO `{{project_id}}.{{lookup_dataset_id}}.{{hpo_id_bucket_table_id}}` 
        (hpo_id, bucket_name, service) VALUES 
        ('{{hpo_id_nyc}}', '{{bucket_name_nyc}}', '{{service_name}}'),
        ('{{hpo_id_pitt}}', '{{bucket_name_pitt}}', '{{service_name}}'),
        ('{{hpo_id_fake}}', '{{bucket_name_fake}}', '{{service_name}}')
        """)

    insert_lookup_table = INSERT_LOOKUP_TABLE.render(
        project_id=client.project,
        lookup_dataset_id=dataset_id,
        hpo_id_bucket_table_id=HPO_ID_BUCKET_NAME_TABLE_ID,
        hpo_id_nyc=NYC_HPO_ID,
        bucket_name_nyc=NYC_BUCKET_NAME,
        hpo_id_pitt=PITT_HPO_ID,
        bucket_name_pitt=PITT_BUCKET_NAME,
        hpo_id_fake=FAKE_HPO_ID,
        bucket_name_fake=FAKE_BUCKET_NAME,
        service_name=GAE_SERVICE)

    job = client.query(insert_lookup_table)
    job.result()


def drop_hpo_id_bucket_name_table(client, dataset_id):
    """
    Drops `hpo_id_bucket_name` table that `get_hpo_bucket()` looks up.
    
    :param client: a BigQueryClient
    :param dataset_id: dataset id where the lookup table is located
    """

    DROP_LOOKUP_TABLE = common.JINJA_ENV.from_string("""
        DROP TABLE IF EXISTS `{{project_id}}.{{lookup_dataset_id}}.{{hpo_id_bucket_table_id}}` 
        """)

    drop_lookup_table = DROP_LOOKUP_TABLE.render(
        project_id=client.project,
        lookup_dataset_id=dataset_id,
        hpo_id_bucket_table_id=HPO_ID_BUCKET_NAME_TABLE_ID)

    job = client.query(drop_lookup_table)
    job.result()


def mock_google_cloud_error(content: bytes = b'418: I\'m a teapot'):
    return GoogleCloudError(message=content.decode())


def mock_google_service_unavailable_error(
        content: bytes = b'418: I\'m a teapot'):
    return ServiceUnavailable(message=content.decode())
