#!/usr/bin/env python
import logging
import os

import cloudstorage
from cloudstorage import cloudstorage_api
from flask import Flask
from google.appengine.api import app_identity

import api_util
import common

# from google.cloud import bigquery

# from oauth2client.client import GoogleCredentials
# credentials=GoogleCredentials.get_application_default()
# from googleapiclient.discovery import build

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
    bucket_name = hpo_gcs_path(hpo_id)
    found_cdm_files = _find_cdm_files(bucket_name)
    result = [
        (cdm_file, 1 if cdm_file in map(lambda f: f.filename, found_cdm_files) else 0) for cdm_file in common.CDM_FILES
    ]
    _save_result_in_gcs('/%s/result.csv' % bucket_name, result)


    # creating a dataset
    '''  dataset_resource_body = { "kind": "bigquery#dataset", "etag": etag, "id": string, "selfLink": string,
            "datasetReference": { "datasetId": string, "projectId": string },
                        "friendlyName": string, "description": string, "defaultTableExpirationMs": long,
                        "labels": { (key): string },
                        "access": [ {
                            "role": string, "userByEmail": string, "groupByEmail": string, "domain": string, "specialGroup": string,
                                "view": { "projectId": string, "datasetId": string, "tableId": string }
                                } ],
                        "creationTime": long, "lastModifiedTime": long, "location": string }
    '''

    resource_body = {"kind": "bigquery#dataset",
                     "datasetReference": {"datasetId": "test_create"}
                     }

    # response = list(bigquery_client.list_datasets())


    # bigquery=build('bigquery', 'v2', credentials=credentials)


    # logging.info('CREATED BIGQUERY SERVICE')
    # response = \
    #        bigquery.datasets().insert(projectId=PROJECTID,body=resource_body).execute()

    # response=bigquery.datasets().list(projectId=PROJECTID).execute()

    # self.response.out.write('<h3>Datasets.list raw response after creating\
    #        test_create:</h3>')
    # self.response.out.write('<pre>%s</pre>' % json.dumps(response, sort_keys=True, indent=4, separators=(',', ': ')))

    ## deleting the dataset
    # bigquery.datasets().delete(projectId=PROJECTID,datasetId=resource_body['datasetReference']['datasetId']).execute()

    # response = bigquery.datasets().list(projectId=PROJECTID).execute()
    # self.response.out.write('<h3>Datasets.list raw response after deleting\
    #        test_create:</h3>')
    # self.response.out.write('<pre>%s</pre>' % json.dumps(response, sort_keys=True, indent=4, separators=(',', ': ')))
    return '{"report-generator-status": "started"}'


def hpo_gcs_path(hpo_id):
    # TODO determine how to map bucket
    return os.environ.get('BUCKET_NAME', app_identity.get_default_gcs_bucket_name())


def _find_cdm_files(cloud_bucket_name):
    """
    Returns list of GCSFileStat of CDM files found in the bucket
    :param cloud_bucket_name:
    :return:
    """
    bucket_stat_list = list(cloudstorage_api.listbucket('/' + cloud_bucket_name))
    # GCS does not really have the concept of directories (it's just a filename convention), so all
    # directory listings are recursive and we must filter out subdirectory contents.
    bucket_stat_list = [
        s for s in bucket_stat_list
        if s.filename.lower() in map(lambda t: '/' + cloud_bucket_name + '/%s' % t, common.CDM_FILES)]
    return bucket_stat_list


def _save_result_in_gcs(gcs_path, cdm_file_results):
    """
    Save the validation results in specified path
    :param gcs_path: full GCS path
    :param cdm_file_results: list of tuples (<cdm_file_name>, <found>)
    :return:
    """
    with cloudstorage.open(gcs_path, 'w', content_type='text/plain') as f:
        f.write('"cdm_file_name","found"\n')
        for (cdm_file_name, found) in cdm_file_results:
            line = '"%(cdm_file_name)s","%(found)s"\n' % locals()
            f.write(line)


def read_file(self, filename):
    self.response.write(
        'Abbreviated file content (first line and last 1K):\n')

    with cloudstorage.open(filename) as cloudstorage_file:
        self.response.write(cloudstorage_file.readline())
        cloudstorage_file.seek(-1024, os.SEEK_END)
        self.response.write(cloudstorage_file.read())


def stat_file(self, filename):
    self.response.write('File stat:\n')

    stat = cloudstorage.stat(filename)
    self.response.write(repr(stat))


def create_files_for_list_bucket(self, bucket):
    self.response.write('Creating more files for listbucket...\n')
    filenames = [bucket + n for n in [
        '/foo1', '/foo2', '/bar', '/bar/1', '/bar/2', '/boo/']]
    for f in filenames:
        self.create_file(f)


def list_bucket_directory_mode(self, bucket):
    self.response.write('Listbucket directory mode result:\n')
    for stat in cloudstorage.listbucket(bucket + '/b', delimiter='/'):
        self.response.write(stat)
        self.response.write('\n')
        if stat.is_dir:
            for subdir_file in cloudstorage.listbucket(
                    stat.filename, delimiter='/'):
                self.response.write('  {}'.format(subdir_file))
                self.response.write('\n')


def delete_files(self):
    self.response.write('Deleting files...\n')
    for filename in self.tmp_filenames_to_clean_up:
        self.response.write('Deleting file {}\n'.format(filename))
        try:
            cloudstorage.delete(filename)
        except cloudstorage.NotFoundError:
            pass


app.add_url_rule(
    PREFIX + 'ValidateHpoFiles/<string:hpo_id>',
    endpoint='validate_hpo_files',
    view_func=validate_hpo_files,
    methods=['GET'])
