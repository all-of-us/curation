#!/usr/bin/env python

# author: Aahlad
# date: 10 July 2017

"""
    Running appengine with storage and bigquery libraries.
    storage doesn't require a credential set.
    bigQuery goes through the google-api-client.
"""

# [START imports]
from __future__ import absolute_import

import os
import uuid
import sys
import json
import logging

from flask import Flask

from report import api_util

import cloudstorage
from google.appengine.api import app_identity
# from google.cloud import bigquery

# from oauth2client.client import GoogleCredentials
# credentials=GoogleCredentials.get_application_default()
# from googleapiclient.discovery import build

# auth purposes
import webapp2

# [END imports]
# setting the environment variable and extra imports

dev_flag = True 
source_filename = "tester.csv"
PROJECTID = 'bamboo-creek-172917'
real_bucket_name = "tester-for-cloudstorage"
real_bucket_name = "share-test-1729"
gcs_path_prepend = os.path.dirname(os.path.realpath(__file__))

if os.getenv('SERVER_SOFTWARE', '').startswith('Google App Engine/'):
    # print "setting environment"
    dev_flag = False
    gcs_path_prepend = "gs://"


@api_util.auth_required_cron
def run_report():
    # do something
    # and then redirect
    # self.redirect("/report.html")
    bucket_name = os.environ.get(
        'BUCKET_NAME', app_identity.get_default_gcs_bucket_name())


    if real_bucket_name is not None:
        bucket_name = real_bucket_name

    self.response.headers['Content-Type'] = 'text/plain'
    self.response.write(
        'Demo GCS Application running from Version: {}\n'.format(
            os.environ['CURRENT_VERSION_ID']))
    self.response.write('Using bucket name: \n\n'.format(bucket_name))

    logging.critical(os.system('bq mk os-test'))

    bucket = '/' + bucket_name
    filename = bucket + '/tester.csv'
    self.tmp_filenames_to_clean_up = []

    logging.info('CREATED GETS IN SERVICE')

    # self.create_file(filename)
    self.response.write("\n\n")

    self.list_bucket(bucket)
    self.response.write("\n\n")
    self.response.write('\n\n')

    #self.stat_file(filename)
    #self.response.write("\n\n")
    #self.response.write('\n\n')

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

    resource_body = { "kind": "bigquery#dataset",
            "datasetReference": { "datasetId": "test_create"}
            }

    # response = list(bigquery_client.list_datasets())


    #bigquery=build('bigquery', 'v2', credentials=credentials)


    #logging.info('CREATED BIGQUERY SERVICE')
    #response = \
    #        bigquery.datasets().insert(projectId=PROJECTID,body=resource_body).execute()

    #response=bigquery.datasets().list(projectId=PROJECTID).execute()

    #self.response.out.write('<h3>Datasets.list raw response after creating\
    #        test_create:</h3>')
    #self.response.out.write('<pre>%s</pre>' % json.dumps(response, sort_keys=True, indent=4, separators=(',', ': ')))

    ## deleting the dataset
    #bigquery.datasets().delete(projectId=PROJECTID,datasetId=resource_body['datasetReference']['datasetId']).execute()

    #response = bigquery.datasets().list(projectId=PROJECTID).execute()
    #self.response.out.write('<h3>Datasets.list raw response after deleting\
    #        test_create:</h3>')
    #self.response.out.write('<pre>%s</pre>' % json.dumps(response, sort_keys=True, indent=4, separators=(',', ': ')))
    return '{"report-generator-status": "started"}'


# [START write]
def create_file(self, filename):
    """Create a file."""

    self.response.write('Creating file {}\n'.format(filename))

    # The retry_params specified in the open call will override the default
    # retry params for this particular file handle.
    write_retry_params = cloudstorage.RetryParams(backoff_factor=1.1)
    with cloudstorage.open(
        filename, 'w', content_type='text/plain', options={
            'x-goog-meta-foo': 'foo', 'x-goog-meta-bar': 'bar'},
            retry_params=write_retry_params) as cloudstorage_file:
                cloudstorage_file.write('abcde\n')
                cloudstorage_file.write(('f'*1024+',')*4 + '\n')
    self.tmp_filenames_to_clean_up.append(filename)
# [END write]

# [START read]
def read_file(self, filename):
    self.response.write(
        'Abbreviated file content (first line and last 1K):\n')

    with cloudstorage.open(filename) as cloudstorage_file:
        self.response.write(cloudstorage_file.readline())
        cloudstorage_file.seek(-1024, os.SEEK_END)
        self.response.write(cloudstorage_file.read())
# [END read]

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

# [START list_bucket]
def list_bucket(self, bucket):
    """Create several files and paginate through them."""

    self.response.write('Listbucket result:\n')

    # Production apps should set page_size to a practical value.
    page_size = 3
    stats = cloudstorage.listbucket(bucket + '/t', max_keys=page_size)
    while True:
        count = 0
        for stat in stats:
            count += 1
            self.response.write(repr(stat))
            self.response.write('\n')

        if count != page_size or count == 0:
            break
        stats = cloudstorage.listbucket(
            bucket + '/t', max_keys=page_size, marker=stat.filename)
# [END list_bucket]

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

# [START delete_files]
def delete_files(self):
    self.response.write('Deleting files...\n')
    for filename in self.tmp_filenames_to_clean_up:
        self.response.write('Deleting file {}\n'.format(filename))
        try:
            cloudstorage.delete(filename)
        except cloudstorage.NotFoundError:
            pass
# [END delete_files]

PREFIX = '/report/v1/'

app = Flask(__name__)

app.add_url_rule(
    PREFIX + 'Report',
    endpoint='report_gen',
    view_func=run_report,
    methods=['GET'])