#!/usr/bin/env python

# author: Aahlad
# date: Aug 16, 2017

"""
    tasks script for cron jobs
"""

# [START imports]
from __future__ import absolute_import

# util imports 
import os
import uuid
import sys
import json
import logging

# app requirements
from flask import Flask, url_for
from flask_flatpages import FlatPages
from flask_frozen import Freezer
from main import app as app_to_freeze

# gcloud imports
import cloudstorage
from cloudstorage import cloudstorage_api
from google.appengine.api import app_identity

# import api_util

# [END imports]
# setting the environment variable and extra imports

PROJECTID = 'bamboo-creek-172917'
gcs_path_prepend = os.path.dirname(os.path.realpath(__file__))

if os.getenv('SERVER_SOFTWARE', '').startswith('Google App Engine/'):
    # print "setting environment"
    dev_flag = False
    gcs_path_prepend = "gs://"

# [START bucket info]
def bucket_info():
    bucket_name = os.environ.get(
        'BUCKET_NAME', app_identity.get_default_gcs_bucket_name())

    logging.info('Using bucket name: \n\n'.format(bucket_name))

    bucket = '/' + bucket_name
    filename = bucket + '/tester.csv'

    self.list_bucket_directory_mode(bucket)
    sefl.response.write()

# [END bucket info]

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

def list_bucket_directory_mode(self, bucket):
    self.response.write('Listbucket directory mode result:\n')
    for stat in cloudstorage.listbucket(bucket + '/', delimiter='/'):
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

PREFIX = '/tasks/'
SITE_ROOT = ''

DEBUG = True

app = Flask(__name__)
app.config.from_object(__name__)

@app.route(PREFIX + '<string:path>/')
def page(path): 
    if path == 'sitegen':
        # freezer = Freezer(app_to_freeze)
        # freezer.freeze()
        for rule in app_to_freeze.url_map.iter_rules():
            logging.info('{}'.format(rule.endpoint)) # url_for(rule.endpoint)))
            if "GET" in rule.methods:
                logging.info(url_for(rule.endpoint))
            else:
                logging.info('No Url.')
        logging.info('----fin----')
        return 'Done!'
    return 'Does not exist!'

