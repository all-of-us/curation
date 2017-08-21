#!/usr/bin/env python

"""
    tasks script for cron jobs
"""

from __future__ import absolute_import

# util imports
import json
import os
import unicodedata

# gcloud imports
import cloudstorage
import logging

# app requirements
from flask import Flask
from flask_flatpages import FlatPages
import jinja2
import api_util

_DRC_SHARED_BUCKET = 'aou-drc-shared'
PREFIX = '/tasks/'
SITE_ROOT = os.path.dirname(os.path.abspath(__file__))
DEBUG = True

FLATPAGES_AUTO_RELOAD = DEBUG
FLATPAGES_EXTENSION = '.md'
FLATPAGES_ROOT = SITE_ROOT + '/pages/'

LOG_FILE = SITE_ROOT + '/log.json'

app = Flask(__name__, template_folder=SITE_ROOT + '/templates')
app.config.from_object(__name__)
pages = FlatPages(app)
j2_env = jinja2.Environment(loader=jinja2.FileSystemLoader(SITE_ROOT + '/templates/'), trim_blocks=True)


# @app.route(PREFIX + '<string:path>.html')
def _page(name):
    """
    Read a file from pages directory and render as HTML
    :param name: name of the file in the pages directory to render (without extension)
    :return:
    """
    page = pages.get_or_404(name)
    template_to_use = page.meta.get('template', None) or 'page'
    template_to_use += '.html'
    data = json.load(open(LOG_FILE))

    # this is pure html content. can be exported
    html = j2_env.get_template(template_to_use).render(page=page, pages=pages, logs=data)
    return html


def _create_file(filename, content):
    """
    Create a file in gcs bucket.
    
    :param filename: file name = <bucket name>/<file name>
    :param contet: content to write into file.
    """

    # The retry_params specified in the open call will override the default
    # retry params for this particular file handle.
    write_retry_params = cloudstorage.RetryParams(backoff_factor=1.1)
    with cloudstorage.open(filename, 'w',
                           content_type='text/html',
                           retry_params=write_retry_params) as cloudstorage_file:
        cloudstorage_file.write(content)


@api_util.auth_required_cron
def _generate_site(bucket=_DRC_SHARED_BUCKET):
    """
    Construct html pages for each report, data_model, file_transfer and index.

    :param : 
    :return: returns 'okay' if succesful. logs critical error otherwise.
    """

    for endpoint in ['report', 'data_model', 'index', 'file_transfer_procedures']:
        # generate the page
        html = _page(endpoint)
        html = unicodedata.normalize('NFKD', html).encode('ascii', 'ignore')

        # write it to the drc shared bucket
        _create_file('/{}/{}.html'.format(bucket, endpoint), html)

        logging.info('{} done'.format(endpoint))

    return 'okay'


app.add_url_rule(
    PREFIX + 'sitegen',
    endpoint='sitegen',
    view_func=_generate_site,
    methods=['GET'])
