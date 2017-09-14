#!/usr/bin/env python

"""
    tasks script for cron jobs
"""

from __future__ import absolute_import

# util imports
import StringIO
import json
import os
import unicodedata
import csv

# gcloud imports
import logging

# app requirements
from flask import Flask
import markdown
from flask_flatpages import FlatPages
import jinja2
import api_util
import gcs_utils
import resources
from common import RESULT_CSV, LOG_JSON

_DRC_SHARED_BUCKET = 'aou-drc-shared'
PREFIX = '/tasks/'
SITE_ROOT = os.path.dirname(os.path.abspath(__file__))
DEBUG = True

FLATPAGES_AUTO_RELOAD = DEBUG
FLATPAGES_EXTENSION = '.md'
FLATPAGES_ROOT = SITE_ROOT + '/pages/'

LOG_FILE = SITE_ROOT + '/_data/log.json'
HPO_FILE = SITE_ROOT + '/_data/hpo.csv'

app = Flask(__name__, template_folder=SITE_ROOT + '/templates')
app.config.from_object(__name__)
pages = FlatPages(app)
j2_env = jinja2.Environment(loader=jinja2.FileSystemLoader(SITE_ROOT + '/templates/'), trim_blocks=True)

md = markdown.Markdown(extensions=['meta','markdown.extensions.tables'])
j2_env.filters['markdown'] = lambda text: jinja2.Markup(md.convert(text))
j2_env.globals['get_title'] = lambda: md.Meta['title'][0]
j2_env.trim_blocks = True
j2_env.lstrip_blocks = True

md_convert = lambda text: jinja2.Markup(md.convert(text))


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

    with open(HPO_FILE,'r') as infile:
        reader = csv.reader(infile)
        reader.next()
        hpos = [{'hpo_id':rows[0],'name':rows[1]} for rows in reader]
    
    # this is pure html content. can be exported
    
    markdown_string_template  = j2_env.from_string(page.body)
    processed_md = markdown_string_template.render(hpos = hpos,
                                                   page = page)
    content = md_convert(processed_md)

    html = j2_env.get_template(template_to_use).render(content=content,
                                                       page=page,
                                                       hpos=hpos, 
                                                       pages=pages,
                                                       logs=data)
    return html


def hpo_log_item_to_obj(hpo_id, item):
    file_name = item['cdm_file_name']
    table_name = file_name.split('.')[0]
    return dict(hpo_id=hpo_id,
                file_name=file_name,
                table_name=table_name,
                found=item['found'] == '1',
                parsed=item['parsed'] == '1',
                loaded=item['loaded'] == '1')


def get_full_result_log():
    full_log = []
    for hpo in resources.hpo_csv():
        hpo_id = hpo['hpo_id']
        hpo_bucket = gcs_utils.get_hpo_bucket(hpo_id)

        obj_metadata = gcs_utils.get_metadata(hpo_bucket, RESULT_CSV)

        if obj_metadata is None:
            logging.info('%s was not found in %s' % (RESULT_CSV, hpo_bucket))
        else:
            hpo_result = gcs_utils.get_object(hpo_bucket, RESULT_CSV)
            hpo_result_file = StringIO.StringIO(hpo_result)
            hpo_result_items = resources._csv_file_to_list(hpo_result_file)
            result_objects = map(lambda item: hpo_log_item_to_obj(hpo_id, item), hpo_result_items)
            full_log.extend(result_objects)
    return full_log


@api_util.auth_required_cron
def _generate_site():
    """
    Construct html pages for each report, data_model, file_transfer and index.

    :param : 
    :return: returns 'okay' if succesful. logs critical error otherwise.
    """
    bucket = gcs_utils.get_drc_bucket()

    for endpoint in ['report', 'data_model', 'index', 'file_transfer_procedures']:
        # generate the page
        html = _page(endpoint)
        html = unicodedata.normalize('NFKD', html).encode('ascii', 'ignore')
        fp = StringIO.StringIO(html)

        # write it to the drc shared bucket
        file_name = endpoint + '.html'
        gcs_utils.upload_object(bucket, file_name, fp)

    # aggregate result logs and write to bucket
    full_result_log = get_full_result_log()
    content = json.dumps(full_result_log)
    fp = StringIO.StringIO(content)
    gcs_utils.upload_object(bucket, LOG_JSON, fp)
    return 'okay'

app.add_url_rule(
    PREFIX + 'sitegen',
    endpoint='sitegen',
    view_func=_generate_site,
    methods=['GET'])
