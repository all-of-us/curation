import csv
import hashlib
import inspect
import json
import os

import cachetools

from common import ACHILLES_TABLES, ACHILLES_HEEL_TABLES, VOCABULARY_TABLES, PROCESSED_TXT, RESULTS_HTML
from io import open

base_path = os.path.dirname(
    os.path.abspath(inspect.getfile(inspect.currentframe())))

# tools/*
tools_path = os.path.join(base_path, 'tools')

# resources/*
DEID_PATH = os.path.join(base_path, 'deid')
resource_path = os.path.join(base_path, 'resources')
config_path = os.path.join(base_path, 'config')
fields_path = os.path.join(resource_path, 'fields')
cdm_csv_path = os.path.join(resource_path, 'cdm.csv')
hpo_site_mappings_path = os.path.join(config_path, 'hpo_site_mappings.csv')
achilles_index_path = os.path.join(resource_path, 'curation_report')
AOU_VOCAB_PATH = os.path.join(resource_path, 'aou_vocab')
AOU_VOCAB_CONCEPT_CSV_PATH = os.path.join(AOU_VOCAB_PATH, 'concept.csv')
TEMPLATES_PATH = os.path.join(resource_path, 'templates')
HPO_REPORT_HTML = 'hpo_report.html'
html_boilerplate_path = os.path.join(TEMPLATES_PATH, HPO_REPORT_HTML)
CRON_TPL_YAML = 'cron.tpl.yaml'

DATASOURCES_JSON = os.path.join(achilles_index_path, 'data/datasources.json')

domain_mappings_path = os.path.join(resource_path, 'domain_mappings')
field_mappings_replaced_path = os.path.join(domain_mappings_path,
                                            'field_mappings_replaced.csv')
table_mappings_path = os.path.join(domain_mappings_path, 'table_mappings.csv')
field_mappings_path = os.path.join(domain_mappings_path, 'field_mappings.csv')
value_mappings_path = os.path.join(domain_mappings_path, 'value_mappings.csv')


@cachetools.cached(cache={})
def csv_to_list(csv_path):
    """
    Yield a list of `dict` from a CSV file
    :param csv_path: absolute path to a well-formed CSV file
    :return:
    """
    with open(csv_path, mode='r') as csv_file:
        list_of_dicts = _csv_file_to_list(csv_file)
    return list_of_dicts


def _csv_file_to_list(csv_file):
    """
    Yield a list of `dict` from a file-like object with records in CSV format
    :param csv_file: file-like object containing records in CSV format
    :return: list of `dict`
    """
    items = []
    reader = csv.reader(csv_file)
    field_names = next(reader)
    for csv_line in reader:
        item = dict(zip(field_names, csv_line))
        items.append(item)
    return items


def table_mappings_csv():
    return csv_to_list(table_mappings_path)


def field_mappings_csv():
    return csv_to_list(field_mappings_path)


def value_mappings_csv():
    return csv_to_list(value_mappings_path)


def cdm_csv():
    return csv_to_list(cdm_csv_path)


def achilles_index_files():
    achilles_index_files = []
    for path, subdirs, files in os.walk(achilles_index_path):
        for name in files:
            achilles_index_files.append(os.path.join(path, name))
    return achilles_index_files


def fields_for(table):
    json_path = os.path.join(fields_path, table + '.json')
    with open(json_path, 'r') as fp:
        fields = json.load(fp)
    return fields


def is_internal_table(table_id):
    """
    Return True if specified table is an internal table for pipeline (e.g. mapping tables)

    :param table_id: identifies the table
    :return: True if specified table is an internal table, False otherwise
    """
    return table_id.startswith('_')


def is_pii_table(table_id):
    """
    Return True if specified table is a pii table

    :param table_id: identifies the table
    :return: True if specified table is a pii table, False otherwise
    """
    return table_id.startswith('pii') or table_id.startswith('participant')


def is_id_match(table_id):
    """
    Return True if specified table is a identity_match table

    :param table_id:
    :return:
    """
    return table_id.startswith('identity_')


def cdm_schemas(include_achilles=False, include_vocabulary=False):
    """
    Get a dictionary mapping table_name -> schema

    :param include_achilles:
    :param include_vocabulary:
    :return:
    """
    result = dict()
    for f in os.listdir(fields_path):
        file_path = os.path.join(fields_path, f)
        with open(file_path, 'r') as fp:
            file_name = os.path.basename(f)
            table_name = file_name.split('.')[0]
            schema = json.load(fp)
            include_table = True
            if table_name in VOCABULARY_TABLES and not include_vocabulary:
                include_table = False
            elif table_name in ACHILLES_TABLES + ACHILLES_HEEL_TABLES and not include_achilles:
                include_table = False
            elif is_internal_table(table_name):
                include_table = False
            elif is_pii_table(table_name):
                include_table = False
            elif is_id_match(table_name):
                include_table = False
            if include_table:
                result[table_name] = schema
    return result


def hash_dir(in_dir):
    """
    Generate an MD5 digest from the contents of a directory
    """
    file_names = os.listdir(in_dir)
    hash_obj = hashlib.sha256()
    for file_name in file_names:
        file_path = os.path.join(in_dir, file_name)
        with open(file_path, 'rb') as fp:
            hash_obj.update(fp.read())
    return hash_obj.hexdigest()


CDM_TABLES = list(cdm_schemas().keys())
ACHILLES_INDEX_FILES = achilles_index_files()
CDM_FILES = [table + '.csv' for table in CDM_TABLES]
ALL_ACHILLES_INDEX_FILES = [
    name.split(resource_path + os.sep)[1].strip()
    for name in ACHILLES_INDEX_FILES
]
IGNORE_LIST = [PROCESSED_TXT, RESULTS_HTML] + ALL_ACHILLES_INDEX_FILES


def get_domain_id_field(domain_table):
    """
    A helper function to create the id field
    :param domain_table: the cdm domain table
    :return: the id field
    """
    return domain_table + '_id'


def get_domain_concept_id(domain_table):
    """
    A helper function to create the domain_concept_id field
    :param domain_table: the cdm domain table
    :return: the domain_concept_id
    """
    return domain_table.split('_')[0] + '_concept_id'


def get_domain_source_concept_id(domain_table):
    """
    A helper function to create the domain_source_concept_id field
    :param domain_table: the cdm domain table
    :return: the domain_source_concept_id
    """
    return domain_table.split('_')[0] + '_source_concept_id'


def get_domain(domain_table):
    """
    A helper function to get the domain for the corresponding cdm domain table
    :param domain_table: the cdm domain table
    :return: the domains
    """
    domain = domain_table.split('_')[0].capitalize()
    return domain
