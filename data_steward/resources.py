import hashlib
import inspect
import os
import csv
import cachetools
import json

base_path = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))

CONCEPT = 'concept'
CONCEPT_ANCESTOR = 'concept_ancestor'
CONCEPT_CLASS = 'concept_class'
CONCEPT_RELATIONSHIP = 'concept_relationship'
CONCEPT_SYNONYM = 'concept_synonym'
DOMAIN = 'domain'
DRUG_STRENGTH = 'drug_strength'
RELATIONSHIP = 'relationship'
VOCABULARY = 'vocabulary'
VOCABULARY_TABLES = [CONCEPT, CONCEPT_ANCESTOR, CONCEPT_CLASS, CONCEPT_RELATIONSHIP, CONCEPT_SYNONYM, DOMAIN,
                     DRUG_STRENGTH, RELATIONSHIP, VOCABULARY]

ACHILLES_ANALYSIS = 'achilles_analysis'
ACHILLES_RESULTS = 'achilles_results'
ACHILLES_RESULTS_DIST = 'achilles_results_dist'
ACHILLES_TABLES = [ACHILLES_ANALYSIS, ACHILLES_RESULTS, ACHILLES_RESULTS_DIST]

ACHILLES_HEEL_RESULTS = 'achilles_heel_results'
ACHILLES_RESULTS_DERIVED = 'achilles_results_derived'
ACHILLES_HEEL_TABLES = [ACHILLES_HEEL_RESULTS, ACHILLES_RESULTS_DERIVED]

# spec/_data/*
data_path = os.path.join(base_path, 'spec', '_data')
hpo_csv_path = os.path.join(data_path, 'hpo.csv')

# resources/*
resource_path = os.path.join(base_path, 'resources')
fields_path = os.path.join(resource_path, 'fields')
cdm_csv_path = os.path.join(resource_path, 'cdm.csv')
achilles_index_path = os.path.join(resource_path, 'curation_report')
AOU_GENERAL_PATH = os.path.join(resource_path, 'aou_general')
AOU_GENERAL_CONCEPT_CSV_PATH = os.path.join(AOU_GENERAL_PATH, 'concept.csv')

html_boilerplate_path = os.path.join(resource_path, 'html_boilerplate.txt')


@cachetools.cached(cache={})
def _csv_to_list(csv_path):
    """
    Yield a list of `dict` from a CSV file
    :param csv_path: absolute path to a well-formed CSV file
    :return:
    """
    with open(csv_path, mode='r') as csv_file:
        return _csv_file_to_list(csv_file)


def _csv_file_to_list(csv_file):
    """
    Yield a list of `dict` from a file-like object with records in CSV format
    :param csv_file: file-like object containing records in CSV format
    :return: list of `dict`
    """
    items = []
    reader = csv.reader(csv_file)
    field_names = reader.next()
    for csv_line in reader:
        item = dict(zip(field_names, csv_line))
        items.append(item)
    return items


def cdm_csv():
    return _csv_to_list(cdm_csv_path)


def hpo_csv():
    # TODO get this from file; currently limited for pre- alpha release
    return _csv_to_list(hpo_csv_path)


def achilles_index_files():
    achilles_index_files = []
    for path, subdirs, files in os.walk(achilles_index_path):
        for name in files:
            achilles_index_files.append(os.path.join(path, name))
    return achilles_index_files


def fields_for(table):
    json_path = os.path.join(fields_path, table + '.json')
    with open(json_path, 'r') as fp:
        return json.load(fp)


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
            table_name, _ = file_name.split('.')
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
            if include_table:
                result[table_name] = schema
    return result


def hash_dir(in_dir):
    """
    Generate an MD5 digest from the contents of a directory
    """
    fs = os.listdir(in_dir)
    hash_obj = hashlib.md5()
    for f in fs:
        p = os.path.join(in_dir, f)
        hash_obj.update(open(p, 'rb').read())
    return hash_obj.hexdigest()
