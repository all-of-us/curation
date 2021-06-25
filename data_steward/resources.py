import csv
import hashlib
import inspect
import json
import logging
import os
from io import open
from typing import List

import cachetools

from common import (ACHILLES_TABLES, ACHILLES_HEEL_TABLES, VOCABULARY_TABLES,
                    PROCESSED_TXT, RESULTS_HTML, FITBIT_TABLES, PID_RID_MAPPING,
                    COPE_SURVEY_MAP)

LOGGER = logging.getLogger(__name__)

base_path = os.path.dirname(
    os.path.abspath(inspect.getfile(inspect.currentframe())))

# tools/*
tools_path = os.path.join(base_path, 'tools')

# resources/*
DEID_PATH = os.path.join(base_path, 'deid')
resource_files_path = os.path.join(base_path, 'resource_files')
config_path = os.path.join(base_path, 'config')
fields_path = os.path.join(resource_files_path, 'schemas')
cdm_fields_path = os.path.join(fields_path, 'cdm')
rdr_fields_path = os.path.join(fields_path, 'rdr')
internal_fields_path = os.path.join(fields_path, 'internal')
mapping_fields_path = os.path.join(internal_fields_path, 'mapping_tables')
extension_fields_path = os.path.join(fields_path, 'extension_tables')
aou_files_path = os.path.join(resource_files_path, 'schemas')
cdm_csv_path = os.path.join(resource_files_path, 'cdm.csv')
hpo_site_mappings_path = os.path.join(config_path, 'hpo_site_mappings.csv')
achilles_index_path = os.path.join(resource_files_path, 'curation_report')
AOU_VOCAB_PATH = os.path.join(resource_files_path, 'aou_vocab')
AOU_VOCAB_CONCEPT_CSV_PATH = os.path.join(AOU_VOCAB_PATH, 'concept.csv')
TEMPLATES_PATH = os.path.join(resource_files_path, 'templates')
HPO_REPORT_HTML = 'hpo_report.html'
html_boilerplate_path = os.path.join(TEMPLATES_PATH, HPO_REPORT_HTML)
CRON_TPL_YAML = 'cron.tpl.yaml'

achilles_images_path = os.path.join(achilles_index_path, 'images')
achilles_data_path = os.path.join(achilles_index_path, 'data')
DATASOURCES_JSON = os.path.join(achilles_data_path, 'datasources.json')

domain_mappings_path = os.path.join(resource_files_path, 'domain_mappings')
field_mappings_replaced_path = os.path.join(domain_mappings_path,
                                            'field_mappings_replaced.csv')
table_mappings_path = os.path.join(domain_mappings_path, 'table_mappings.csv')
field_mappings_path = os.path.join(domain_mappings_path, 'field_mappings.csv')
value_mappings_path = os.path.join(domain_mappings_path, 'value_mappings.csv')
CDR_CLEANER_PATH = os.path.join(resource_files_path, 'cdr_cleaner')
DC732_CONCEPT_LOOKUP_CSV_PATH = os.path.join(CDR_CLEANER_PATH,
                                             'dc732_concept_lookup.csv')
PPI_BRANCHING_PATH = os.path.join(CDR_CLEANER_PATH, 'ppi_branching')
BASICS_CSV_PATH = os.path.join(PPI_BRANCHING_PATH, 'basics.csv')
COPE_CSV_PATH = os.path.join(PPI_BRANCHING_PATH, 'cope.csv')
FAMILY_HISTORY_CSV_PATH = os.path.join(PPI_BRANCHING_PATH, 'family_history.csv')
HEALTHCARE_ACCESS_CSV_PATH = os.path.join(PPI_BRANCHING_PATH,
                                          'healthcare_access.csv')
LIFESTYLE_CSV_PATH = os.path.join(PPI_BRANCHING_PATH, 'lifestyle.csv')
OVERALL_HEALTH_CSV_PATH = os.path.join(PPI_BRANCHING_PATH, 'overall_health.csv')
PERSONAL_MEDICAL_HISTORY_CSV_PATH = os.path.join(
    PPI_BRANCHING_PATH, 'personal_medical_history.csv')
PPI_BRANCHING_RULE_PATHS = [
    BASICS_CSV_PATH, COPE_CSV_PATH, FAMILY_HISTORY_CSV_PATH,
    HEALTHCARE_ACCESS_CSV_PATH, LIFESTYLE_CSV_PATH, OVERALL_HEALTH_CSV_PATH,
    PERSONAL_MEDICAL_HISTORY_CSV_PATH
]


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
    for path, _, files in os.walk(achilles_index_path):
        for name in files:
            achilles_index_files.append(os.path.join(path, name))
    return achilles_index_files


def fields_for(table, sub_path=None):
    """
    Return the json schema for any table identified in the schemas directory.

    Uses os.walk to traverse subdirectories

    :param table: The table to get a schema for
    :param sub_path: A string identifying a sub-directory in resource_files/schemas.
        If provided, this directory will be searched.
    :returns: a json object representing the schemas for the named table
    """
    path = os.path.join(fields_path, sub_path if sub_path else '')

    # default setting
    json_path = os.path.join(path, table + '.json')

    unique_count = 0
    for dirpath, _, files in os.walk(path):
        if sub_path and os.path.basename(sub_path) != os.path.basename(dirpath):
            continue

        for filename in files:
            if filename[:-5] == table:
                json_path = os.path.join(dirpath, filename)
                unique_count = unique_count + 1

    if unique_count > 1:
        raise RuntimeError(
            f"Unable to read schema file because multiple schemas exist for:\t"
            f"{table} in path {path}")
    elif unique_count == 0:
        raise RuntimeError(
            f"Unable to find schema file for {table} in path {path}")

    with open(json_path, 'r') as fp:
        fields = json.load(fp)

    return fields


def is_internal_table(table_id):
    """
    Return True if specified table is an internal table or mapping table for
    pipeline (e.g. logging tables or mapping tables)

    :param table_id: identifies the table
    :return: True if specified table is an internal table, False otherwise
    """
    return table_id.startswith('_')


def is_extension_table(table_id):
    """
    Return True if specified table is an OMOP extension table.

    Extension tables provide additional detail about an OMOP records taht does
    not inherently fit in with the OMOP common data model.

    :param table_id: identifies the table
    :return: True if specified table is an internal table, False otherwise
    """
    return table_id.endswith('_ext')


def is_deid_table(table_id):
    """
    Return True if specified table is an deid generated table.

    Currently the deid mapping table is generated by deid
    :param table_id: identifies the table
    :return: True if specified table is a deid table, False otherwise
    """
    return table_id == '_deid_map'


def is_wearables_table(table_id):
    """
    Return True if specified table is a wearables table, currently for FitBit.

    Custom tables for the AOU program to ingest wearables data
    :param table_id: identifies the table
    :return: True if specified table is a wearables table (eg fitbit), False otherwise
    """
    return table_id in FITBIT_TABLES


def is_additional_rdr_table(table_id):
    """
        Return True if specified table is an additional table submitted by RDR.

        Currently includes pid_rid_mapping
        :param table_id: identifies the table
        :return: True if specified table is an additional table submitted by RDR
        """
    return table_id in [PID_RID_MAPPING, COPE_SURVEY_MAP]


def is_mapping_table(table_id):
    """
    Return True if specified table is a mapping table

    :param table_id: identifies the table
    :return: True if specified table is an mapping table, False otherwise
    """
    return table_id.startswith('_mapping_')


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
    # TODO:  update this code as part of DC-1015 and remove this comment
    for dir_path, _, files in os.walk(cdm_fields_path):
        for f in files:
            file_path = os.path.join(dir_path, f)
            with open(file_path, 'r', encoding='utf-8') as fp:
                file_name = os.path.basename(f)
                table_name = file_name.split('.')[0]
                schema = json.load(fp)
                include_table = True
                if table_name in VOCABULARY_TABLES and not include_vocabulary:
                    include_table = False
                elif table_name in ACHILLES_TABLES + ACHILLES_HEEL_TABLES and not include_achilles:
                    include_table = False
                if include_table:
                    result[table_name] = schema

    return result


def rdr_specific_schemas():
    """
    Get a dictionary mapping table_name -> schema

    :return:
    """
    result = dict()
    for dir_path, _, files in os.walk(rdr_fields_path):
        for f in files:
            file_path = os.path.join(dir_path, f)
            with open(file_path, 'r', encoding='utf-8') as fp:
                file_name = os.path.basename(f)
                table_name = file_name.split('.')[0]
                schema = json.load(fp)
                result[table_name] = schema

    return result


def get_person_id_tables(domain_tables):
    """
    A helper function to get list of CDM_tables with person_id
    :param domain_tables: List of domain tables
    return: list of domain tables containing person_id field.
    """
    person_id_tables = []
    for table in domain_tables:
        fields = fields_for(table)
        field_names = [field['name'] for field in fields]
        if 'person_id' in field_names:
            person_id_tables.append(table)
    return person_id_tables


def mapping_schemas():
    result = dict()
    for f in os.listdir(mapping_fields_path):
        file_path = os.path.join(mapping_fields_path, f)
        table_name = f.split('.')[0]

        if is_mapping_table(table_name):
            # only open and load mapping tables, instead of all tables
            with open(file_path, 'r') as fp:
                result[table_name] = json.load(fp)

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
MAPPING_TABLES = list(mapping_schemas().keys())
ACHILLES_INDEX_FILES = achilles_index_files()
CDM_FILES = [table + '.csv' for table in CDM_TABLES]
ALL_ACHILLES_INDEX_FILES = [
    name.split(resource_files_path + os.sep)[1].strip()
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


def get_concept_id_fields(table_name) -> List[str]:
    """
    Determine if column is a concept_id column

    :param table_name:
    :return: all *concept_id schemas given a table
    """
    return [
        field_name['name']
        for field_name in fields_for(table_name)
        if field_name['name'].endswith('concept_id')
    ]


def has_domain_table_id(table_name):
    """
    Determines if a table has domain_table_id

    :param table_name: Name of a cdm domain table
    :return: True/False if domain_table_id is available is table schemas
    """
    return f'{table_name}_id' in [
        field.get('name', '') for field in fields_for(table_name)
    ]
