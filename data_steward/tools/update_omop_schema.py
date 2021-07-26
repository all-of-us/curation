"""
Update the OMOP json files in-place to CDMv5.3.1 based on the CommonDataModel v5.3.1_fixes branch
(see https://github.com/OHDSI/CommonDataModel/blob/v5.3.1_fixes/OMOP_CDMv5.3.1_Field_Level.csv).

Minimizes formatting changes to reduce diff noise. Descriptions from current schemas are
preferred over updated ones. Note that running this script will overwrite
`resource_files/schemas/cdm/*.json`
"""

import csv
from collections import OrderedDict
from itertools import groupby
import json
import os

from resources import cdm_fields_path, resource_files_path

CDM_CSV = os.path.join(resource_files_path, 'OMOP_CDMv5.3.1_Field_Level.csv')
"""Downloaded from
   https://github.com/OHDSI/CommonDataModel/blob/v5.3.1_fixes/OMOP_CDMv5.3.1_Field_Level.csv"""


def find_current_path(filename):
    """
    Get the path to current schema file, if it exists
    :param filename: name of the JSON file
    :return: 
    """
    for root, dirs, files in os.walk(cdm_fields_path):
        if filename in files:
            return os.path.join(root, filename)


def convert_type(in_type):
    """
    Convert the CDM data type to a BQ type
    :param in_type: value specified in cdmDatatype column of csv
    :return: a BQ type (str)
    """
    column_type = in_type.lower().split('(')[0]
    if (column_type == "bigint"):
        t = "integer"
    elif (column_type == "integer"):
        t = "integer"
    elif (column_type in ("timestamp", "datetime", "datetime2")):
        t = "timestamp"
    elif (column_type == "clob"):
        t = "string"
    elif (column_type == "character"):
        t = "string"
    elif (column_type == "varchar"):
        t = "string"
    elif (column_type == "text"):
        t = "string"
    elif (column_type == "character"):
        t = "string"
    elif (column_type == "boolean"):
        t = "string"  # todo - translate "t" and "f" to something else
    elif (column_type == "double"):
        t = "float"
    elif (column_type == "numeric"):
        t = "float"
    elif (column_type == "float"):
        t = "float"
    elif (column_type == "date"):
        t = "date"
    elif (column_type == "datetime"):
        t = "datetime"
    else:
        assert False, "Unknown type: %s" % column_type
    return t


def read():
    with open(CDM_CSV, encoding='utf8') as csvfile:
        reader = csv.DictReader(csvfile)
        in_fields = list(reader)
    for in_field in in_fields:
        yield dict(table=in_field['cdmTableName'].lower(),
                   type=convert_type(in_field['cdmDatatype']),
                   name=in_field['cdmFieldName'].lower(),
                   mode='required' if in_field['isRequired'] == 'Yes' else 'nullable',
                   description=in_field['userGuidance'])


def get_table(row):
    return row['table']


def update_fields():
    """
    Update schema files to version 5.3.1
    :return: 
    """
    csv_rows = list(read())
    for table, fields in groupby(csv_rows, key=get_table):
        filename = f'{table}.json'
        old_schema = dict()
        current_path = find_current_path(filename)
        if current_path:
            with open(current_path, 'r', encoding='utf8') as schema_fp:
                old_fields = json.load(schema_fp)
                for old_field in old_fields:
                    old_schema[old_field['name']] = old_field

        field_items = []
        for field in fields:
            field_item = OrderedDict(type=field['type'],
                                     name=field['name'],
                                     mode=field['mode'])
            # prefer old description, if one exists
            description = field['description']
            old_field = old_schema.get(field['name'])
            if old_field:
                description = old_field.get('description', field['description'])
            # do not add a description property if empty
            if description:
                field_item['description'] = description
            field_items.append(field_item)

        # overwrite existing files, wherever they are
        # and write new schema files to cdm directory
        output_path = find_current_path(filename) or os.path.join(cdm_fields_path, filename)
        print(f'Writing to {output_path}...')
        with open(output_path, 'w', encoding='utf8') as fp:
            json.dump(field_items, fp, indent=2, ensure_ascii=False)


if __name__ == '__main__':
    update_fields()
