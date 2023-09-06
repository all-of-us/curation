# Python imports
import os
from glob import glob
from io import open

# Project imports
import bq_utils
import resources
from common import BIGQUERY_DATASET_ID

EXPORT_PATH = os.path.join(resources.resource_files_path, 'export')
RESULTS_SCHEMA_PLACEHOLDER = '@results_database_schema.'
VOCAB_SCHEMA_PLACEHOLDER = '@vocab_database_schema.'
UNIONED_EHR = 'unioned_ehr'


def list_files(base_path):
    return [
        y for x in os.walk(base_path) for y in glob(os.path.join(x[0], '*.sql'))
    ]


def render(sql, hpo_id, results_schema, vocab_schema=''):
    table_id = resources.get_table_id(table_name='', hpo_id=hpo_id)
    vocab_replacement = f'{vocab_schema}.' if vocab_schema else ''
    sql = sql.replace(RESULTS_SCHEMA_PLACEHOLDER,
                      f'{results_schema}.{table_id}')
    sql = sql.replace(VOCAB_SCHEMA_PLACEHOLDER, vocab_replacement)
    return sql


def attribute_name(file_path):
    parts = file_path.split(os.sep)
    file_name = parts[-1]
    len(file_name) - 4
    name = file_name[0:len(file_name) - 4]
    parent_dirname = parts[-2]
    if name == parent_dirname:
        return None
    else:
        return name


def list_files_only(root):
    for _, _, filenames in os.walk(root):
        return filenames


def list_dirs_only(root):
    for _, dirnames, _ in os.walk(root):
        return dirnames


def is_hpo_id(hpo_id):
    return hpo_id in [item['hpo_id'] for item in bq_utils.get_hpo_info()]


# TODO Make this function more generic.
def export_from_path(p, datasource_id):
    """
    Export results
    :param p: path to SQL file
    :param datasource_id: HPO or aggregate dataset to run export for
    :return: `dict` structured for report render
    """
    result = dict()
    if not is_hpo_id(datasource_id) and datasource_id != UNIONED_EHR:
        datasource_id = None
    for f in list_files_only(p):
        name = f[0:-4].upper()
        abs_path = os.path.join(p, f)
        with open(abs_path, 'r') as fp:
            sql = fp.read()
            sql = render(sql,
                         datasource_id,
                         results_schema=BIGQUERY_DATASET_ID,
                         vocab_schema='')
            query_result = bq_utils.query(sql)
            # TODO reshape results
            result[name] = query_result_to_payload(query_result)

    for d in list_dirs_only(p):
        abs_path = os.path.join(p, d)
        name = d.upper()
        # recursive call
        dir_result = export_from_path(abs_path, datasource_id)
        if name in result:
            # a sql file generated the item already
            result[name].update(dir_result)
        else:
            # add the item
            result[name] = dir_result
    return result


def convert_value(value, tpe):
    """
    Cast to specified type
    :param value: value to cast
    :param tpe: name of the type as returned by bq
    :return: casted value or the input value if it is falsey
    """
    if value:
        if tpe == 'INTEGER':
            return int(value)
        if tpe == 'FLOAT':
            return float(value)
    return value


def query_result_to_payload(qr):
    """
    Convert query result to the report format (which was based on rjson)
    :param qr: query result
    :return:
    """
    result = dict()
    rows = qr['rows'] if int(qr['totalRows']) > 0 else []
    fields = qr['schema']['fields']
    field_count = len(fields)
    for i in range(0, field_count):
        field = fields[i]
        key = field['name'].upper()
        tpe = field['type'].upper()
        values = [convert_value(r['f'][i]['v'], tpe) for r in rows]
        # according to AchillesWeb rjson serializes dataframes with 1 row as single element properties
        # see https://github.com/OHDSI/AchillesWeb/blob/master/js/app/common.js#L134
        result[key] = values[0] if len(values) == 1 else values
    return result
