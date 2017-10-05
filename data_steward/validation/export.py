from glob import glob
import resources
import os
import bq_utils

EXPORT_PATH = os.path.join(resources.resource_path, 'export')
RESULTS_SCHEMA_PLACEHOLDER = '@results_database_schema.'
VOCAB_SCHEMA_PLACEHOLDER = '@vocab_database_schema.'


def list_files(base_path):
    return [y for x in os.walk(base_path) for y in glob(os.path.join(x[0], '*.sql'))]


def render(sql, hpo_id, results_schema, vocab_schema):
    table_id = bq_utils.get_table_id(hpo_id, '')
    sql = sql.replace(RESULTS_SCHEMA_PLACEHOLDER, results_schema + '.' + table_id)
    sql = sql.replace(VOCAB_SCHEMA_PLACEHOLDER, vocab_schema + '.')
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
    for dirname, dirnames, filenames in os.walk(root):
        return filenames


def list_dirs_only(root):
    for dirname, dirnames, filenames in os.walk(root):
        return dirnames


def export_from_path(p, hpo_id):
    result = dict()
    for f in list_files_only(p):
        name = f[0:-4].upper()
        abs_path = os.path.join(p, f)
        with open(abs_path, 'r') as fp:
            sql = fp.read()
            sql = render(sql, hpo_id, results_schema=bq_utils.get_dataset_id(), vocab_schema='synpuf_100')
            query_result = bq_utils.query(sql)
            # TODO reshape results
            result[name] = ['bq_utils.query(sql)']

    for d in list_dirs_only(p):
        abs_path = os.path.join(p, d)
        name = d.upper()
        result[name] = export_from_path(abs_path)
    return result


