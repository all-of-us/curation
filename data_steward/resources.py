import inspect
import os
import csv
import cachetools
import json

base_path = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))

# spec/_data/*
data_path = os.path.join(base_path, 'spec', '_data')
hpo_csv_path = os.path.join(data_path, 'hpo.csv')

# resources/*
resource_path = os.path.join(base_path, 'resources')
fields_path = os.path.join(resource_path, 'fields')
cdm_csv_path = os.path.join(resource_path, 'cdm.csv')
achilles_index_path = os.path.join(resource_path, 'curation_report')


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