import inspect
import os
import csv
import cachetools

base_path = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))

# spec/_data/*
data_path = os.path.join(base_path, 'spec', '_data')
hpo_csv_path = os.path.join(data_path, 'hpo.csv')

# resources/*
resource_path = os.path.join(base_path, 'resources')
fields_path = os.path.join(resource_path, 'fields')
cdm_csv_path = os.path.join(resource_path, 'cdm.csv')


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
    return [dict(hpo_id='nyc', name='New York City Consortium'),
            dict(hpo_id='pitt', name='University of Pittsburgh at Pittsburgh')]
