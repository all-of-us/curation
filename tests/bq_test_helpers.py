"""
Shared helper functions for BigQuery-related tests
"""
import datetime
import warnings
from collections import OrderedDict
from typing import Any, List, Union, Dict

import mock
from google.cloud import bigquery
from google.cloud.bigquery import TableReference
from google.cloud.bigquery.table import TableListItem

_TYPE_TO_FIELD_TYPE = {
    str(int): 'INT64',
    str(str): 'STRING',
    str(float): 'FLOAT64',
    str(datetime.date): 'DATE',
    str(datetime.datetime): 'TIMESTAMP'
}
dict_types = Union[Dict, OrderedDict]


def _field_from_key_value(key: str, value: Any) -> bigquery.SchemaField:
    """
    Get a schema field object from a key and value
    
    :param key: name of the field
    :param value: value of the field
    :return: an appropriate schema field object
    """
    tpe = str(type(value))
    field_type = _TYPE_TO_FIELD_TYPE.get(tpe)
    if not field_type:
        raise NotImplementedError(f'The type for {value} is not supported')
    return bigquery.SchemaField(name=key, field_type=field_type)


def fields_from_dict(row):
    """
    Get schema fields from a row represented as a dictionary

    :param row: the dictionary to infer schema for
    :return: list of schema field objects
    
    Example:
        >>> from tests import bq_test_helpers
        >>> d = {'item': 'book', 'qty': 2, 'price': 1.99}
        >>> bq_test_helpers.fields_from_dict(d)
            [SchemaField('item', 'STRING', 'NULLABLE', None, ()),
             SchemaField('qty', 'INT64', 'NULLABLE', None, ()),
             SchemaField('price', 'FLOAT64', 'NULLABLE', None, ())]
    """
    return [_field_from_key_value(key, value) for key, value in row.items()]


def mock_query_result(rows: List[dict_types]):
    """
    Create a mock RowIterator as returned by :meth:`bigquery.QueryJob.result`
    from rows represented as a list of dictionaries

    :param rows: a list of dictionaries representing result rows
    :return: a mock RowIterator
    """
    mock_row_iter = mock.MagicMock(spec=bigquery.table.RowIterator)
    mock_row_iter.total_rows = len(rows)
    row0 = rows[0]
    if isinstance(row0, OrderedDict):
        _rows = rows
    else:
        warnings.warn('Provide rows as `OrderedDict` if type conversion '
                      'results in unexpected behavior.')
        _rows = [OrderedDict(row) for row in rows]
        row0 = _rows[0]

    mock_row_iter.schema = list(fields_from_dict(row0))
    field_to_index = {key: i for i, key in enumerate(row0.keys())}
    mock_row_iter.__iter__ = mock.Mock(return_value=iter([
        bigquery.table.Row(list(row.values()), field_to_index) for row in _rows
    ]))
    return mock_row_iter


def list_item_from_table_id(table_id: str) -> TableListItem:
    """
    Get a table list item as returned by :meth:`bigquery.Client.list_tables` 
    from a table ID
    
    :param table_id: A table ID including project ID, dataset ID, and table ID, 
      each separated by ``.``. 
    :return: a table list item
    """
    resource = {
        "tableReference": TableReference.from_string(table_id).to_api_repr()
    }
    return TableListItem(resource)
