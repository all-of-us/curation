import argparse
import json
import os

import bq_utils
import resources
from utils import bq

METADATA_TABLE = 'cdr_metadata'
ETL_VERSION = 'etl_version'
COPY = 'copy'
CREATE = 'create'
INSERT = 'insert'
NAME = 'name'
TYPE = 'type'
DATE = 'date'
DESCRIPTION = 'description'

JOB_COMPONENTS = [COPY, CREATE, INSERT]

fields_filename = os.path.join(resources.fields_path, METADATA_TABLE + '.json')
with open(fields_filename, 'r') as fields_file:
    fields = json.load(fields_file)
UPDATE_STRING_COLUMNS = "{field} = \'{field_value}\'"
UPDATE_DATE_COLUMNS = "{field} = cast(\'{field_value}\' as DATE)"

ETL_VERSION_CHECK = """
select {etl} from `{dataset}.{table}`
"""
ADD_ETL_METADATA_QUERY = """
insert into `{dataset}.cdr_metadata` ({etl_version}) values(\'{field_value}\')
"""

UPDATE_QUERY = """
update `{dataset}.{table}` set {statement} where {etl_version} = \'{etl_value}\'
"""

COPY_QUERY = """
select * from `{dataset}.cdr_metadata`
"""


def create_metadata_table(dataset_id, fields_list):
    """
    Creates a metadata table in a given dataset.
    :param dataset_id: name of the dataset
    :param fields_list: name of the dataset
    :return:
    """
    if not bq_utils.table_exists(METADATA_TABLE, dataset_id):
        bq_utils.create_table(table_id=METADATA_TABLE,
                              fields=fields_list,
                              dataset_id=dataset_id)


def copy_metadata_table(source_dataset_id, target_dataset_id, fields):
    """

    :param source_dataset_id:
    :param target_dataset_id:
    :param fields:
    :return:
    """
    create_metadata_table(target_dataset_id, fields)
    query = COPY_QUERY.format(datset=source_dataset_id)
    bq_utils.query(query,
                   destination_dataset_id=target_dataset_id,
                   destination_table_id=METADATA_TABLE)


def add_metadata(dataset_id, project_id, field_values=None):
    """

    :param dataset_id: 
    :param project_id: 
    :param field_values:
    :return: 
    """
    q = ETL_VERSION_CHECK.format(etl=ETL_VERSION,
                                 dataset=dataset_id,
                                 table=METADATA_TABLE)
    etl_check = bq.query(q, project_id=project_id)[ETL_VERSION].tolist()
    if not etl_check:
        q = ADD_ETL_METADATA_QUERY.format(dataset=dataset_id,
                                          etl_version=ETL_VERSION,
                                          field_value=field_values[ETL_VERSION])
        bq_utils.query(q)
    statement_list = []
    field_types = dict()
    for field_name in fields:
        field_types[field_name[NAME]] = field_name[TYPE]
    for field_name in field_values:
        if field_name != ETL_VERSION and field_values[field_name] is not None:
            if field_types[field_name] == DATE:
                statement_list.append(
                    UPDATE_DATE_COLUMNS.format(
                        field=field_name, field_value=field_values[field_name]))
            else:
                statement_list.append(
                    UPDATE_STRING_COLUMNS.format(
                        field=field_name, field_value=field_values[field_name]))
    if len(statement_list) != 0:
        update_statement = ' ,'.join(statement_list)
        print(update_statement)
        q = UPDATE_QUERY.format(dataset=dataset_id,
                                table=METADATA_TABLE,
                                statement=update_statement,
                                etl_version=ETL_VERSION,
                                etl_value=field_values[ETL_VERSION])
        bq_utils.query(q)


if __name__ == '__main__':

    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        '--component',
        required=True,
        help='Job specification for adding data to metadata table',
        choices=list(JOB_COMPONENTS))
    parser.add_argument('--project_id',
                        required=True,
                        help='Identifies the dataset to copy metadata from')
    parser.add_argument('--target_dataset',
                        default=True,
                        help='Identifies the dataset to copy metadata from')
    parser.add_argument('--source_dataset',
                        default=None,
                        help='Identifies the dataset to copy metadata from')

    for field in fields:
        parser.add_argument(f'--{field[NAME]}',
                            default=None,
                            help=f'{field[DESCRIPTION]}')
    args = parser.parse_args()
    if args.component == CREATE:
        create_metadata_table(args.target_dataset, fields)
    if args.component == COPY:
        copy_metadata_table(args.source_dataset, args.target_dataset, fields)
    field_values_dict = dict(
        zip([field[NAME] for field in fields], [
            args.etl_version, args.ehr_source, args.ehr_cutoff_date,
            args.rdr_source, args.rdr_export_date, args.cdr_generation_date,
            args.qa_handoff_date, args.vocabulary_version
        ]))
    print(field_values_dict)
    if args.component == INSERT:
        add_metadata(args.target_dataset, args.project_id, field_values_dict)
