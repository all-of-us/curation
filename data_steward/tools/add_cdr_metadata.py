import argparse

import bq_utils
import resources
from utils import bq

METADATA_TABLE = '_cdr_metadata'
ETL_VERSION = 'etl_version'
COPY = 'copy'
CREATE = 'create'
INSERT = 'insert'
NAME = 'name'
TYPE = 'type'
DATE = 'date'
DESCRIPTION = 'description'

JOB_COMPONENTS = [COPY, CREATE, INSERT]
UPDATE_STRING_COLUMNS = "{field} = \'{field_value}\'"
UPDATE_DATE_COLUMNS = "{field} = cast(\'{field_value}\' as DATE)"

ETL_VERSION_CHECK = """
select {etl} from `{dataset}.{table}`
"""
ADD_ETL_METADATA_QUERY = """
insert into `{project}.{dataset}._cdr_metadata` ({etl_version}) values(\'{field_value}\')
"""

UPDATE_QUERY = """
update `{project}.{dataset}.{table}` set {statement} where {etl_version} = \'{etl_value}\'
"""

COPY_QUERY = """
select * from `{project}.{dataset}._cdr_metadata`
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


def copy_metadata_table(project_id, source_dataset_id, target_dataset_id,
                        table_fields):
    """

    :param project_id:
    :param source_dataset_id:
    :param target_dataset_id:
    :param table_fields:
    :return:
    """
    create_metadata_table(target_dataset_id, table_fields)
    query = COPY_QUERY.format(project=project_id, dataset=source_dataset_id)
    bq_utils.query(query,
                   destination_dataset_id=target_dataset_id,
                   destination_table_id=METADATA_TABLE)


def parse_update_statement(table_fields, field_values):
    statement_list = []
    field_types = dict()
    for field_name in table_fields:
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
        return ', '.join(statement_list)
    else:
        return ''


def get_etl_version(dataset_id, project_id):
    etl_version = bq.query(ETL_VERSION_CHECK.format(etl=ETL_VERSION,
                                                    project=project_id,
                                                    dataset=dataset_id,
                                                    table=METADATA_TABLE),
                           project_id=project_id)[ETL_VERSION].tolist()
    return etl_version


def add_metadata(dataset_id, project_id, table_fields, field_values=None):
    """
    Adds the metadata value passed in as parameters to the metadata table

    :param dataset_id: Name of the dataset
    :param project_id: Name of the project
    :param table_fields: field list of a table
    :param field_values: dictionary of field values passed as parameters
    :return: None
    """
    etl_check = get_etl_version(dataset_id, project_id)
    if not etl_check:
        add_etl_query = ADD_ETL_METADATA_QUERY.format(
            project=project_id,
            dataset=dataset_id,
            etl_version=ETL_VERSION,
            field_value=field_values[ETL_VERSION])
        bq.query(add_etl_query, project_id=project_id)

    update_statement = parse_update_statement(table_fields, field_values)
    if update_statement != '':
        q = UPDATE_QUERY.format(project=project_id,
                                dataset=dataset_id,
                                table=METADATA_TABLE,
                                statement=update_statement,
                                etl_version=ETL_VERSION,
                                etl_value=field_values[ETL_VERSION])
        bq.query(q, project_id=project_id)


if __name__ == '__main__':
    fields = resources.fields_for(METADATA_TABLE)
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
        copy_metadata_table(args.project_id, args.source_dataset,
                            args.target_dataset, fields)
    field_values_dict = dict(
        zip([field[NAME] for field in fields], [
            args.etl_version, args.ehr_source, args.ehr_cutoff_date,
            args.rdr_source, args.rdr_export_date, args.cdr_generation_date,
            args.qa_handoff_date, args.vocabulary_version
        ]))
    print(field_values_dict)
    if args.component == INSERT:
        add_metadata(args.target_dataset, args.project_id, fields,
                     field_values_dict)
