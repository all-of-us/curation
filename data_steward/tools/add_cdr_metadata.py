"""
This module will populate the cdr_metadata table with CDR metadata in order to version control the pipeline for each
    release. The metadata to be included is etl_version, ehr_source, ehr_cutoff_date, rdr_source, rdr_export_date,
    cdr_generation_date, qa_handoff_date, and vocabulary_version.

Original Issues: DC-1378, DC-347
"""

# Python imports
import argparse

# Project imports
import bq_utils
import resources
from utils import bq
from common import JINJA_ENV
from cdr_cleaner import clean_cdr

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

ETL_VERSION_CHECK = JINJA_ENV.from_string("""
select {{etl}} from `{{dataset}}.{{table}}`
""")

ADD_ETL_METADATA_QUERY = JINJA_ENV.from_string("""
insert into `{{project}}.{{dataset}}.{{metadata_table}}` ({{etl_version}}) values(\'{{field_value}}\')
""")

UPDATE_QUERY = JINJA_ENV.from_string("""
update `{{project}}.{{dataset}}.{{table}}` set {{statement}} where {{etl_version}} = \'{{etl_value}}\'
""")

COPY_QUERY = JINJA_ENV.from_string("""
select * from `{{project}}.{{dataset}}.{{metadata_table}}`
""")


def create_metadata_table(dataset_id, fields_list):
    """
    Creates a metadata table in a given dataset.

    :param dataset_id: name of the dataset
    :param fields_list: name of the dataset
    :return: None
    """
    if not bq_utils.table_exists(METADATA_TABLE, dataset_id):
        bq_utils.create_table(table_id=METADATA_TABLE,
                              fields=fields_list,
                              dataset_id=dataset_id)


def copy_metadata_table(project_id, source_dataset_id, target_dataset_id,
                        table_fields):
    """
    Copies the metadata table

    :param project_id: identifies the project
    :param source_dataset_id: name of the source dataset
    :param target_dataset_id: name of the target dataset
    :param table_fields: field list of the table
    :return: None
    """
    create_metadata_table(target_dataset_id, table_fields)
    query = COPY_QUERY.render(project=project_id,
                              dataset=source_dataset_id,
                              metadata_table=METADATA_TABLE)
    bq_utils.query(query,
                   destination_dataset_id=target_dataset_id,
                   destination_table_id=METADATA_TABLE)


def parse_update_statement(table_fields, field_values):
    """
    Generates an update statement consisting of the field name corresponding value

    :param table_fields: field list of the table
    :param field_values: dictionary of field values passed as parameters
    :return: update statement string
    """
    statement_list = []
    field_types = dict()

    # Retrieves the data type of the field
    for field_name in table_fields:
        field_types[field_name[NAME]] = field_name[TYPE]

    for field_name in field_values:
        # Will generate update statements for date and string types if value is not empty and
        # field does not equal etl_version
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
    """
    Gets the etl version

    :param dataset_id: Name of the dataset
    :param project_id: Name of the project
    :return: etl version
    """
    etl_version = bq.query(ETL_VERSION_CHECK.render(etl=ETL_VERSION,
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
        add_etl_query = ADD_ETL_METADATA_QUERY.render(
            project=project_id,
            dataset=dataset_id,
            metadata_table=METADATA_TABLE,
            etl_version=ETL_VERSION,
            field_value=field_values[ETL_VERSION])
        bq.query(add_etl_query, project_id=project_id)

    update_statement = parse_update_statement(table_fields, field_values)
    if update_statement != '':
        q = UPDATE_QUERY.render(project=project_id,
                                dataset=dataset_id,
                                table=METADATA_TABLE,
                                statement=update_statement,
                                etl_version=ETL_VERSION,
                                etl_value=field_values[ETL_VERSION])
        bq.query(q, project_id=project_id)


def parse_cdr_metadata_args(args=None):
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
                        help='Identifies the dataset to copy metadata to')
    parser.add_argument('--source_dataset',
                        default=None,
                        help='Identifies the dataset to copy metadata from')

    for field in fields:
        parser.add_argument(f'--{field[NAME]}',
                            default=None,
                            help=f'{field[DESCRIPTION]}')

    cdr_metadata_args, unknown_args = parser.parse_known_args(args)
    custom_args = clean_cdr._get_kwargs(unknown_args)
    return cdr_metadata_args, custom_args


def main(raw_args=None):
    args, kwargs = parse_cdr_metadata_args(raw_args)

    fields = resources.fields_for(METADATA_TABLE)

    if args.component == CREATE:
        create_metadata_table(args.target_dataset, fields)
    if args.component == COPY:
        copy_metadata_table(args.project_id, args.source_dataset,
                            args.target_dataset, fields)

    if args.component == INSERT:
        field_values_dict = dict(
            zip([field[NAME] for field in fields], [
                args.etl_version, args.ehr_source, args.ehr_cutoff_date,
                args.rdr_source, args.rdr_export_date, args.cdr_generation_date,
                args.qa_handoff_date, args.vocabulary_version
            ]))
        add_metadata(args.target_dataset, args.project_id, fields,
                     field_values_dict)


if __name__ == '__main__':
    main()
