"""
Generate and clean fitbit dataset
"""
import logging
from argparse import ArgumentParser, RawDescriptionHelpFormatter
from datetime import datetime

from google.cloud.bigquery import Table

from cdr_cleaner import clean_cdr, args_parser
from common import FITBIT_TABLES, JINJA_ENV
from utils import auth, pipeline_logging
from gcloud.bq import BigQueryClient
from constants.cdr_cleaner import clean_cdr as consts

LOGGER = logging.getLogger(__name__)

SCOPES = [
    'https://www.googleapis.com/auth/bigquery',
    'https://www.googleapis.com/auth/cloud-platform'
]

INSERT_QUERY = JINJA_ENV.from_string("""
INSERT INTO `{{fq_dest_table}}` ({{fields}})
SELECT {{fields_casted}}
FROM `{{client.project}}.{{from_dataset}}.{{table_prefix}}{{table}}`""")


def create_fitbit_datasets(client, release_tag):
    """
    Creates staging, sandbox, backup and clean datasets with descriptions and labels

    :param client: a BigQueryClient
    :param release_tag: string of the form "YYYYqNrN"
    :return: dict of dataset names with keys 'clean', 'backup', 'staging', 'sandbox'
    """
    fitbit_datasets = {
        consts.CLEAN: f'{release_tag}_fitbit',
        consts.BACKUP: f'{release_tag}_fitbit_backup',
        consts.STAGING: f'{release_tag}_fitbit_staging',
        consts.SANDBOX: f'{release_tag}_fitbit_sandbox'
    }

    fitbit_desc = {
        consts.CLEAN:
            f'Cleaned version of {fitbit_datasets[consts.BACKUP]}',
        consts.BACKUP:
            f'Backup dataset during generation of {fitbit_datasets[consts.STAGING]}',
        consts.STAGING:
            f'Intermediary dataset to apply cleaning rules on {fitbit_datasets[consts.BACKUP]}',
        consts.SANDBOX:
            (f'Sandbox created for storing records affected by the '
             f'cleaning rules applied to {fitbit_datasets[consts.STAGING]}'),
    }

    for phase in fitbit_datasets:
        labels = {
            "owner": "curation",
            "phase": phase,
            "release_tag": release_tag,
            "de_identified": "false"
        }
        dataset_object = client.define_dataset(fitbit_datasets[phase],
                                               fitbit_desc[phase], labels)
        client.create_dataset(dataset_object)
        LOGGER.info(
            f'Created dataset `{client.project}.{fitbit_datasets[phase]}`')

    return fitbit_datasets


def cast_to_schema_type(field, schema_type):
    """
    generates cast expression to the type specified by the schema

    :param field: field name
    :param schema_type: type of the field as specified in the schema
    :return: string of cast expression
    """
    bq_int_float = {'integer': 'INT64', 'float': 'FLOAT64'}

    if schema_type.lower() not in bq_int_float:
        col = f'SAFE_CAST({field} AS {schema_type.upper()}) AS {field}'
    else:
        col = f'SAFE_CAST({field} AS {bq_int_float[schema_type.lower()]}) AS {field}'
    return col


def copy_fitbit_tables_from_views(client, from_dataset, to_dataset,
                                  table_prefix):
    """
    Copies tables from views with prefix

    :param client: a BigQueryClient
    :param from_dataset: dataset containing views
    :param to_dataset: dataset to create tables
    :param table_prefix: prefix added to table_ids
    :return:
    """
    for table in FITBIT_TABLES:
        schema_list = client.get_table_schema(table)
        fq_dest_table = f'{client.project}.{to_dataset}.{table}'
        dest_table = Table(fq_dest_table, schema=schema_list)
        dest_table = client.create_table(dest_table)
        LOGGER.info(f'Created empty table {fq_dest_table}')

        fields_name_str = ',\n'.join([item.name for item in schema_list])
        fields_casted_str = ',\n'.join([
            cast_to_schema_type(item.name, item.field_type)
            for item in schema_list
        ])
        content_query = INSERT_QUERY.render(fq_dest_table=fq_dest_table,
                                            fields=fields_name_str,
                                            fields_casted=fields_casted_str,
                                            client=client,
                                            from_dataset=from_dataset,
                                            table_prefix=table_prefix,
                                            table=table)
        job = client.query(content_query)
        job.result()

    LOGGER.info(f'Copied fitbit tables from `{from_dataset}` to `{to_dataset}`')


def validate_date_string(date_string):
    """
    Validates the date string is a valid date in the YYYY-MM-DD format.

    If the string is valid, it returns the string.  Otherwise, it raises either
    a ValueError or TypeError.

    :param date_string: The string to validate

    :return:  a valid date string
    :raises:  A ValueError if the date string is not a valid date or
        doesn't conform to the specified format.
    """
    datetime.strptime(date_string, '%Y-%m-%d')
    return date_string


def get_fitbit_parser():
    parser = ArgumentParser(description=__doc__,
                            formatter_class=RawDescriptionHelpFormatter)
    parser.add_argument('-p',
                        '--project_id',
                        action='store',
                        dest='project_id',
                        help='Curation project containing fitbit data',
                        required=True)
    parser.add_argument('-f',
                        '--fitbit_dataset',
                        action='store',
                        dest='fitbit_dataset',
                        help='fitbit dataset to backup and clean.',
                        required=True)
    parser.add_argument('-s',
                        '--console_log',
                        dest='console_log',
                        action='store_true',
                        required=False,
                        help='Log to the console as well as to a file.')
    parser.add_argument('-e',
                        '--run_as_email',
                        action='store',
                        dest='run_as_email',
                        help='Service account email address to impersonate',
                        required=True)
    parser.add_argument(
        '-r',
        '--release_tag',
        action='store',
        dest='release_tag',
        help='Release tag for naming and labeling the cleaned dataset with.',
        required=True)

    parser.add_argument(
        '-c',
        '--reference_dataset_id',
        action='store',
        dest='reference_dataset_id',
        help='Combined dataset to use as reference containing valid PIDs',
        required=True)

    parser.add_argument(
        '-q',
        '--api_project_id',
        action='store',
        dest='api_project_id',
        help='Identifies the RDR project for participant summary API',
        required=True)

    parser.add_argument(
        '--truncation_date',
        dest='truncation_date',
        action='store',
        help=('Cutoff date for data based on <table_name>_date fields.  '
              'Should be in the form YYYY-MM-DD.'),
        required=True,
        type=validate_date_string)

    return parser


def main(raw_args=None):
    """
    Truncate and store fitbit data.

    Assumes you are passing arguments either via command line or a
    list.
    """
    parser = get_fitbit_parser()
    args, kwargs = clean_cdr.fetch_args_kwargs(parser, raw_args)

    pipeline_logging.configure(level=logging.INFO,
                               add_console_handler=args.console_log)

    # Identify the cleaning classes being run for specified data_stage
    # and validate if all the required arguments are supplied
    cleaning_classes = clean_cdr.DATA_STAGE_RULES_MAPPING[consts.FITBIT]
    clean_cdr.validate_custom_params(cleaning_classes, **kwargs)

    # get credentials and create client
    impersonation_creds = auth.get_impersonation_credentials(
        args.run_as_email, SCOPES)

    bq_client = BigQueryClient(args.project_id, credentials=impersonation_creds)

    # create staging, sandbox, backup and clean datasets with descriptions and labels
    fitbit_datasets = create_fitbit_datasets(bq_client, args.release_tag)

    copy_fitbit_tables_from_views(bq_client,
                                  args.fitbit_dataset,
                                  fitbit_datasets[consts.BACKUP],
                                  table_prefix='v_')
    bq_client.copy_dataset(
        f'{args.project_id}.{fitbit_datasets[consts.BACKUP]}',
        f'{args.project_id}.{fitbit_datasets[consts.STAGING]}')

    common_cleaning_args = [
        '-p',
        args.project_id,
        '-d',
        fitbit_datasets[consts.STAGING],
        '-b',
        fitbit_datasets[consts.SANDBOX],
        '-s',
        '-a',
        consts.FITBIT,
        '--run_as',
        args.run_as_email,
        '--api_project_id',
        args.api_project_id,
        '--reference_dataset_id',
        args.reference_dataset_id,
        '--truncation_date',
        args.truncation_date,
    ]

    fitbit_cleaning_args = args_parser.add_kwargs_to_args(
        common_cleaning_args, kwargs)

    clean_cdr.main(args=fitbit_cleaning_args)

    # Snapshot the staging dataset to final dataset
    bq_client.build_and_copy_contents(fitbit_datasets[consts.STAGING],
                                      fitbit_datasets[consts.CLEAN])


if __name__ == '__main__':
    main()
