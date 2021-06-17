import logging

from cdr_cleaner import clean_cdr, args_parser
from common import FITBIT_TABLES
from utils import auth, bq, pipeline_logging
from constants.cdr_cleaner import clean_cdr as consts

LOGGER = logging.getLogger(__name__)

SCOPES = ['https://www.googleapis.com/auth/bigquery']


def create_fitbit_datasets(client, release_tag):
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
            "phase": phase,
            "release_tag": release_tag,
            "de_identified": "false"
        }
        dataset_object = bq.define_dataset(client.project,
                                           fitbit_datasets[phase],
                                           fitbit_desc[phase], labels)
        client.create_dataset(dataset_object)
        LOGGER.info(
            f'Created dataset `{client.project}.{fitbit_datasets[phase]}`')

    return fitbit_datasets


def copy_fitbit_tables(client, from_dataset, to_dataset, table_prefix):
    for table in FITBIT_TABLES:
        table_view = f'{client.project}.{from_dataset}.{table_prefix}{table}'
        output_table = f'{client.project}.{to_dataset}.{table}'
        client.copy_table(table_view, output_table)

    LOGGER.info(f'Copied fitbit tables from `{from_dataset}` to `{to_dataset}`')


def get_fitbit_parser():
    parser = clean_cdr.get_parser()
    parser.add_argument('-p',
                        '--curation_project',
                        action='store',
                        dest='curation_project_id',
                        help='Curation project to load the fitbit data into.',
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

    client = bq.get_client(args.curation_project_id,
                           credentials=impersonation_creds)

    # create staging, sandbox, and clean datasets with descriptions and labels
    fitbit_datasets = create_fitbit_datasets(client, args.release_tag)

    copy_fitbit_tables(client,
                       args.fitbit_dataset,
                       fitbit_datasets[consts.BACKUP],
                       table_prefix='v_')
    copy_fitbit_tables(client,
                       args.fitbit_dataset,
                       fitbit_datasets[consts.STAGING],
                       table_prefix='v_')

    common_cleaning_args = [
        '-p', args.project_id, '-d', fitbit_datasets[consts.STAGING], '-b',
        fitbit_datasets[consts.SANDBOX], '-s', '-a', consts.FITBIT
    ]
    fitbit_cleaning_args = args_parser.add_kwargs_to_args(
        common_cleaning_args, kwargs)

    clean_cdr.main(args=fitbit_cleaning_args)

    # Snapshot the staging dataset to final dataset
    bq.build_and_copy_contents(client, fitbit_datasets[consts.STAGING],
                               fitbit_datasets[consts.CLEAN])


if __name__ == '__main__':
    main()
