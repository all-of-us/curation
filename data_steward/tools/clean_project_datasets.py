"""
An administrative utility to remove datasets.

Original purpose is to identify and remove datasets with a given
substring in the dataset name.
"""
# Python imports
import argparse
import logging

# Third party imports
from googleapiclient.errors import HttpError

# Project imports
from utils import bq
from utils import pipeline_logging

LOGGER = logging.getLogger(__name__)


def _delete_datasets(client, datasets_to_delete_list):
    """
    Deletes datasets using their dataset_ids

    :param client: client object associated with project to delete datasets from
    :param datasets_to_delete_list: list of dataset_ids to delete
    :return:
    """
    failed_to_delete = []
    for dataset in datasets_to_delete_list:
        dataset_id = f'{client.project}.{dataset}'
        try:
            client.delete_dataset(dataset_id,
                                  delete_contents=True,
                                  not_found_ok=True)
            LOGGER.info(f'Deleted dataset {dataset_id}')
        except HttpError:
            LOGGER.exception(f'Could not delete dataset {dataset_id}')
            failed_to_delete.append(dataset_id)

    if failed_to_delete:
        LOGGER.info(
            f'The following datasets could not be deleted: {failed_to_delete}')


def run_deletion(project_id, name_substrings):
    """
    Deletes datasets from project containing any of the name_substrings

    :param project_id: identifies the project
    :param name_substrings: Identifies substrings that help identify datasets to delete
    :return:
    """
    # make the developer running this script approve the environment.
    proceed = input(f'This will remove datasets from the `{project_id}` '
                    f'environment.\nAre you sure you with to proceed?  '
                    f'[Y/y/N/n]:  ')

    LOGGER.info(f'User entered: "{proceed}"')

    if proceed.lower() != 'y':
        LOGGER.info(f'User requested to exit the deletion script.\n'
                    f'Exiting clean_project_datasets script now.')
        return

    LOGGER.info('Continuing with dataset deletions...')

    client = bq.get_client(project_id)

    all_datasets = [
        dataset.dataset_id for dataset in list(client.list_datasets())
    ]

    datasets_with_substrings = [
        dataset for dataset in all_datasets for substring in name_substrings
        if substring in dataset
    ]

    LOGGER.info(f'{len(datasets_with_substrings)} Datasets marked for '
                f'deletion in project `{project_id}`: ')
    for dataset in datasets_with_substrings:
        LOGGER.info(f'\t{dataset}')

    LOGGER.info(f'After reviewing datasets, proceed?  This action '
                f'cannot be reversed.')
    response = get_response()

    if response == "Y":
        _delete_datasets(client, datasets_with_substrings)
    else:
        LOGGER.info("Proper consent was not given.  Aborting deletion.")

    LOGGER.info("Dataset deletion completed.")


# Make sure user types Y to proceed
def get_response():
    """Return input from user denoting yes/no"""
    prompt_text = 'Please press Y/n\n'
    response = input(prompt_text)
    while response not in ('Y', 'n', 'N'):
        response = input(prompt_text)
    return response


def get_arguments(raw_args=None):
    """
    Parse arguments.

    Can be instantiated from command line or other modules.
    """
    parser = argparse.ArgumentParser(
        description=
        'Deletes datasets containing specific strings in the dataset_id.',
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('-p',
                        '--project_id',
                        action='store',
                        dest='project_id',
                        help='Identifies the project to delete datasets from',
                        required=True)
    parser.add_argument(
        '-n',
        '--name_substrings',
        nargs='+',
        dest='name_substrings',
        help=('Identifies substrings that help identify datasets to delete. '
              'A dataset containing any of these substrings within in their '
              'dataset_id will be deleted.'),
        required=True)
    parser.add_argument('-s',
                        '--console_log',
                        dest='console_log',
                        action='store_false',
                        help='Send logs to console.  Turned on be default.')

    return parser.parse_args(raw_args)


def main(raw_args=None):
    args = get_arguments(raw_args)

    pipeline_logging.configure(add_console_handler=args.console_log)

    run_deletion(args.project_id, args.name_substrings)


if __name__ == '__main__':
    main()
