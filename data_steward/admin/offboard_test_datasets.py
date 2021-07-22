"""
An administrative utility to remove datasets from the test environment.

Original purpose is to remove datasets associated with developers who have 
left the project.
"""
# Python Imports
import argparse
import logging
import sys

# Third party imports
from google.cloud import bigquery

# Project imports
from utils.bq import get_client
from utils import pipeline_logging

LOGGER = logging.getLogger(__name__)


def delete_test_datasets(client, dataset_identifiers):
    """
    Delete the given datasets from the test environment.

    :param client: python client object.  Should be initialized to test project.
        Will fail the safety check if using any other project.
    :param dataset_identifiers: dataset ids to remove from the
        test project.  Will remove empty and non-empty datasets

    :raises:  RuntimeError if attempting to delete datasets from non-test
        environment.
    """
    # a fail safe to prevent deleting datasets in environments other than test.
    if 'test' not in client.project:
        raise RuntimeError(
            "Attempting to delete datasets out of non-test environment!!")

    for dset_id in dataset_identifiers:
        dataset_id = f'{client.project}.{dset_id}'

        client.delete_dataset(dataset_id,
                              delete_contents=True,
                              not_found_ok=True)

        LOGGER.info(f"Deleted dataset '{dataset_id}'.".format(dataset_id))


def get_args(raw_args=[]):
    """
    Argument parser for module.

    :param raw_args: If left empty, will read from command line.  Otherwise,
        will parse provided list.
    """
    parser = argparse.ArgumentParser(
        description='Parse developer offboarding arguments',
        formatter_class=argparse.RawDescriptionHelpFormatter)

    parser.add_argument('-p',
                        '--project_id',
                        action='store',
                        dest='project_id',
                        help=('Project to remove developer datasets from'),
                        required=True)
    parser.add_argument(
        '-n',
        '--names',
        action='store',
        dest='monnikers',
        nargs='+',
        help=('List of usernames to search for in dataset names.  '
              'May be the github username or personal name(s).'),
        required=True)
    parser.add_argument('-s',
                        '--console_log',
                        dest='console_log',
                        action='store_true',
                        help='Send logs to console')

    return parser.parse_args(raw_args)


def get_removal_candidates(client, id_list):
    """
    Determine dataset removal candidates based on dataset names.

    Given the client object, it will return a list of datasets whose name
    contains one of the searched ids in the list.

    :param client: python client object.  Expected to be instantiated to the
        test project.
    :param id_list: list of identifiers to search for in dataset names.
        Datasets whose names contain one of these identifiers will listed
        as a removal candidate.

    :returns: a list of strings where each string is a dataset_id.  Module
        will exist if no datasets are found that match the identifier.
    """
    datasets = list(client.list_datasets())  # Make an API request.
    project = client.project

    removal_list = [
        dset.dataset_id
        for dset in datasets
        for rem in id_list
        if rem in dset.dataset_id
    ]
    if removal_list:
        LOGGER.info(
            f"Datasets, {len(removal_list)} total, for removal in project {project}:"
        )
        for dataset in removal_list:
            LOGGER.info(f"\t{dataset}")
    else:
        LOGGER.info(
            f"{project} project does not contain any datasets.  Exiting.")
        sys.exit(0)

    return removal_list


def main(raw_args=[]):
    """
    Main function for deleting a user's test datasets.

    Can be called from command line or fram another module.
    """
    ARGS = get_args(raw_args)
    pipeline_logging.configure(add_console_handler=ARGS.console_log)
    LOGGER.info(f"Offboarding dev datasets called with: {ARGS}")

    client = get_client(ARGS.project_id)

    datasets = get_removal_candidates(client, ARGS.monnikers)

    # force the user to validate their choice
    proceed = input("\n\nAfter reviewing the above datasets,would you\n"
                    "like to proceed with deleting these datasets? This action "
                    "cannot be reversed.  [Y/y/N/n]:   ")

    if proceed.lower() != 'y':
        LOGGER.info(f"Consent to continue was not given.  "
                    f"User provided: {proceed}.  Exiting.")
        sys.exit(0)
    else:
        delete_test_datasets(client, datasets)


if __name__ == '__main__':
    main()
