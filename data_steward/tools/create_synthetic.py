# Python imports
import argparse
import logging
import re
from datetime import datetime

# Third party imports

# Project imports
from cdr_cleaner import clean_cdr
from cdr_cleaner.args_parser import add_kwargs_to_args
from common import CDR_SCOPES
from constants.cdr_cleaner import clean_cdr as consts
from constants.tools import create_combined_backup_dataset as combine_consts
from gcloud.bq import BigQueryClient
from tools import import_rdr_omop
from tools.create_combined_backup_dataset import generate_combined_mapping_tables
from tools.recreate_person import update_person
from utils import auth, pipeline_logging

LOGGER = logging.getLogger(__name__)


def validate_release_tag_param(arg_value: str) -> str:
    """
    User defined helper function to validate that the release_tag parameter follows the correct naming convention

    :param arg_value: release tag parameter passed through either the command line arguments
    :return: arg_value
    """

    release_tag_regex = re.compile(r'[0-9]{4}q[0-9]r[0-9]')
    if not re.match(release_tag_regex, arg_value):
        msg = f"Parameter ERROR {arg_value} is in an incorrect format, accepted: YYYYq#r#"
        LOGGER.error(msg)
        raise argparse.ArgumentTypeError(msg)
    return arg_value


def synthetic_project(value: str) -> str:
    """
    Verifies this is executing in a non-production environment.

    :param value: The value of the command line argument
    """
    if 'stable' not in value:
        raise ValueError(f"Expecting a project_id for a stable "
                         f"environment.  Provided value is:\t{value}")

    return value


def create_datasets(client: BigQueryClient, name: str, input_dataset: str,
                    release_tag: str) -> dict:
    """
    Creates backup, staging, sandbox, and final datasets with the proper descriptions
    and tag/labels applied

    :param client: a BigQueryClient
    :param name: the base name of the dataset to be created
    :param input_dataset: name of the input dataset
    :param release_tag: release tag parameter passed through either the command line arguments
    :return: dictionary of created dataset names
    """

    if not client:
        raise RuntimeError("Specify a BigQueryClient object")
    if not name:
        raise RuntimeError(
            "Specify the base name of the datasets to be created")
    if not input_dataset:
        raise RuntimeError("Specify the name of the input dataset")
    if not release_tag:
        raise RuntimeError(
            "Specify the release tag for the dataset in the format of YYYY#q#r")

    # Construct names of datasets needed
    final_dataset_id = name
    staging_dataset_id = f'{name}_{consts.STAGING}'
    sandbox_dataset_id = f'{name}_{consts.SANDBOX}'

    datasets = {
        consts.CLEAN: final_dataset_id,
        consts.STAGING: staging_dataset_id,
        consts.SANDBOX: sandbox_dataset_id
    }

    # base labels and tags for the datasets
    base_labels_and_tags = {
        'release_tag': release_tag,
        'tier': consts.SYNTHETIC
    }

    description = f'dataset created from {input_dataset} for {release_tag} synthetic CDR'

    # Creation of dataset objects and dataset label and description updates
    for phase, dataset_id in datasets.items():
        dataset_object = client.define_dataset(dataset_id, description,
                                               base_labels_and_tags)
        client.create_dataset(dataset_object, exists_ok=True)
        dataset = client.get_dataset(dataset_id)
        new_labels = client.update_labels_and_tags(dataset_id,
                                                   base_labels_and_tags, {
                                                       'phase': phase,
                                                       'de-identified': 'false'
                                                   })
        dataset.labels = new_labels
        dataset.description = f'{phase} {description}' + dataset.description
        client.update_dataset(dataset, ["labels", "description"])
        LOGGER.info(f'Updated dataset {dataset} with labels {new_labels}')

    return datasets


def create_tier(project_id: str, input_dataset: str, release_tag: str,
                run_as: str, **kwargs) -> dict:
    """
    This function is the main entry point for the deid process.
    It passes the required parameters to the implementing functions.

    :param project_id: project_id associated with the input dataset
    :param input_dataset: name of the input dataset
    :param release_tag: release tag for dataset in the format of YYYYq#r#
    :param run_as: email address of the service account to impersonate
    :return: dict of created dataset names
    """
    # get credentials and create client
    impersonation_creds = auth.get_impersonation_credentials(run_as, CDR_SCOPES)

    bq_client = BigQueryClient(project_id, credentials=impersonation_creds)

    # Get Final Dataset name
    final_dataset_name = f"{release_tag}_synthetic"

    # Create intermediary datasets and copy tables from input dataset to newly created dataset
    datasets = create_datasets(bq_client, final_dataset_name, input_dataset,
                               release_tag)
    bq_client.copy_dataset(f'{project_id}.{input_dataset}',
                           f'{project_id}.{datasets[consts.STAGING]}')

    # 1. add mapping tables
    for domain_table in combine_consts.DOMAIN_TABLES:
        LOGGER.info(f'Mapping {domain_table}...')
        generate_combined_mapping_tables(bq_client, domain_table,
                                         datasets[consts.STAGING], '',
                                         datasets[consts.STAGING])

    # Run cleaning rules
    cleaning_args = [
        '-p', project_id, '-d', datasets[consts.STAGING], '-b',
        datasets[consts.SANDBOX], '--data_stage', consts.SYNTHETIC, '--run_as',
        run_as, '--console_log'
    ]

    synthetic_cleaning_args = add_kwargs_to_args(cleaning_args, kwargs)
    # run synthetic data rules.  will run synthetic extension table generation too.
    clean_cdr.main(args=synthetic_cleaning_args)

    # TODO:
    # 2. mimic publishing guidelines so the person table looks correct.  publish internally first to
    # verify all required datatypes exist.  Afterward, can copy to the correct dev environment.
    update_person(bq_client, datasets[consts.STAGING])

    # Snapshot the staging dataset to final dataset
    bq_client.build_and_copy_contents(datasets[consts.STAGING],
                                      final_dataset_name)

    return datasets


def parse_deid_args(args=None):
    parser = argparse.ArgumentParser(description='Parse command line arguments')
    parser.add_argument('--run_as',
                        dest='target_principal',
                        action='store',
                        help='Email address of service account to impersonate.',
                        required=True)
    parser.add_argument('-p',
                        '--project_id',
                        action='store',
                        dest='project_id',
                        help=('Project associated with the '
                              'input dataset.'),
                        type=synthetic_project,
                        required=True)
    parser.add_argument('-r',
                        '--release_tag',
                        action='store',
                        dest='release_tag',
                        help=('release tag for dataset '
                              'in the format of YYYYq#r#'),
                        required=True,
                        type=validate_release_tag_param)
    parser.add_argument('-b',
                        '--bucket_name',
                        action='store',
                        dest='bucket_name',
                        help=("Path to the rdr bucket "
                              "containing the data to load"),
                        required=True)
    parser.add_argument('-l',
                        '--console_log',
                        dest='console_log',
                        action='store_true',
                        required=False,
                        help='Log to the console as well as to a file.')
    parser.add_argument('--vocab_dataset',
                        dest='vocab_dataset',
                        action='store',
                        help=('Vocabulary dataset location.'),
                        required=True)

    common_args, unknown_args = parser.parse_known_args(args)
    custom_args = clean_cdr._get_kwargs(unknown_args)
    return common_args, custom_args


def main(raw_args=None) -> dict:
    # Parses the required arguments and keyword arguments required by cleaning rules
    args, kwargs = parse_deid_args(raw_args)
    # Sets logging level
    pipeline_logging.configure(add_console_handler=args.console_log)
    # Identify the cleaning classes being run for specified data_stage
    # and validate if all the required arguments are supplied
    cleaning_classes = clean_cdr.DATA_STAGE_RULES_MAPPING[consts.SYNTHETIC]
    clean_cdr.validate_custom_params(cleaning_classes, **kwargs)

    # load synthetic data from the bucket
    import_rdr_omop.main([
        '--rdr_bucket', args.bucket_name, '--run_as', args.target_principal,
        '--export_date',
        datetime.now().strftime("%Y-%m-%d"), '--curation_project',
        args.project_id, '--vocab_dataset', args.vocab_dataset, '--console_log'
    ])

    # # Creates synthetic dataset and runs a subset of cleaning rules marked for synthetic data
    # datasets = create_tier(args.credentials_filepath, args.project_id,
    #                        args.idataset, args.release_tag,
    #                        args.target_principal, **kwargs)

    # return datasets


if __name__ == '__main__':
    main()
