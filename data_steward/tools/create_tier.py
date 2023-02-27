# Python imports
import argparse
import logging
from datetime import datetime

# Third party imports

# Project imports
from cdr_cleaner import clean_cdr
from cdr_cleaner.args_parser import add_kwargs_to_args
from common import CDR_SCOPES, PIPELINE_TABLES, ZIP3_SES_MAP, DE_IDENTIFIED
from constants.cdr_cleaner import clean_cdr as consts
from gcloud.bq import BigQueryClient
from tools import add_cdr_metadata
from utils import auth
from utils import pipeline_logging
from utils.parameter_validators import validate_release_tag_param

LOGGER = logging.getLogger(__name__)

TIER_LIST = ['controlled', 'registered']
DEID_STAGE_LIST = ['deid', 'deid_base', 'deid_clean', 'fitbit']


def validate_tier_param(tier):
    """
    helper function to validate the tier parameter passed is either 'controlled' or 'registered'

    :param tier: tier parameter passed through from either a list or command line argument
    :return: nothing, breaks if not valid
    """
    if tier.lower() not in TIER_LIST:
        msg = f"Parameter ERROR: {tier} is an incorrect input for the tier parameter, accepted: controlled or " \
              f"registered"
        LOGGER.error(msg)
        raise TypeError(msg)


def validate_deid_stage_param(deid_stage):
    """
    helper function to validate the deid_stage parameter passed is correct, must be 'deid', 'base' or 'clean'

    :param deid_stage: deid_stage parameter passed through from either a list or command line argument
    :return: nothing, breaks if not valid
    """
    if deid_stage not in DEID_STAGE_LIST:
        msg = f"Parameter ERROR: {deid_stage} is an incorrect input for the deid_stage parameter, accepted: deid, " \
              f"base, clean"
        LOGGER.error(msg)
        raise TypeError(msg)


def validate_create_tier_args(tier, stage, tag):
    """
    User defined helper function to validate that the tier, deid_stage, release_tag parameter
     follows the correct naming convention
    :param tier: tier parameter passed through from either a list or command line argument
    :param stage: deid_stage parameter passed through from either a list or command line argument
    :param tag: release tag parameter passed through either the command line arguments
    :return: None, breaks if not valid params are passed
    """
    validate_tier_param(tier)
    validate_deid_stage_param(stage)
    validate_release_tag_param(tag)


def get_dataset_name(tier, release_tag, deid_stage):
    """
    Helper function to create the dataset name based on the given criteria
    This function should return a name for the final dataset only (not all steps along the way)
    The function returns a string in the form: [C|R]{release_tag}_deid[_base|_clean]

    :param tier: controlled or registered tier intended for the output dataset
    :param release_tag: release tag for dataset in the format of YYYYq#r#
    :param deid_stage: deid stage (deid, base or clean)
    :return: a string for the dataset name
    """
    # validate parameters
    validate_create_tier_args(tier, deid_stage, release_tag)

    tier = tier[0].upper()

    dataset_name = f"{tier}{release_tag}_{deid_stage}"

    return dataset_name


def create_datasets(client, name, input_dataset, tier, release_tag):
    """
    Creates backup, staging, sandbox, and final datasets with the proper descriptions
    and tag/labels applied

    :param client: a BigQueryClient
    :param name: the base name of the dataset to be created
    :param input_dataset: name of the input dataset
    :param tier: tier parameter passed through from either a list or command line argument
    :param release_tag: release tag parameter passed through either the command line arguments
    :return: tuple of created dataset names
    """

    if not client:
        raise RuntimeError("Please specify a BigQueryClient object")
    if not name:
        raise RuntimeError(
            "Please specify the base name of the datasets to be created")
    if not input_dataset:
        raise RuntimeError("Please specify the name of the input dataset")
    if not tier:
        raise RuntimeError(
            "Please specify the tier intended for the output datasets")
    if not release_tag:
        raise RuntimeError(
            "Please specify the release tag for the dataset in the format of YYYY#q#r"
        )

    # Construct names of datasets need as part of the deid process
    final_dataset_id = name
    staging_dataset_id = f'{name}_{consts.STAGING}'
    sandbox_dataset_id = f'{name[1:]}_{consts.SANDBOX}'

    datasets = {
        consts.CLEAN: final_dataset_id,
        consts.STAGING: staging_dataset_id,
        consts.SANDBOX: sandbox_dataset_id
    }

    deid_datasets = [final_dataset_id, staging_dataset_id]

    # base labels and tags for the datasets
    base_labels_and_tags = {
        'owner': 'curation',
        'release_tag': release_tag,
        'data_tier': tier
    }

    description = f'dataset created from {input_dataset} for {tier}{release_tag} CDR run'

    # Creation of dataset objects and dataset label and description updates
    for phase, dataset_id in datasets.items():
        dataset_object = client.define_dataset(dataset_id, description,
                                               base_labels_and_tags)
        client.create_dataset(dataset_object, exists_ok=True)
        dataset = client.get_dataset(dataset_id)
        if dataset_id in deid_datasets:
            new_labels = client.update_labels_and_tags(dataset_id,
                                                       base_labels_and_tags, {
                                                           'phase': phase,
                                                           DE_IDENTIFIED: 'true'
                                                       })
            dataset.labels = new_labels
            dataset.description = f'{phase} {description}'
            client.update_dataset(dataset, ["labels", "description"])
        else:
            new_labels = client.update_labels_and_tags(
                dataset_id, base_labels_and_tags, {
                    'phase': phase,
                    DE_IDENTIFIED: 'false'
                })
            dataset.labels = new_labels
            dataset.description = f'{phase} {description}'
            client.update_dataset(dataset, ["labels", "description"])
        LOGGER.info(f'Updated dataset {dataset} with labels {new_labels}')

    return datasets


def create_tier(credentials_filepath, project_id, tier, input_dataset,
                release_tag, deid_stage, run_as, **kwargs):
    """
    This function is the main entry point for the deid process.
    It passes the required parameters to the implementing functions.

    :param credentials_filepath: filepath to credentials to access GCP
    :param project_id: project_id associated with the input dataset
    :param tier: controlled or registered tier intended for the output dataset
    :param input_dataset: name of the input dataset
    :param release_tag: release tag for dataset in the format of YYYYq#r#
    :param deid_stage: deid stage (deid, base or clean)
    :param run_as: email address of the service account to impersonate
    :return: name of created controlled or registered dataset
    """
    # validation of params
    validate_create_tier_args(tier, deid_stage, release_tag)

    # today's date for QA handoff
    qa_handoff_date = datetime.strftime(datetime.now(), '%Y-%m-%d')

    # get credentials and create client
    impersonation_creds = auth.get_impersonation_credentials(
        run_as, CDR_SCOPES, credentials_filepath)

    bq_client = BigQueryClient(project_id, credentials=impersonation_creds)

    # Get Final Dataset name
    final_dataset_name = get_dataset_name(tier, release_tag, deid_stage)

    # Create intermediary datasets and copy tables from input dataset to newly created dataset
    datasets = create_datasets(bq_client, final_dataset_name, input_dataset,
                               tier, release_tag)
    bq_client.copy_dataset(f'{project_id}.{input_dataset}',
                           f'{project_id}.{datasets[consts.STAGING]}')

    # Run cleaning rules
    cleaning_args = [
        '-p', project_id, '-d', datasets[consts.STAGING], '-b',
        datasets[consts.SANDBOX], '--data_stage', f'{tier}_tier_{deid_stage}',
        '--run_as', run_as, '--console_log'
    ]

    # Will update the qa_handoff_date to current date
    if 'base' in deid_stage:
        versions = add_cdr_metadata.get_etl_version(datasets[consts.STAGING],
                                                    project_id)
        if not versions:
            raise RuntimeError(
                'etl version does not exist, make sure _cdr_metadata table was created in combined step'
            )
        add_cdr_metadata.main([
            '--component', add_cdr_metadata.INSERT, '--project_id', project_id,
            '--target_dataset', datasets[consts.STAGING], '--qa_handoff_date',
            qa_handoff_date, '--etl_version', versions[0]
        ])

        if tier == 'controlled':
            bq_client.copy_table(
                f'{project_id}.{PIPELINE_TABLES}.{ZIP3_SES_MAP}',
                f'{project_id}.{datasets[consts.STAGING]}.{ZIP3_SES_MAP}')
    else:
        LOGGER.info(
            f'deid_stage was not base, no data inserted into _cdr_metadata table'
        )

    controlled_tier_cleaning_args = add_kwargs_to_args(cleaning_args, kwargs)
    clean_cdr.main(args=controlled_tier_cleaning_args)

    # Snapshot the staging dataset to final dataset
    bq_client.build_and_copy_contents(datasets[consts.STAGING],
                                      final_dataset_name)

    return datasets


def parse_deid_args(args=None):
    parser = argparse.ArgumentParser(
        description='Parse deid command line arguments')
    parser.add_argument('-c',
                        '--credentials_filepath',
                        dest='credentials_filepath',
                        action='store',
                        default='',
                        help='file path to credentials for GCP to access BQ',
                        required=False)
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
                        required=True)
    parser.add_argument('-t',
                        '--tier',
                        action='store',
                        dest='tier',
                        help='controlled or registered tier',
                        required=True,
                        choices=TIER_LIST)
    parser.add_argument('-i',
                        '--idataset',
                        action='store',
                        dest='idataset',
                        help='Name of the input dataset',
                        required=True)
    parser.add_argument(
        '-r',
        '--release_tag',
        action='store',
        dest='release_tag',
        help='release tag for dataset in the format of YYYYq#r#',
        required=True,
        type=validate_release_tag_param)
    parser.add_argument('-d',
                        '--deid_stage',
                        action='store',
                        dest='deid_stage',
                        help='deid stage (deid, base or clean)',
                        required=True,
                        choices=DEID_STAGE_LIST)
    parser.add_argument('-l',
                        '--console_log',
                        dest='console_log',
                        action='store_true',
                        required=False,
                        help='Log to the console as well as to a file.')

    common_args, unknown_args = parser.parse_known_args(args)
    custom_args = clean_cdr._get_kwargs(unknown_args)
    return common_args, custom_args


def main(raw_args=None):
    # Parses the required arguments and keyword arguments required by cleaning rules
    args, kwargs = parse_deid_args(raw_args)
    # Sets logging level
    pipeline_logging.configure(add_console_handler=args.console_log)
    # Identify the cleaning classes being run for specified data_stage
    # and validate if all the required arguments are supplied
    cleaning_classes = clean_cdr.DATA_STAGE_RULES_MAPPING[
        f'{args.tier}_tier_{args.deid_stage}']
    clean_cdr.validate_custom_params(cleaning_classes, **kwargs)

    # Runs create_tier in order to generate the {args.tier}_tier_{args.data_stage} datasets and apply cleaning rules
    datasets = create_tier(args.credentials_filepath, args.project_id,
                           args.tier, args.idataset, args.release_tag,
                           args.deid_stage, args.target_principal, **kwargs)
    return datasets


if __name__ == '__main__':
    main()
