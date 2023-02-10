"""
This script copies a dataset from a source project to the output prod project
and runs the recreate_person tool.
"""

# Python imports
import logging
import argparse

# Project imports
from gcloud.bq import BigQueryClient
from tools.recreate_person import update_person
from utils import auth, pipeline_logging
from utils.parameter_validators import validate_output_release_tag_param

SCOPES = [
    'https://www.googleapis.com/auth/bigquery',
    'https://www.googleapis.com/auth/devstorage.read_write',
    'https://www.googleapis.com/auth/cloud-platform'
]
LOGGER = logging.getLogger(__name__)

TIER_LIST = ['controlled', 'registered']
DEID_STAGE_LIST = ['deid', 'base', 'clean']

# modes for running script
MODE_HOTFIX = 'hotfix'
MODE_NEW_CDR = 'new'

MODES = [MODE_HOTFIX, MODE_NEW_CDR]


def get_dataset_name(tier, release_tag, deid_stage):
    """
    Helper function to create the output prod dataset name based on the given criteria
    This function should return a name for the final dataset only (not all steps along the way)
    The function returns a string in the form: [C|R]{release_tag}[_base|_clean]

    :param tier: controlled or registered tier intended for the output dataset
    :param release_tag: release tag for dataset in the format of YYYYQ#R#
    :param deid_stage: deid stage (deid, base or clean)
    :return: a string for the dataset name
    """

    tier = tier[0].upper()
    release_tag = release_tag.upper()
    deid_stage = f'_{deid_stage}' if deid_stage == 'base' else ''

    dataset_name = f"{tier}{release_tag}{deid_stage}"

    return dataset_name


def get_arg_parser() -> argparse.ArgumentParser:
    """
    Copy dataset from curation project to output-prod
    """
    parent_argument_parser = argparse.ArgumentParser(description=__doc__,
                                                     add_help=False)
    parent_argument_parser.add_argument('--run_as',
                                        action='store',
                                        dest='run_as_email',
                                        help=('Service account email '
                                              'address to impersonate'),
                                        required=True)
    parent_argument_parser.add_argument(
        '-s',
        '--src_project_id',
        dest='src_project_id',
        action='store',
        help=('Identifies the project containing '
              'the source dataset'),
        required=True)
    parent_argument_parser.add_argument(
        '-o',
        '--output_prod_project_id',
        dest='output_prod_project_id',
        action='store',
        help='Identifies the output-prod project.',
        required=True)
    parent_argument_parser.add_argument('-d',
                                        '--src_dataset_id',
                                        dest='src_dataset_id',
                                        action='store',
                                        help='The source dataset to copy.',
                                        required=True)
    parent_argument_parser.add_argument('-r',
                                        '--release_tag',
                                        action='store',
                                        dest='release_tag',
                                        help=('release tag for dataset in '
                                              'the format of YYYYQ#R#'),
                                        type=validate_output_release_tag_param,
                                        required=True)
    parent_argument_parser.add_argument('-t',
                                        '--tier',
                                        action='store',
                                        dest='tier',
                                        help='controlled or registered tier',
                                        required=True,
                                        choices=TIER_LIST)
    parent_argument_parser.add_argument('--deid_stage',
                                        action='store',
                                        dest='deid_stage',
                                        help='deid stage (deid, base or clean)',
                                        required=True,
                                        choices=DEID_STAGE_LIST)

    argument_parser = argparse.ArgumentParser()

    subparsers = argument_parser.add_subparsers(
        help='Mode to execute copy script.')

    argument_parser_new = subparsers.add_parser(
        MODE_NEW_CDR,
        parents=[parent_argument_parser],
        help="Generate new output prod dataset with fitbit copy.")
    argument_parser_hotfix = subparsers.add_parser(
        MODE_HOTFIX,
        parents=[parent_argument_parser],
        help="Apply hotfix to output prod dataset.")

    argument_parser_new.add_argument('-f',
                                     '--fitbit_dataset',
                                     action='store',
                                     dest='fitbit_dataset_id',
                                     help='fitbit dataset to copy',
                                     required=True)

    argument_parser_new.set_defaults(func=generate_new)
    argument_parser_hotfix.set_defaults(func=generate_hotfix)

    return argument_parser


def generate_hotfix(args):
    """ Generate hotfix for existing output prod dataset  """
    generate_output_prod(args.tier, args.release_tag, args.deid_stage,
                         args.src_project_id, args.src_dataset_id,
                         args.output_prod_project_id, args.run_as_email)


def generate_new(args):
    """ Generate new output prod dataset, including copying fitbit dataset """
    generate_output_prod(args.tier,
                         args.release_tag,
                         args.deid_stage,
                         args.src_project_id,
                         args.src_dataset_id,
                         args.output_prod_project_id,
                         args.run_as_email,
                         copy_fitbit=True,
                         fitbit_dataset_id=args.fitbit_dataset_id)


def generate_output_prod(tier,
                         release_tag,
                         deid_stage,
                         src_project_id,
                         src_dataset_id,
                         output_prod_project_id,
                         run_as_email,
                         copy_fitbit=True,
                         fitbit_dataset_id=None):
    #Get credentials and instantiate client
    impersonation_creds = auth.get_impersonation_credentials(
        run_as_email, SCOPES)

    bq_client = BigQueryClient(output_prod_project_id,
                               credentials=impersonation_creds)

    #Create dataset with labels
    output_dataset_name = get_dataset_name(tier, release_tag, deid_stage)
    description = f'{deid_stage} dataset created from {src_dataset_id} for {tier}{release_tag} CDR run'
    labels = {
        'clean': 'yes' if deid_stage == 'clean' else 'no',
        'data_tier': tier.lower(),
        'release_tag': release_tag.lower()
    }

    LOGGER.info(
        f'Creating dataset {output_dataset_name} in {output_prod_project_id}...'
    )
    dataset_object = bq_client.define_dataset(output_dataset_name, description,
                                              labels)
    bq_client.create_dataset(dataset_object, exists_ok=False)

    # Optionally copy fitbit tables to source dataset
    if copy_fitbit and fitbit_dataset_id:
        LOGGER.info(
            f'Copying fitbit tables from dataset {src_project_id}.{fitbit_dataset_id} to {src_project_id}.{src_dataset_id}...'
        )

        _ = bq_client.copy_dataset(
            f'{src_project_id}.{fitbit_dataset_id}',
            f'{src_project_id}.{src_dataset_id}')

    #Copy tables from source to output-prod
    LOGGER.info(
        f'Copying tables from dataset {src_project_id}.{src_dataset_id} to {output_prod_project_id}.{output_dataset_name}...'
    )
    _ = bq_client.copy_dataset(
        f'{src_project_id}.{src_dataset_id}',
        f'{output_prod_project_id}.{output_dataset_name}')

    #Append extra columns to person table
    LOGGER.info(f'Appending extract columns to the person table...')
    update_person(bq_client, output_dataset_name)

    LOGGER.info(f'Completed successfully.')


if __name__ == '__main__':
    #Get arguments
    parser = get_arg_parser()
    args = parser.parse_args()

    #Set up pipeline logging
    pipeline_logging.configure(level=logging.DEBUG, add_console_handler=True)

    # Execute function based on selected mode
    args.func(args)
