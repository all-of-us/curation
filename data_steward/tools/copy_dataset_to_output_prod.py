"""
This script copies a dataset from a source project to the output prod project
and runs the recreate_person tool.
"""

# Python imports
import logging
import argparse

# Project imports
from utils import bq, auth, pipeline_logging
from tools.create_tier import (TIER_LIST, DEID_STAGE_LIST, get_dataset_name)
from tools.recreate_person import update_person

SCOPES = [
    'https://www.googleapis.com/auth/bigquery',
    'https://www.googleapis.com/auth/devstorage.read_write',
    'https://www.googleapis.com/auth/cloud-platform'
]
LOGGER = logging.getLogger(__name__)


def get_arg_parser() -> argparse.ArgumentParser:
    """
    Copy dataset from curation project to output-prod
    """
    argument_parser = argparse.ArgumentParser(description=__doc__)
    argument_parser.add_argument(
        '--run_as',
        action='store',
        dest='run_as_email',
        help='Service account email address to impersonate',
        required=True)
    argument_parser.add_argument(
        '-s',
        '--src_project_id',
        dest='src_project_id',
        action='store',
        help='Identifies the project containing the source dataset',
        required=True)
    argument_parser.add_argument('-o',
                                 '--output_prod_project_id',
                                 dest='output_prod_project_id',
                                 action='store',
                                 help='Identifies the output-prod project.',
                                 required=True)
    argument_parser.add_argument('-d',
                                 '--src_dataset_id',
                                 dest='src_dataset_id',
                                 action='store',
                                 help='The source dataset to copy.',
                                 required=True)
    argument_parser.add_argument(
        '-r',
        '--release_tag',
        action='store',
        dest='release_tag',
        help='release tag for dataset in the format of YYYYq#r#',
        required=True)
    argument_parser.add_argument('-t',
                                 '--tier',
                                 action='store',
                                 dest='tier',
                                 help='controlled or registered tier',
                                 required=True,
                                 choices=TIER_LIST)
    argument_parser.add_argument('--deid_stage',
                                 action='store',
                                 dest='deid_stage',
                                 help='deid stage (deid, base or clean)',
                                 required=True,
                                 choices=DEID_STAGE_LIST)

    return argument_parser


if __name__ == '__main__':
    #Get arguments
    parser = get_arg_parser()
    args = parser.parse_args()

    #Set up pipeline logging
    pipeline_logging.configure(level=logging.DEBUG, add_console_handler=True)

    #Get credentials and instantiate client
    impersonation_creds = auth.get_impersonation_credentials(
        args.run_as_email, SCOPES)

    client = bq.get_client(args.output_prod_project_id,
                           credentials=impersonation_creds)

    #Create dataset with labels
    output_dataset_name = get_dataset_name(args.tier, args.release_tag,
                                           args.deid_stage)
    description = f'{args.deid_stage} dataset created from {args.src_dataset_id} for {args.tier}{args.release_tag} CDR run'
    labels = {
        'clean': 'yes' if args.deid_stage == 'deid_clean' else 'no',
        'data_tier': args.tier,
        'release_tag': args.release_tag
    }

    LOGGER.info(
        f'Creating dataset {output_dataset_name} in {args.output_prod_project_id}...'
    )
    dataset_object = bq.define_dataset(args.output_prod_project_id,
                                       output_dataset_name, description, labels)
    client.create_dataset(dataset_object, exists_ok=False)

    #Copy tables from source to destination
    LOGGER.info(
        f'Copying tables from dataset {args.src_project_id}.{args.src_dataset_id} to {args.output_prod_project_id}.{output_dataset_name}...'
    )
    bq.copy_datasets(client, f'{args.src_project_id}.{args.src_dataset_id}',
                     f'{args.output_prod_project_id}.{output_dataset_name}')

    #Append extra columns to person table
    LOGGER.info(f'Appending extract columns to the person table...')
    update_person(client, args.output_prod_project_id, output_dataset_name)

    LOGGER.info(f'Completed successfully.')
