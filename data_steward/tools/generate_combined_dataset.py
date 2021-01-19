import argparse
import logging
import os

import cdm
import utils.bq as bq
from cdr_cleaner import clean_cdr, clean_cdr_engine
from tools import (combine_ehr_rdr, snapshot_by_query)
from utils.bq import table_copy, update_dataset
from utils.cdr_utils import get_project_id, get_git_tag

LOGGER = logging.getLogger(__name__)


def generate_combined_dataset(key_file, vocab_dataset, unioned_ehr_dataset,
                              rdr_dataset, validation_dataset,
                              dataset_release_tag, ehr_cutoff, rdr_export_date):
    """
    generates combined dataset and combined_release dataset

    :param key_file: path to service account key file
    :param vocab_dataset: identifies the latest vocabulary dataset
    :param unioned_ehr_dataset: identifies latest unioned_ehr_dataset 
    :param rdr_dataset: identifies latest rdr_dataset
    :param validation_dataset: identifies latest validation_results_dataset
    :param dataset_release_tag: CDR dataset_release tag
    :param ehr_cutoff: ehr submission cutoff date
    :param rdr_export_date: latest rdr export date
    :return: 
    """
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = key_file
    project_id = get_project_id(key_file)
    version = get_git_tag()
    os.environ['GOOGLE_CLOUD_PROJECT'] = project_id

    client = bq.get_client(project_id)

    combined = f"{dataset_release_tag}_combined"
    combined_backup = f"{combined}_backup"
    combined_sandbox = f"{combined}_sandbox"
    combined_staging = f"{combined}_staging"
    combined_staging_sandbox = f"{combined_staging}_sandbox"

    os.environ['RDR_DATASET_ID'] = rdr_dataset
    os.environ['UNIONED_DATASET_ID'] = unioned_ehr_dataset
    os.environ['COMBINED_DATASET_ID'] = combined_backup
    os.environ['BIGQUERY_DATASET_ID'] = unioned_ehr_dataset
    os.environ['VOCABULARY_DATASET'] = vocab_dataset
    os.environ['VALIDATION_RESULTS_DATASET_ID'] = validation_dataset
    # required by populate_route_ids cleaning rule
    # set env variable for cleaning rule remove_non_matching_participant.py

    bq.create_dataset(
        project_id, combined_backup,
        f'{version} combined raw version of  {rdr_dataset} + {unioned_ehr_dataset}',
        {
            'phase': 'backup',
            'release_tag': dataset_release_tag,
            'de_identified': 'False'
        })

    # Create the clinical tables for combined dataset
    cdm.create_all_tables(combined_backup)

    # Copy OMOP vocabulary to CDR EHR data set
    cdm.create_vocabulary_tables(combined_backup)
    table_copy(project_id, project_id, vocab_dataset, combined_backup)

    # Combine EHR and PPI data sets
    combine_ehr_rdr.main()

    # Add cdr_meta data table
    # TODO: Fix add_cdr_metadeta.py parameters

    # create an intermediary table to apply cleaning rules on
    bq.create_dataset(
        project_id, combined_staging,
        f'intermediary dataset to apply cleaning rules on {combined_backup}', {
            'phase': 'staging',
            'release_tag': dataset_release_tag,
            'de_identified': 'False'
        })
    table_copy(client, project_id, project_id, combined_backup,
               combined_staging)

    # create empty sandbox dataset to apply cleaning rules on staging dataset
    bq.create_dataset(
        project_id, combined_staging_sandbox,
        f'Sandbox created for storing records affected by the cleaning rules applied to {combined_staging}',
        {
            'phase': 'sandbox',
            'release_tag': dataset_release_tag,
            'de_identified': 'False'
        })

    os.environ['COMBINED_DATASET_ID'] = combined_staging
    os.environ['BIGQUERY_DATASET_ID'] = combined_staging
    data_stage = 'combined'

    # run cleaning_rules on combined staging dataset
    clean_cdr_engine.clean_dataset(
        project_id=project_id,
        dataset_id=combined_staging,
        sandbox_dataset_id=combined_staging_sandbox,
        rules=clean_cdr.DATA_STAGE_RULES_MAPPING[data_stage])

    # Create a snapshot dataset with the result
    snapshot_by_query.create_snapshot_dataset(project_id, combined_staging,
                                              combined)
    update_dataset(
        project_id, combined_staging_sandbox,
        f'{version} combined clean version of {rdr_dataset} + {unioned_ehr_dataset}',
        {
            'phase': 'clean',
            'release_tag': dataset_release_tag,
            'de_identified': 'False'
        })

    # copy sandbox dataset
    bq.create_dataset(
        project_id, combined_sandbox,
        f'Sandbox created for storing records affected by the cleaning rules applied to {combined}',
        {
            'phase': 'sandbox',
            'release_tag': dataset_release_tag,
            'de_identified': 'False'
        })

    table_copy(project_id, project_id, combined_staging_sandbox,
               combined_sandbox)

    combined_release = f"{combined}_release"

    # Create a dataset for data browser team
    bq.create_dataset(
        project_id, combined_staging,
        f'{version} Release version of combined dataset with {rdr_dataset} + {unioned_ehr_dataset}',
        {
            'phase': 'release',
            'release_tag': dataset_release_tag,
            'de_identified': 'False'
        })
    table_copy(project_id, project_id, combined, combined_release)


def get_args_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('-k',
                        '--key_file',
                        dest='key_file',
                        action='store',
                        help='Path to gcloud service account key file',
                        required=True)
    parser.add_argument(
        '-v',
        '--vocab_dataset',
        dest='vocab_dataset',
        action='store',
        help=
        'Identifies the vocabulary dataset needs to be used to generate the combined dataset.',
        required=True)
    parser.add_argument(
        '-u',
        '--unioned_ehr_dataset',
        dest='unioned_ehr_dataset',
        action='store',
        help=
        'Identifies the unioned_ehr dataset which is to be used to generate the combined dataset',
        required=True)
    parser.add_argument(
        '-r',
        '--rdr_dataset',
        dest='rdr_dataset',
        action='store',
        help=
        'Identifies the rdr dataset which is to be used to generate the combined dataset',
        required=True)
    parser.add_argument(
        '-v',
        '--validation_dataset',
        dest='validation_dataset',
        action='store',
        help=
        'Identifies the validation dataset which is to be used to by remove_non_matching_participant cleaning rule',
        required=True)
    parser.add_argument('-r',
                        '--dataset_release_tag',
                        dest='dataset_release_tag',
                        action='store',
                        help='Identifies the CDR release tag',
                        required=True)
    parser.add_argument('-ec',
                        '--ehr_cutoff',
                        dest='ehr_cut_off',
                        action='store',
                        help='Identifies the ehr submissions cut-off date',
                        required=True)
    parser.add_argument('-re',
                        '--rdr_export_date',
                        dest='rdr_export_date',
                        action='store',
                        help='Identifies the ehr submissions cut-off date',
                        required=True)
    return parser


if __name__ == '__main__':
    args_parser = get_args_parser()
    args = args_parser.parse_args()
    generate_combined_dataset(args.key_file, args.vocab_dataset,
                              args.unioned_ehr_dataset, args.rdr_dataset,
                              args.validation_dataset, args.dataset_release_tag,
                              args.ehr_cutoff, args.rdr_export_date)
