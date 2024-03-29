#!/usr/bin/env bash
# This Script automates the process of generating the rdr_snapshot and apply rdr cleaning rules

# Python standard imports
import logging
from argparse import ArgumentParser

# Third-party imports
from google.cloud.bigquery.job import CopyJobConfig, WriteDisposition

# Project level imports
from cdr_cleaner import clean_cdr
from cdr_cleaner.args_parser import add_kwargs_to_args
from gcloud.bq import BigQueryClient
import app_identity
from resources import mapping_table_for
from utils import auth, pipeline_logging
from common import (AOU_DEATH, CDR_SCOPES, DEATH, METADATA, PID_RID_MAPPING,
                    QUESTIONNAIRE_RESPONSE_ADDITIONAL_INFO, FACT_RELATIONSHIP,
                    COPE_SURVEY_MAP, VOCABULARY_TABLES, BIGQUERY_DATASET_ID,
                    EHR_CONSENT_VALIDATION, WEAR_CONSENT, CDM_SOURCE, COHORT,
                    COHORT_ATTRIBUTE, PDR_WITHDRAWALS_LIST, PDR_EHR_LIST)

LOGGER = logging.getLogger(__name__)


def parse_rdr_args(raw_args=None):
    parser = ArgumentParser(
        description='Arguments pertaining to cleaning an RDR dataset.')

    parser.add_argument('--run_as',
                        action='store',
                        dest='run_as_email',
                        help='Service account email address to impersonate',
                        required=True)
    parser.add_argument(
        '--release_tag',
        action='store',
        dest='release_tag',
        help='Release tag for naming and labeling the cleaned dataset with.',
        required=True)
    parser.add_argument('--curation_project',
                        action='store',
                        dest='curation_project_id',
                        help='Curation project to load the RDR data into.',
                        required=True)
    parser.add_argument('--rdr_dataset',
                        action='store',
                        dest='rdr_dataset',
                        help='RDR dataset to backup and clean.',
                        required=True)
    parser.add_argument('-l',
                        '--console_log',
                        dest='console_log',
                        action='store_true',
                        required=False,
                        help='Log to the console as well as to a file.')
    parser.add_argument(
        '--truncation_date',
        action='store',
        dest='truncation_date',
        required=False,
        help=('date to truncate the RDR data to.  The cleaning rule defaults '
              'to the current date if unset.'))
    parser.add_argument(
        '--export_date',
        action='store',
        dest='export_date',
        required=False,
        help=
        'Raw rdr export date. store_pid_rid_mappings default to current date if unset'
    )

    common_args, unknown_args = parser.parse_known_args(raw_args)
    custom_args = clean_cdr._get_kwargs(unknown_args)
    return common_args, custom_args


def main(raw_args=None):
    """
    Clean an RDR import.

    Assumes you are passing arguments either via command line or a
    list.
    """
    args, kwargs = parse_rdr_args(raw_args)

    pipeline_logging.configure(level=logging.INFO,
                               add_console_handler=args.console_log)

    # specific check on truncation_date. It should not cause a failure if it is not set.
    if not args.truncation_date:
        LOGGER.info('truncation_date is unset.  It will default to the current '
                    'date in the truncation cleaning rule.')

    # validate we've got all required data before continuing
    cleaning_classes = clean_cdr.DATA_STAGE_RULES_MAPPING.get('rdr')
    clean_cdr.validate_custom_params(cleaning_classes, **kwargs)

    # get credentials and create client
    impersonation_creds = auth.get_impersonation_credentials(
        args.run_as_email, CDR_SCOPES)

    bq_client = BigQueryClient(args.curation_project_id,
                               credentials=impersonation_creds)

    # create staging, sandbox, and clean datasets with descriptions and labels
    datasets = create_datasets(bq_client, args.rdr_dataset, args.release_tag)

    job_config = CopyJobConfig(write_disposition=WriteDisposition.WRITE_EMPTY)
    bq_client.copy_dataset(input_dataset=args.rdr_dataset,
                           output_dataset=datasets.get('staging'),
                           job_config=job_config)
    LOGGER.info(
        f'RDR dataset COPY from `{args.rdr_dataset}` to `{datasets.get("staging")}` has completed'
    )

    # Create mapping tables. aou_death and Metadata do not need mapping tables so they are excluded.
    # death does not exist in the RDR staging so it is not in the list.
    domain_tables = [
        table.table_id for table in bq_client.list_tables(
            f'{bq_client.project}.{datasets.get("staging")}')
    ]
    skip_tables = [
        AOU_DEATH, COPE_SURVEY_MAP, PID_RID_MAPPING, WEAR_CONSENT,
        QUESTIONNAIRE_RESPONSE_ADDITIONAL_INFO, EHR_CONSENT_VALIDATION,
        CDM_SOURCE, COHORT, COHORT_ATTRIBUTE, PDR_WITHDRAWALS_LIST, PDR_EHR_LIST
    ] + VOCABULARY_TABLES

    for domain_table in domain_tables:
        if domain_table in skip_tables:
            continue
        else:
            if domain_table != METADATA:
                logging.info(f'Mapping {domain_table}...')
                mapping(bq_client, datasets.get("staging"), domain_table)
            drop_src_id(bq_client, datasets.get("staging"), domain_table)

    # clean the RDR staging dataset
    cleaning_args = [
        '-p', args.curation_project_id, '-d',
        datasets.get('staging', 'UNSET'), '-b',
        datasets.get('sandbox', 'UNSET'), '--data_stage', 'rdr',
        '--truncation_date', args.truncation_date, '--export_date',
        args.export_date, '--run_as', args.run_as_email
    ]

    # Create an empty DEATH for clean RDR. Actual data is in AOU_DEATH.
    _ = bq_client.create_tables(
        [f"{bq_client.project}.{datasets.get('staging', 'UNSET')}.{DEATH}"])

    all_cleaning_args = add_kwargs_to_args(cleaning_args, kwargs)
    clean_cdr.main(args=all_cleaning_args)

    bq_client.build_and_copy_contents(datasets.get('staging', 'UNSET'),
                                      datasets.get('clean', 'UNSET'))

    # update sandbox description and labels
    sandbox_dataset = bq_client.get_dataset(datasets.get(
        'sandbox', 'UNSET'))  # Make an API request.
    sandbox_dataset.description = (
        f'Sandbox created for storing records affected by the cleaning '
        f'rules applied to {datasets.get("clean")}')
    sandbox_dataset.labels['phase'] = 'sandbox'
    sandbox_dataset = bq_client.update_dataset(
        sandbox_dataset, ["description"])  # Make an API request.

    full_dataset_id = f'{sandbox_dataset.project}.{sandbox_dataset.dataset_id}'
    LOGGER.info(
        f'Updated dataset `{full_dataset_id}` with description `{sandbox_dataset.description}`'
    )

    LOGGER.info(f'RDR snapshot and cleaning, '
                f'`{bq_client.project}.{datasets.get("clean")}`, is complete.')


def create_datasets(client, rdr_dataset, release_tag):
    rdr_clean = f'{release_tag}_rdr'
    rdr_staging = f'{rdr_clean}_staging'
    rdr_sandbox = f'{rdr_clean}_sandbox'

    staging_desc = f'Intermediary dataset to apply cleaning rules on {rdr_dataset}'
    labels = {
        "owner": "curation",
        "phase": "staging",
        "release_tag": release_tag,
        "de_identified": "false"
    }
    staging_dataset_object = client.define_dataset(rdr_staging, staging_desc,
                                                   labels)
    client.create_dataset(staging_dataset_object)
    LOGGER.info(f'Created dataset `{client.project}.{rdr_staging}`')

    sandbox_desc = (f'Sandbox created for storing records affected by the '
                    f'cleaning rules applied to {rdr_staging}')
    labels["phase"] = "sandbox"
    sandbox_dataset_object = client.define_dataset(rdr_sandbox, sandbox_desc,
                                                   labels)
    client.create_dataset(sandbox_dataset_object)
    LOGGER.info(f'Created dataset `{client.project}.{rdr_sandbox}`')

    version = 'implement getting software version'
    clean_desc = (f'{version} clean version of {rdr_dataset}')
    labels["phase"] = "clean"
    clean_dataset_object = client.define_dataset(rdr_clean, clean_desc, labels)
    client.create_dataset(clean_dataset_object)
    LOGGER.info(f'Created dataset `{client.project}.{rdr_clean}`')

    return {'clean': rdr_clean, 'staging': rdr_staging, 'sandbox': rdr_sandbox}


def drop_src_id(client, dataset, table):
    """
    Drop the src_id field from a table

    :param client: a BigQueryClient
    :param dataset: identifies the BQ dataset containing the input table
    :param table: identifies the table containing the src_id field
    """
    query = f'''
        ALTER TABLE `{client.project}.{dataset}.{table}`
        DROP COLUMN src_id
    '''
    query_job = client.query(query)
    query_job.result()


def mapping(client, input_dataset_id, domain_table):
    """
    Create and load a  mappping table that stores the domain id and src_id.
    Note: Overwrites destination table if it already exists

    :param client: a BigQueryClient
    :param input_dataset_id: identifies dataset containing the domain table.
    :param domain_table: name of domain table to create mapping table for.
    :return:
    """
    query = mapping_query(domain_table, input_dataset_id, client.project)
    mapping_table = mapping_table_for(domain_table)
    logging.info(f'Query for {mapping_table} is {query}')
    query_job = client.query(query)
    query_job.result()


def mapping_query(table_name, dataset_id=None, project_id=None):
    """
    Get query used to generate the new mapping table

    :param table_name: name of CDM table
    :param dataset_id: identifies the BQ dataset containing the input table
    :param project_id: identifies the GCP project containing the dataset
    :return: the query
    """
    if dataset_id is None:
        dataset_id = BIGQUERY_DATASET_ID
    if project_id is None:
        project_id = app_identity.get_application_id()

    domain_id = f'{table_name}_id'
    if table_name == FACT_RELATIONSHIP:
        query = f'''
            CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.{mapping_table_for(table_name)}` (
                {domain_id} INTEGER,
                src_id INTEGER
            ) 
        '''

    else:
        query = f'''
            CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.{mapping_table_for(table_name)}` AS (
            SELECT
                dt.{domain_id}, dt.src_id
            FROM
                `{project_id}.{dataset_id}.{table_name}` as dt 
            )
        '''
    return query


if __name__ == '__main__':
    main()
