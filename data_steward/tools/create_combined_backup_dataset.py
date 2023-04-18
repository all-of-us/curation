"""
Combine data sets `ehr` and `rdr` to form another data set `combined_backup`

 * Create all the CDM tables in `combined_backup`

 * Load `person_id` of those who have consented to share EHR data in `combined_sandbox.ehr_consent`

 * Copy all `rdr.person` records to `combined_backup.person`

 * Copy `ehr.death` records that link to `combined_sandbox.ehr_consent`

 * Load `combined_backup.visit_mapping(dst_visit_occurrence_id, src_dataset, src_visit_occurrence_id)`
   with UNION ALL of:
     1) all `rdr.visit_occurrence_id`s and
     2) `ehr.visit_occurrence_id`s that link to `combined_sandbox.ehr_consent`

 * Load tables `combined_backup.{visit_occurrence, condition_occurrence, procedure_occurrence}` etc. from UNION ALL
   of `ehr` and `rdr` records that link to `combined_backup.person`. Use `combined_backup.visit_mapping.dest_visit_occurrence_id`
   for records that have a (valid) `visit_occurrence_id`.

## Notes
Assumptions made:
 * The RDR dataset exists. It must be loaded from the GCS bucket where RDR dumps are placed (i.e. using
 `import_rdr_omop.sh`).

TODO
 * Load fact relationship records from RDR
   - measurement: concept_id 21
   - observation: concept_id 27
 * Load `combined.<hpo>_observation` with records derived from values in `ehr.<hpo>_person`
 * Communicate to data steward EHR records not matched with RDR
"""

# Python Imports
import logging
from argparse import ArgumentParser
from datetime import datetime

# Third party imports
from google.cloud.exceptions import GoogleCloudError
from google.cloud import bigquery

# Project imports
from common import (AOU_DEATH, CDR_SCOPES, DEATH, FACT_RELATIONSHIP,
                    MEASUREMENT_DOMAIN_CONCEPT_ID,
                    OBSERVATION_DOMAIN_CONCEPT_ID, PERSON, RDR_ID_CONSTANT,
                    SURVEY_CONDUCT, VISIT_DETAIL)
from resources import (fields_for, get_git_tag, has_person_id,
                       mapping_table_for, CDM_TABLES)
from utils import auth, pipeline_logging
from gcloud.bq import BigQueryClient
from constants.bq_utils import WRITE_APPEND, WRITE_TRUNCATE
from constants.tools import create_combined_backup_dataset as combine_consts
from tools import add_cdr_metadata
from tools.create_combined_dataset import create_dataset

LOGGER = logging.getLogger(__name__)


def assert_tables_in(client: BigQueryClient, dataset_id: str):
    """
    Raise assertion error if any CDM tables missing from a dataset

    :param client: a BigQueryClient
    :param dataset_id: dataset to check for tables in
    """
    tables = client.list_tables(dataset_id)
    table_ids = set([table.table_id for table in tables])
    LOGGER.info(f'Confirming dataset, {dataset_id}, has tables: {table_ids}')
    for table in combine_consts.TABLES_TO_PROCESS:
        if table not in table_ids:
            raise RuntimeError(
                f'Dataset {dataset_id} is missing table {table}. Aborting.')


def assert_ehr_and_rdr_tables(client: BigQueryClient,
                              unioned_ehr_dataset_id: str, rdr_dataset_id: str):
    """
    Raise assertion error if any CDM tables missing from EHR or RDR dataset

    :param client: a BigQueryClient
    :param unioned_ehr_dataset_id: Identifies unioned_ehr dataset name
    :param rdr_dataset_id: Identifies the rdr dataset name

    :return: None
    """
    assert_tables_in(client, unioned_ehr_dataset_id)
    assert_tables_in(client, rdr_dataset_id)


def create_cdm_tables(client: BigQueryClient, combined_backup: str):
    """
    Create all CDM tables. NOTE AOU_DEATH is not included.

    :param client: BigQueryClient
    :param combined_backup: Combined backup dataset name
    :return: None

    Note: Recreates any existing tables
    """
    for table in CDM_TABLES:
        LOGGER.info(f'Creating table {combined_backup}.{table}...')
        schema_list = client.get_table_schema(table_name=table)
        dest_table = f'{client.project}.{combined_backup}.{table}'
        dest_table = bigquery.Table(dest_table, schema=schema_list)
        table = client.create_table(dest_table)  # Make an API request.
        LOGGER.info(f"Created table: `{table.table_id}`")


def query(client: BigQueryClient,
          q: str,
          dst_dataset_id: str = None,
          dst_table_id: str = None,
          write_disposition: str = None):
    """
    Run query and block until job is done

    :param client: a BigQueryClient
    :param q: SQL statement
    :param dst_dataset_id: if set, output is saved in a dataset with the specified id
    :param dst_table_id: if set, output is saved in a table with the specified id
    :param write_disposition: WRITE_TRUNCATE, WRITE_EMPTY, or WRITE_APPEND (default, to preserve schema)
    """
    job_config = bigquery.job.QueryJobConfig()
    if dst_table_id and dst_dataset_id:
        job_config.destination = f'{client.project}.{dst_dataset_id}.{dst_table_id}'
        job_config.write_disposition = write_disposition

    prefix = f'cb_{dst_table_id if dst_table_id else ""}_{datetime.now().strftime("%m%d%H%M%S")}_'
    query_job = client.query(q, job_config, job_id_prefix=prefix)
    result = query_job.result()
    LOGGER.info(f"Query with prefix: `{prefix}` has completed")

    if hasattr(result, 'errors') and result.errors:
        LOGGER.error(f"Error running job {result.job_id}: {result.errors}")
        raise GoogleCloudError(
            f"Error running job {result.job_id}: {result.errors}")


def ehr_consent(client: BigQueryClient, rdr_dataset_id: str,
                combined_sandbox: str):
    """
    Create and load ehr consent table in combined dataset

    :param client: a BigQueryClient
    :param rdr_dataset_id: Identifies the name of rdr dataset
    :param combined_sandbox: Identifies the name of combined sandbox dataset

    :return: None
    """
    q = combine_consts.EHR_CONSENT_QUERY.render(
        dataset_id=rdr_dataset_id,
        source_value_ehr_consent=combine_consts.SOURCE_VALUE_EHR_CONSENT,
        concept_id_consent_permission_yes=combine_consts.
        CONCEPT_ID_CONSENT_PERMISSION_YES)
    fq_table_name = f'{client.project}.{combined_sandbox}.{combine_consts.EHR_CONSENT_TABLE_ID}'
    table = bigquery.Table(fq_table_name)
    table = client.create_table(table, exists_ok=True)
    LOGGER.info(
        f'Query for {combined_sandbox}.{combine_consts.EHR_CONSENT_TABLE_ID} is {q}'
    )
    query(client,
          q,
          dst_dataset_id=combined_sandbox,
          dst_table_id=combine_consts.EHR_CONSENT_TABLE_ID,
          write_disposition=WRITE_APPEND)


def mapping_query(domain_table: str, rdr_dataset: str, unioned_ehr_dataset: str,
                  combined_sandbox: str):
    """
    Returns query used to get mapping of all records from RDR combined with EHR records of consented participants

    :param domain_table: one of the domain tables (e.g. 'visit_occurrence', 'condition_occurrence')
    :param rdr_dataset: rdr dataset identifier
    :param unioned_ehr_dataset: unioned_ehr dataset identifier
    :param combined_sandbox: combined_sandbox dataset identifier
    :return:
    """
    return combine_consts.MAPPING_QUERY.render(
        rdr_dataset_id=rdr_dataset,
        ehr_dataset_id=unioned_ehr_dataset,
        combined_sandbox_dataset_id=combined_sandbox,
        domain_table=domain_table,
        mapping_constant=RDR_ID_CONSTANT,
        person_id_flag=has_person_id(domain_table),
        ehr_consent_table_id=combine_consts.EHR_CONSENT_TABLE_ID)


def generate_combined_mapping_tables(client: BigQueryClient, domain_table: str,
                                     rdr_dataset: str, unioned_ehr_dataset: str,
                                     combined_backup: str,
                                     combined_sandbox: str):
    """
    Create and load a mapping of all records from RDR combined with EHR records of consented participants

    :param client: Bigquery client
    :param domain_table: cdm table
    :param rdr_dataset: rdr dataset identifier
    :param unioned_ehr_dataset: unioned_ehr dataset identifier
    :param combined_backup: combined_backup dataset identifier
    :param combined_sandbox: combined_sandbox dataset identifier
    :return:
    """
    if domain_table in combine_consts.DOMAIN_TABLES + [SURVEY_CONDUCT]:
        q = mapping_query(domain_table, rdr_dataset, unioned_ehr_dataset,
                          combined_sandbox)
        mapping_table = mapping_table_for(domain_table)
        LOGGER.info(f'Query for {combined_backup}.{mapping_table} is {q}')
        fq_mapping_table = f'{client.project}.{combined_backup}.{mapping_table}'
        schema = fields_for(mapping_table)
        table = bigquery.Table(fq_mapping_table, schema=schema)
        table = client.create_table(table, exists_ok=True)
        query(client, q, combined_backup, mapping_table, WRITE_APPEND)
    else:
        LOGGER.info(
            f'Excluding table {domain_table} from mapping query because it does not exist'
        )


def join_expression_generator(domain_table, combined_backup):
    """
    adds table aliases as references to columns and generates join expression

    :param domain_table: Name of the cdm table
    :param combined_backup: name of the dataset where the tables are present
    :return: returns cols and join expression strings.
    """
    field_names = [field['name'] for field in fields_for(domain_table)]
    fields_to_join = []
    primary_key = []
    join_expression = []
    col_exprs = []
    cols = ''
    for field_name in field_names:
        if field_name == f'{domain_table}_id' and field_name != 'person_id':
            primary_key.append(field_name)

        elif field_name in combine_consts.FOREIGN_KEYS_FIELDS:
            fields_to_join.append(field_name)

        if field_name in fields_to_join:
            col_expr = f'{field_name[:3] + "_" + field_name[-7:-3] if "_" in field_name else field_name[:3]}.' \
                       + field_name

        elif field_name in primary_key:
            col_expr = 'm.' + field_name

        else:
            col_expr = field_name
        col_exprs.append(col_expr)
        cols = ', '.join(col_exprs)

    for key in combine_consts.FOREIGN_KEYS_FIELDS:
        if key in fields_to_join:
            if domain_table == PERSON:
                table_alias = mapping_table_for(f'{key[:-3]}')
                join_expression.append(
                    combine_consts.LEFT_JOIN_PERSON.render(
                        dataset_id=combined_backup,
                        prefix=
                        f'{key[:3] + "_" + key[-7:-3] if "_" in key else key[:3]}',
                        field=key,
                        table=table_alias))

            elif domain_table == VISIT_DETAIL and key == combine_consts.VISIT_OCCURRENCE_ID:
                table_alias = mapping_table_for(f'{key[:-3]}')
                join_expression.append(
                    combine_consts.JOIN_VISIT.render(
                        dataset_id=combined_backup,
                        prefix=
                        f'{key[:3] + "_" + key[-7:-3] if "_" in key else key[:3]}',
                        field=key,
                        table=table_alias))

            else:
                table_alias = mapping_table_for(f'{key[:-3]}')
                join_expression.append(
                    combine_consts.LEFT_JOIN.render(
                        dataset_id=combined_backup,
                        prefix=
                        f'{key[:3] + "_" + key[-7:-3] if "_" in key else key[:3]}',
                        field=key,
                        table=table_alias))
    full_join_expression = " ".join(join_expression)
    return cols, full_join_expression


def load_query(domain_table: str, rdr_dataset: str, unioned_ehr_dataset: str,
               combined_backup: str):
    """
    Returns query used to load a domain table

    :param domain_table: one of the domain tables (e.g. 'visit_occurrence', 'condition_occurrence')
    :param rdr_dataset: identifies RDR dataset name
    :param unioned_ehr_dataset: identifies unioned ehr dataset name
    :param combined_backup: identifies combined backup dataset name
    :return:
    """
    mapping_table = mapping_table_for(domain_table)
    cols, join_expression = join_expression_generator(domain_table,
                                                      combined_backup)

    return combine_consts.LOAD_QUERY.render(
        cols=cols,
        domain_table=domain_table,
        rdr_dataset_id=rdr_dataset,
        ehr_dataset_id=unioned_ehr_dataset,
        mapping_table=mapping_table,
        join_expr=join_expression,
        combined_backup_dataset_id=combined_backup)


def load(client: BigQueryClient, domain_table: str, rdr_dataset: str,
         unioned_ehr_dataset: str, combined_backup: str):
    """
    Load a domain table

    :param client: a BigQuery client object
    :param domain_table: one of the domain tables (e.g. 'visit_occurrence', 'condition_occurrence')
    :param rdr_dataset: identifies RDR dataset name
    :param unioned_ehr_dataset: identifies unioned ehr dataset name
    :param combined_backup: identifies combined dataset name
    """
    q = load_query(domain_table, rdr_dataset, unioned_ehr_dataset,
                   combined_backup)
    LOGGER.info(f'Query for {combined_backup}.{domain_table} is {q}')
    query(client, q, combined_backup, domain_table, WRITE_APPEND)


def load_fact_relationship(client: BigQueryClient, rdr_dataset: str,
                           unioned_ehr_dataset: str, combined_backup: str):
    """
    Load fact_relationship table

    :param client: a BigQuery client object
    :param rdr_dataset: Identifies the rdr dataset name
    :param unioned_ehr_dataset: Identifies unioned_ehr dataset name
    :param combined_backup: Identifies combined backup dataset name

    :return: None
    """
    q = combine_consts.FACT_RELATIONSHIP_QUERY.render(
        rdr_dataset_id=rdr_dataset,
        combined_backup_dataset_id=combined_backup,
        mapping_measurement=mapping_table_for('measurement'),
        ehr_dataset=unioned_ehr_dataset,
        mapping_observation=mapping_table_for('observation'),
        measurement_domain_concept_id=MEASUREMENT_DOMAIN_CONCEPT_ID,
        observation_domain_concept_id=OBSERVATION_DOMAIN_CONCEPT_ID)
    LOGGER.info(f'Query for {combined_backup}.{FACT_RELATIONSHIP} is {q}')
    query(client, q, combined_backup, FACT_RELATIONSHIP, WRITE_APPEND)


def person_query(table_name: str, combined_backup: str):
    """
    Maps location and care_Site id in person table

    :return: query
    """
    columns, join_expression = join_expression_generator(
        table_name, combined_backup)
    return combine_consts.MAPPED_PERSON_QUERY.render(cols=columns,
                                                     dataset=combined_backup,
                                                     table=table_name,
                                                     join_expr=join_expression)


def load_mapped_person(client: BigQueryClient, combined_backup: str):
    q = person_query(PERSON, combined_backup)
    LOGGER.info(f'Query for {combined_backup}.{PERSON} table is {q}')
    query(client, q, combined_backup, PERSON, write_disposition=WRITE_TRUNCATE)


def create_load_aou_death(bq_client, project_id, combined_dataset, rdr_dataset,
                          unioned_ehr_dataset) -> None:
    """Create and load AOU_DEATH table.
    :param project_id: project containing the datasets
    TODO Add comments
    """
    query = combine_consts.LOAD_AOU_DEATH.render(
        project=project_id,
        combined_dataset=combined_dataset,
        rdr_dataset=rdr_dataset,
        unioned_ehr_dataset=unioned_ehr_dataset,
        aou_death=AOU_DEATH,
        death=DEATH)
    job = bq_client.query(query)
    _ = job.result()

    query = combine_consts.UPDATE_PRIMARY_DEATH.render(
        project=project_id,
        combined_dataset=combined_dataset,
        aou_death=AOU_DEATH)
    job = bq_client.query(query)
    _ = job.result()


def parse_combined_args(raw_args=None):
    parser = ArgumentParser(
        description='Arguments pertaining to an combined dataset generation')

    parser.add_argument('--run_as',
                        action='store',
                        dest='run_as_email',
                        help='Service account email address to impersonate',
                        required=True)
    parser.add_argument(
        '--project_id',
        action='store',
        dest='project_id',
        help='Curation project to create a combined_dataset in.',
        required=True)
    parser.add_argument(
        '--release_tag',
        action='store',
        dest='release_tag',
        help='Release tag for naming and labeling the cleaned dataset with.',
        required=True)
    parser.add_argument(
        '--rdr_dataset',
        action='store',
        dest='rdr_dataset',
        help='rdr dataset dataset used to generate combined dataset',
        required=True)
    parser.add_argument(
        '--unioned_ehr_dataset',
        action='store',
        dest='unioned_ehr_dataset',
        help='unioned_ehr dataset dataset used to generate combined dataset',
        required=True)
    parser.add_argument(
        '--vocab_dataset',
        action='store',
        dest='vocab_dataset',
        help='Vocabulary dataset used by RDR to create this data dump.',
        required=True)
    parser.add_argument('--ehr_cutoff_date',
                        action='store',
                        dest='ehr_cutoff_date',
                        required=True,
                        help='date to truncate the combined data to')
    parser.add_argument('--rdr_export_date',
                        action='store',
                        dest='rdr_export_date',
                        required=True,
                        help='date rdr etl was run and submitted data to us.')
    parser.add_argument('-l',
                        '--console_log',
                        dest='console_log',
                        action='store_true',
                        required=False,
                        help='Log to the console as well as to a file.')

    return parser.parse_args(raw_args)


def main(raw_args=None):
    args = parse_combined_args(raw_args)
    pipeline_logging.configure(level=logging.INFO,
                               add_console_handler=args.console_log)

    impersonation_creds = auth.get_impersonation_credentials(
        args.run_as_email, CDR_SCOPES)

    client = BigQueryClient(args.project_id, credentials=impersonation_creds)

    combined_backup = create_dataset(client, args.release_tag, 'backup')

    LOGGER.info('Creating destination CDM tables...')
    create_cdm_tables(client, combined_backup)

    LOGGER.info(
        f'Copying vocabulary tables from {args.vocab_dataset} to {combined_backup}'
    )
    client.copy_dataset(args.vocab_dataset, combined_backup)
    LOGGER.info(
        f'Finished Copying vocabulary tables from {args.vocab_dataset} to {combined_backup}'
    )

    LOGGER.info('EHR + RDR combine started')
    LOGGER.info('Verifying all CDM tables in EHR and RDR datasets...')
    assert_ehr_and_rdr_tables(client, args.rdr_dataset,
                              args.unioned_ehr_dataset)

    combined_sandbox = create_dataset(client, args.release_tag, 'sandbox')
    ehr_consent(client, args.rdr_dataset, combined_sandbox)

    for table in combine_consts.RDR_TABLES_TO_COPY:
        LOGGER.info(f'Copying {table} table from RDR...')
        client.copy_table(f'{client.project}.{args.rdr_dataset}.{table}',
                          f'{client.project}.{combined_backup}.{table}')

    LOGGER.info('Generating combined mapping tables ...')
    for domain_table in combine_consts.DOMAIN_TABLES + [SURVEY_CONDUCT]:
        LOGGER.info(f'Mapping {domain_table}...')
        generate_combined_mapping_tables(client, domain_table, args.rdr_dataset,
                                         args.unioned_ehr_dataset,
                                         combined_backup, combined_sandbox)

    LOGGER.info('Loading Domain tables ...')
    for domain_table in combine_consts.DOMAIN_TABLES:
        LOGGER.info(f'Loading {domain_table}...')
        load(client, domain_table, args.rdr_dataset, args.unioned_ehr_dataset,
             combined_backup)

    LOGGER.info('Loading fact_relationship...')
    load_fact_relationship(client, args.rdr_dataset, args.unioned_ehr_dataset,
                           combined_backup)
    LOGGER.info('Loading foreign key Mapped Person table...')
    load_mapped_person(client, combined_backup)

    logging.info(f'Creating and loading {AOU_DEATH}...')
    create_load_aou_death(client, args.project_id, combined_backup,
                          args.rdr_dataset, args.unioned_ehr_dataset)
    logging.info(f'Completed {AOU_DEATH} load.')

    LOGGER.info(f'Adding _cdr_metadata table to {combined_backup}')
    add_cdr_metadata.main([
        '--component', add_cdr_metadata.CREATE, '--project_id', client.project,
        '--target_dataset', combined_backup
    ])
    today = datetime.now().strftime('%Y-%m-%d')
    add_cdr_metadata.main([
        '--component', add_cdr_metadata.INSERT, '--project_id', client.project,
        '--target_dataset', combined_backup, '--etl_version',
        get_git_tag(), '--ehr_source', args.unioned_ehr_dataset,
        '--ehr_cutoff_date', args.ehr_cutoff_date, '--rdr_source',
        args.rdr_dataset, '--cdr_generation_date', today,
        '--vocabulary_version', args.vocab_dataset, '--rdr_export_date',
        args.rdr_export_date
    ],
                          bq_client=client)
    LOGGER.info('EHR + RDR combine completed')


if __name__ == '__main__':
    main()
