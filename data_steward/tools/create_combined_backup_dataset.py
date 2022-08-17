"""
Combine data sets `ehr` and `rdr` to form another data set `combined`

 * Create all the CDM tables in `combined`

 * Load `person_id` of those who have consented to share EHR data in `combined.ehr_consent`

 * Copy all `rdr.person` records to `combined.person`

 * Copy `ehr.death` records that link to `combined.ehr_consent`

 * Load `combined.visit_mapping(dst_visit_occurrence_id, src_dataset, src_visit_occurrence_id)`
   with UNION ALL of:
     1) all `rdr.visit_occurrence_id`s and
     2) `ehr.visit_occurrence_id`s that link to `combined.ehr_consent`

 * Load tables `combined.{visit_occurrence, condition_occurrence, procedure_occurrence}` etc. from UNION ALL
   of `ehr` and `rdr` records that link to `combined.person`. Use `combined.visit_mapping.dest_visit_occurrence_id`
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
import common
import resources
from resources import mapping_table_for
from utils import auth, pipeline_logging
from gcloud.bq import BigQueryClient
from constants.tools import create_combined_backup_dataset as combine_consts
from tools import add_cdr_metadata

LOGGER = logging.getLogger(__name__)

WRITE_APPEND = 'WRITE_APPEND'
WRITE_TRUNCATE = 'WRITE_TRUNCATE'


def assert_tables_in(client: BigQueryClient, dataset_id: str):
    """
    Raise assertion error if any CDM tables missing from a dataset

    :param client: a BigQueryClient
    :param dataset_id: dataset to check for tables in
    """
    tables = client.list_tables(dataset_id)
    table_ids = set([table.table_id for table in tables])
    LOGGER.info(f'Confirming dataset, {dataset_id}, has tables: {tables}')
    if table_ids != set(combine_consts.TABLES_TO_PROCESS):
        raise RuntimeError(
            f'Dataset, {dataset_id}, is missing tables {set(combine_consts.TABLES_TO_PROCESS) - table_ids}.  '
            f'Aborting.')


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


def create_cdm_tables(client: BigQueryClient, combined_dataset_id: str):
    """
    Create all CDM tables

    :param client: BigQueryClient
    :param combined_dataset_id: Identifies combined dataaset name

    Note: Recreates any existing tables
    """
    for table in resources.CDM_TABLES:
        LOGGER.info(f'Creating table {combined_dataset_id}.{table}...')
        schema_list = client.get_table_schema(table_name=table)
        dest_table = f'{client.project}.{combined_dataset_id}.{table}'
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
                combined_dataset: str):
    """
    Create and load ehr consent table in combined dataset

    :param client: a BigQueryClient
    :param rdr_dataset_id: Identifies the name of rdr dataset
    :param combined_dataset: Identifies the name of combined dataset

    :return:
    """
    q = combine_consts.EHR_CONSENT_QUERY.render(
        dataset_id=rdr_dataset_id,
        source_value_ehr_consent=combine_consts.SOURCE_VALUE_EHR_CONSENT,
        concept_id_consent_permission_yes=combine_consts.
        CONCEPT_ID_CONSENT_PERMISSION_YES)
    fq_table_name = f'{client.project}.{combined_dataset}.{combine_consts.EHR_CONSENT_TABLE_ID}'
    table = bigquery.Table(fq_table_name)
    table = client.create_table(table, exists_ok=True)
    LOGGER.info(f'Query for {combine_consts.EHR_CONSENT_TABLE_ID} is {q}')
    query(client,
          q,
          dst_dataset_id=combined_dataset,
          dst_table_id=combine_consts.EHR_CONSENT_TABLE_ID,
          write_disposition=WRITE_APPEND)


def copy_ehr_table(client: BigQueryClient, table: str, unioned_ehr_dataset: str,
                   combined_dataset: str):
    """
    Copy table from EHR (consenting participants only) to the combined dataset without regenerating ids

    """
    fields = resources.fields_for(table)
    field_names = [field['name'] for field in fields]
    if 'person_id' not in field_names:
        raise RuntimeError(
            f'Cannot copy EHR table {table}. It is missing columns needed for consent filter'
        )
    q = combine_consts.COPY_EHR_QUERY.render(
        ehr_dataset_id=unioned_ehr_dataset,
        table=table,
        ehr_consent_table_id=combine_consts.EHR_CONSENT_TABLE_ID,
        combined_dataset_id=combined_dataset)
    LOGGER.info(f'Query for {table} is `{q}`')
    query(client, q, combined_dataset, table, WRITE_APPEND)


def mapping_query(domain_table: str, rdr_dataset: str, unioned_ehr_dataset: str,
                  combined_dataset: str):
    """
    Returns query used to get mapping of all records from RDR combined with EHR records of consented participants

    :param domain_table: one of the domain tables (e.g. 'visit_occurrence', 'condition_occurrence')
    :return:
    """
    return combine_consts.MAPPING_QUERY.render(
        rdr_dataset_id=rdr_dataset,
        ehr_dataset_id=unioned_ehr_dataset,
        combined_dataset_id=combined_dataset,
        domain_table=domain_table,
        mapping_constant=common.RDR_ID_CONSTANT,
        person_id_flag=resources.has_person_id(domain_table),
        ehr_consent_table_id=combine_consts.EHR_CONSENT_TABLE_ID)


def generate_combined_mapping_tables(client: BigQueryClient, domain_table: str,
                                     rdr_dataset: str, unioned_ehr_dataset: str,
                                     combined_dataset: str):
    """
    Create and load a mapping of all records from RDR combined with EHR records of consented participants

    :param client: Bigquery client
    :param domain_table: cdm table
    :return:
    """
    if domain_table in combine_consts.DOMAIN_TABLES:
        q = mapping_query(domain_table, rdr_dataset, unioned_ehr_dataset,
                          combined_dataset)
        mapping_table = mapping_table_for(domain_table)
        LOGGER.info(f'Query for {mapping_table} is {q}')
        fq_mapping_table = f'{client.project}.{combined_dataset}.{mapping_table}'
        schema = resources.fields_for(mapping_table)
        table = bigquery.Table(fq_mapping_table, schema=schema)
        table = client.create_table(table, exists_ok=True)
        query(client, q, combined_dataset, mapping_table, WRITE_APPEND)
    else:
        LOGGER.info(
            f'Excluding table {domain_table} from mapping query because it does not exist'
        )


def join_expression_generator(domain_table, combined_dataset):
    """
    adds table aliases as references to columns and generates join expression

    :param domain_table: Name of the cdm table
    :param combined_dataset_id: name of the datqset where the tables are present
    :return: returns cols and join expression strings.
    """
    field_names = [
        field['name'] for field in resources.fields_for(domain_table)
    ]
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
            col_expr = f'{field_name[:3]}.' + field_name

        elif field_name in primary_key:
            col_expr = 'm.' + field_name

        else:
            col_expr = field_name
        col_exprs.append(col_expr)
        cols = ', '.join(col_exprs)

    for key in combine_consts.FOREIGN_KEYS_FIELDS:
        if key in fields_to_join:
            if domain_table == combine_consts.PERSON_TABLE:
                table_alias = mapping_table_for(f'{key[:-3]}')
                join_expression.append(
                    combine_consts.LEFT_JOIN_PERSON.render(
                        dataset_id=combined_dataset,
                        prefix=key[:3],
                        field=key,
                        table=table_alias))

            elif domain_table == combine_consts.VISIT_DETAIL and key == combine_consts.VISIT_OCCURRENCE_ID:
                table_alias = mapping_table_for(f'{key[:-3]}')
                join_expression.append(
                    combine_consts.JOIN_VISIT.render(
                        dataset_id=combined_dataset,
                        prefix=key[:3],
                        field=key,
                        table=table_alias))

            else:
                table_alias = mapping_table_for(f'{key[:-3]}')
                join_expression.append(
                    combine_consts.LEFT_JOIN.render(dataset_id=combined_dataset,
                                                    prefix=key[:3],
                                                    field=key,
                                                    table=table_alias))
    full_join_expression = " ".join(join_expression)
    return cols, full_join_expression


def load_query(domain_table: str, rdr_dataset: str, unioned_ehr_dataset: str,
               combined_dataset: str):
    """
    Returns query used to load a domain table

    :param domain_table: one of the domain tables (e.g. 'visit_occurrence', 'condition_occurrence')
    :param rdr_dataset: identifies RDR dataset name
    :param unioned_ehr_dataset: identifies unioned ehr dataset name
    :param combined_dataset: identifies combined dataset name
    :return:
    """
    mapping_table = mapping_table_for(domain_table)
    cols, join_expression = join_expression_generator(domain_table,
                                                      combined_dataset)

    return combine_consts.LOAD_QUERY.render(
        cols=cols,
        domain_table=domain_table,
        rdr_dataset_id=rdr_dataset,
        ehr_dataset_id=unioned_ehr_dataset,
        mapping_table=mapping_table,
        join_expr=join_expression,
        combined_dataset_id=combined_dataset)


def load(client: BigQueryClient, domain_table: str, rdr_dataset: str,
         unioned_ehr_dataset: str, combined_dataset: str):
    """
    Load a domain table

    :param client: a BigQuery client object
    :param domain_table: one of the domain tables (e.g. 'visit_occurrence', 'condition_occurrence')
    :param rdr_dataset: identifies RDR dataset name
    :param unioned_ehr_dataset: identifies unioned ehr dataset name
    :param combined_dataset: identifies combined dataset name
    """
    q = load_query(domain_table, rdr_dataset, unioned_ehr_dataset,
                   combined_dataset)
    LOGGER.info(f'Query for {domain_table} is {q}')
    query(client, q, combined_dataset, domain_table, WRITE_APPEND)


def load_fact_relationship(client: BigQueryClient, rdr_dataset: str,
                           unioned_ehr_dataset: str, combined_dataset: str):
    """
    Load fact_relationship table

    :param client: a BigQuery client object
    :param rdr_dataset: Identifies the rdr dataset name
    :param unioned_ehr_dataset: Identifies unioned_ehr dataset name
    :param combined_dataset: Identifies combined dataset name

    :return: None
    """
    q = combine_consts.FACT_RELATIONSHIP_QUERY.render(
        rdr_dataset_id=rdr_dataset,
        combined_dataset_id=combined_dataset,
        mapping_measurement=mapping_table_for('measurement'),
        ehr_dataset=unioned_ehr_dataset,
        mapping_observation=mapping_table_for('observation'),
        measurement_domain_concept_id=common.MEASUREMENT_DOMAIN_CONCEPT_ID,
        observation_domain_concept_id=common.OBSERVATION_DOMAIN_CONCEPT_ID)
    LOGGER.info(f'Query for fact_relationship is {q}')
    query(client, q, combined_dataset, common.FACT_RELATIONSHIP, WRITE_APPEND)


def person_query(table_name: str, combined_dataset: str):
    """
    Maps location and care_Site id in person table

    :return: query
    """
    columns, join_expression = join_expression_generator(
        table_name, combined_dataset)
    return combine_consts.MAPPED_PERSON_QUERY.render(cols=columns,
                                                     dataset=combined_dataset,
                                                     table=table_name,
                                                     join_expr=join_expression)


def load_mapped_person(client: BigQueryClient, combined_dataset: str):
    q = person_query(common.PERSON, combined_dataset)
    LOGGER.info(f'Query for Person table is {q}')
    query(client,
          q,
          combined_dataset,
          common.PERSON,
          write_disposition=WRITE_TRUNCATE)


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
        args.run_as_email, common.CDR_SCOPES)

    client = BigQueryClient(args.project_id, credentials=impersonation_creds)

    combined_dataset = f'{args.release_tag}_combined_backup'
    LOGGER.info(
        f'Creating Combined backup dataset: `{client.project}.{combined_dataset}` '
    )
    combined_backup_desc = f'combined raw version of {args.rdr_dataset} + {args.unioned_ehr_dataset}'
    labels = {
        "phase": "backup",
        "release_tag": args.release_tag,
        "de_identified": "false"
    }
    combined_dataset_object = client.define_dataset(combined_dataset,
                                                    combined_backup_desc,
                                                    labels)
    client.create_dataset(combined_dataset_object)
    LOGGER.info(f'Created dataset `{client.project}.{combined_dataset}`')
    LOGGER.info('Creating destination CDM tables...')
    create_cdm_tables(client, combined_dataset)

    LOGGER.info(
        f'Copying vocabulary tables from {args.vocab_dataset} to {combined_dataset}'
    )
    client.copy_dataset(args.vocab_dataset, combined_dataset)
    LOGGER.info(
        f'Finished Copying vocabulary tables from {args.vocab_dataset} to {combined_dataset}'
    )

    LOGGER.info('EHR + RDR combine started')
    LOGGER.info('Verifying all CDM tables in EHR and RDR datasets...')
    assert_ehr_and_rdr_tables(client, args.rdr_dataset,
                              args.unioned_ehr_dataset)
    ehr_consent(client, args.rdr_dataset, combined_dataset)

    for table in combine_consts.RDR_TABLES_TO_COPY:
        LOGGER.info(f'Copying {table} table from RDR...')
        client.copy_table(f'{client.project}.{args.rdr_dataset}.{table}',
                          f'{client.project}.{combined_dataset}.{table}')

    LOGGER.info(
        f'Translating {combine_consts.EHR_TABLES_TO_COPY} table from EHR...')
    for table in combine_consts.EHR_TABLES_TO_COPY:
        LOGGER.info(f'Copying {table} table from EHR...')
        copy_ehr_table(client, table, args.unioned_ehr_dataset,
                       combined_dataset)

    LOGGER.info('Generating combined mapping tables ...')
    for domain_table in combine_consts.DOMAIN_TABLES:
        LOGGER.info(f'Mapping {domain_table}...')
        generate_combined_mapping_tables(client, domain_table, args.rdr_dataset,
                                         args.unioned_ehr_dataset,
                                         combined_dataset)

    LOGGER.info('Loading Domain tables ...')
    for domain_table in combine_consts.DOMAIN_TABLES:
        LOGGER.info(f'Loading {domain_table}...')
        load(client, domain_table, args.rdr_dataset, args.unioned_ehr_dataset,
             combined_dataset)

    LOGGER.info('Loading fact_relationship...')
    load_fact_relationship(client, args.rdr_dataset, args.unioned_ehr_dataset,
                           combined_dataset)
    LOGGER.info('Loading foreign key Mapped Person table...')
    load_mapped_person(client, combined_dataset)

    LOGGER.info(f'Adding _cdr_metadata table to {combined_dataset}')

    add_cdr_metadata.main([
        '--component', add_cdr_metadata.CREATE, '--project_id', client.project,
        '--target_dataset', combined_dataset
    ])
    today = datetime.now().strftime('%Y-%m-%d')
    add_cdr_metadata.main([
        '--component', add_cdr_metadata.INSERT, '--project_id', client.project,
        '--target_dataset', combined_dataset, '--etl_version',
        resources.get_git_tag(), '--ehr_source', args.unioned_ehr_dataset,
        '--ehr_cutoff_date', args.ehr_cutoff_date, '--rdr_source',
        args.rdr_dataset, '--cdr_generation_date', today,
        '--vocabulary_version', args.vocab_dataset, '--rdr_export_date',
        args.rdr_export_date
    ])
    LOGGER.info('EHR + RDR combine completed')


if __name__ == '__main__':
    main()
