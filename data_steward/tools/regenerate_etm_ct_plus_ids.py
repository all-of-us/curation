"""
Regenerate Primary Key IDs in a Controlled Tier Plus (CT+) Dataset for ETM Tables.

The four Explore the Mind tables (delaydiscounting, emorecog, flanker, gradcpt) are not
in CDM_TABLES, so regenerate_ct_plus_ids.py leaves them to its runtime schema detection
path, which remaps person_id and copies every other column through. Their sitting_id
primary key therefore reaches CT+ identical to its CT value, and a holder of both tiers
can join on it, align rows one to one, and recover the CT to CT+ person_id
correspondence for every participant with ETM data. That is the alignment the whole
re-key exists to prevent.

This script is the CT+ counterpart of regenerate_etm_rt_ids.py. Run it AFTER
regenerate_ct_plus_ids.py, against the same input, output and mapping datasets. It
replaces the four ETM tables that run copied, and touches nothing else.

It differs from regenerate_etm_rt_ids.py in five ways, each of which is a defect in that
script rather than a tier difference:

1) sitting_id is a random permutation rather than ROW_NUMBER() ORDER BY sitting_id. An
   order preserving draw is undone by sorting, which is the whole exposure.

2) The sitting_id mapping is appended to across releases rather than rebuilt, so a row
   published once keeps its CT+ sitting_id. The RT script writes WRITE_TRUNCATE and
   reassigns every id on every run.

3) The person join matches the CT+ src_table_id stamp from person_mapping_src_table_id()
   rather than a hardcoded Registered Tier view name. Matching the wrong stamp would
   hand CT+ the RT person_ids with no error.

4) Writes go through run_query_to_table(), so the destination project comes from
   --project_id and a failed job fails the run. bq_utils.query does neither.

5) Rows dropped by the mapping joins are counted and raise, rather than vanishing. The
   RT script inner joins both mappings and reports nothing.

Usage:
  python regenerate_etm_ct_plus_ids.py \
    --project_id <project> \
    --input_dataset_id <controlled_tier_dataset> \
    --output_dataset_id <controlled_tier_plus_dataset> \
    --pipeline_dataset_id <pipeline_tables_dataset> \
    --ct_plus_ids_view <rdr_participant_research_ids_view> \
    --mapping_dataset_id <mapping_tables_dataset>
"""
import argparse
import logging

from google.cloud.bigquery import Table

from common import PERSON
from gcloud.bq import BigQueryClient
from tools.regenerate_ct_plus_ids import (
    DEFAULT_MAPPING_NAMESPACE, assert_person_ids_have_not_moved,
    assert_person_mapping_is_ct_plus, assert_person_mapping_is_one_to_one,
    mapping_table_for, person_mapping_src_table_id, run_query_to_table)
from utils import pipeline_logging

LOGGER = logging.getLogger(__name__)

# The ETM tables, matching ETM_TABLES in regenerate_etm_rt_ids.py.
ETM_TABLES = ['delaydiscounting', 'emorecog', 'flanker', 'gradcpt']

# Every ETM table keys on this rather than on <table>_id, which is why these tables
# cannot go through regenerate_ct_plus_ids.py's mapping loop as they stand.
ETM_ID_COLUMN = 'sitting_id'


def mapping_query(table_name,
                  input_dataset_id,
                  project_id,
                  mapping_namespace,
                  mapping_dataset_id,
                  append=False):
    """
    Get the query that assigns new sitting_id values for one ETM table.

    Mirrors the domain table branch of regenerate_ct_plus_ids.mapping_query(), including
    the append semantics, and differs only in keying on sitting_id instead of
    <table>_id. Kept as its own function rather than parameterising the original,
    because the original derives its id column from the table name.

    :param table_name: name of the ETM table
    :param input_dataset_id: identifies the BQ dataset containing the input table
    :param project_id: identifies the GCP project containing the dataset
    :param mapping_namespace: constant stamped into src_table_id
    :param mapping_dataset_id: identifies the dataset where mapping tables are stored
    :param append: if True, preserve existing mappings and only draw ids for new rows
    :return: the query
    """
    mapping_table = mapping_table_for(table_name)
    src_column = f'src_{ETM_ID_COLUMN}'

    # When appending, start above every id already handed out and skip source ids that
    # already have one, so previously published rows keep their CT+ sitting_id.
    if append:
        offset_expr = (
            f'(SELECT COALESCE(MAX({ETM_ID_COLUMN}), 0) '
            f'FROM `{project_id}.{mapping_dataset_id}.{mapping_table}`)')
        where_clause = f"""
    WHERE NOT EXISTS (
        SELECT 1
        FROM `{project_id}.{mapping_dataset_id}.{mapping_table}` mt
        WHERE mt.{src_column} = t.{src_column} AND
              mt.src_table_id = '{mapping_namespace}.{table_name}'
    )"""
    else:
        offset_expr = '0'
        where_clause = ''

    return f'''
    SELECT
        '{mapping_namespace}.{table_name}' AS src_table_id,
        {src_column},
        {offset_expr} + ROW_NUMBER() OVER (ORDER BY shuffle_key) AS {ETM_ID_COLUMN}
    FROM (
        -- RAND() is drawn outside the DISTINCT: pulling it into the same SELECT
        -- would make every duplicate of {ETM_ID_COLUMN} a distinct row and survive it
        SELECT {src_column}, RAND() AS shuffle_key
        FROM (
            SELECT DISTINCT {ETM_ID_COLUMN} AS {src_column}
            FROM `{project_id}.{input_dataset_id}.{table_name}`
        )
    ) t
    {where_clause}
    '''


def mapping(table_name, input_dataset_id, project_id, mapping_namespace,
            mapping_dataset_id):
    """
    Create or extend the table that assigns new sitting_id values.

    :param table_name: name of the ETM table
    :param input_dataset_id: identifies dataset with source data
    :param project_id: identifies the GCP project that contains the datasets
    :param mapping_namespace: constant stamped into src_table_id
    :param mapping_dataset_id: identifies the dataset where mapping tables are stored
    """
    mapping_namespace = mapping_namespace or DEFAULT_MAPPING_NAMESPACE
    mapping_table = mapping_table_for(table_name)
    client = BigQueryClient(project_id)

    append = client.table_exists(mapping_table, mapping_dataset_id)
    if append:
        LOGGER.info(f"Appending new '{table_name}' ids to {mapping_table}. "
                    f'Existing mappings are preserved.')
        write_disposition = 'WRITE_APPEND'
    else:
        LOGGER.info(f'Creating mapping table {mapping_table}...')
        write_disposition = 'WRITE_TRUNCATE'

    q = mapping_query(table_name, input_dataset_id, project_id,
                      mapping_namespace, mapping_dataset_id, append)
    LOGGER.info(f'Query for {mapping_table} is {q}')
    run_query_to_table(project_id,
                       q,
                       destination_dataset_id=mapping_dataset_id,
                       destination_table_id=mapping_table,
                       write_disposition=write_disposition)


def table_query(client, table_name, input_dataset_id, project_id,
                pipeline_dataset_id, ct_plus_ids_view, mapping_namespace,
                mapping_dataset_id):
    """
    Get the query that loads one ETM table with its new ids.

    Both joins are inner joins, matching the RT script. A missing mapping row therefore
    drops the source row rather than loading it with a NULL id, which is the safer of
    the two failures. assert_no_rows_dropped() turns any such drop into an error, so it
    cannot pass unnoticed.

    :param client: BigQueryClient, used to read the source schema
    :param table_name: name of the ETM table
    :param input_dataset_id: identifies dataset with source data
    :param project_id: identifies the GCP project that contains the datasets
    :param pipeline_dataset_id: identifies the pipeline_tables dataset
    :param ct_plus_ids_view: view containing person_id to CT+ id mapping
    :param mapping_namespace: constant stamped into src_table_id
    :param mapping_dataset_id: identifies the dataset where mapping tables are stored
    :return: the query
    """
    schema = client.get_table(
        f'{project_id}.{input_dataset_id}.{table_name}').schema

    col_exprs = []
    for field in schema:
        if field.name == ETM_ID_COLUMN:
            col_exprs.append(f'm.{ETM_ID_COLUMN}')
        elif field.name == 'person_id':
            col_exprs.append('mp.person_id')
        else:
            col_exprs.append(f't.{field.name}')

    cols = ',\n        '.join(col_exprs)
    mapping_table = mapping_table_for(table_name)
    mapping_person = mapping_table_for(PERSON)
    person_stamp = person_mapping_src_table_id(pipeline_dataset_id,
                                               ct_plus_ids_view)

    return f'''
    SELECT
        {cols}
    FROM `{project_id}.{input_dataset_id}.{table_name}` t
    JOIN `{project_id}.{mapping_dataset_id}.{mapping_table}` m
        ON t.{ETM_ID_COLUMN} = m.src_{ETM_ID_COLUMN}
        AND m.src_table_id = '{mapping_namespace}.{table_name}'
    JOIN `{project_id}.{mapping_dataset_id}.{mapping_person}` mp
        ON t.person_id = mp.src_person_id
        AND mp.src_table_id = '{person_stamp}'
    '''


def assert_output_is_a_ct_plus_run(client, output_dataset_id):
    """
    Stop the run if the output dataset does not look like a finished CT+ run.

    This script deletes and recreates the four ETM tables in the output dataset, so a
    mistyped --output_dataset_id destroys them wherever it points. It cannot use
    assert_output_dataset_is_safe(), which requires an empty dataset: this script is
    meant to run into a dataset regenerate_ct_plus_ids.py has already filled. Requiring
    person plus all four ETM tables to be present is the cheapest check that
    distinguishes the intended target from anything else.

    :param client: BigQueryClient
    :param output_dataset_id: identifies the dataset the run would write to
    :raises RuntimeError: if the dataset does not hold a completed CT+ run
    """
    try:
        existing = {
            table.table_id for table in client.list_tables(output_dataset_id)
        }
    except Exception:
        raise RuntimeError(
            f'{output_dataset_id} does not exist. Run regenerate_ct_plus_ids.py '
            f'first; this script replaces the ETM tables that run produces.')

    required = {PERSON} | set(ETM_TABLES)
    missing = required - existing
    if missing:
        raise RuntimeError(
            f'{output_dataset_id} is missing {sorted(missing)}, so it does not hold a '
            f'finished CT+ run. This script would delete and recreate '
            f'{sorted(ETM_TABLES)} in it. Point --output_dataset_id at the dataset '
            f'regenerate_ct_plus_ids.py wrote.')


def assert_no_rows_dropped(client, project_id, table_name, input_dataset_id,
                           output_dataset_id):
    """
    Stop the run if the load lost rows to an unmatched mapping join.

    Both joins in table_query() are inner joins, so a sitting_id or a participant with
    no mapping row silently removes that row from the released data. The count is the
    only place that becomes visible.

    :param client: BigQueryClient
    :param project_id: identifies the GCP project
    :param table_name: name of the ETM table
    :param input_dataset_id: identifies dataset with source data
    :param output_dataset_id: identifies the dataset the run wrote to
    :raises RuntimeError: if the output holds fewer rows than the input
    """
    q = f'''
    SELECT
        (SELECT COUNT(*) FROM `{project_id}.{input_dataset_id}.{table_name}`)
            AS rows_in,
        (SELECT COUNT(*) FROM `{project_id}.{output_dataset_id}.{table_name}`)
            AS rows_out
    '''
    row = list(client.query(q).result())[0]
    rows_in, rows_out = row['rows_in'], row['rows_out']
    if rows_in == rows_out:
        LOGGER.info(
            f'{table_name}: {rows_out} rows loaded, matching the input.')
        return

    raise RuntimeError(
        f'{table_name} lost {rows_in - rows_out} of {rows_in} rows between '
        f'{input_dataset_id} and {output_dataset_id}. A row is dropped when its '
        f'{ETM_ID_COLUMN} or its participant has no mapping row. Establish which '
        f'before releasing this dataset.')


def main(input_dataset_id, output_dataset_id, project_id, pipeline_dataset_id,
         ct_plus_ids_view, mapping_dataset_id, mapping_namespace):
    """
    Replace the ETM tables in a CT+ dataset with re-keyed versions.

    :param input_dataset_id: identifies dataset with source data
    :param output_dataset_id: identifies the dataset written to
    :param project_id: identifies the GCP project that contains the datasets
    :param pipeline_dataset_id: identifies the pipeline_tables dataset
    :param ct_plus_ids_view: view containing person_id to CT+ id mapping
    :param mapping_dataset_id: identifies the dataset where mapping tables are stored
    :param mapping_namespace: constant stamped into src_table_id
    """
    bq_client = BigQueryClient(project_id)
    mapping_namespace = mapping_namespace or DEFAULT_MAPPING_NAMESPACE

    LOGGER.info('ETM CT+ ID regeneration started')

    # This script consumes the person mapping rather than building one, so a missing
    # mapping dataset means regenerate_ct_plus_ids.py has not run against it.
    mapping_person = mapping_table_for(PERSON)
    if not bq_client.table_exists(mapping_person, mapping_dataset_id):
        raise RuntimeError(
            f'{mapping_dataset_id}.{mapping_person} does not exist. Run '
            f'regenerate_ct_plus_ids.py against this mapping dataset first; this '
            f'script reuses the person mapping it builds.')

    # Same three person guards the main script applies, for the same reasons: a mapping
    # from another tier would supply the wrong person_ids, a moved CT+ id would re-key
    # participants silently, and a mapping that is not one to one would duplicate or
    # merge them.
    assert_person_mapping_is_ct_plus(bq_client, project_id, mapping_dataset_id,
                                     pipeline_dataset_id, ct_plus_ids_view)
    assert_person_ids_have_not_moved(bq_client, project_id, mapping_dataset_id,
                                     pipeline_dataset_id, ct_plus_ids_view)
    assert_person_mapping_is_one_to_one(bq_client, project_id,
                                        mapping_dataset_id)

    # Refuse to delete ETM tables out of anything that is not a finished CT+ run.
    assert_output_is_a_ct_plus_run(bq_client, output_dataset_id)

    for table_name in ETM_TABLES:
        if not bq_client.table_exists(table_name, input_dataset_id):
            LOGGER.info(
                f'Table {table_name} not found in {input_dataset_id}, skipping')
            continue

        LOGGER.info(f'Recreating {output_dataset_id}.{table_name}...')
        source_table = bq_client.get_table(
            f'{project_id}.{input_dataset_id}.{table_name}')
        fq_table_name = f'{project_id}.{output_dataset_id}.{table_name}'

        table_to_create = Table(fq_table_name, schema=source_table.schema)
        if any(field.name == 'person_id' for field in source_table.schema):
            table_to_create.clustering_fields = ['person_id']

        bq_client.delete_table(fq_table_name, not_found_ok=True)
        bq_client.create_table(table_to_create)

        mapping(table_name, input_dataset_id, project_id, mapping_namespace,
                mapping_dataset_id)

        LOGGER.info(f'Loading {table_name} into {output_dataset_id}...')
        q = table_query(bq_client, table_name, input_dataset_id, project_id,
                        pipeline_dataset_id, ct_plus_ids_view,
                        mapping_namespace, mapping_dataset_id)
        run_query_to_table(project_id,
                           q,
                           destination_dataset_id=output_dataset_id,
                           destination_table_id=table_name,
                           write_disposition='WRITE_EMPTY')

        assert_no_rows_dropped(bq_client, project_id, table_name,
                               input_dataset_id, output_dataset_id)

    LOGGER.info('ETM CT+ ID regeneration complete')


if __name__ == '__main__':
    pipeline_logging.configure(logging.INFO, add_console_handler=True)
    parser = argparse.ArgumentParser(
        description='Regenerate ETM IDs in a Controlled Tier Plus dataset',
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--project_id',
                        dest='project_id',
                        required=True,
                        help='Project associated with the datasets')
    parser.add_argument('--input_dataset_id',
                        dest='input_dataset_id',
                        required=True,
                        help='Dataset containing source data')
    parser.add_argument(
        '--output_dataset_id',
        dest='output_dataset_id',
        required=True,
        help=
        'Dataset regenerate_ct_plus_ids.py wrote. Its ETM tables are replaced.')
    parser.add_argument(
        '--pipeline_dataset_id',
        dest='pipeline_dataset_id',
        required=True,
        help=
        'Dataset containing rdr_participant_research_ids_view for person mapping'
    )
    parser.add_argument(
        '--ct_plus_ids_view',
        dest='ct_plus_ids_view',
        required=True,
        help=
        'View containing controlled_tier_id to controlled_tier_plus_id mapping')
    parser.add_argument(
        '--mapping_dataset_id',
        dest='mapping_dataset_id',
        required=True,
        help=
        'Dataset holding the mapping tables regenerate_ct_plus_ids.py built. '
        'Use the same one for every release so ids persist between them.')
    parser.add_argument(
        '--mapping_namespace',
        dest='mapping_namespace',
        required=False,
        default=DEFAULT_MAPPING_NAMESPACE,
        help='Constant stamped into src_table_id on every mapping row. Leave at '
        f"the default ('{DEFAULT_MAPPING_NAMESPACE}'); a per release value breaks "
        'id persistence silently.')

    args = parser.parse_args()
    main(args.input_dataset_id, args.output_dataset_id, args.project_id,
         args.pipeline_dataset_id, args.ct_plus_ids_view,
         args.mapping_dataset_id, args.mapping_namespace)
