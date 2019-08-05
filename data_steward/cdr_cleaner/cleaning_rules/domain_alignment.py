from domain_mapping import get_domain_id_field
from domain_mapping import METADATA_DOMAIN
from domain_mapping import EMPTY_STRING
import constants.bq_utils as bq_consts
import bq_utils
import logging
import constants.cdr_cleaner.clean_cdr as cdr_consts
import domain_mapping

LOGGER = logging.getLogger(__name__)

# Define constants for SQL reserved values
AND = ' AND '
NULL_VALUE = 'NULL'
UNION_ALL = '\n\tUNION ALL\n'

# Define the name of the domain alignment table name
DOMAIN_ALIGNMENT_TABLE_NAME = '_mapping_domain_alignment'

DOMAIN_REROUTE_INCLUDED_INNER_QUERY = (
    '''
    SELECT 
        '{src_table}' AS src_table,
        '{dest_table}' AS dest_table, 
        {src_id} AS src_id,
        {dest_id} AS dest_id,
        True AS is_rerouted
    FROM `{project_id}.{dataset_id}.{src_table}` AS s
    JOIN `{project_id}.{dataset_id}.concept` AS c
        ON s.{domain_concept_id} = c.concept_id
    WHERE c.domain_id in ({domain})
    '''
)

DOMAIN_REROUTE_EXCLUDED_INNER_QUERY = (
    '''
    SELECT 
        '{src_table}' AS src_table,
        CAST(NULL AS STRING) AS dest_table, 
        s.{src_id} AS src_id,
        NULL AS dest_id,
        False AS is_rerouted
    FROM `{project_id}.{dataset_id}.{src_table}` AS s
    LEFT JOIN `{project_id}.{dataset_id}._mapping_domain_alignment` AS m
        ON s.{src_id} = m.src_id
            AND m.src_table = '{src_table}'
    WHERE m.src_id IS NULL
    '''
)

MAXIMUM_DOMAIN_ID_QUERY = (
    '''
    SELECT 
        MAX({domain_id_field}) AS max_id
    FROM `{project_id}.{dataset_id}.{domain_table}`
    '''
)

DOMAIN_MAPPING_OUTER_QUERY = (
    '''
SELECT 
    u.src_table,
    u.dest_table,
    u.src_id,
    ROW_NUMBER() OVER() + src.max_id AS dest_id,
    u.is_rerouted
FROM 
(
    {union_query}
) u
CROSS JOIN
(
    {domain_query}
) src
    '''
)

REROUTE_DOMAIN_RECORD_QUERY = (
    '''
SELECT
\tm.dest_id AS {dest_domain_id_field},
\t{field_mapping_expr}
FROM `{project_id}.{dataset_id}.{src_table}` AS s 
JOIN `{project_id}.{dataset_id}._mapping_domain_alignment` AS m
    ON s.{src_domain_id_field} = m.src_id
      AND m.src_table = \'{src_table}\'
      AND m.dest_table = \'{dest_table}\'
      AND m.is_rerouted = True
    '''
)

SELECT_DOMAIN_RECORD_QUERY = (
    'SELECT * FROM `{project_id}.{dataset_id}.{table_id}`'
)

CASE_STATEMENT = (
    '\tCASE {src_field}\n'
    '\t\t{statements}\n'
    '\t\tELSE NULL\n'
    '\tEND AS {dest_field}'
)

WHEN_STATEMENT = 'WHEN {src_value} THEN {dest_value}'

SRC_FIELD_AS_DEST_FIELD = '{src_field} AS {dest_field}'

NULL_AS_DEST_FIELD = 'NULL AS {dest_field}'


def parse_domain_mapping_query_cross_domain(project_id, dataset_id, dest_table):
    """

    :param project_id:
    :param dataset_id:
    :param dest_table:
    :return:
    """
    union_query = EMPTY_STRING

    domain = domain_mapping.get_domain(dest_table)
    dest_id_field = domain_mapping.get_domain_id_field(dest_table)

    for src_table in domain_mapping.DOMAIN_TABLE_NAMES:

        if src_table != dest_table and domain_mapping.exist_domain_mappings(src_table, dest_table):

            src_id_field = domain_mapping.get_domain_id_field(src_table)
            domain_concept_id = domain_mapping.get_domain_concept_id(src_table)

            if union_query != EMPTY_STRING:
                union_query += UNION_ALL

            union_query += DOMAIN_REROUTE_INCLUDED_INNER_QUERY.format(project_id=project_id,
                                                                      dataset_id=dataset_id,
                                                                      src_table=src_table,
                                                                      dest_table=dest_table,
                                                                      src_id=src_id_field,
                                                                      dest_id=NULL_VALUE,
                                                                      domain_concept_id=domain_concept_id,
                                                                      domain='\'{}\''.format(domain))

            criteria = domain_mapping.get_rerouting_criteria(src_table, dest_table)

            if criteria != EMPTY_STRING:
                union_query += AND + criteria

    output_query = EMPTY_STRING

    if union_query != EMPTY_STRING:
        # the query to get the max id for the dest table
        domain_query = MAXIMUM_DOMAIN_ID_QUERY.format(project_id=project_id,
                                                      dataset_id=dataset_id,
                                                      domain_table=dest_table,
                                                      domain_id_field=dest_id_field)

        output_query = DOMAIN_MAPPING_OUTER_QUERY.format(union_query=union_query,
                                                         domain_query=domain_query)
    return output_query


def parse_domain_mapping_query_for_same_domains(project_id, dataset_id):
    union_query = EMPTY_STRING

    for domain_table in domain_mapping.DOMAIN_TABLE_NAMES:

        domain = domain_mapping.get_domain(domain_table)
        domain_id_field = domain_mapping.get_domain_id_field(domain_table)
        domain_concept_id = domain_mapping.get_domain_concept_id(domain_table)

        if union_query != EMPTY_STRING:
            union_query += UNION_ALL

        union_query += DOMAIN_REROUTE_INCLUDED_INNER_QUERY.format(project_id=project_id,
                                                                  dataset_id=dataset_id,
                                                                  src_table=domain_table,
                                                                  dest_table=domain_table,
                                                                  src_id=domain_id_field,
                                                                  dest_id=domain_id_field,
                                                                  domain_concept_id=domain_concept_id,
                                                                  domain='\'{}\''.format(
                                                                      '\',\''.join([domain, METADATA_DOMAIN])))
    return union_query


def parse_domain_mapping_query_for_excluded_records(project_id, dataset_id):
    union_query = EMPTY_STRING

    for domain_table in domain_mapping.DOMAIN_TABLE_NAMES:

        domain_id_field = get_domain_id_field(domain_table)

        if union_query != EMPTY_STRING:
            union_query += UNION_ALL

        union_query += DOMAIN_REROUTE_EXCLUDED_INNER_QUERY.format(project_id=project_id,
                                                                  dataset_id=dataset_id,
                                                                  src_table=domain_table,
                                                                  src_id=domain_id_field,
                                                                  src_domain_id_field=domain_id_field)
    return union_query


def get_domain_mapping_queries(project_id, dataset_id):
    # Create _mapping_domain_alignment
    bq_utils.create_standard_table(DOMAIN_ALIGNMENT_TABLE_NAME,
                                   DOMAIN_ALIGNMENT_TABLE_NAME,
                                   drop_existing=True,
                                   dataset_id=dataset_id)

    queries = []

    for domain_table in domain_mapping.DOMAIN_TABLE_NAMES:
        query = dict()
        query[cdr_consts.QUERY] = parse_domain_mapping_query_cross_domain(project_id,
                                                                          dataset_id,
                                                                          domain_table)
        query[cdr_consts.DESTINATION_TABLE] = DOMAIN_ALIGNMENT_TABLE_NAME
        query[cdr_consts.DISPOSITION] = bq_consts.WRITE_APPEND
        query[cdr_consts.DESTINATION_DATASET] = dataset_id
        queries.append(query)

    # Create the query for creating field_mappings for the records moving between the same domain
    query = dict()
    query[cdr_consts.QUERY] = parse_domain_mapping_query_for_same_domains(project_id, dataset_id)
    query[cdr_consts.DESTINATION_TABLE] = DOMAIN_ALIGNMENT_TABLE_NAME
    query[cdr_consts.DISPOSITION] = bq_consts.WRITE_APPEND
    query[cdr_consts.DESTINATION_DATASET] = dataset_id
    queries.append(query)

    # Create the query for the records that are in the wrong domain but will not be moved
    query = dict()
    query[cdr_consts.QUERY] = parse_domain_mapping_query_for_excluded_records(project_id, dataset_id)
    query[cdr_consts.DESTINATION_TABLE] = DOMAIN_ALIGNMENT_TABLE_NAME
    query[cdr_consts.DISPOSITION] = bq_consts.WRITE_APPEND
    query[cdr_consts.DESTINATION_DATASET] = dataset_id
    queries.append(query)

    return queries


def resolve_field_mappings(src_table, dest_table):
    select_statements = []

    field_mappings = domain_mapping.get_field_mappings(src_table, dest_table)

    for dest_field, src_field in field_mappings.iteritems():
        if domain_mapping.value_requires_translation(src_table, dest_table, src_field, dest_field):
            value_mappings = domain_mapping.get_value_mappings(src_table, dest_table, src_field, dest_field)

            if len(value_mappings) == 0:
                case_statements = NULL_AS_DEST_FIELD.format(dest_field=dest_field)
            else:
                case_statements = '\n\t\t'.join(
                    [WHEN_STATEMENT.format(src_value=s, dest_value=d) for d, s in
                     value_mappings.iteritems()])

                case_statements = CASE_STATEMENT.format(src_field=src_field,
                                                        dest_field=dest_field,
                                                        statements=case_statements)
            select_statements.append(case_statements)
        else:
            select_statements.append(SRC_FIELD_AS_DEST_FIELD.format(src_field=src_field, dest_field=dest_field))

    return ',\n\t'.join(select_statements)


def parse_reroute_domain_query(project_id, dataset_id, dest_table):
    union_query = EMPTY_STRING

    for src_table in domain_mapping.DOMAIN_TABLE_NAMES:
        if src_table == dest_table or domain_mapping.exist_domain_mappings(src_table, dest_table):

            src_domain_id_field = get_domain_id_field(src_table)
            dest_domain_id_field = get_domain_id_field(dest_table)
            field_mapping_expr = resolve_field_mappings(src_table, dest_table)

            if union_query != EMPTY_STRING:
                union_query += UNION_ALL

            union_query += REROUTE_DOMAIN_RECORD_QUERY.format(project_id=project_id,
                                                              dataset_id=dataset_id,
                                                              src_table=src_table,
                                                              dest_table=dest_table,
                                                              src_domain_id_field=src_domain_id_field,
                                                              dest_domain_id_field=dest_domain_id_field,
                                                              field_mapping_expr=field_mapping_expr)
    return union_query


def get_reroute_domain_queries(project_id, dataset_id, snapshot_dataset_id):
    dataset_result = bq_utils.create_dataset(
        project_id=project_id,
        dataset_id=snapshot_dataset_id,
        description='Snapshot of {dataset_id}'.format(dataset_id=dataset_id),
        overwrite_existing=True)

    validation_dataset = dataset_result.get(bq_consts.DATASET_REF, {})
    snapshot_dataset_id = validation_dataset.get(bq_consts.DATASET_ID, EMPTY_STRING)
    LOGGER.info('Snapshot dataset {} has been created'.format(snapshot_dataset_id))

    # Create the empty tables in the new snapshot dataset
    for table_id in bq_utils.list_all_table_ids(dataset_id):
        metadata = bq_utils.get_table_info(table_id, dataset_id)
        fields = metadata['schema']['fields']
        bq_utils.create_table(table_id, fields, drop_existing=True, dataset_id=snapshot_dataset_id)

    # Copy the table content from the current dataset to the snapshot dataset
    copy_table_job_ids = []
    for table_id in bq_utils.list_all_table_ids(dataset_id):
        query = SELECT_DOMAIN_RECORD_QUERY.format(project_id=project_id, dataset_id=dataset_id, table_id=table_id)
        results = bq_utils.query(query, use_legacy_sql=False, destination_table_id=table_id,
                                 destination_dataset_id=snapshot_dataset_id, batch=True)
        copy_table_job_ids.append(results['jobReference']['jobId'])

    incomplete_jobs = bq_utils.wait_on_jobs(copy_table_job_ids)
    if len(incomplete_jobs) > 0:
        raise bq_utils.BigQueryJobWaitError(incomplete_jobs)

    queries = []

    for domain_table in domain_mapping.DOMAIN_TABLE_NAMES:
        query = dict()
        query[cdr_consts.QUERY] = parse_reroute_domain_query(project_id, snapshot_dataset_id, domain_table)
        query[cdr_consts.DESTINATION_TABLE] = domain_table
        query[cdr_consts.DISPOSITION] = bq_consts.WRITE_TRUNCATE
        query[cdr_consts.DESTINATION_DATASET] = dataset_id
        query[cdr_consts.BATCH] = True
        queries.append(query)

    return queries


def parse_args():
    additional_argument = {parser.SHORT_ARGUMENT: '-n',
                           parser.LONG_ARGUMENT: '--snapshot_dataset_id',
                           parser.ACTION: 'store',
                           parser.DEST: 'snapshot_dataset_id',
                           parser.HELP: 'Create a snapshot of the dataset',
                           parser.REQUIRED: True}
    args = parser.default_parse_args([additional_argument])
    return args


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parse_args()

    clean_engine.add_console_logging(ARGS.console_log)
    query_list = get_domain_mapping_queries(ARGS.project_id, ARGS.dataset_id)
    clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id, query_list)
    query_list = get_reroute_domain_queries(ARGS.project_id, ARGS.dataset_id, ARGS.snapshot_dataset_id)
    clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id, query_list)