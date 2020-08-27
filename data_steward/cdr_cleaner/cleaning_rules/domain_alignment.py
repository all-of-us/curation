"""
COMBINED_SNAPSHOT should be set to create a new snapshot dataset while running this cleaning rule.
"""
import logging

import bq_utils
import constants.bq_utils as bq_consts
import constants.cdr_cleaner.clean_cdr as cdr_consts
from cdr_cleaner.cleaning_rules import domain_mapping, field_mapping
import resources
from cdr_cleaner.cleaning_rules.domain_mapping import EMPTY_STRING, METADATA_DOMAIN
from resources import get_domain_id_field

LOGGER = logging.getLogger(__name__)

# Define constants for SQL reserved values
AND = ' AND '
NULL_VALUE = 'NULL'
UNION_ALL = '\n\tUNION ALL\n'

# Define the name of the domain alignment table name
DOMAIN_ALIGNMENT_TABLE_NAME = '_logging_domain_alignment'

DOMAIN_REROUTE_INCLUDED_INNER_QUERY = (
    '    SELECT '
    '        \'{src_table}\' AS src_table, '
    '        \'{dest_table}\' AS dest_table, '
    '        {src_id} AS src_id, '
    '        {dest_id} AS dest_id, '
    '        True AS is_rerouted '
    '    FROM `{project_id}.{dataset_id}.{src_table}` AS s '
    '    JOIN `{project_id}.{dataset_id}.concept` AS c '
    '        ON s.{domain_concept_id} = c.concept_id '
    '    WHERE c.domain_id in ({domain}) ')

DOMAIN_REROUTE_EXCLUDED_INNER_QUERY = (
    '    SELECT  '
    '        \'{src_table}\' AS src_table, '
    '        CAST(NULL AS STRING) AS dest_table, '
    '        s.{src_id} AS src_id, '
    '        NULL AS dest_id, '
    '        False AS is_rerouted '
    '    FROM `{project_id}.{dataset_id}.{src_table}` AS s '
    '    LEFT JOIN `{project_id}.{dataset_id}._logging_domain_alignment` AS m '
    '        ON s.{src_id} = m.src_id '
    '            AND m.src_table = \'{src_table}\' '
    '    WHERE m.src_id IS NULL')

MAXIMUM_DOMAIN_ID_QUERY = (
    '    SELECT '
    '        MAX({domain_id_field}) AS max_id '
    '    FROM `{project_id}.{dataset_id}.{domain_table}` ')

DOMAIN_MAPPING_OUTER_QUERY = (
    'SELECT '
    '    u.src_table, '
    '    u.dest_table, '
    '    u.src_id, '
    '    ROW_NUMBER() OVER(ORDER BY u.src_table, u.src_id) + src.max_id AS dest_id, '
    '    u.is_rerouted '
    'FROM  '
    '( '
    '    {union_query} '
    ') u '
    'CROSS JOIN '
    '( '
    '    {domain_query} '
    ') src ')

REROUTE_DOMAIN_RECORD_QUERY = (
    'SELECT '
    'm.dest_id AS {dest_domain_id_field}, '
    '{field_mapping_expr} '
    'FROM `{project_id}.{dataset_id}.{src_table}` AS s '
    'JOIN `{project_id}.{dataset_id}._logging_domain_alignment` AS m '
    'ON s.{src_domain_id_field} = m.src_id '
    'AND m.src_table = \'{src_table}\' '
    'AND m.dest_table = \'{dest_table}\' '
    'AND m.is_rerouted = True ')

CASE_STATEMENT = (' CASE {src_field} '
                  ' {statements} '
                  ' ELSE NULL '
                  ' END AS {dest_field} ')

WHEN_STATEMENT = 'WHEN {src_value} THEN {dest_value}'

SRC_FIELD_AS_DEST_FIELD = '{src_field} AS {dest_field}'

NULL_AS_DEST_FIELD = 'NULL AS {dest_field}'

ZERO_AS_DEST_FIELD = '0 AS {dest_field}'


def parse_domain_mapping_query_cross_domain(project_id, dataset_id, dest_table):
    """
    This function creates a query that generates id mappings in _logging_domain_alignment
    for the rerouting records for dest_table

    :param project_id: the project_id in which the query is run
    :param dataset_id: the dataset_id in which the query is run
    :param dest_table: the destination table to which the records are rerouted
    :return: the query that generates id mappings for the rerouting records
    """
    union_query = EMPTY_STRING

    domain = resources.get_domain(dest_table)
    dest_id_field = resources.get_domain_id_field(dest_table)

    for src_table in domain_mapping.DOMAIN_TABLE_NAMES:

        if src_table != dest_table and domain_mapping.exist_domain_mappings(
                src_table, dest_table):

            src_id_field = resources.get_domain_id_field(src_table)
            domain_concept_id = resources.get_domain_concept_id(src_table)

            if union_query != EMPTY_STRING:
                union_query += UNION_ALL

            union_query += DOMAIN_REROUTE_INCLUDED_INNER_QUERY.format(
                project_id=project_id,
                dataset_id=dataset_id,
                src_table=src_table,
                dest_table=dest_table,
                src_id=src_id_field,
                dest_id=NULL_VALUE,
                domain_concept_id=domain_concept_id,
                domain='\'{}\''.format(domain))

            criteria = domain_mapping.get_rerouting_criteria(
                src_table, dest_table)

            if criteria != EMPTY_STRING:
                union_query += AND + criteria

    output_query = EMPTY_STRING

    if union_query != EMPTY_STRING:
        # the query to get the max id for the dest table
        domain_query = MAXIMUM_DOMAIN_ID_QUERY.format(
            project_id=project_id,
            dataset_id=dataset_id,
            domain_table=dest_table,
            domain_id_field=dest_id_field)

        output_query = DOMAIN_MAPPING_OUTER_QUERY.format(
            union_query=union_query, domain_query=domain_query)
    return output_query


def parse_domain_mapping_query_for_same_domains(project_id, dataset_id):
    """
    This function generates a query that generates id mappings in _logging_domain_alignment for
    the records being copied to the same domain table

    :param project_id: the project_id in which the query is run
    :param dataset_id: the dataset_id in which the query is run
    :return: a query that generates id mappings for the records that will get copied over to the same domain
    """
    union_query = EMPTY_STRING

    for domain_table in domain_mapping.DOMAIN_TABLE_NAMES:

        domain = resources.get_domain(domain_table)
        domain_id_field = resources.get_domain_id_field(domain_table)
        domain_concept_id = resources.get_domain_concept_id(domain_table)

        if union_query != EMPTY_STRING:
            union_query += UNION_ALL

        union_query += DOMAIN_REROUTE_INCLUDED_INNER_QUERY.format(
            project_id=project_id,
            dataset_id=dataset_id,
            src_table=domain_table,
            dest_table=domain_table,
            src_id=domain_id_field,
            dest_id=domain_id_field,
            domain_concept_id=domain_concept_id,
            domain='\'{}\''.format('\',\''.join([domain, METADATA_DOMAIN])))
    return union_query


def parse_domain_mapping_query_for_excluded_records(project_id, dataset_id):
    """
    This function generates a query that generates id mappings in _logging_domain_alignment for the records
    that will get dropped during rerouting because those records either fail the rerouting criteria or rerouting
    is not possible between src_table and dest_table such as condition_occurrence -> measurement

    :param project_id: the project_id in which the query is run
    :param dataset_id: the dataset_id in which the query is run
    :return: a query that generates id mappings for the records that will get dropped
    """
    union_query = EMPTY_STRING

    for domain_table in domain_mapping.DOMAIN_TABLE_NAMES:

        domain_id_field = get_domain_id_field(domain_table)

        if union_query != EMPTY_STRING:
            union_query += UNION_ALL

        union_query += DOMAIN_REROUTE_EXCLUDED_INNER_QUERY.format(
            project_id=project_id,
            dataset_id=dataset_id,
            src_table=domain_table,
            src_id=domain_id_field,
            src_domain_id_field=domain_id_field)
    return union_query


def get_domain_mapping_queries(project_id, dataset_id):
    """
    This function generates a list of query dicts for creating id mappings in _logging_domain_alignment.
    The list will get consumed clean_engine

    :param project_id: the project_id in which the query is run
    :param dataset_id: the dataset_id in which the query is run
    :return: a list of query dicts for creating id mappings in _logging_domain_alignment
    """
    # Create _logging_domain_alignment
    bq_utils.create_standard_table(DOMAIN_ALIGNMENT_TABLE_NAME,
                                   DOMAIN_ALIGNMENT_TABLE_NAME,
                                   drop_existing=True,
                                   dataset_id=dataset_id)

    queries = []

    for domain_table in domain_mapping.DOMAIN_TABLE_NAMES:
        query = dict()
        query[cdr_consts.QUERY] = parse_domain_mapping_query_cross_domain(
            project_id, dataset_id, domain_table)
        query[cdr_consts.DESTINATION_TABLE] = DOMAIN_ALIGNMENT_TABLE_NAME
        query[cdr_consts.DISPOSITION] = bq_consts.WRITE_APPEND
        query[cdr_consts.DESTINATION_DATASET] = dataset_id
        queries.append(query)

    # Create the query for creating field_mappings for the records moving between the same domain
    query = dict()
    query[cdr_consts.QUERY] = parse_domain_mapping_query_for_same_domains(
        project_id, dataset_id)
    query[cdr_consts.DESTINATION_TABLE] = DOMAIN_ALIGNMENT_TABLE_NAME
    query[cdr_consts.DISPOSITION] = bq_consts.WRITE_APPEND
    query[cdr_consts.DESTINATION_DATASET] = dataset_id
    queries.append(query)

    # Create the query for the records that are in the wrong domain but will not be moved
    query = dict()
    query[cdr_consts.QUERY] = parse_domain_mapping_query_for_excluded_records(
        project_id, dataset_id)
    query[cdr_consts.DESTINATION_TABLE] = DOMAIN_ALIGNMENT_TABLE_NAME
    query[cdr_consts.DISPOSITION] = bq_consts.WRITE_APPEND
    query[cdr_consts.DESTINATION_DATASET] = dataset_id
    queries.append(query)

    return queries


def resolve_field_mappings(src_table, dest_table):
    """
    This function generates the content of SQL select statement for the given src_table and dest_table.
    :param src_table: the source CDM table for rerouting
    :param dest_table: the destination CDM table for rerouting
    :return: the content of the SQL select statements
    """
    select_statements = []

    field_mappings = domain_mapping.get_field_mappings(src_table, dest_table)

    for dest_field, src_field in field_mappings.items():
        if domain_mapping.value_requires_translation(src_table, dest_table,
                                                     src_field, dest_field):
            value_mappings = domain_mapping.get_value_mappings(
                src_table, dest_table, src_field, dest_field)

            if len(value_mappings) == 0:
                if field_mapping.is_field_required(dest_table, dest_field):
                    case_statements = ZERO_AS_DEST_FIELD.format(
                        dest_field=dest_field)
                else:
                    case_statements = NULL_AS_DEST_FIELD.format(
                        dest_field=dest_field)
            else:
                case_statements = '\n\t\t'.join([
                    WHEN_STATEMENT.format(src_value=s, dest_value=d)
                    for d, s in value_mappings.items()
                ])

                case_statements = CASE_STATEMENT.format(
                    src_field=src_field,
                    dest_field=dest_field,
                    statements=case_statements)
            select_statements.append(case_statements)
        else:
            select_statements.append(
                SRC_FIELD_AS_DEST_FIELD.format(src_field=src_field,
                                               dest_field=dest_field))

    return ',\n\t'.join(select_statements)


def parse_reroute_domain_query(project_id, dataset_id, dest_table):
    """
    This function generates a query that reroutes the records from all domain tables for the given dest_table.
    It uses _mapping_alignment_table to determine in which domain table the records should land.

    :param project_id: the project_id in which the query is run
    :param dataset_id: the dataset_id in which the query is run
    :param dest_table: the destination CDM table for rerouting
    :return: a query that reroutes the records from all domain tables for the given dest_table
    """
    union_query = EMPTY_STRING

    for src_table in domain_mapping.DOMAIN_TABLE_NAMES:
        if src_table == dest_table or domain_mapping.exist_domain_mappings(
                src_table, dest_table):

            src_domain_id_field = get_domain_id_field(src_table)
            dest_domain_id_field = get_domain_id_field(dest_table)
            field_mapping_expr = resolve_field_mappings(src_table, dest_table)

            if union_query != EMPTY_STRING:
                union_query += UNION_ALL

            union_query += REROUTE_DOMAIN_RECORD_QUERY.format(
                project_id=project_id,
                dataset_id=dataset_id,
                src_table=src_table,
                dest_table=dest_table,
                src_domain_id_field=src_domain_id_field,
                dest_domain_id_field=dest_domain_id_field,
                field_mapping_expr=field_mapping_expr)
    return union_query


def get_reroute_domain_queries(project_id, dataset_id):
    """
    This function creates a new dataset called snapshot_dataset_id and copies all content from dataset_id to it.
    It generates a list of query dicts for rerouting the records to the corresponding destination table.

    :param project_id: the project_id in which the query is run
    :param dataset_id: the dataset_id in which the query is run
    :return: a list of query dicts for rerouting the records to the corresponding destination table
    """

    queries = []

    for domain_table in domain_mapping.DOMAIN_TABLE_NAMES:
        query = dict()
        query[cdr_consts.QUERY] = parse_reroute_domain_query(
            project_id, dataset_id, domain_table)
        query[cdr_consts.DESTINATION_TABLE] = domain_table
        query[cdr_consts.DISPOSITION] = bq_consts.WRITE_TRUNCATE
        query[cdr_consts.DESTINATION_DATASET] = dataset_id
        query[cdr_consts.BATCH] = True
        queries.append(query)

    return queries


def domain_alignment(project_id, dataset_id):
    """

    This function returns a list of dictionaries containing query parameters required for applying domain alignment.

    :param project_id: the project_id in which the query is run
    :param dataset_id: the dataset_id in which the query is run
    :return: a list of query dicts for rerouting the records to the corresponding destination table
    """
    queries_list = []
    queries_list.extend(get_domain_mapping_queries(project_id, dataset_id))
    queries_list.extend(get_reroute_domain_queries(project_id, dataset_id))

    return queries_list


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.parse_args()

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(ARGS.project_id,
                                                 ARGS.dataset_id,
                                                 [(domain_alignment,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   [(domain_alignment,)])
