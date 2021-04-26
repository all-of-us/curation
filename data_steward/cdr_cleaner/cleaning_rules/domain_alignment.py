"""
COMBINED_SNAPSHOT should be set to create a new snapshot dataset while running this cleaning rule.
"""
# Python imports
import logging

# Project imports
import constants.bq_utils as bq_consts
import constants.cdr_cleaner.clean_cdr as cdr_consts
from cdr_cleaner.cleaning_rules import domain_mapping, field_mapping
import resources
from resources import get_domain_id_field
from common import JINJA_ENV
from cdr_cleaner.cleaning_rules.domain_mapping import EMPTY_STRING, METADATA_DOMAIN
from tools.combine_ehr_rdr import mapping_table_for
from utils import bq

LOGGER = logging.getLogger(__name__)

# issue numbers
ISSUE_NUMBERS = ['DC402', 'DC1466']

# Define constants for SQL reserved values
AND = ' AND '
NULL_VALUE = 'NULL'
UNION_ALL = '\n\tUNION ALL\n'

# Define the name of the domain alignment table name
DOMAIN_ALIGNMENT_TABLE_NAME = '_logging_domain_alignment'

DOMAIN_REROUTE_INCLUDED_INNER_QUERY = JINJA_ENV.from_string("""
SELECT 
    '{{src_table}}' AS src_table, 
    '{{dest_table}}' AS dest_table, 
    {{src_id}} AS src_id, 
    {{dest_id}} AS dest_id, 
    True AS is_rerouted 
FROM `{{project_id}}.{{dataset_id}}.{{src_table}}` AS s 
JOIN `{{project_id}}.{{dataset_id}}.concept` AS c 
    ON s.{{domain_concept_id}} = c.concept_id 
WHERE c.domain_id in ({{domain}}) 
""")

DOMAIN_REROUTE_EXCLUDED_INNER_QUERY = JINJA_ENV.from_string("""
SELECT  
    '{{src_table}}' AS src_table, 
    CAST(NULL AS STRING) AS dest_table, 
    s.{{src_id}} AS src_id, 
    NULL AS dest_id, 
    False AS is_rerouted 
FROM `{{project_id}}.{{dataset_id}}.{{src_table}}` AS s 
LEFT JOIN `{{project_id}}.{{dataset_id}}._logging_domain_alignment` AS m 
    ON s.{{src_id}} = m.src_id 
        AND m.src_table = '{{src_table}}' 
WHERE m.src_id IS NULL
""")

MAXIMUM_DOMAIN_ID_QUERY = JINJA_ENV.from_string("""
SELECT
    MAX({{domain_id_field}}) AS max_id
FROM `{{project_id}}.{{dataset_id}}.{{domain_table}}`
""")

DOMAIN_MAPPING_OUTER_QUERY = JINJA_ENV.from_string("""
SELECT 
    u.src_table, 
    u.dest_table, 
    u.src_id, 
    ROW_NUMBER() OVER(ORDER BY u.src_table, u.src_id) + src.max_id AS dest_id, 
    u.is_rerouted 
FROM  
( 
    {{union_query}} 
) u 
CROSS JOIN 
( 
    {{domain_query}} 
) src 
""")

REROUTE_DOMAIN_RECORD_QUERY = JINJA_ENV.from_string("""
SELECT 
    m.dest_id AS {{dest_domain_id_field}}, 
    {{field_mapping_expr}} 
FROM `{{project_id}}.{{dataset_id}}.{{src_table}}` AS s 
JOIN `{{project_id}}.{{dataset_id}}._logging_domain_alignment` AS m 
ON s.{{src_domain_id_field}} = m.src_id 
    AND m.src_table = '{{src_table}}' 
    AND m.dest_table = '{{dest_table}}' 
    AND m.is_rerouted = True 
""")

SELECT_DOMAIN_RECORD_QUERY = JINJA_ENV.from_string("""
SELECT
    {{dest_domain_id_field}},
    {{field_mapping_expr}} 
FROM `{{project_id}}.{{dataset_id}}.{{dest_table}}`
""")

SANDBOX_DOMAIN_RECORD_QUERY_TEMPLATE = JINJA_ENV.from_string("""
SELECT
  d.*
FROM `{{project_id}}.{{dataset_id}}.{{domain_table}}` AS d
LEFT JOIN `{{project_id}}.{{dataset_id}}._logging_domain_alignment` AS m
  ON d.{{domain_table}}_id = m.dest_id 
    AND m.dest_table = '{{domain_table}}'
    AND m.is_rerouted = True 
WHERE m.dest_id IS NULL
""")

CLEAN_DOMAIN_RECORD_QUERY_TEMPLATE = JINJA_ENV.from_string("""
SELECT
  d.*
FROM `{{project_id}}.{{dataset_id}}.{% if is_mapping %}_mapping_{% endif %}{{domain_table}}` AS d
LEFT JOIN `{{project_id}}.{{sandbox_dataset_id}}.{{sandbox_table}}` AS s
  ON d.{{domain_table}}_id = s.{{domain_table}}_id
WHERE s.{{domain_table}}_id IS NULL
""")

REROUTE_DOMAIN_MAPPING_RECORD_QUERY = JINJA_ENV.from_string("""
{% for src_table in src_tables %}
    {% if loop.previtem is defined %}{{'\n'}}UNION ALL{{'\n\n'}}{% endif %}
    
-- if src_table is the same as dest_table, we want to keep all the records --
{% if src_table == dest_table %}
SELECT
    src.src_{{src_table}}_id,
    src.{{src_table}}_id,
    src.src_dataset_id,
    src.src_hpo_id,
    src.src_table_id
FROM `{{project_id}}.{{dataset_id}}._mapping_{{src_table}}` AS src
{% else %}
-- if src_table and dest_table are not the same -- 
-- we want to reroute the mapping records from _mapping_src_table to the _mapping_dest_table --
SELECT
    src.src_{{src_table}}_id AS src_{{dest_table}}_id,
    m.dest_id AS {{dest_table}}_id,
    src.src_dataset_id,
    src.src_hpo_id,
    src.src_table_id
FROM `{{project_id}}.{{dataset_id}}._logging_domain_alignment` AS m
JOIN `{{project_id}}.{{dataset_id}}._mapping_{{src_table}}` AS src
    ON m.src_id = src.{{src_table}}_id 
        AND m.src_table = '{{src_table}}'
        AND m.dest_table = '{{dest_table}}'
WHERE m.is_rerouted = True
{% endif %}
{% endfor %}
""")

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

            union_query += DOMAIN_REROUTE_INCLUDED_INNER_QUERY.render(
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
        domain_query = MAXIMUM_DOMAIN_ID_QUERY.render(
            project_id=project_id,
            dataset_id=dataset_id,
            domain_table=dest_table,
            domain_id_field=dest_id_field)

        output_query = DOMAIN_MAPPING_OUTER_QUERY.render(
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

        union_query += DOMAIN_REROUTE_INCLUDED_INNER_QUERY.render(
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

        union_query += DOMAIN_REROUTE_EXCLUDED_INNER_QUERY.render(
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
    client = bq.get_client(project_id)
    table_id = f'{project_id}.{dataset_id}.{DOMAIN_ALIGNMENT_TABLE_NAME}'
    client.delete_table(table_id, not_found_ok=True)
    bq.create_tables(client, project_id, [table_id], exists_ok=False)

    domain_mapping_queries = []

    for domain_table in domain_mapping.DOMAIN_TABLE_NAMES:
        query = parse_domain_mapping_query_cross_domain(project_id, dataset_id,
                                                        domain_table)
        domain_mapping_queries.append(query)

    # Create the query for creating field_mappings for the records moving between the same domain
    query = parse_domain_mapping_query_for_same_domains(project_id, dataset_id)
    domain_mapping_queries.append(query)

    # Create the query for the records that are in the wrong domain but will not be moved
    query = parse_domain_mapping_query_for_excluded_records(
        project_id, dataset_id)
    domain_mapping_queries.append(query)

    unioned_query = {
        cdr_consts.QUERY: UNION_ALL.join(domain_mapping_queries),
        cdr_consts.DESTINATION_TABLE: DOMAIN_ALIGNMENT_TABLE_NAME,
        cdr_consts.DISPOSITION: bq_consts.WRITE_EMPTY,
        cdr_consts.DESTINATION_DATASET: dataset_id
    }

    return [unioned_query]


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
    union_queries = []

    for src_table in domain_mapping.DOMAIN_TABLE_NAMES:
        src_domain_id_field = get_domain_id_field(src_table)
        dest_domain_id_field = get_domain_id_field(dest_table)
        field_mapping_expr = resolve_field_mappings(src_table, dest_table)

        if src_table == dest_table:
            # We are doing this to make sure the schema doesn't change and also keep all the
            # records in the domain table for later rerouting to the other domains
            union_queries.append(
                SELECT_DOMAIN_RECORD_QUERY.render(
                    project_id=project_id,
                    dataset_id=dataset_id,
                    dest_table=dest_table,
                    field_mapping_expr=field_mapping_expr,
                    dest_domain_id_field=dest_domain_id_field))
        elif domain_mapping.exist_domain_mappings(src_table, dest_table):
            # We are only rerouting the records between domain tables that are not the same
            union_queries.append(
                REROUTE_DOMAIN_RECORD_QUERY.render(
                    project_id=project_id,
                    dataset_id=dataset_id,
                    src_table=src_table,
                    dest_table=dest_table,
                    src_domain_id_field=src_domain_id_field,
                    dest_domain_id_field=dest_domain_id_field,
                    field_mapping_expr=field_mapping_expr))

    return UNION_ALL.join(union_queries)


def get_reroute_domain_queries(project_id, dataset_id):
    """
    This function creates a new dataset called snapshot_dataset_id and copies all content from
    dataset_id to it. It generates a list of query dicts for rerouting the records to the
    corresponding destination table.

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


def get_clean_domain_queries(project_id, dataset_id, sandbox_dataset_id):
    """
    This function generates a list of query dicts for dropping records that do not belong to the
    domain table after rerouting.
    
    :param project_id: the project_id in which the query is run
    :param dataset_id: the dataset_id in which the query is run
    :param sandbox_dataset_id: sandbox dataset for dataset_id
    :return: list of query dicts to run
    """

    queries = []
    sandbox_queries = []
    for domain_table in domain_mapping.DOMAIN_TABLE_NAMES:
        sandbox_queries.append({
            cdr_consts.QUERY:
                SANDBOX_DOMAIN_RECORD_QUERY_TEMPLATE.render(
                    project_id=project_id,
                    dataset_id=dataset_id,
                    domain_table=domain_table),
            cdr_consts.DESTINATION_TABLE:
                sandbox_name_for(domain_table),
            cdr_consts.DISPOSITION:
                bq_consts.WRITE_TRUNCATE,
            cdr_consts.DESTINATION_DATASET:
                sandbox_dataset_id
        })
        # add the clean-up query for the domain table
        queries.append({
            cdr_consts.QUERY:
                CLEAN_DOMAIN_RECORD_QUERY_TEMPLATE.render(
                    project_id=project_id,
                    dataset_id=dataset_id,
                    sandbox_dataset_id=sandbox_dataset_id,
                    domain_table=domain_table,
                    sandbox_table=sandbox_name_for(domain_table),
                    is_mapping=False),
            cdr_consts.DESTINATION_TABLE:
                domain_table,
            cdr_consts.DISPOSITION:
                bq_consts.WRITE_TRUNCATE,
            cdr_consts.DESTINATION_DATASET:
                dataset_id
        })
        # add the clean-up query for the corresponding mapping of the domain table
        queries.append({
            cdr_consts.QUERY:
                CLEAN_DOMAIN_RECORD_QUERY_TEMPLATE.render(
                    project_id=project_id,
                    dataset_id=dataset_id,
                    sandbox_dataset_id=sandbox_dataset_id,
                    domain_table=domain_table,
                    sandbox_table=sandbox_name_for(domain_table),
                    is_mapping=True),
            cdr_consts.DESTINATION_TABLE:
                mapping_table_for(domain_table),
            cdr_consts.DISPOSITION:
                bq_consts.WRITE_TRUNCATE,
            cdr_consts.DESTINATION_DATASET:
                dataset_id
        })
    return sandbox_queries + queries


def get_reroute_domain_mapping_queries(project_id, dataset_id):
    """
    The functions generates a list of query dicts for rerouting the mapping records to the
    approapriate domain.

    :param project_id: the project_id in which the query is run
    :param dataset_id: the dataset_id in which the query is run
    :return: a list of query dicts for rerouting the mapping records to the corresponding mapping
    table
    """
    queries = []

    for dest_table in domain_mapping.DOMAIN_TABLE_NAMES:
        # Figure out all possible rerouting source tables for a given destination table
        src_tables = [
            src_table for src_table in domain_mapping.DOMAIN_TABLE_NAMES
            if (src_table == dest_table) or
            domain_mapping.exist_domain_mappings(src_table, dest_table)
        ]

        queries.append({
            cdr_consts.QUERY:
                REROUTE_DOMAIN_MAPPING_RECORD_QUERY.render(
                    project_id=project_id,
                    dataset_id=dataset_id,
                    src_tables=src_tables,
                    dest_table=dest_table),
            cdr_consts.DESTINATION_TABLE:
                mapping_table_for(dest_table),
            cdr_consts.DISPOSITION:
                bq_consts.WRITE_TRUNCATE,
            cdr_consts.DESTINATION_DATASET:
                dataset_id
        })
    return queries


def sandbox_name_for(domain_table):
    """
    This function is used temporarily and can be replaced by the class method once this CR is
    upgraded to the baseclass

    :param domain_table: CDM table name
    :return: sandbox table name for the CDM table
    """
    return f'{"_".join(ISSUE_NUMBERS).lower()}_{domain_table}'


def domain_alignment(project_id, dataset_id, sandbox_dataset_id=None):
    """

    This function returns a list of dictionaries containing query parameters required for applying domain alignment.

    :param project_id: the project_id in which the query is run
    :param dataset_id: the dataset_id in which the query is run
    :param sandbox_dataset_id: Identifies the sandbox dataset to store rows
    #TODO use sandbox_dataset_id for CR
    :return: a list of query dicts for rerouting the records to the corresponding destination table
    """
    queries_list = []
    queries_list.extend(get_domain_mapping_queries(project_id, dataset_id))
    queries_list.extend(get_reroute_domain_queries(project_id, dataset_id))
    queries_list.extend(
        get_reroute_domain_mapping_queries(project_id, dataset_id))
    queries_list.extend(
        get_clean_domain_queries(project_id, dataset_id, sandbox_dataset_id))

    return queries_list


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.parse_args()

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(ARGS.project_id,
                                                 ARGS.dataset_id,
                                                 ARGS.sandbox_dataset_id,
                                                 [(domain_alignment,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(domain_alignment,)])
