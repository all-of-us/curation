import bq_utils
import constants.bq_utils as bq_consts
import constants.cdr_cleaner.clean_cdr as cdr_consts
import domain_mapping
import resources

SRC_CONCEPT_ID_TABLE_NAME = '_logging_standard_concept_id_replacement'

SRC_CONCEPT_ID_MAPPING_QUERY = (
    'SELECT '
    '  DISTINCT \'{table_name}\' AS domain_table,'
    '  domain.{table_name}_id AS src_id,'
    '  domain.{table_name}_id AS dest_id,'
    '  domain.{domain_concept_id} AS concept_id,'
    '  domain.{domain_source} AS src_concept_id,'
    '  coalesce(dcr.concept_id_2,'
    '    scr.concept_id_2,'
    '    domain.{domain_concept_id}) AS new_concept_id,'
    '  CASE'
    '   WHEN domain.{domain_source} = 0 THEN domain.{domain_concept_id}'
    '  ELSE'
    '   domain.{domain_source}'
    ' END'
    ' AS new_src_concept_id,'
    ' dcr.concept_id_2 IS NOT NULL AS lookup_concept_id, '
    ' dcr.concept_id_2 IS NULL AND scr.concept_id_2 IS NOT NULL AS lookup_src_concept_id,'
    ' domain.{domain_source} = 0 AND dcr.concept_id_2 IS NOT NULL AS is_src_concept_id_replaced,'
    '  CASE'
    '    WHEN dcr.concept_id_2 IS NOT NULL THEN \'replaced using concept_id\' '
    '    WHEN scr.concept_id_2 IS NOT NULL THEN \'replaced using source_concept_id\' '
    '  ELSE '
    '  \'kept the original concept_id\' '
    'END '
    '  AS action '
    'FROM '
    '  `{project}.{dataset}.{table_name}` AS domain '
    'LEFT JOIN '
    '  `{project}.{dataset}.concept` AS dc '
    'ON '
    '  domain.{domain_concept_id} = dc.concept_id '
    'LEFT JOIN '
    '  `{project}.{dataset}.concept_relationship` AS dcr '
    'ON '
    '  dcr.concept_id_1 = dc.concept_id '
    '  AND dcr.relationship_id = \'Maps to\' '
    'LEFT JOIN '
    '  `{project}.{dataset}.concept` AS sc '
    'ON '
    '  domain.{domain_source} = sc.concept_id '
    'LEFT JOIN '
    '  `{project}.{dataset}.concept_relationship` AS scr '
    'ON '
    '  scr.concept_id_1 = sc.concept_id '
    '  AND scr.relationship_id = \'Maps to\' '
    'WHERE '
    '  dc.standard_concept IS NULL'
)

DUPLICATE_ID_UPDATE_QUERY = (
    'UPDATE '
    '  `{project}.{dataset}.{logging_table}` AS to_update '
    'SET '
    '  to_update.dest_id = v.dest_id'
    ' FROM ('
    '  SELECT'
    '    a.src_id,'
    '    a.domain_table,'
    '    a.new_concept_id,'
    '    ROW_NUMBER() OVER() + src.max_id AS dest_id'
    '  FROM'
    '    `{project}.{dataset}.{logging_table}` AS a'
    '  JOIN ('
    '    SELECT'
    '      src_id'
    '    FROM'
    '      `{project}.{dataset}.{logging_table}`'
    '    WHERE'
    '      domain_table = \'{table_name}\''
    '    GROUP BY'
    '      src_id'
    '    HAVING'
    '      COUNT(*) > 1 ) b'
    '  ON'
    '    a.src_id = b.src_id'
    '    AND a.domain_table = \'{table_name}\''
    '  CROSS JOIN ('
    '    SELECT'
    '      MAX({table_name}_id) AS max_id'
    '    FROM'
    '      `{project}.{dataset}.{table_name}` ) src ) v'
    ' WHERE'
    '   v.src_id = to_update.src_id'
    '   AND v.domain_table = to_update.domain_table'
    '   AND v.new_concept_id = to_update.new_concept_id'
)

SRC_CONCEPT_ID_UPDATE_QUERY = (
    'SELECT'
    '  {cols} '
    'FROM'
    '  `{project}.{dataset}.{domain_table}` '
    'LEFT JOIN '
    '  `{project}.{dataset}.{logging_table}` '
    'ON'
    '  domain_table = \'{domain_table}\' '
    '  AND src_id = {domain_table}_id '
)


def parse_src_concept_id_update_query(project_id, dataset_id, table_name):
    """
    This method goes into list of fields of particular table and find out the fields which match a specified conditions,
    if found they will be added to a dictionary along with the field which needs to be joined on.
    :param project_id: the project_id in which the query is run
    :param dataset_id: the dataset_id in which the query is run
    :param table_name: Name of a domain table
    :return: a string of columns
    """
    fields = [field['name'] for field in resources.fields_for(table_name)]
    col_exprs = []
    fields_to_replace = {
        resources.get_domain_id_field(table_name): 'dest_id',
        resources.get_domain_concept_id(table_name): 'new_concept_id',
        resources.get_domain_source_concept_id(table_name): 'new_src_concept_id'
    }
    for field_name in fields:
        if field_name in fields_to_replace:
            col_expr = 'coalesce({replace_field}, {field}) AS {field}'.format(
                replace_field=fields_to_replace[field_name],
                field=field_name)
        else:
            col_expr = field_name
        col_exprs.append(col_expr)
    cols = ', '.join(col_exprs)

    return SRC_CONCEPT_ID_UPDATE_QUERY.format(cols=cols,
                                              project=project_id,
                                              dataset=dataset_id,
                                              domain_table=table_name,
                                              logging_table=SRC_CONCEPT_ID_TABLE_NAME)


def get_src_concept_id_update_queries(project_id, dataset_id):
    """

    It generates a list of query dicts for replacing the standard concept ids in domain tables.

    :param project_id: the project_id in which the query is run
    :param dataset_id: the dataset_id in which the query is run
    :return: a list of query dicts for updating the standard_concept_ids
    """

    queries = []
    for domain_table in domain_mapping.DOMAIN_TABLE_NAMES:
        query = dict()
        query[cdr_consts.QUERY] = parse_src_concept_id_update_query(project_id, dataset_id, domain_table)
        query[cdr_consts.DESTINATION_TABLE] = domain_table
        query[cdr_consts.DISPOSITION] = bq_consts.WRITE_TRUNCATE
        query[cdr_consts.DESTINATION_DATASET] = dataset_id

        queries.append(query)

    return queries


def parse_duplicate_id_update_query(project_id, dataset_id, domain_table):
    """

    Generates a domain_table specific duplicate_id_update_query

    :param project_id: the project_id in which the query is run
    :param dataset_id: the dataset_id in which the query is run
    :param domain_table: name of the domain_table for which a query needs to be generated.
    :return: a domain_table specific update query
    """
    query = DUPLICATE_ID_UPDATE_QUERY.format(table_name=domain_table,
                                             project=project_id,
                                             dataset=dataset_id,
                                             logging_table=SRC_CONCEPT_ID_TABLE_NAME
                                             )

    return query


def parse_src_concept_id_logging_query(project_id, dataset_id, domain_table):
    """

    This function generates a query for each domain for _mapping_standard_concept_id

    :param project_id: the project_id in which the query is run
    :param dataset_id: the dataset_id in which the query is run
    :param domain_table: name of the domain_table for which a query needs to be generated.
    :return:
    """
    dom_concept_id = resources.get_domain_source_concept_id(domain_table)
    dom_src_concept_id = resources.get_domain_source_concept_id(domain_table)

    query = SRC_CONCEPT_ID_MAPPING_QUERY.format(table_name=domain_table,
                                                project=project_id,
                                                dataset=dataset_id,
                                                domain_concept_id=dom_concept_id,
                                                domain_source=dom_src_concept_id)

    return query


def get_src_concept_id_logging_queries(project_id, dataset_id):
    """
    This function generates a list of query dicts for creating concept_id mappings in
    _logging_standard_concept_id_replacement.

    :param project_id: the project_id in which the query is run
    :param dataset_id: the dataset_id in which the query is run
    :return: a list of query dicts tto gather logging records
    """
    # Create _mapping_domain_alignment
    bq_utils.create_standard_table(SRC_CONCEPT_ID_TABLE_NAME,
                                   SRC_CONCEPT_ID_TABLE_NAME,
                                   drop_existing=True,
                                   dataset_id=dataset_id)

    queries = []
    for domain_table in domain_mapping.DOMAIN_TABLE_NAMES:
        query = dict()
        query[cdr_consts.QUERY] = parse_src_concept_id_logging_query(project_id,
                                                                     dataset_id,
                                                                     domain_table)
        query[cdr_consts.DESTINATION_TABLE] = SRC_CONCEPT_ID_TABLE_NAME
        query[cdr_consts.DISPOSITION] = bq_consts.WRITE_APPEND
        query[cdr_consts.DESTINATION_DATASET] = dataset_id
        queries.append(query)

    for domain_table in domain_mapping.DOMAIN_TABLE_NAMES:
        query = dict()
        query[cdr_consts.QUERY] = parse_duplicate_id_update_query(project_id,
                                                                  dataset_id,
                                                                  domain_table)
        query[cdr_consts.DESTINATION_DATASET] = dataset_id
        queries.append(query)

    return queries


def replace_standard_id_in_domain_tables(project_id, dataset_id):
    """

    :param project_id: the project_id in which the query is run
    :param dataset_id: the dataset_id in which the query is run
    :return: a list of query dicts for replacing standard_concept_ids in domain_tables
    """
    queries_list = []
    queries_list.extend(get_src_concept_id_logging_queries(project_id, dataset_id))
    queries_list.extend(get_src_concept_id_update_queries(project_id, dataset_id))

    return queries_list


def parse_args():
    """
    This function expands the default argument list defined in cdr_cleaner.args_parser
    :return: an expanded argument list object
    """

    import cdr_cleaner.args_parser as parser

    additional_argument = {parser.SHORT_ARGUMENT: '-n',
                           parser.LONG_ARGUMENT: '--snapshot_dataset_id',
                           parser.ACTION: 'store',
                           parser.DEST: 'snapshot_dataset_id',
                           parser.HELP: 'Create a snapshot of the dataset',
                           parser.REQUIRED: True}
    args = parser.default_parse_args([additional_argument])
    return args


if __name__ == '__main__':
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parse_args()

    # Uncomment this line if testing locally
    # from bq_utils import create_snapshot_dataset
    # create_snapshot_dataset(ARGS.project_id, ARGS.dataset_id, ARGS.snapshot_dataset_id)

    clean_engine.add_console_logging(ARGS.console_log)
    query_list = replace_standard_id_in_domain_tables(ARGS.project_id, ARGS.snapshot_dataset_id)
    clean_engine.clean_dataset(ARGS.project_id, ARGS.snapshot_dataset_id, query_list)
