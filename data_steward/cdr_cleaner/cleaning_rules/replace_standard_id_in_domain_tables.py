"""
## Purpose
Given a CDM dataset, ensure the "primary" concept fields (e.g. condition_occurrence.condition_concept_id) contain
standard concept_ids (based on vocabulary in same dataset) in the tables in DOMAIN_TABLE_NAMES.

## Overview
For all primary concept fields in domain tables being processed we do the following:
If the concept id is 0, set it to the source concept id.
If the associated concept.standard_concept='S', keep it.
Otherwise, replace it with the concept having 'Maps to' relationship in concept_relationship.
If a standard concept mapping to the field cannot be found, find a standard concept associated with the source concept.
If a standard concept cannot be found to either, we keep the original concept

## Steps
As a cleaning rule, this module generates a list of queries to be run, but there are three main steps:
 1) Create an intermediate table _logging_standard_concept_id_replacement which describes, for each row in each domain
    table, what action is to be taken.
 2) Update the domain tables
 3) Update the mapping tables

## One to Many Standard Concepts
Some concepts map to multiple standard concepts. In these cases, we create multiple rows in the domain table.
For example, we may have condition_occurrence records whose condition_concept_id is

  19934 "Person boarding or alighting a pedal cycle injured in collision with fixed or stationary object"

which maps to the three standard concepts:

  4053428 "Accident while boarding or alighting from motor vehicle on road"
  438921 "Collision between pedal cycle and fixed object"
  433099 "Victim, cyclist in vehicular AND/OR traffic accident"

In this case we remove the original row and add three records having the three standard concept ids above. New ids
are generated for these records, and, during the last step, the mapping tables are updated.

TODO account for "non-primary" concept fields
TODO when the time comes, include care_site, death, note, provider, specimen
"""
import logging

import bq_utils
from constants import bq_utils as bq_consts
from constants.cdr_cleaner import clean_cdr as cdr_consts
import resources
from validation.ehr_union import mapping_table_for

LOGGER = logging.getLogger(__name__)

DOMAIN_TABLE_NAMES = [
    'condition_occurrence', 'procedure_occurrence', 'drug_exposure',
    'device_exposure', 'observation', 'measurement', 'visit_occurrence'
]

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
    '  dc.standard_concept IS NULL or dc.standard_concept = \'C\' ')

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
    '   AND v.new_concept_id = to_update.new_concept_id')

SRC_CONCEPT_ID_UPDATE_QUERY = ('SELECT'
                               '  {cols} '
                               'FROM'
                               '  `{project}.{dataset}.{domain_table}` '
                               'LEFT JOIN '
                               '  `{project}.{dataset}.{logging_table}` '
                               'ON'
                               '  domain_table = \'{domain_table}\' '
                               '  AND src_id = {domain_table}_id ')

UPDATE_MAPPING_TABLES_QUERY = (
    'SELECT '
    '   {cols} '
    'FROM'
    '  `{project}.{dataset}.{mapping_table}` as domain '
    'LEFT JOIN'
    '  `{project}.{dataset}.{logging_table}` as log '
    'ON'
    '  src_id = {domain_table}_id'
    '  AND domain_table = \'{domain_table}\' ')


def parse_mapping_table_update_query(project_id, dataset_id, table_name,
                                     mapping_table_name):
    """

    Fill in mapping tables query so it either gets dest_id from the logging table or the domain table

    :param project_id: identifies the project containing the dataset
    :param dataset_id: identifies the dataset containing the OMOP data
    :param table_name: name of the domain table for which the query needs to be parsed
    :param mapping_table_name: name of the mapping_table for which the query needs to be parsed
    :return:
    """
    fields = [
        field['name'] for field in resources.fields_for(mapping_table_name)
    ]
    col_exprs = []
    for field_name in fields:
        if field_name == resources.get_domain_id_field(table_name):
            col_expr = 'coalesce(dest_id, {field}) AS {field}'.format(
                field=field_name)
        else:
            col_expr = field_name
        col_exprs.append(col_expr)
    cols = ', '.join(col_exprs)
    return UPDATE_MAPPING_TABLES_QUERY.format(
        cols=cols,
        project=project_id,
        dataset=dataset_id,
        mapping_table=mapping_table_name,
        logging_table=SRC_CONCEPT_ID_TABLE_NAME,
        domain_table=table_name)


def get_mapping_table_update_queries(project_id, dataset_id):
    """
    Generates a list of query dicts for adding newly generated rows to corresponding mapping_tables

    :param project_id: identifies the project containing the dataset
    :param dataset_id: identifies the dataset containing the OMOP data
    :return: list of query dicts for updating mapping_tables
    """
    queries = []
    for domain_table in DOMAIN_TABLE_NAMES:
        mapping_table = mapping_table_for(domain_table)
        query = dict()
        query[cdr_consts.QUERY] = parse_mapping_table_update_query(
            project_id, dataset_id, domain_table, mapping_table)
        query[cdr_consts.DESTINATION_TABLE] = mapping_table
        query[cdr_consts.DISPOSITION] = bq_consts.WRITE_TRUNCATE
        query[cdr_consts.DESTINATION_DATASET] = dataset_id

        queries.append(query)

    return queries


def parse_src_concept_id_update_query(project_id, dataset_id, table_name):
    """
    Fill in template query used to generate updated domain table

    :param project_id: identifies the project containing the dataset
    :param dataset_id: identifies the dataset containing the OMOP data
    :param table_name: name of a domain table
    :return: parsed src_concept_id_update query
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
                replace_field=fields_to_replace[field_name], field=field_name)
        else:
            col_expr = field_name
        col_exprs.append(col_expr)
    cols = ', '.join(col_exprs)

    return SRC_CONCEPT_ID_UPDATE_QUERY.format(
        cols=cols,
        project=project_id,
        dataset=dataset_id,
        domain_table=table_name,
        logging_table=SRC_CONCEPT_ID_TABLE_NAME)


def get_src_concept_id_update_queries(project_id, dataset_id):
    """
    Generates a list of query dicts for replacing the standard concept ids in domain tables

    :param project_id: identifies the project containing the dataset
    :param dataset_id: identifies the dataset containing the OMOP data
    :return: a list of query dicts for updating the standard_concept_ids
    """

    queries = []
    for domain_table in DOMAIN_TABLE_NAMES:
        query = dict()
        query[cdr_consts.QUERY] = parse_src_concept_id_update_query(
            project_id, dataset_id, domain_table)
        query[cdr_consts.DESTINATION_TABLE] = domain_table
        query[cdr_consts.DISPOSITION] = bq_consts.WRITE_TRUNCATE
        query[cdr_consts.DESTINATION_DATASET] = dataset_id

        queries.append(query)

    return queries


def parse_duplicate_id_update_query(project_id, dataset_id, domain_table):
    """
    Generates a domain_table specific duplicate_id_update_query

    :param project_id: identifies the project containing the dataset
    :param dataset_id: identifies the dataset containing the OMOP data
    :param domain_table: name of the domain_table for which a query needs to be generated.
    :return: a domain_table specific update query
    """
    query = DUPLICATE_ID_UPDATE_QUERY.format(
        table_name=domain_table,
        project=project_id,
        dataset=dataset_id,
        logging_table=SRC_CONCEPT_ID_TABLE_NAME)

    return query


def parse_src_concept_id_logging_query(project_id, dataset_id, domain_table):
    """
    Generates a query for each domain table for _logging_standard_concept_id_replacement

    :param project_id: identifies the project containing the dataset
    :param dataset_id: identifies the dataset containing the OMOP data
    :param domain_table: name of the domain_table for which a query needs to be generated.
    :return:
    """
    dom_concept_id = resources.get_domain_concept_id(domain_table)
    dom_src_concept_id = resources.get_domain_source_concept_id(domain_table)

    return SRC_CONCEPT_ID_MAPPING_QUERY.format(table_name=domain_table,
                                               project=project_id,
                                               dataset=dataset_id,
                                               domain_concept_id=dom_concept_id,
                                               domain_source=dom_src_concept_id)


def get_src_concept_id_logging_queries(project_id, dataset_id):
    """
    Creates logging table and generates a list of query dicts for populating it

    :param project_id: identifies the project containing the dataset
    :param dataset_id: identifies the dataset containing the OMOP data
    :return: a list of query dicts to gather logging records
    """
    # Create _logging_standard_concept_id_replacement
    bq_utils.create_standard_table(SRC_CONCEPT_ID_TABLE_NAME,
                                   SRC_CONCEPT_ID_TABLE_NAME,
                                   drop_existing=True,
                                   dataset_id=dataset_id)

    queries = []
    for domain_table in DOMAIN_TABLE_NAMES:
        query = dict()
        query[cdr_consts.QUERY] = parse_src_concept_id_logging_query(
            project_id, dataset_id, domain_table)
        query[cdr_consts.DESTINATION_TABLE] = SRC_CONCEPT_ID_TABLE_NAME
        query[cdr_consts.DISPOSITION] = bq_consts.WRITE_APPEND
        query[cdr_consts.DESTINATION_DATASET] = dataset_id
        queries.append(query)

    # For new rows added as a result of one-to-many standard concepts, we give newly generated rows new ids
    for domain_table in DOMAIN_TABLE_NAMES:
        query = dict()
        query[cdr_consts.QUERY] = parse_duplicate_id_update_query(
            project_id, dataset_id, domain_table)
        query[cdr_consts.DESTINATION_DATASET] = dataset_id
        queries.append(query)

    return queries


def replace_standard_id_in_domain_tables(project_id, dataset_id):
    """

    :param project_id: identifies the project containing the dataset
    :param dataset_id: identifies the dataset containing the OMOP data
    :return: a list of query dicts for replacing standard_concept_ids in domain_tables
    """
    queries_list = []
    queries_list.extend(
        get_src_concept_id_logging_queries(project_id, dataset_id))
    queries_list.extend(
        get_src_concept_id_update_queries(project_id, dataset_id))
    queries_list.extend(get_mapping_table_update_queries(
        project_id, dataset_id))

    return queries_list


def parse_args():
    """
    This function expands the default argument list defined in cdr_cleaner.args_parser
    :return: an expanded argument list object
    """

    import cdr_cleaner.args_parser as parser

    additional_argument = {
        parser.SHORT_ARGUMENT: '-n',
        parser.LONG_ARGUMENT: '--snapshot_dataset_id',
        parser.ACTION: 'store',
        parser.DEST: 'snapshot_dataset_id',
        parser.HELP: 'Create a snapshot of the dataset',
        parser.REQUIRED: True
    }
    args = parser.default_parse_args([additional_argument])
    return args


if __name__ == '__main__':
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parse_args()

    # Uncomment this line if testing locally
    # from bq_utils import create_snapshot_dataset
    # create_snapshot_dataset(ARGS.project_id, ARGS.dataset_id, ARGS.snapshot_dataset_id)

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(
            ARGS.project_id, ARGS.dataset_id,
            [(replace_standard_id_in_domain_tables,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   [(replace_standard_id_in_domain_tables,)])
