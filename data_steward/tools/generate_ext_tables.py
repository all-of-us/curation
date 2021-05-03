# Python imports
import logging

# Third party imports
from google.cloud.bigquery import DatasetReference

# Project imports
import bq_utils
import constants.bq_utils as bq_consts
import constants.cdr_cleaner.clean_cdr as cdr_consts
from utils import bq
from common import JINJA_ENV
from resources import fields_for, MAPPING_TABLES

LOGGER = logging.getLogger(__name__)

EXT_FIELD_TEMPLATE = [{
    "type": "integer",
    "name": "{table}_id",
    "mode": "nullable",
    "description": "The {table}_id used in the {table} table."
}, {
    "type": "string",
    "name": "src_id",
    "mode": "nullable",
    "description": "The provenance of the data associated with the {table}_id."
}]

EXT_TABLE_SUFFIX = '_ext'
MAPPING_PREFIX = '_mapping_'
SITE_TABLE_ID = '_site_mappings'

REPLACE_SRC_QUERY = JINJA_ENV.from_string("""
SELECT m.{{cdm_table_id}}_id, s.src_id
FROM `{{project_id}}.{{mapping_dataset_id}}.{{mapping_table_id}}` m
JOIN `{{project_id}}.{{sandbox_dataset_id}}.{{site_mappings_table_id}}` s
ON m.src_hpo_id = s.hpo_id
""")


def get_table_fields(table, ext_table_id):
    """
    Generates fields for ext tables for the provided cdm table

    :param table: cdm table to generate ext fields for
    :param ext_table_id: cdm table extension name.  used to load schema
        defintion if it exists as a json file
    :return: dict containing ext fields for the cdm table
    """
    table_fields = []

    try:
        table_fields = fields_for(ext_table_id)
        LOGGER.info(
            f"using json schema file definition for table: {ext_table_id}")
    except (RuntimeError):
        for field in EXT_FIELD_TEMPLATE:
            table_field = dict()
            for key in field:
                table_field[key] = field[key].format(table=table)
            table_fields.append(table_field)
        LOGGER.info(
            f"using dynamic extension table schema for table: {ext_table_id}")

    return table_fields


def get_mapping_table_ids(project_id, mapping_dataset_id):
    """
    returns all the mapping table ids found in the dataset
    :param project_id: project_id containing the dataset
    :param mapping_dataset_id: dataset_id containing mapping tables
    :return: returns mapping table ids
    """
    client = bq.get_client(project_id)
    dataset_ref = DatasetReference(project_id, mapping_dataset_id)
    table_objs = bq.list_tables(client, dataset_ref)
    mapping_table_ids = []
    for table_obj in table_objs:
        if table_obj.table_id in MAPPING_TABLES:
            mapping_table_ids.append(table_obj.table_id)
    return mapping_table_ids


def get_cdm_table_from_mapping(mapping_table_id):
    """
    Returns the cdm table after stripping off the mapping table prefix
    :param mapping_table_id: mapping table id to generate the cdm table for
    :return: cdm table id
    """
    return mapping_table_id[len(MAPPING_PREFIX):]


def get_generate_ext_table_queries(project_id, dataset_id, sandbox_dataset_id,
                                   mapping_dataset_id):
    """
    Generate the queries for generating the ext tables
    :param project_id: project_id containing the dataset to generate ext tables in
    :param dataset_id: dataset_id to generate ext tables in
    :param sandbox_dataset_id: sandbox_dataset_id to store sandboxed rows.
    :param mapping_dataset_id: mapping_tables_dataset_id to use the mapping tables from
    :return: list of query dicts
    """
    queries = []

    # FIXME: Remove ths reference in future
    LOGGER.info(f'sandbox_dataset_id : {sandbox_dataset_id}')

    mapping_table_ids = get_mapping_table_ids(project_id, mapping_dataset_id)

    for mapping_table_id in mapping_table_ids:
        cdm_table_id = get_cdm_table_from_mapping(mapping_table_id)
        ext_table_id = cdm_table_id + EXT_TABLE_SUFFIX
        ext_table_fields = get_table_fields(cdm_table_id, ext_table_id)
        bq_utils.create_table(ext_table_id,
                              ext_table_fields,
                              drop_existing=True,
                              dataset_id=dataset_id)
        query = dict()
        query[cdr_consts.QUERY] = REPLACE_SRC_QUERY.render(
            project_id=project_id,
            sandbox_dataset_id=sandbox_dataset_id,
            mapping_dataset_id=mapping_dataset_id,
            mapping_table_id=mapping_table_id,
            site_mappings_table_id=SITE_TABLE_ID,
            cdm_table_id=cdm_table_id)
        query[cdr_consts.DESTINATION_TABLE] = ext_table_id
        query[cdr_consts.DESTINATION_DATASET] = dataset_id
        query[cdr_consts.DISPOSITION] = bq_consts.WRITE_EMPTY
        queries.append(query)

    return queries


def parse_args():
    """
    Expands the default argument list defined in cdr_cleaner.args_parser
    :return: an expanded argument list object
    """
    import cdr_cleaner.args_parser as parser

    argument_parser = parser.get_argument_parser()

    argument_parser.add_argument(
        '-m',
        '--mapping_dataset_id',
        dest='mapping_dataset_id',
        action='store',
        help=
        'The dataset containing mapping tables, typically the combined_dataset',
        required=True)

    return argument_parser.parse_args()


if __name__ == '__main__':
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parse_args()

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(
            ARGS.project_id,
            ARGS.dataset_id,
            ARGS.sandbox_dataset_id, [(get_generate_ext_table_queries,)],
            mapping_dataset_id=ARGS.mapping_dataset_id)
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id,
                                   ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(get_generate_ext_table_queries,)],
                                   mapping_dataset_id=ARGS.mapping_dataset_id)
