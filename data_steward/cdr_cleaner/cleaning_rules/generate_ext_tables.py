import random
import bq_utils


# create mapping table
import resources

EXT_FIELD_TEMPLATE = [
    {
        "type": "integer",
        "name": "{table}_id",
        "mode": "nullable",
        "description": "The {table}_id used in the {table} table."
    },
    {
        "type": "string",
        "name": "src_id",
        "mode": "nullable",
        "description": "The provenance of the data associated with the {table}_id."
    }]

SITE_MAPPING_FIELDS = [
    {
        "type": "string",
        "name": "hpo_id",
        "mode": "nullable",
        "description": "The hpo_id of the hpo site/RDR."
    },
    {
        "type": "string",
        "name": "src_id",
        "mode": "nullable",
        "description": "The masked id of the hpo site/RDR."
    }]

EXT_TABLE_SUFFIX = '_ext'
MAPPING_PREFIX = '_mapping_'
SITE_TABLE_ID = 'site_mappings'


def get_table_fields(table):
    """
    Generates fields for ext tables for the provided cdm table
    :param table: cdm table to generate ext fields for
    :return: dict containing ext fields for the cdm table
    """
    table_fields = []
    for field in EXT_FIELD_TEMPLATE:
        table_field = dict()
        for key in field:
            table_field[key] = field[key].format(table=table)
        table_fields.append(table_field)
    return table_fields


def get_mapping_table_ids(mapping_dataset_id):
    """
    returns all the mapping table ids found in the dataset
    :param mapping_dataset_id: dataset_id containing mapping tables
    :return: returns mapping table ids
    """
    table_objs = bq_utils.list_tables(mapping_dataset_id)
    mapping_table_ids = []
    for table_obj in table_objs:
        table_id = bq_utils.get_table_id_from_obj(table_obj)
        if MAPPING_PREFIX in table_id:
            mapping_table_ids.append(table_id)
    return mapping_table_ids


def generate_site_mappings():
    """
    Generates the mapping table for the site names and the masked names
    :return:
    """
    hpo_list = resources.hpo_csv()
    rand_list = random.sample(range(100, 999), len(hpo_list))
    mapping_list = dict()
    for i in range(len(hpo_list)):
        mapping_list[hpo_list[i]["hpo_id"]] = rand_list[i]
    return mapping_list
    # bq_utils.create_table(SITE_TABLE_ID, SITE_MAPPING_FIELDS, dataset_id=dataset_id)


def get_cdm_table_from_mapping(mapping_table_id):
    """
    Returns the cdm table after stripping off the mapping table prefix
    :param mapping_table_id: mapping table id to generate the cdm table for
    :return: cdm table id
    """
    return mapping_table_id[len(MAPPING_PREFIX):]


def get_generate_ext_table_queries(project_id, dataset_id, mapping_dataset_id=None):
    """
    Generate the queries for generating the ext tables
    :param project_id: project_id containing the dataset to generate ext tables in
    :param dataset_id: dataset_id to generate ext tables in
    :param mapping_dataset_id: dataset_id to use the mapping tables from (if different from dataset_id)
    :return: list of query dicts
    """
    queries = []

    if mapping_dataset_id is None:
        mapping_dataset_id = dataset_id

    mapping_table_ids = get_mapping_table_ids(mapping_dataset_id)

    for mapping_table_id in mapping_table_ids:
        cdm_table_id = get_cdm_table_from_mapping(mapping_table_id)
        ext_table_id = cdm_table_id + EXT_TABLE_SUFFIX
        ext_table_fields = get_table_fields(cdm_table_id)
        bq_utils.create_table(ext_table_id, ext_table_fields, dataset_id=dataset_id)

    return queries


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.parse_args()
    clean_engine.add_console_logging(ARGS.console_log)
    query_list = get_generate_ext_table_queries(ARGS.project_id, ARGS.dataset_id)
    clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id, query_list)
