import random

import bq_utils
import constants.bq_utils as bq_consts
import constants.cdr_cleaner.clean_cdr as cdr_consts

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

SITE_MAPPING_FIELDS = [{
    "type": "string",
    "name": "hpo_id",
    "mode": "nullable",
    "description": "The hpo_id of the hpo site/RDR."
}, {
    "type": "string",
    "name": "src_id",
    "mode": "nullable",
    "description": "The masked id of the hpo site/RDR."
}]

EXT_TABLE_SUFFIX = '_ext'
MAPPING_PREFIX = '_mapping_'
SITE_TABLE_ID = '_site_mappings'
EHR_SITE_PREFIX = 'EHR site '
RDR = 'rdr'
PPI_PM = 'PPI/PM'

INSERT_SITE_MAPPINGS_QUERY = """
INSERT INTO `{project_id}.{combined_dataset_id}.{table_id}` (hpo_id, src_id)
VALUES{values}
"""

REPLACE_SRC_QUERY = """
SELECT m.{cdm_table_id}_id, s.src_id
FROM `{project_id}.{combined_dataset_id}.{mapping_table_id}` m
JOIN `{project_id}.{combined_dataset_id}.{site_mappings_table_id}` s
ON m.src_hpo_id = s.hpo_id
"""


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


def get_mapping_table_ids(project_id, combined_dataset_id):
    """
    returns all the mapping table ids found in the dataset
    :param project_id: project_id containing the dataset
    :param combined_dataset_id: dataset_id containing mapping tables
    :return: returns mapping table ids
    """
    table_objs = bq_utils.list_tables(combined_dataset_id,
                                      project_id=project_id)
    mapping_table_ids = []
    for table_obj in table_objs:
        table_id = bq_utils.get_table_id_from_obj(table_obj)
        if MAPPING_PREFIX in table_id:
            mapping_table_ids.append(table_id)
    return mapping_table_ids


def generate_site_mappings():
    """
    Generates the mapping table for the site names and the masked names
    :return: returns dict with key: hpo_id, value: rand int
    """
    hpo_list = bq_utils.get_hpo_info()
    rand_list = random.sample(range(100, 999), len(hpo_list))
    mapping_dict = dict()
    for i, hpo_dict in enumerate(hpo_list):
        mapping_dict[hpo_dict["hpo_id"]] = rand_list[i]
    return mapping_dict


def get_hpo_and_rdr_mappings():
    """
    generates list of lists containing the hpo_id and the identifier
    :return: list of lists (eg. [[hpo_id_1, id_1], [hpo_id_2, id_2], ...)
    """
    site_mapping_dict = generate_site_mappings()
    mappings_list = []
    for hpo_id in site_mapping_dict:
        mappings_list.append(
            [hpo_id, EHR_SITE_PREFIX + str(site_mapping_dict[hpo_id])])
    mappings_list.append([RDR, PPI_PM])
    return mappings_list


def convert_to_bq_string(mapping_list):
    """
    Converts list of lists to bq INSERT friendly string
    :param mapping_list: list of lists where the inner lists have two items
    :return: bq INSERT formatted string
    """
    bq_insert_list = []
    for hpo_rdr_item in mapping_list:
        bq_insert_list.append("(\"{hpo_rdr_id}\", \"{src_id}\")".format(
            hpo_rdr_id=hpo_rdr_item[0], src_id=hpo_rdr_item[1]))
    bq_insert_string = ', '.join(bq_insert_list)
    return bq_insert_string


def get_cdm_table_from_mapping(mapping_table_id):
    """
    Returns the cdm table after stripping off the mapping table prefix
    :param mapping_table_id: mapping table id to generate the cdm table for
    :return: cdm table id
    """
    return mapping_table_id[len(MAPPING_PREFIX):]


def create_and_populate_source_mapping_table(project_id, dataset_id):
    """
    creates the site mapping table and inserts the site mappings
    :param project_id: project_id containing the dataset
    :param dataset_id: dataset to create the mapping table in
    :return: number of rows inserted in string from
    """
    mapping_list = get_hpo_and_rdr_mappings()
    site_mapping_insert_string = convert_to_bq_string(mapping_list)
    result = bq_utils.create_table(SITE_TABLE_ID,
                                   SITE_MAPPING_FIELDS,
                                   drop_existing=True,
                                   dataset_id=dataset_id)
    site_mappings_insert_query = INSERT_SITE_MAPPINGS_QUERY.format(
        combined_dataset_id=dataset_id,
        project_id=project_id,
        table_id=SITE_TABLE_ID,
        values=site_mapping_insert_string)
    result = bq_utils.query(site_mappings_insert_query)
    rows_affected = result['numDmlAffectedRows']
    return rows_affected


def get_generate_ext_table_queries(project_id, deid_dataset_id,
                                   combined_dataset_id):
    """
    Generate the queries for generating the ext tables
    :param project_id: project_id containing the dataset to generate ext tables in
    :param deid_dataset_id: deid_dataset_id to generate ext tables in
    :param combined_dataset_id: combined_dataset_id to use the mapping tables from
    :return: list of query dicts
    """
    queries = []

    mapping_table_ids = get_mapping_table_ids(project_id, combined_dataset_id)
    create_and_populate_source_mapping_table(project_id, combined_dataset_id)

    for mapping_table_id in mapping_table_ids:
        cdm_table_id = get_cdm_table_from_mapping(mapping_table_id)
        ext_table_id = cdm_table_id + EXT_TABLE_SUFFIX
        ext_table_fields = get_table_fields(cdm_table_id)
        bq_utils.create_table(ext_table_id,
                              ext_table_fields,
                              drop_existing=True,
                              dataset_id=deid_dataset_id)
        query = dict()
        query[cdr_consts.QUERY] = REPLACE_SRC_QUERY.format(
            project_id=project_id,
            combined_dataset_id=combined_dataset_id,
            mapping_table_id=mapping_table_id,
            site_mappings_table_id=SITE_TABLE_ID,
            cdm_table_id=cdm_table_id)
        query[cdr_consts.DESTINATION_TABLE] = ext_table_id
        query[cdr_consts.DESTINATION_DATASET] = deid_dataset_id
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
        '-c',
        '--combined_dataset_id',
        dest='combined_dataset_id',
        action='store',
        help='The combined dataset used to generate the deid dataset',
        required=True)

    return argument_parser.parse_args()


if __name__ == '__main__':
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parse_args()
    clean_engine.add_console_logging(ARGS.console_log)
    query_list = get_generate_ext_table_queries(ARGS.project_id,
                                                ARGS.dataset_id,
                                                ARGS.combined_dataset_id)
    clean_engine.clean_dataset(ARGS.project_id, query_list)
