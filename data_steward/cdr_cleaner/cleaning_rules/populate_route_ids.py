"""
Using the drug_concept_id, one can infer the values to populate the route concept ID field
pseudoephedrine hydrochloride 7.5 MG Chewable Tablet (OMOP: 43012486) would have route as oral
This cleaning rule populates null and wrong route_concept_ids based on the drug_concept_id
"""
import os
import logging

import bq_utils
import common
import resources
from tools import retract_data_gcs as rdg
import constants.bq_utils as bq_consts
import constants.cdr_cleaner.clean_cdr as cdr_consts

LOGGER = logging.getLogger(__name__)

ROUTES_TABLE_ID = "route_mappings"

ROUTE_FIELDS = [
    {
        "type": "integer",
        "name": "drug_concept_id",
        "mode": "required",
        "description": "The drug_concept_id of the drug."
    },
    {
        "type": "integer",
        "name": "route_concept_id",
        "mode": "required",
        "description": "The route_concept_id indicating the typical route used for administering the drug."
    }
]

INSERT_ROUTES_QUERY = (
    "INSERT INTO `{project_id}.{dataset_id}.{routes_table_id}` (drug_concept_id, route_concept_id) "
    "VALUES{mapping_list}"
)

FILL_ROUTE_ID_QUERY = (
    "SELECT {cols} "
    "FROM `{project_id}.{dataset_id}.{drug_exposure_table}` {drug_exposure_prefix} "
    "LEFT JOIN `{project_id}.{route_mapping_dataset}.{route_mapping_table}` {route_mapping_prefix} "
    "ON {drug_exposure_prefix}.drug_concept_id = {route_mapping_prefix}.drug_concept_id "
)


def get_mapping_list(route_mappings_list):
    """
    Filters out name columns from route_mappings.csv file and returns list of mappings suitable for BQ

    :param route_mappings_list:
    :return: formatted list suitable for insert in BQ:
            (drug_concept_id1, route_concept_id1), (drug_concept_id1, route_concept_id1)
    """
    mapping_list = []
    for route_mapping_dict in route_mappings_list:
        mapping_list.append((rdg.get_integer(route_mapping_dict["drug_concept_id"]),
                             rdg.get_integer(route_mapping_dict["route_concept_id"])))
    formatted_mapping_list = str(mapping_list).strip('[]')
    return formatted_mapping_list


def create_route_mappings_table(project_id, dataset_id=None):
    """
    Creates "route_mappings" table with only id columns from resources/route_mappings.csv

    :param project_id:
    :param dataset_id: BQ dataset_id
    :return:
    """
    if dataset_id is None:
        # Using table created in bq_dataset instead of re-creating in every dataset
        dataset_id = bq_utils.get_dataset_id()

    LOGGER.info("Creating %s.%s", dataset_id, ROUTES_TABLE_ID)

    # create empty table
    bq_utils.create_table(ROUTES_TABLE_ID, ROUTE_FIELDS, drop_existing=True, dataset_id=dataset_id)

    route_mappings_csv = os.path.join(resources.resource_path, ROUTES_TABLE_ID + ".csv")
    route_mappings_list = resources._csv_to_list(route_mappings_csv)
    routes_populate_query = INSERT_ROUTES_QUERY.format(dataset_id=dataset_id,
                                                       project_id=project_id,
                                                       routes_table_id=ROUTES_TABLE_ID,
                                                       mapping_list=get_mapping_list(route_mappings_list))
    result = bq_utils.query(routes_populate_query)
    LOGGER.info("Created %s.%s", dataset_id, ROUTES_TABLE_ID)
    return result


def get_cols_and_prefixes():
    """
    Generates the column string and table prefixes for selecting columns from appropriate tables

    :return: Tuple of strings (column_string, table_prefix_1, table_prefix_2)
    """
    fields = resources.fields_for(common.DRUG_EXPOSURE)
    route_field = "route_concept_id"
    drug_exposure_prefix = "de"
    route_mapping_prefix = "rm"
    col_exprs = []
    for field in fields:
        # by default we set to prefix for drug exposure
        col_expr = drug_exposure_prefix + '.' + field["name"]
        if field["name"] == route_field:
            col_expr = route_mapping_prefix + '.' + field["name"]
        col_exprs.append(col_expr)
    cols = ', '.join(col_exprs)
    return cols, drug_exposure_prefix, route_mapping_prefix


def get_route_mapping_queries(project_id, dataset_id, route_mapping_dataset_id=None):
    """
    Generates queries to populate route_concept_ids correctly

    :param project_id: the project containing the dataset
    :param dataset_id: dataset containing the OMOP clinical data
    :param route_mapping_dataset_id: dataset containing the drug-route lookup table
    :return:
    """
    queries = []
    if route_mapping_dataset_id is None:
        route_mapping_dataset_id = bq_utils.get_dataset_id()
    result = create_route_mappings_table(project_id, route_mapping_dataset_id)
    table = common.DRUG_EXPOSURE
    cols, drug_exposure_prefix, route_mapping_prefix = get_cols_and_prefixes()
    query = dict()
    query[cdr_consts.QUERY] = FILL_ROUTE_ID_QUERY.format(dataset_id=dataset_id,
                                                         project_id=project_id,
                                                         drug_exposure_table=table,
                                                         route_mapping_dataset=route_mapping_dataset_id,
                                                         route_mapping_table=ROUTES_TABLE_ID,
                                                         cols=cols,
                                                         drug_exposure_prefix=drug_exposure_prefix,
                                                         route_mapping_prefix=route_mapping_prefix)
    query[cdr_consts.DESTINATION_TABLE] = table
    query[cdr_consts.DISPOSITION] = bq_consts.WRITE_TRUNCATE
    query[cdr_consts.DESTINATION_DATASET] = dataset_id
    queries.append(query)
    return queries


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.parse_args()
    clean_engine.add_console_logging(ARGS.console_log)
    query_list = get_route_mapping_queries(ARGS.project_id, ARGS.dataset_id)
    clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id, query_list)
