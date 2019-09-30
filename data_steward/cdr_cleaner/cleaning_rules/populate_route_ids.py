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
import constants.bq_utils as bq_consts
import constants.cdr_cleaner.clean_cdr as cdr_consts

LOGGER = logging.getLogger(__name__)

DOSE_FORM_ROUTES_FILE = "dose_form_route_mappings"
DOSE_FORM_ROUTES_TABLE_ID = "_logging_dose_form_route_mappings"
DRUG_ROUTES_TABLE_ID = "_logging_drug_route_mappings"

DOSE_FORM_ROUTE_FIELDS = [
    {
        "type": "integer",
        "name": "dose_form_concept_id",
        "mode": "required",
        "description": "The dose_form_concept_id of the dose form."
    },
    {
        "type": "integer",
        "name": "route_concept_id",
        "mode": "required",
        "description": "The route_concept_id indicating the typical route used for administering the drug."
    }
]

DRUG_ROUTE_FIELDS = [
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

INSERT_ROUTES_QUERY = """
INSERT INTO `{project_id}.{dataset_id}.{routes_table_id}` (dose_form_concept_id, route_concept_id)
VALUES {mapping_list}
"""

# If a drug maps to multiple dose forms, this can potentially create duplicate records in drug_exposure table
# We include only those drugs that map to different dose forms which in turn map to the same route
# We exclude drugs that map to different dose forms which in turn map to the different routes
# However, even with the following checks it is to be noted that there is potential
# for spurious duplicate records with different route_concept_ids to be created at this step and they must be removed
GET_DRUGS_FROM_DOSE_FORM = """
WITH drug_concept AS
(SELECT *
FROM `{project_id}.{vocabulary_dataset}.concept`
WHERE domain_id = 'Drug'),
 
drug_dose_form AS
(SELECT *
FROM drug_concept dc 
JOIN `{project_id}.{vocabulary_dataset}.concept_relationship` cr 
ON dc.concept_id = cr.concept_id_1
WHERE cr.relationship_id = 'RxNorm has dose form'),

drug_route AS
(SELECT DISTINCT 
  ddf.concept_id_1 drug_concept_id, 
  {route_mapping_prefix}.route_concept_id 
FROM drug_dose_form ddf
LEFT JOIN `{project_id}.{route_mapping_dataset_id}.{dose_form_route_mapping_table}` {route_mapping_prefix}
ON ddf.concept_id_2 = {route_mapping_prefix}.dose_form_concept_id
WHERE {route_mapping_prefix}.route_concept_id IS NOT NULL),

drug_route_single AS
(SELECT
  drug_concept_id,
  COUNT(1) n
FROM drug_route 
GROUP BY drug_concept_id
HAVING n = 1)

SELECT
 drug_concept_id,
 route_concept_id
FROM drug_route dr
WHERE EXISTS
(SELECT 1 
FROM drug_route_single drs 
WHERE dr.drug_concept_id = drs.drug_concept_id)
"""

FILL_ROUTE_ID_QUERY = """
SELECT {cols}
FROM `{project_id}.{dataset_id}.{drug_exposure_table}` {drug_exposure_prefix}
LEFT JOIN `{project_id}.{route_mapping_dataset_id}.{drug_route_mapping_table}` {route_mapping_prefix}
ON {drug_exposure_prefix}.drug_concept_id = {route_mapping_prefix}.drug_concept_id
"""

DRUG_EXPOSURE_ALIAS = "de"
ROUTE_MAPPING_ALIAS = "rm"


def get_mapping_list(route_mappings_list):
    """
    Filters out name columns from route_mappings.csv file and returns list of mappings suitable for BQ

    :param route_mappings_list:
    :return: formatted list suitable for insert in BQ:
            (dose_form_concept_id1, route_concept_id1), (dose_form_concept_id1, route_concept_id1)
    """
    pair_exprs = []
    for route_mapping_dict in route_mappings_list:
        pair_expr = '({dose_form_concept_id}, {route_concept_id})'.format(**route_mapping_dict)
        pair_exprs.append(pair_expr)
    formatted_mapping_list = ', '.join(pair_exprs)
    return formatted_mapping_list


def create_dose_form_route_mappings_table(project_id, dataset_id=None):
    """
    Creates "_logging_dose_form_route_mappings" table with only id columns from resources/dose_form_route_mappings.csv

    :param project_id:
    :param dataset_id: BQ dataset_id
    :return: upload metadata for created table
    """
    if dataset_id is None:
        # Using table created in bq_dataset instead of re-creating in every dataset
        dataset_id = bq_utils.get_dataset_id()

    dose_form_routes_table_id = DOSE_FORM_ROUTES_TABLE_ID

    LOGGER.info("Creating %s.%s", dataset_id, DOSE_FORM_ROUTES_TABLE_ID)

    # create empty table
    bq_utils.create_table(DOSE_FORM_ROUTES_TABLE_ID, DOSE_FORM_ROUTE_FIELDS, drop_existing=True, dataset_id=dataset_id)

    dose_form_route_mappings_csv = os.path.join(resources.resource_path, DOSE_FORM_ROUTES_FILE + ".csv")
    dose_form_route_mappings_list = resources._csv_to_list(dose_form_route_mappings_csv)
    dose_form_routes_populate_query = INSERT_ROUTES_QUERY.format(
                                                    dataset_id=dataset_id,
                                                    project_id=project_id,
                                                    routes_table_id=DOSE_FORM_ROUTES_TABLE_ID,
                                                    mapping_list=get_mapping_list(dose_form_route_mappings_list))
    result = bq_utils.query(dose_form_routes_populate_query)
    LOGGER.info("Created %s.%s", dataset_id, dose_form_routes_table_id)
    return result


def create_drug_route_mappings_table(project_id, route_mapping_dataset_id, dose_form_routes_table_id,
                                     route_mapping_prefix):
    """
    Creates "drug_route_mappings" table using the query GET_DRUGS_FROM_DOSE_FORM
    
    :param project_id: the project containing the routes dataset
    :param route_mapping_dataset_id: dataset where the dose_form_route mapping table exists
            and where the drug_route mapping table will be created
    :param dose_form_routes_table_id: table_id of the dose_form_routes mapping table
    :param route_mapping_prefix: prefix for the dose_form_routes_mapping_table
    :return: upload metadata and created drug_route_table_id
    """
    if route_mapping_dataset_id is None:
        # Using table created in bq_dataset instead of re-creating in every dataset
        route_mapping_dataset_id = bq_utils.get_dataset_id()

    LOGGER.info("Creating %s.%s", route_mapping_dataset_id, DRUG_ROUTES_TABLE_ID)

    # create empty table
    bq_utils.create_table(DRUG_ROUTES_TABLE_ID, DRUG_ROUTE_FIELDS, drop_existing=True,
                          dataset_id=route_mapping_dataset_id)

    drug_routes_populate_query = GET_DRUGS_FROM_DOSE_FORM.format(
                                                    project_id=project_id,
                                                    vocabulary_dataset=common.VOCABULARY_DATASET,
                                                    route_mapping_dataset_id=route_mapping_dataset_id,
                                                    dose_form_route_mapping_table=dose_form_routes_table_id,
                                                    route_mapping_prefix=route_mapping_prefix)
    result = bq_utils.query(q=drug_routes_populate_query,
                            write_disposition='WRITE_TRUNCATE',
                            destination_dataset_id=route_mapping_dataset_id,
                            destination_table_id=DRUG_ROUTES_TABLE_ID,
                            batch=True)
    incomplete_jobs = bq_utils.wait_on_jobs([result['jobReference']['jobId']])
    if incomplete_jobs:
        LOGGER.debug('Failed job id {id}'.format(id=incomplete_jobs[0]))
        raise bq_utils.BigQueryJobWaitError(incomplete_jobs)
    LOGGER.info("Created %s.%s", route_mapping_dataset_id, DRUG_ROUTES_TABLE_ID)
    return result


def get_col_exprs():
    """
    Get the expressions used to populate drug_exposure

    :return: List of strings
    """
    fields = resources.fields_for(common.DRUG_EXPOSURE)
    route_field = "route_concept_id"
    col_exprs = []
    for field in fields:
        # by default we set to prefix for drug exposure
        col_expr = DRUG_EXPOSURE_ALIAS + '.' + field["name"]
        if field["name"] == route_field:
            # COALESCE(rm.route_concept_id, de.route_concept_id)
            col_expr = "COALESCE(%s, %s) AS route_concept_id" % (ROUTE_MAPPING_ALIAS + '.' + field["name"],
                                                                 DRUG_EXPOSURE_ALIAS + '.' + field["name"])
        col_exprs.append(col_expr)
    return col_exprs


def get_route_mapping_queries(project_id, dataset_id, route_mapping_dataset_id=None):
    """
    Generates queries to populate route_concept_ids correctly

    :param project_id: the project containing the dataset
    :param dataset_id: dataset containing the OMOP clinical data
    :param route_mapping_dataset_id: dataset containing the dose_form-route lookup table
    :return:
    """
    queries = []
    if route_mapping_dataset_id is None:
        route_mapping_dataset_id = bq_utils.get_dataset_id()
    result = create_dose_form_route_mappings_table(project_id, route_mapping_dataset_id)
    table = common.DRUG_EXPOSURE
    col_exprs = get_col_exprs()
    result = create_drug_route_mappings_table(project_id,
                                              route_mapping_dataset_id,
                                              DOSE_FORM_ROUTES_TABLE_ID,
                                              ROUTE_MAPPING_ALIAS)
    cols = ', '.join(col_exprs)
    query = dict()
    query[cdr_consts.QUERY] = FILL_ROUTE_ID_QUERY.format(project_id=project_id,
                                                         dataset_id=dataset_id,
                                                         drug_exposure_table=table,
                                                         route_mapping_dataset_id=route_mapping_dataset_id,
                                                         drug_route_mapping_table=DRUG_ROUTES_TABLE_ID,
                                                         cols=cols,
                                                         drug_exposure_prefix=DRUG_EXPOSURE_ALIAS,
                                                         route_mapping_prefix=ROUTE_MAPPING_ALIAS)
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
