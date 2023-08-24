# Python imports
from io import open

# Third party imports

# Project imports
import bq_utils
import resources
from common import BIGQUERY_DATASET_ID
from constants.validation.metrics import completeness as consts


def get_hpo_ids():
    """
    Get identifiers for all HPO sites

    :return: A list of HPO ids
    """
    return [hpo_item[consts.HPO_ID] for hpo_item in bq_utils.get_hpo_info()]


def get_cols(dataset_id):
    """
    Retrieves summary of all domain table columns in a specified dataset

    :param dataset_id: identifies the dataset
    :return: list of dict with keys table_name, omop_table_name, column_name, row_count
    """
    query = consts.COLUMNS_QUERY_FMT.format(dataset_id=dataset_id)
    query_response = bq_utils.query(query)
    cols = bq_utils.response2rows(query_response)
    results = []
    # Only yield columns associated with OMOP tables and populate omop_table_name
    for col in cols:
        omop_table_name = get_standard_table_name(col[consts.TABLE_NAME])
        if omop_table_name:
            col[consts.OMOP_TABLE_NAME] = omop_table_name
            if col[consts.TABLE_ROW_COUNT] > 0:
                results.append(col)
    return results


def create_completeness_query(dataset_id, columns):
    subqueries = []
    for column in columns:
        concept_zero_expr = "0"
        if column[consts.COLUMN_NAME].endswith('concept_id'):
            concept_zero_expr = consts.CONCEPT_ZERO_CLAUSE.format(**column)
        subquery = consts.COMPLETENESS_SUBQUERY_FMT.format(
            dataset_id=dataset_id,
            concept_zero_expr=concept_zero_expr,
            **column)
        subqueries.append(subquery)
    union_all_subqueries = consts.UNION_ALL.join(subqueries)

    if not union_all_subqueries:
        result = consts.EMPTY_COMPLETENESS_QUERY
    else:
        result = consts.COMPLETENESS_QUERY_FMT.format(
            union_all_subqueries=union_all_subqueries)

    return result


def is_omop_col(col):
    """
    True if col belongs to an OMOP table

    :param col: column summary
    :return: True if col belongs to a domain table, False otherwise
    """
    for cdm_table in resources.CDM_TABLES:
        if col[consts.TABLE_NAME].endswith(cdm_table):
            return True
    return False


def is_hpo_col(hpo_id, col):
    """
    True if col is on specified HPO table

    :param hpo_id: identifies the HPO
    :param col: column summary
    :return: True if col is on specified HPO table, False otherwise
    """
    hpo_tables = [
        resources.get_table_id(table, hpo_id=hpo_id)
        for table in resources.CDM_TABLES
    ]
    return col[consts.TABLE_NAME] in hpo_tables


def get_standard_table_name(table_name):
    """
    Get the name of the CDM table associated with a column (whether hpo-specific or not)

    :param table_name: table
    :return: string name of the associated CDM table or None otherwise
    """
    for cdm_table in sorted(resources.CDM_TABLES, key=len, reverse=True):
        # skip system tables
        if table_name.startswith('_'):
            continue
        if table_name.endswith(cdm_table):
            return cdm_table
    return None


def column_completeness(dataset_id, columns):
    """
    Determines completeness metrics for a list of columns in a dataset

    :param dataset_id: identifies the dataset
    :param columns: list of column summaries
    :return:
    """
    query = create_completeness_query(dataset_id, columns)
    query_response = bq_utils.query(query)
    results = bq_utils.response2rows(query_response)
    return results


def get_hpo_completeness_query(hpo_id, dataset_id=None):
    """
    Get the query used to compute completeness for tables in an HPO submission

    :param hpo_id:
    :param dataset_id:
    :return:
    """
    if dataset_id is None:
        dataset_id = BIGQUERY_DATASET_ID
    cols = get_cols(dataset_id)
    hpo_cols = [col for col in cols if is_hpo_col(hpo_id, col)]
    query = create_completeness_query(dataset_id, hpo_cols)
    return query


def hpo_completeness(dataset_id, hpo_id):
    """
    Generate completeness metrics for OMOP dataset of specified HPO

    :param dataset_id:
    :param hpo_id:
    :return: list of dict with table_name, column_name, total_rows, num_nonnulls_zeros, non_populated_rows
    """
    cols = get_cols(dataset_id)
    hpo_cols = [col for col in cols if is_hpo_col(hpo_id, col)]
    results = column_completeness(dataset_id, hpo_cols)
    return results


if __name__ == '__main__':
    import argparse
    import json
    import os

    JSON_INDENT = 4

    def get_creds(creds_path):
        with open(creds_path, 'r') as creds_fp:
            return json.load(creds_fp)

    def run_with_args(credentials, dataset_id, hpo_id):
        """
        Generate completeness metrics for OMOP dataset
        """
        creds = get_creds(credentials)
        project_id = creds.get(consts.PROJECT_ID)
        os.environ[consts.APPLICATION_ID] = project_id
        os.environ[consts.GOOGLE_APPLICATION_CREDENTIALS] = credentials
        os.environ[consts.BIGQUERY_DATASET_ID] = dataset_id

        hpo_ids = [hpo_id] if hpo_id else get_hpo_ids()
        cols = get_cols(dataset_id)

        results = dict()
        for hpo_id in hpo_ids:
            hpo_cols = [col for col in cols if is_hpo_col(hpo_id, col)]
            hpo_results = column_completeness(dataset_id, hpo_cols)
            results[hpo_id] = hpo_results
        return results

    parser = argparse.ArgumentParser(
        description=
        'Generate completeness metrics for OMOP dataset of specified HPO')
    parser.add_argument('-c',
                        '--credentials',
                        required=True,
                        help='Path to GCP credentials file')
    parser.add_argument(
        '-d',
        '--dataset_id',
        required=True,
        help='Identifies the dataset containing the OMOP tables to report on')
    parser.add_argument(
        'hpo_id',
        nargs='?',
        help='Identifies an HPO site to report on; all sites by default')
    ARGS = parser.parse_args()
    completeness_rows = run_with_args(ARGS.credentials, ARGS.dataset_id,
                                      ARGS.hpo_id)
    print(json.dumps(completeness_rows, indent=JSON_INDENT, sort_keys=True))
