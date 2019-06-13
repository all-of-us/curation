import bq_utils
import resources
import constants.validation.metrics.completeness as consts


def get_hpo_ids():
    """
    Get identifiers for all HPO sites

    :return: A list of HPO ids
    """
    return [hpo_item[consts.HPO_ID] for hpo_item in resources.hpo_csv()]


def get_cols(dataset_id):
    """
    Retrieves summary of all domain table columns in a specified dataset

    :param dataset_id: identifies the dataset
    :return: list of dict with keys table_name, column_name, row_count
    """
    query = consts.COLUMNS_QUERY_FMT.format(dataset_id=dataset_id)
    query_response = bq_utils.query(query)
    cols = bq_utils.response2rows(query_response)
    results = filter(is_domain_col, cols)
    return results


def create_completeness_query(dataset_id, columns):
    subqueries = []
    for column in columns:
        concept_zero_expr = "0"
        if column[consts.COLUMN_NAME].endswith('concept_id'):
            concept_zero_expr = consts.CONCEPT_ZERO_CLAUSE.format(**column)
        subquery = consts.COMPLETENESS_QUERY_FMT.format(dataset_id=dataset_id, concept_zero_expr=concept_zero_expr, **column)
        subqueries.append(subquery)
    return '\nUNION ALL\n'.join(subqueries)


def is_domain_col(col):
    """
    True if col belongs to a domain table

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
    return col[consts.TABLE_NAME].startswith(hpo_id)


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


def hpo_completeness(dataset_id, hpo_id):
    """
    Generate completeness metrics for OMOP dataset of specified HPO

    :param dataset_id:
    :param hpo_id:
    :return: list of dict with table_name, column_name, total_rows, num_nonnulls_zeros, non_populated_rows
    """
    cols = get_cols(dataset_id)
    hpo_cols = filter(lambda col: is_hpo_col(hpo_id, col), cols)
    results = column_completeness(dataset_id, hpo_cols)
    return results


if __name__ == '__main__':
    import argparse
    import json
    import os

    def get_creds(creds_path):
        with open(creds_path, 'rb') as creds_fp:
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
            hpo_cols = filter(lambda col: is_hpo_col(hpo_id, col), cols)
            hpo_results = column_completeness(dataset_id, hpo_cols)
            results[hpo_id] = hpo_results
        return results

    parser = argparse.ArgumentParser(description=hpo_completeness.__doc__)
    parser.add_argument('--credentials',
                        required=True,
                        help='Path to GCP credentials file')
    parser.add_argument('--dataset_id',
                        required=True,
                        help='Identifies the dataset containing the OMOP tables to report on')
    parser.add_argument('hpo_id', nargs='?', help='Identifies an HPO site to report on; all sites by default')
    ARGS = parser.parse_args()
    print run_with_args(ARGS.credentials, ARGS.dataset_id, ARGS.hpo_id)
