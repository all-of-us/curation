import bq_utils
import resources

COLUMNS_QUERY_FMT = """
SELECT table_name, 
 column_name, 
 t.row_count as table_row_count 
FROM {dataset_id}.INFORMATION_SCHEMA.COLUMNS c
 JOIN {dataset_id}.__TABLES__ t on c.table_name = t.table_id
  WHERE t. table_id NOT LIKE '\\\\_%' 
  AND c.IS_HIDDEN = 'NO'
 ORDER BY table_name, c.ORDINAL_POSITION
"""

COMPLETENESS_QUERY_FMT = """
SELECT *,
 CASE 
  WHEN table_row_count=0 THEN NULL 
  ELSE 1 - (null_count + concept_zero_count)/(table_row_count)
 END as percent_populated 
FROM (
 SELECT '{table_name}' AS table_name, 
  {table_row_count} AS table_row_count,
  '{column_name}' AS column_name,
  {table_row_count} - count({column_name}) as null_count,
  {concept_zero_expr} AS concept_zero_count
 FROM {dataset_id}.{table_name}
) AS counts
"""


def get_hpo_ids():
    return [hpo_item['hpo_id'] for hpo_item in resources.hpo_csv()]


def get_columns(dataset_id):
    """
    Retrieves summary of all domain tables in a specified dataset

    :param dataset_id: identifies the dataset
    :param hpo_id: identifies the HPO
    :return: list of dict with keys table_name, column_name, row_count
    """
    query = COLUMNS_QUERY_FMT.format(dataset_id=dataset_id)
    query_response = bq_utils.query(query)
    rows = bq_utils.response2rows(query_response)
    results = filter_domain_columns(rows)
    return results


def create_completeness_query(dataset_id, columns):
    subqueries = []
    for column in columns:
        concept_zero_expr = "0"
        if column['column_name'].endswith('concept_id'):
            concept_zero_expr = "SUM(CASE WHEN {column_name}=0 THEN 1 ELSE 0 END)".format(**column)
        subquery = COMPLETENESS_QUERY_FMT.format(dataset_id=dataset_id, concept_zero_expr=concept_zero_expr, **column)
        subqueries.append(subquery)
    return '\nUNION ALL\n'.join(subqueries)


def filter_domain_columns(columns):
    result = []
    for column in columns:
        for cdm_table in resources.CDM_TABLES:
            if column['table_name'].endswith(cdm_table):
                result.append(column)
                break
    return result


def filter_hpo_columns(columns, hpo_id):
    for column in columns:
        if column['table_name'].startswith(hpo_id):
            yield column


def column_completeness(dataset_id, columns):
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
    columns = get_columns(dataset_id)
    hpo_columns = filter_hpo_columns(columns, hpo_id)
    results = column_completeness(dataset_id, hpo_columns)
    return results


if __name__ == '__main__':
    import argparse
    import json
    import os

    def get_creds(creds_path):
        with open(creds_path, 'rb') as creds_fp:
            return json.load(creds_fp)

    def run_with_args(credentials, dataset_id, hpo_id):
        creds = get_creds(credentials)
        project_id = creds.get('project_id')
        os.environ['APPLICATION_ID'] = project_id
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials
        os.environ['BIGQUERY_DATASET_ID'] = dataset_id

        hpo_ids = [hpo_id] if hpo_id else get_hpo_ids()
        columns = get_columns(dataset_id)

        results = dict()
        for hpo_id in hpo_ids:
            hpo_columns = filter_hpo_columns(columns, hpo_id)
            hpo_results = column_completeness(dataset_id, hpo_columns)
            results[hpo_id] = hpo_results
        return results

    parser = argparse.ArgumentParser(description=hpo_completeness.__doc__)
    parser.add_argument('--credentials', help='Path to credentials file')
    parser.add_argument('--dataset_id', help='Identifies the dataset containing the OMOP tables to report on')
    parser.add_argument('hpo_id', nargs='?', help='Identifies an HPO site to report on; all sites by default')
    ARGS = parser.parse_args()
    print run_with_args(ARGS.credentials, ARGS.dataset_id, ARGS.hpo_id)
