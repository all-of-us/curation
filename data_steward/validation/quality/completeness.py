import bq_utils
import resources


def get_hpo_ids():
    return [hpo_item['hpo_id'] for hpo_item in resources.hpo_csv()]


def get_table_columns(dataset_id):
    """
    Retrieves summary of all domain tables in a specified dataset associated with a given hpo_id

    :param dataset_id: identifies the dataset
    :param hpo_id: identifies the HPO
    :return: list of dict with keys table_name, column_name, row_count
    """
    query = """SELECT table_name, column_name, t.row_count as table_row_count 
               FROM {dataset_id}.INFORMATION_SCHEMA.COLUMNS c
               JOIN {dataset_id}.__TABLES__ t on c.table_name=t.table_id
               WHERE 
               table_id NOT like '_mapping%' AND 
                (
                  table_id like '%person' OR
                  table_id like '%visit_occurrence' OR
                  table_id like '%condition_occurrence' OR
                  table_id like '%procedure_occurrence' OR
                  table_id like '%drug_exposure' OR
                  table_id like '%measurement' OR
                  table_id like '%observation' OR
                  table_id like '%device_exposure' OR
                  table_id like '%death' OR
                  table_id like '%provider' OR
                  table_id like '%specimen' OR
                  table_id like '%location' OR
                  table_id like '%care_site' OR
                  table_id like '%note'
                  )""".format(dataset_id=dataset_id)
    query_response = bq_utils.query(query)
    rows = bq_utils.response2rows(query_response)
    return rows


def create_hpo_completeness_query(dataset_id, table_columns):
    query_with_concept_id = """
      SELECT current_datetime() as report_run_time, 
        x.*, 
        CASE WHEN total_rows=0 THEN 0 ELSE (num_nonnulls_zeros)/(total_rows) END as percent_field_populated 
      FROM (
        SELECT '{table_name}' as table_name,
               '{column_name}' as column_name,
               {table_row_count} as total_rows,
               sum(case when {column_name}=0 then 0 else 1 end) as num_nonnulls_zeros,
               ({table_row_count} - count({column_name})) as non_populated_rows
        FROM {dataset_id}.{table_name} 
      ) as x 
    """
    query_without_concept_id = """
    SELECT current_datetime() as report_run_time, 
      x.*,
      CASE WHEN total_rows=0 THEN 0 ELSE (num_nonnulls_zeros)/(total_rows) END as percent_field_populated
      FROM (
        SELECT '{table_name}' as table_name, 
          '{column_name}' as column_name,
          {table_row_count} as total_rows,
          count({column_name}) as num_nonnulls_zeros,
          ({table_row_count} - count({column_name})) as non_populated_rows
        FROM {dataset_id}.{table_name} 
      ) as x 
    """
    queries = []
    for row in table_columns:
        if row['column_name'] == '_PARTITIONTIME':
            continue
        query_format = query_with_concept_id if row['column_name'].endswith('concept_id') else query_without_concept_id
        x = query_format.format(dataset_id=dataset_id, **row)
        queries.append(x)

    return " union all ".join(queries)


def run_hpo(project_id, dataset_id, hpo_id):
    """
    Generate completeness metrics for OMOP dataset of specified HPO

    :param project_id:
    :param dataset_id:
    :param hpo_id:
    :return: list of dict with table_name, column_name, total_rows, num_nonnulls_zeros, non_populated_rows
    """
    all_table_columns = get_table_columns(dataset_id)
    hpo_table_columns = []
    for table_column in all_table_columns:
        if table_column['table_name'].startswith(hpo_id):
            hpo_table_columns.extend(table_column)
    query = create_hpo_completeness_query(dataset_id, hpo_table_columns)
    query_response = bq_utils.query(query)
    rows = bq_utils.response2rows(query_response)
    return rows


def run_all(project_id, dataset_id):
    """
    Generate completeness metrics for all HPO omops in a bigquery dataset

    :param project_id: identifies the project
    :param dataset_id: identifies the bigquery dataset
    :return: A dict with hpo_id => completeness rows
    """
    result = dict()
    hpo_ids = get_hpo_ids()
    for hpo_id in hpo_ids:
        rows = run_hpo(project_id, dataset_id, hpo_id)
        result[hpo_id] = rows
    return result


if __name__ == '__main__':
    import argparse
    import json
    import os

    parser = argparse.ArgumentParser(description=run_all.__doc__)
    parser.add_argument('--credentials', help='Path to credentials file')
    parser.add_argument('--dataset_id', help='Identifies the dataset containing the OMOP tables to report on')
    parser.add_argument('hpo_id', nargs='?', help='Identifies an HPO site to report on; all sites by default')
    args = parser.parse_args()

    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = args.credentials
    os.environ['BIGQUERY_DATASET_ID'] = args.dataset_id

    with open(args.credentials, 'rb') as creds_fp:
        creds = json.load(creds_fp)
        PROJECT_ID = creds.get('project_id')
        os.environ['APPLICATION_ID'] = PROJECT_ID
        if args.hpo_id:
            RESULT = run_hpo(PROJECT_ID, args.dataset_id, args.hpo_id)
        else:
            RESULT = run_all(PROJECT_ID, args.dataset_id)
        print RESULT
