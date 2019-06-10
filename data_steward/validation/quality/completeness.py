import bq_utils
import resources


def get_hpo_ids():
    return [hpo_item['hpo_id'] for hpo_item in resources.hpo_csv()]


def get_hpo_table_columns(dataset_id, hpo_id):
    '''
    Retrieves summary of all domain tables in a specified dataset associated with a given hpo_id

    :param dataset_id: identifies the dataset
    :param hpo_id: identifies the HPO
    :return: list of dict with keys table_name, column_name, row_count
    '''
    query = """SELECT table_name, column_name, t.row_count as table_row_count 
               FROM {dataset_id}.INFORMATION_SCHEMA.COLUMNS c
               JOIN {dataset_id}.__TABLES__ t on c.table_name=t.table_id
               WHERE STARTS_WITH(table_id, '{hpo_id}')=true AND
               NOT(table_id like '_mapping%') AND 
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
                  )""".format(hpo_id=hpo_id.lower(), dataset_id=dataset_id)
    query_response = bq_utils.query(query)
    rows = bq_utils.response2rows(query_response)
    return rows


def create_hpo_completeness_query(dataset_id, table_columns):
    query_with_concept_id = """SELECT current_datetime() as report_run_time, x.*, CASE WHEN total_rows=0 THEN 0 ELSE (num_nonnulls_zeros)/(total_rows) END as percent_field_populated 
       FROM (
            SELECT '{table_name}' as table_name, '{column_name}' as column_name, 
                   {table_row_count} as total_rows, 
                   sum(case when {column_name}=0 then 0 else 1 end) as num_nonnulls_zeros,
                   ({table_row_count} - count({column_name})) as non_populated_rows 
                   FROM {dataset_id}.{table_name} 
        ) as x 
    """
    query_without_concept_id = """SELECT current_datetime() as report_run_time, x.*, CASE WHEN total_rows=0 THEN 0 ELSE (num_nonnulls_zeros)/(total_rows) END as percent_field_populated 
       FROM (
            SELECT '{table_name}' as table_name, '{column_name}' as column_name, 
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


def run_completeness(project_id, dataset_id, hpo_id):
    table_columns = get_hpo_table_columns(dataset_id, hpo_id)
    query = create_hpo_completeness_query(dataset_id, table_columns)
    query_response = bq_utils.query(query)
    rows = bq_utils.response2rows(query_response)
    return rows


# Output the data completeness for the omop tables
# All the data is written to a file per site.
if __name__ == '__main__':
    import json
    import os

    GOOGLE_APPLICATION_CREDENTIALS = '/path/to/json'
    DATASET_ID = ''
    HPO_ID = ''

    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = GOOGLE_APPLICATION_CREDENTIALS
    os.environ['BIGQUERY_DATASET_ID'] = DATASET_ID
    with open(GOOGLE_APPLICATION_CREDENTIALS, 'rb') as creds_fp:
        creds = json.load(creds_fp)
        APPLICATION_ID = creds.get('project_id')
        os.environ['APPLICATION_ID'] = APPLICATION_ID
        RESULT = run_completeness(DATASET_ID, HPO_ID)
        print RESULT
