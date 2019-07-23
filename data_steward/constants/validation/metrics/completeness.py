HPO_ID = 'hpo_id'
COLUMN_NAME = 'column_name'
TABLE_NAME = 'table_name'
OMOP_TABLE_NAME = 'omop_table_name'
PROJECT_ID = 'project_id'
APPLICATION_ID = 'APPLICATION_ID'
GOOGLE_APPLICATION_CREDENTIALS = 'GOOGLE_APPLICATION_CREDENTIALS'
BIGQUERY_DATASET_ID = 'BIGQUERY_DATASET_ID'

# Template query strings
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
  '{omop_table_name}' AS omop_table_name, 
  {table_row_count} AS table_row_count,
  '{column_name}' AS column_name,
  {table_row_count} - count({column_name}) as null_count,
  {concept_zero_expr} AS concept_zero_count
 FROM {dataset_id}.{table_name}
) AS counts
"""
CONCEPT_ZERO_CLAUSE = "SUM(CASE WHEN {column_name}=0 THEN 1 ELSE 0 END)"
UNION_ALL = '\nUNION ALL\n'
