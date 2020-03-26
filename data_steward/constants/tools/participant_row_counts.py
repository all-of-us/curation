CDM_MAPPING_TABLE_COUNT = """
SELECT '{{table}}' AS table_id, SUM(each_count) AS all_count, COUNT(*) AS ehr_count
FROM (
SELECT *, COUNT(1) over(partition BY {{table_id}}) AS each_count
FROM `{{project}}.{{dataset}}.{{table}}`
WHERE person_id IN {{pids_expr}}) t
LEFT JOIN `{{project}}.{{dataset}}.{{mapping_table}}` m
USING ({{table_id}})
WHERE m.src_hpo_id != 'PPI/PM'
"""

PID_TABLE_COUNT = """
SELECT '{{table}}' AS table_id, COUNT(*) AS all_count, {{ehr_count}} AS ehr_count
FROM `{{project}}.{{dataset}}.{{table}}`
WHERE person_id IN {{pids_expr}}
"""

UNION_ALL = """
UNION ALL
"""

PID_QUERY = """
(SELECT person_id
FROM `{pid_source}`)
"""

DATASET_ID = 'dataset_id'
TABLE_ID = 'table_id'
PERSON_ID = 'person_id'
COUNT = 'count'
EHR_COUNT = 'ehr_count'
ALL_COUNT = 'all_count'

# Query to list all tables within a dataset that contains person_id in the schema
TABLE_INFO_QUERY = """
SELECT *
FROM `{project}.{dataset}.INFORMATION_SCHEMA.COLUMNS`
WHERE COLUMN_NAME = 'person_id'
"""

TABLE_NAME = 'table_name'
COLUMN_NAME = 'column_name'
