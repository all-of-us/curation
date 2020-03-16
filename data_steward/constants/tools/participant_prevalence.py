PARTICIPANT_ROWS = """
SELECT '{table}' AS table_id, {count_types}
FROM `{project}.{dataset}.{table}`
WHERE person_id IN ({pids_string})
"""

SELECT_ALL_COUNT = "COUNT(*) AS all_count"
SELECT_EHR_COUNT = "COUNT(IF({table_id} > {const}, 1, NULL)) as ehr_count"
SELECT_ZERO_COUNT = "0 AS ehr_count"

UNION_ALL = """
UNION ALL
"""

PID_QUERY = """
SELECT person_id
FROM `{pid_project}.{sandbox_dataset}.{pid_table}`
"""

DATASET_ID = 'dataset_id'
TABLE_ID = 'table_id'
COUNT = 'count'
EHR_COUNT = 'ehr_count'
ALL_COUNT = 'all_count'

# Query to list all tables within a dataset that contains person_id in the schema
PERSON_TABLE_QUERY = """
SELECT table_name
FROM `{project}.{dataset}.INFORMATION_SCHEMA.COLUMNS`
WHERE COLUMN_NAME = 'person_id'
AND ordinal_position = 2
"""

TABLE_NAME_COLUMN = 'table_name'
COLUMN_NAME = 'column_name'
