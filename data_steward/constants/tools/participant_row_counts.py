CDM_MAPPING_TABLE_COUNT = """
SELECT table_id, all_count, all_ehr_count, map_ehr_count
FROM (
    SELECT '{{table}}' AS table_id, COUNT(*) AS all_count
    FROM `{{project}}.{{dataset}}.{{table}}`
    WHERE person_id IN {{pids_expr}})
LEFT JOIN (
    SELECT '{{table}}' AS table_id, COUNT(*) AS all_ehr_count
    FROM `{{project}}.{{dataset}}.{{table}}`
    WHERE {{table_id}} >= {{ID_CONST}}
    AND person_id IN {{pids_expr}})
USING (table_id)
LEFT JOIN (
    SELECT '{{table}}' AS table_id, COUNT(*) AS map_ehr_count
    FROM `{{project}}.{{dataset}}.{{table}}`
    LEFT JOIN `{{project}}.{{dataset}}.{{mapping_table}}`
    USING ({{table_id}})
    WHERE person_id IN {{pids_expr}}
    AND {{src_id}} NOT IN ('PPI/PM', 'rdr'))
USING (table_id)
"""

PID_TABLE_COUNT = """
SELECT '{{table}}' AS table_id, COUNT(*) AS all_count, COUNT(*) AS map_count, {{ehr_count}} AS ehr_count
FROM `{{project}}.{{dataset}}.{{table}}`
WHERE person_id IN {{pids_expr}}
"""

UNION_ALL = """
UNION ALL
"""

PID_QUERY = """
(SELECT {pid}
FROM `{pid_source}`)
"""

ALL_DATASETS = 'all_datasets'

DATASET_ID = 'dataset_id'
TABLE_ID = 'table_id'
PERSON_ID = 'person_id'
RESEARCH_ID = 'research_id'

COUNT = 'count'
ALL_COUNT = 'all_count'
ALL_EHR_COUNT = 'all_ehr_count'
MAP_EHR_COUNT = 'map_ehr_count'

# Query to list all tables within a dataset that contains person_id in the schema
TABLE_INFO_QUERY = """
SELECT *
FROM `{project}.{dataset}.INFORMATION_SCHEMA.COLUMNS`
"""

TABLE_NAME = 'table_name'
COLUMN_NAME = 'column_name'
