from common import JINJA_ENV

PARTITIONS_QUERY = JINJA_ENV.from_string("""
SELECT table_name, partition_id
FROM (SELECT 
    table_name,
    partition_id,
    ROW_NUMBER() OVER(PARTITION BY table_name ORDER BY partition_id DESC) r
FROM `{{project_id}}.{{drc_ops_dataset}}.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name LIKE '%identity_match%'
AND partition_id NOT IN ("__NULL__", "__UNPARTITIONED__")
) WHERE r = 1
""")

SNAPSHOT_TABLE_QUERY = JINJA_ENV.from_string("""
INSERT `{{fq_dest_table}}`
SELECT *
FROM `{{fq_source_table}}`
WHERE FORMAT_TIMESTAMP("%Y%m%d%H", _PARTITIONTIME) = "{{partition_date}}"
""")
