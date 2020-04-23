import re

RENAME = 'rename'
SKIP = 'skip'
CREATE = 'create'

CURRENT_RELEASE_REGEX = re.compile('R\d{4}q\dr\d')

RENAME_DEID_MAP_TABLE_QUERY = """
CREATE OR REPLACE TABLE `{project}.{dataset}._deid_map` AS (
SELECT *
FROM `{project}.{dataset}.deid_map`
)
"""
CREATE_DEID_MAP_TABLE_QUERY = """
CREATE OR REPLACE TABLE `{project}.{dataset}._deid_map` AS (
SELECT c.person_id, d.person_id AS research_id
FROM `{project}.{dataset}.observation` c
FULL OUTER JOIN `{project}.{dataset}.observation` d
ON c.observation_id = d.observation_id
)
"""
