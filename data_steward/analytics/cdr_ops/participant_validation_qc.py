# -*- coding: utf-8 -*-
# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.7.1
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# ## Notebook parameters

# + tags=["parameters"]
PROJECT_ID = ""  # identifies the project containing the datasets
DRC_DATASET_ID = ""  # Identifies the DRC dataset
EHR_DATASET_ID = ""  # Identifies the EHR dataset
EHR_SNAPSHOT_ID = ""  # Identifies the EHR snapshot dataset
LOOKUP_DATASET_ID = ""  # Identifies the lookup dataset
VALIDATION_DATASET_ID = ""  # Identifies the validation dataset
EXCLUDED_SITES = "''"  # List of excluded sites passed as string: eg. "'hpo_id1', 'hpo_id_2', 'hpo_id3',..."
EXCLUDE_IDENTITY_MATCH = "'identity_match_" + EXCLUDED_SITES.replace(" '", "'identity_match_")[1:]
RUN_AS = ""
# -

import pandas as pd
from utils import auth
from gcloud.bq import BigQueryClient
from analytics.cdr_ops.notebook_utils import execute, IMPERSONATION_SCOPES

impersonation_creds = auth.get_impersonation_credentials(
    RUN_AS, target_scopes=IMPERSONATION_SCOPES)

client = BigQueryClient(PROJECT_ID, credentials=impersonation_creds)

pd.options.display.max_rows = 1000
pd.options.display.max_colwidth = 0
pd.options.display.max_columns = None
pd.options.display.width = None
# -

# # QC for Participant Validation
# - This notebook checks the data quality of the participant validation tables and the EHR snapshot tables.
# - Run this notebook after generating the EHR snapshot dataset and the validation dataset.

# # Latest identity match table for each EHR site
# - This query lists the latest partitions of the identity match tables for the EHR sites.
# - Look at the columns `partition_id` and `last_modified_time`, and make sure the latest partitions were created recently (ideally within a few days).
# - If any EHR sites' latest partitions are old, try running `create_update_drc_id_match_table.py` for those EHR sites. It will create the latest partitions.

query = f'''
SELECT * EXCEPT (r)
FROM (
    SELECT *, ROW_NUMBER() OVER(PARTITION BY table_name ORDER BY partition_id DESC) r
    FROM `{PROJECT_ID}.{DRC_DATASET_ID}.INFORMATION_SCHEMA.PARTITIONS`
    WHERE partition_id NOT IN ('__NULL__')
    AND table_name NOT IN ({EXCLUDE_IDENTITY_MATCH})
)
WHERE r = 1
ORDER BY total_rows DESC
'''
execute(client, query)

# # Row count comparison (identity match tables and person tables)
# - This query compares the row counts of the idendity table and the person table for each HPO site.
# - `diff` should be 0 for all the HPO sites. If not, investigate.
# - The SQLs in the following paragraphs will be helpful for the investigation.

query = f'''
SELECT partition_id, table_name, table_id, total_rows, row_count, total_rows - row_count AS diff
FROM (
    SELECT *, ROW_NUMBER() OVER(PARTITION BY table_name ORDER BY partition_id DESC) r
    FROM `{PROJECT_ID}.{DRC_DATASET_ID}.INFORMATION_SCHEMA.PARTITIONS`
    WHERE partition_id NOT IN ('__NULL__')
    AND table_name NOT LIKE '%unioned%'
    AND table_name NOT LIKE '%mapping%'
)
FULL JOIN `{PROJECT_ID}.{EHR_DATASET_ID}.__TABLES__`
ON (SUBSTR(table_name, LENGTH('identity_match__')) = REGEXP_EXTRACT(table_id, r'(.*)_person'))
WHERE table_id LIKE '%person%'
AND table_id NOT LIKE '%unioned%'
AND table_id NOT LIKE '%mapping%'
AND r = 1
AND total_rows != row_count
ORDER BY diff DESC
'''
execute(client, query)

# # HPO sites with duplicate person IDs
# - This query lists up the HPO sites that have duplicate person IDs and how many duplicates they have.
# - For the HPO sites with `diff` not 0 in the previous query, see the result of this query and see if duplicates are the cause of the `diff`.
# - If there are duplicate person IDs, report that to EHR Ops and Curation to discuss how to move forward.
# - If the cause of the `diff` is still not clear, the next query might help identify the cause.

query = f'''
WITH hpos AS (
   SELECT LOWER(hpo_id) as hpo_id
   FROM `{PROJECT_ID}.{LOOKUP_DATASET_ID}.hpo_site_id_mappings`
   WHERE TRIM(hpo_id) IS NOT NULL
   AND TRIM(LOWER(hpo_id)) NOT IN ('', {EXCLUDED_SITES})
)
SELECT CONCAT(
   "SELECT * FROM (",
   ARRAY_TO_STRING(ARRAY_AGG(FORMAT(
      """
      SELECT '%s' AS hpo_id, SUM(Individual_Duplicate_ID_Count-1) as count
      FROM (
         SELECT COUNT(person_id) AS Individual_Duplicate_ID_Count
         FROM `{PROJECT_ID}.{EHR_DATASET_ID}.%s_person`
         GROUP BY person_id HAVING COUNT(person_id) > 1
      )
      """, 
      hpo_id, hpo_id)), 
      "UNION ALL"),
   ") WHERE count!=0 "
) as q
FROM hpos
'''
sql_to_run = execute(client, query).iloc[0, 0]
execute(client, sql_to_run)

# # Missing person IDs in the EHR snapshot dataset
# - This query lists up the HPO sites that have person IDs that are not in its identity match table and how many IDs are missing.
# - For the HPO sites with `diff` not 0 in the previous query, see the result of this query and see this is the cause of the `diff`.
# - If there are missing person IDs, report that to EHR Ops and Curation to discuss how to move forward.
# - If the cause of the `diff` is still not clear, continue investigation and discuss within Curation on how to move forward.

query = f'''
WITH hpos AS (
   SELECT LOWER(hpo_id) as hpo_id
   FROM `{PROJECT_ID}.{LOOKUP_DATASET_ID}.hpo_site_id_mappings`
   WHERE TRIM(hpo_id) IS NOT NULL
   AND TRIM(LOWER(hpo_id)) NOT IN ('', {EXCLUDED_SITES})
)
SELECT ARRAY_TO_STRING(ARRAY_AGG(FORMAT(
   """
   SELECT '%s' as hpo_id, COUNT(person_id) as n
   FROM `{PROJECT_ID}.{EHR_SNAPSHOT_ID}.%s_person`
   WHERE person_id NOT IN (
      SELECT person_id
      FROM `{PROJECT_ID}.{VALIDATION_DATASET_ID}.%s_identity_match`
   )
   """,
   hpo_id, hpo_id, hpo_id)), 
   "UNION ALL")
FROM hpos
'''
sql_to_run = execute(client, query).iloc[0, 0]
execute(client, sql_to_run)
