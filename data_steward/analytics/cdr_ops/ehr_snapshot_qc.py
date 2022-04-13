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
BASELINE_EHR_DATASET_ID = ""  # Identifies the dataset the snapshot was created from
EHR_SNAPSHOT_DATASET_ID = ""  # Identifies the snapshot dataset
RELEASE_TAG = ""  # identifies the release tag for the current CDR release
EXCLUDED_SITES = "default"  # List of excluded sites passed as string: eg. "'hpo_id1', 'hpo_id_2', 'hpo_id3',..."
# -

# +
import pandas as pd

from utils.bq import get_client
from analytics.cdr_ops.notebook_utils import execute
from common import MAPPED_CLINICAL_DATA_TABLES

client = get_client(PROJECT_ID)

pd.options.display.max_rows = 1000
pd.options.display.max_colwidth = 0
pd.options.display.max_columns = None
pd.options.display.width = None

# -

stage = 'unioned_ehr'
for suffix in ['_backup', '_staging', '_sandbox', '']:
    dataset_id = f'{RELEASE_TAG}_unioned_ehr{suffix}'
    dataset = client.get_dataset(dataset_id)
    print(f'''{dataset.dataset_id}
  description: {dataset.description}
  labels: {dataset.labels}
  
    ''')

# ## QC for EHR Snapshot
#
# Quality checks performed on a new EHR Snapshot dataset and comparison with the baseline ehr dataset.

# # Table comparison
# The snapshot should contain all existing tables in the baseline dataset.
# In ideal circumstances, this query will not return any results.
# Any missing tables as listed by the below query indicate an erroneous copy operation.
# The missing tables need to be copied over to the snapshot dataset until no
# results appear for the below query.

query = f'''
SELECT table_id, o.row_count as o_row_count, n.row_count as n_row_count, o.row_count - n.row_count as diff
FROM `{PROJECT_ID}.{BASELINE_EHR_DATASET_ID}.__TABLES__` o
LEFT JOIN `{PROJECT_ID}.{EHR_SNAPSHOT_DATASET_ID}.__TABLES__` n
USING (table_id)
WHERE NOT (table_id LIKE '%mapping%' OR table_id LIKE '%unioned%')
AND n.row_count IS NULL
'''
execute(client, query)
# -

# # Row count comparison
# The snapshot tables should have the same row counts as that of the baseline dataset.
# In ideal circumstances, this query will not return any results.
# Any tables with differing row counts have not been copied correctly.
# If the difference is justified due to site exclusion etc., the tables can be ignored.
# Else, such tables need to be copied again so that the below query returns no results.

query = f'''
SELECT table_id, o.row_count as o_row_count, n.row_count as n_row_count, o.row_count - n.row_count as diff
FROM `{PROJECT_ID}.{BASELINE_EHR_DATASET_ID}.__TABLES__` o
LEFT JOIN `{PROJECT_ID}.{EHR_SNAPSHOT_DATASET_ID}.__TABLES__` n
USING (table_id)
WHERE NOT (table_id LIKE '%mapping%' OR table_id LIKE '%unioned%')
AND o.row_count - n.row_count != 0
'''
execute(client, query)

# # Zero row counts
# The snapshot should contain tables with zero rows for sites that are excluded by EHR Ops,
# or if a site has no data to submit for those specific tables.
# In ideal circumstances, these are the only possibilities that lead to tables with zero rows.
# However, any tables that do not meet the above criteria but have zero rows
# need to be investigated and copied again if necessary.

query = f'''
SELECT *
FROM `{PROJECT_ID}.{EHR_SNAPSHOT_DATASET_ID}.__TABLES__`
WHERE row_count = 0
AND REGEXP_CONTAINS(table_id, r'(person)|(observation)|(care_site)|(occurrence)|(death)|(exposure)|(fact_realtionship)|(measurement)|(location)|(note)|(observation)|(provider)|(specimen)')
AND NOT REGEXP_CONTAINS(table_id, r'(sets)')
ORDER BY table_id
'''
execute(client, query)

# # Person_to_observation records observation_date and observation_datetime check
# Check to make sure records added to observation with observation_concept_id in (4013886, 421761, 4135376, 4083587) are
# not using participant’s birth date, as seen in person.birth_datetime.
# Check is successful when result is empty

query = f"""
WITH
  person_birth_date AS (
  SELECT
    birth_datetime,
    DATE(birth_datetime) AS birth_date,
    person_id
  FROM
     `{PROJECT_ID}.{EHR_SNAPSHOT_DATASET_ID}.unioned_ehr_person` )
SELECT
  observation_id,
  person_id,
  observation_concept_id,
  observation_date,
  observation_datetime
FROM
  `{PROJECT_ID}.{EHR_SNAPSHOT_DATASET_ID}.unioned_ehr_observation`
JOIN
  person_birth_date
USING
  (person_id)
WHERE
  observation_concept_id IN (4013886,
    4271761,
    4135376,
    4083587)
  AND ((observation_datetime = birth_datetime)
    OR (observation_date = birth_date))
"""
execute(client, query)

# ## Check for Excluded sites

if EXCLUDED_SITES != "default":
    queries = []
    for cdm_table in MAPPED_CLINICAL_DATA_TABLES:
        queries.append(f""" SELECT
    '{cdm_table}' AS table_name, COUNT(*) AS non_compliant_rows
    FROM `{EHR_SNAPSHOT_DATASET_ID}.unioned_ehr_{cdm_table}`
    JOIN `{EHR_SNAPSHOT_DATASET_ID}._mapping_{cdm_table}`
    USING ({cdm_table}_id)
    WHERE src_hpo_id IN ({EXCLUDED_SITES})""")

    query = ' \n UNION ALL \n'.join(queries)

    execute(client, query)
else:
    print(
        f'Since list of excluded sites is not provided to the script, check for Excluded Sites is skipped.'
    )
# -
