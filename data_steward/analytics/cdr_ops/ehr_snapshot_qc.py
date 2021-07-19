# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.3.0
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

# +
from google.cloud import bigquery
import pandas as pd

CLIENT = bigquery.Client(project=PROJECT_ID)

pd.options.display.max_rows = 1000
pd.options.display.max_colwidth = 0
pd.options.display.max_columns = None
pd.options.display.width = None


def execute(query):
    """
    Execute a bigquery command and return the results in a dataframe

    :param query: the query to execute
    """
    print(query)
    return CLIENT.query(query).to_dataframe()


# -

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
execute(query)
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
execute(query)

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
execute(query)
