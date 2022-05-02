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

# + tags=["parameters"]
project_id = "aou-res-curation-prod"
run_as = "data-analytics@aou-res-curation-prod.iam.gserviceaccount.com"
# -

import pandas as pd
from analytics.cdr_ops.notebook_utils import pdr_client, start_cloud_sql_proxy, stop_cloud_sql_proxy

# # Example of Cloud SQL proxy and another project authentication
#
# ./cloud_sql_proxy must run before we can read data from sql server.

proc = start_cloud_sql_proxy(project_id, run_as)
pdr_client = pdr_client(project_id, run_as)

# # Total ordering in descending order
# Description here

query = '''
  SELECT COUNT(DISTINCT participant_id) AS Total, enrollment_status
   FROM pdr.mv_participant
   GROUP BY 2
   ORDER BY 1 DESC
'''

df = pd.read_sql(query, pdr_client)
print(df)

stop_cloud_sql_proxy(proc)


