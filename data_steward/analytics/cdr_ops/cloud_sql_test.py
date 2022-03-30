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
project_id = ""
# -
import pandas as pd
from notebook_utils import pdr_client

# # cloud_sql_proxy needs to be ran before we can read data from sql server.
# cloud_sql_proxy -instances=aou-pdr-data-prod:us-central1:prod-pdr-5deb-lhty=tcp:7005 --token=$(gcloud auth print-access-token \
# --impersonate-service-account=data-analytics@aou-res-curation-prod.iam.gserviceaccount.com)

db_conn = pdr_client(project_id)

# +
query = '''
  SELECT COUNT(DISTINCT participant_id) AS Total, enrollment_status                    
   FROM pdr.mv_participant                    
   GROUP BY 2                    
   ORDER BY 1 DESC
'''

df = pd.read_sql(query, db_conn)
print(df)
# -
