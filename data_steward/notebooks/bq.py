import os

from google import datalab
from google.datalab import bigquery

# In Windows we found that datalab project_id resolution prioritizes an obscure config.json over env vars
# APPLICATION_ID or PROJECT_ID. This explicitly sets it to env var PROJECT_ID.
datalab.Context.default().set_project_id(project_id=os.getenv('PROJECT_ID'))


# Wrapper so we can more easily swap to whatever client library we prefer in the future
def query(q, use_cache=False):
    return bigquery.Query(q).execute(output_options=bigquery.QueryOutput.dataframe(use_cache=use_cache)).result()
