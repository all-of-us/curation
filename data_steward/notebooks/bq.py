from google.datalab import bigquery


# Wrapper so we can more easily swap to whatever client library we prefer in the future
def query(q, use_cache=False):
    return bigquery.Query(q).execute(output_options=bigquery.QueryOutput.dataframe(use_cache=use_cache)).result()
