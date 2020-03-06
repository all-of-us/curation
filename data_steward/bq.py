from google.cloud import bigquery


# Wrapper so we can more easily swap to whatever client library we prefer in the future
def query(q, use_cache=False):
    client = bigquery.Client()
    query_job_config = bigquery.job.QueryJobConfig(use_query_cache=use_cache)
    return client.query(q, job_config=query_job_config).to_dataframe()
