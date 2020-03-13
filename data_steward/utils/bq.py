from google.cloud import bigquery


def query(q, use_cache=False):
    client = bigquery.Client()
    query_job_config = bigquery.job.QueryJobConfig(use_query_cache=use_cache)
    return client.query(q, job_config=query_job_config).to_dataframe()
