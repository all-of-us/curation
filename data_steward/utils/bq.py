from google.cloud import bigquery

client = bigquery.Client()


def query(q, use_cache=False):
    query_job_config = bigquery.job.QueryJobConfig(use_query_cache=use_cache)
    return client.query(q, job_config=query_job_config).to_dataframe()
