from google.cloud import bigquery


def get_client(project_id=None):
    """
    Get a client for a specified project.
    """
    if project_id is None:
        return bigquery.Client()
    else:
        return bigquery.Client(project=project_id)


def query(q, project_id=None, use_cache=False):
    client = get_client(project_id)
    query_job_config = bigquery.job.QueryJobConfig(use_query_cache=use_cache)
    return client.query(q, job_config=query_job_config).to_dataframe()
