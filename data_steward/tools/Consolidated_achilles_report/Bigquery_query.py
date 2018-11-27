import pandas as pd
from google.cloud import bigquery as bq
import bq_utils

def get_latest_querey():
    df = pd.DataFrame()
    client = bq.Client(project='aou-res-curation-prod')
    Query = """SELECT
  MAX(timestamp) AS upload_timestamp,
  MAX(SUBSTR(protopayload_auditlog.resourceName, 49)) as file_path
FROM
  `aou-res-curation-prod.GcsBucketLogging.cloudaudit_googleapis_com_data_access_2018*` l
WHERE
  _TABLE_SUFFIX > '0801'
  AND protopayload_auditlog.resourceName LIKE '%aou%'
  AND protopayload_auditlog.methodName = 'storage.objects.create'
  AND resource.labels.bucket_name LIKE 'drc-curation-internal'
  AND protopayload_auditlog.resourceName LIKE '%datasources.json'
GROUP BY
  REGEXP_EXTRACT(protopayload_auditlog.resourceName, r".+\/aou[0-9]+")"""
    query_job = client.query(Query)
    # df = query_job.result().to_dataframe()
    # df['upload_timestamp'] = pd.to_datetime(df['upload_timestamp'])
    # df['date'] = df['upload_timestamp'].apply(lambda x: x.strftime('%Y-%m-%d'))
    # df['time'] = df['upload_timestamp'].apply(lambda x: x.strftime('%H:%M:%S'))
    # df['date_time'] = df['date']+'T'+df['time']+'Z'
    # export_data = pd.DataFrame
    # path_prefix = 'gs://drc-curation-internal'
    # df['file_path'] = path_prefix+df['file_path']
    # export_data = df[['date_time', 'file_path']]
    # export_data.to_csv('recent_uploads.txt', index=False, sep=' ', header=None)


get_latest_querey()
