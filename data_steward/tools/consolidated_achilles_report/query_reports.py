from google.appengine.api.app_identity import app_identity

import bq_utils
import gcs_utils
import os
import json

LATEST_REPORTS_JSON = 'latest_reports.json'
LATEST_REPORTS_QUERY = """
 SELECT
  MAX(timestamp) AS upload_timestamp,
  CONCAT('gs://{drc_bucket}', MAX(SUBSTR(protopayload_auditlog.resourceName, 49))) as file_path
FROM
  `{app_id}.GcsBucketLogging.cloudaudit_googleapis_com_data_access_2019*` l
WHERE
protopayload_auditlog.resourceName LIKE '%aou%'
  AND protopayload_auditlog.methodName = 'storage.objects.create'
  AND resource.labels.bucket_name LIKE '{drc_bucket}'
  AND protopayload_auditlog.resourceName LIKE '%datasources.json'
GROUP BY
  REGEXP_EXTRACT(protopayload_auditlog.resourceName, r".+\/aou[0-9]+")"""

LATEST_RESULTS_JSON = 'latest_results.json'
LATEST_RESULTS_QUERY = """
 SELECT
  MAX(timestamp) AS upload_timestamp,
  CONCAT('gs://{drc_bucket}', MAX(SUBSTR(protopayload_auditlog.resourceName, 49))) as file_path
FROM
  `{app_id}.GcsBucketLogging.cloudaudit_googleapis_com_data_access_2019*` l
WHERE
protopayload_auditlog.resourceName LIKE '%aou%'
  AND protopayload_auditlog.methodName = 'storage.objects.create'
  AND resource.labels.bucket_name LIKE '{drc_bucket}'
  AND protopayload_auditlog.resourceName LIKE '%person.csv'
GROUP BY
  REGEXP_EXTRACT(protopayload_auditlog.resourceName, r".+\/aou[0-9]+")"""


def get_most_recent(app_id=None, drc_bucket=None, report_for=None):
    """
    Query audit logs for paths to the most recent datasources.json files in the DRC bucket.

    Note: Results are cached in a local json file to avoid unnecessary queries.
    :param app_id: identifies the GCP project
    :param drc_bucket: identifies the DRC bucket
    :param report_for: denotes which query to use
    :return: list of dict with keys `file_path`, `upload_timestamp`
    """
    if app_id is None:
        app_id = app_identity.get_application_id()
    if drc_bucket is None:
        drc_bucket = gcs_utils.get_drc_bucket()
        if report_for == 'achilles':
            if not os.path.exists(LATEST_REPORTS_JSON):

                query = LATEST_REPORTS_QUERY.format(app_id=app_id, drc_bucket=drc_bucket)
                query_job = bq_utils.query(query)
                result = bq_utils.response2rows(query_job)
                with open(LATEST_REPORTS_JSON, 'w') as fp:
                    json.dump(result, fp, sort_keys=True, indent=4)
            with open(LATEST_REPORTS_JSON, 'r') as fp:
                return json.load(fp)
        elif report_for == 'results':
            if not os.path.exists(LATEST_RESULTS_JSON):
                query = LATEST_RESULTS_QUERY.format(app_id=app_id, drc_bucket=drc_bucket)
                query_job = bq_utils.query(query)
                result = bq_utils.response2rows(query_job)
                with open(LATEST_RESULTS_JSON, 'w') as fp:
                    json.dump(result, fp, sort_keys=True, indent=4)
            with open(LATEST_RESULTS_JSON, 'r') as fp:
                return json.load(fp)
