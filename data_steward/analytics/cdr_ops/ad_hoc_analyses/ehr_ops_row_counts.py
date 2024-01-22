# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.3.0
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# # EHR Operations

# +
import datetime
import utils.bq
from notebooks.parameters import RDR_PROJECT_ID, RDR_DATASET_ID, EHR_DATASET_ID

UPLOADED_SINCE_DAYS = 30

now = datetime.datetime.now()
month_ago = now - datetime.timedelta(days=UPLOADED_SINCE_DAYS)
end_suffix = month_ago.strftime('%Y%m%d')
# -

print('RDR_PROJECT_ID=%s' % RDR_PROJECT_ID)
print('RDR_DATASET_ID=%s' % RDR_DATASET_ID)
print('EHR_DATASET_ID=%s' % EHR_DATASET_ID)

# ## Most Recent Bucket Uploads
# _Based on `storage.objects.create` log events of objects named `person.csv` archived in BigQuery_

query = """SELECT
  h.hpo_id AS hpo_id,
  m.Site_Name AS site_name,
  protopayload_auditlog.authenticationInfo.principalEmail AS email,
  resource.labels.bucket_name AS bucket_name,
  MAX(SUBSTR(protopayload_auditlog.resourceName, 34)) AS resource_name,
  MAX(timestamp) AS upload_timestamp
FROM
  `lookup_tables.hpo_id_bucket_name` h
  JOIN `lookup_tables.hpo_site_id_mappings` m ON h.hpo_id = m.HPO_ID
  LEFT JOIN `{rdr_project}.GcsBucketLogging.cloudaudit_googleapis_com_data_access_*` l
   ON l.resource.labels.bucket_name = h.bucket_name
WHERE
  _TABLE_SUFFIX >= '{end_suffix}'
  AND protopayload_auditlog.authenticationInfo.principalEmail IS NOT NULL
  AND ENDS_WITH(protopayload_auditlog.authenticationInfo.principalEmail, 'pmi-ops.org')
  AND protopayload_auditlog.methodName = 'storage.objects.create'
  AND resource.labels.bucket_name LIKE 'aou%'
  AND protopayload_auditlog.resourceName LIKE '%person.csv'
GROUP BY
  h.hpo_id,
  m.Site_Name,
  resource.labels.bucket_name,
  protopayload_auditlog.authenticationInfo.principalEmail
ORDER BY MAX(timestamp) ASC""".format(rdr_project=RDR_PROJECT_ID,
                                      end_suffix=end_suffix)
utils.bq.query(query)

# ## EHR Site Submission Counts

utils.bq.query('''
SELECT
  l.Org_ID AS org_id,
  l.HPO_ID AS hpo_id,
  l.Site_Name AS site_name,
  table_id AS table_id,
  row_count AS row_count
FROM `{EHR_DATASET_ID}.__TABLES__` AS t
JOIN `lookup_tables.hpo_site_id_mappings` AS l
  ON STARTS_WITH(table_id,lower(l.HPO_ID))=true
WHERE table_id like '%person%' AND
NOT(table_id like '%unioned_ehr_%') AND
l.hpo_id <> ''
ORDER BY Display_Order
'''.format(EHR_DATASET_ID=EHR_DATASET_ID))

# get list of all hpo_ids
hpo_ids = utils.bq.query("""
SELECT REPLACE(table_id, '_person', '') AS hpo_id
FROM `{EHR_DATASET_ID}.__TABLES__`
WHERE table_id LIKE '%person'
AND table_id NOT LIKE '%unioned_ehr_%' AND table_id NOT LIKE '\\\_%'
""".format(EHR_DATASET_ID=EHR_DATASET_ID)).hpo_id.tolist()

# For each site submission, how many person_ids cannot be found in the latest RDR dump (*not_in_rdr*) or are not valid 9-digit participant identifiers (_invalid_).

# ## birth_datetime IS NULL

# +
subqueries = []
subquery = """
SELECT
 '{h}' AS hpo_id,
 COUNT(1) n
FROM {EHR_DATASET_ID}.{h}_person p
WHERE birth_datetime IS NULL
"""
for hpo_id in hpo_ids:
    subqueries.append(subquery.format(h=hpo_id, EHR_DATASET_ID=EHR_DATASET_ID))
q = '\n\nUNION ALL\n'.join(subqueries)
cte = """
WITH birth_datetime_null AS
({q})
SELECT * FROM birth_datetime_null
WHERE n > 0
ORDER BY n DESC
""".format(q=q)

utils.bq.query(cte)
