# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.3'
#       jupytext_version: 0.8.3
#   kernelspec:
#     display_name: Python 2
#     language: python
#     name: python2
#   language_info:
#     codemirror_mode:
#       name: ipython
#       version: 2
#     file_extension: .py
#     mimetype: text/x-python
#     name: python
#     nbconvert_exporter: python
#     pygments_lexer: ipython2
#     version: 2.7.13
# ---

# # EHR Operations

# +
# %load_ext google.datalab.kernel

import google.datalab.bigquery as bq
import os
import datetime

now = datetime.datetime.now()
end_suffix = now.strftime('%m%d')
RDR_DATASET_ID = os.environ.get('RDR_DATASET_ID')
EHR_DATASET_ID = os.environ.get('EHR_DATASET_ID')
# -

print 'RDR_DATASET_ID=%s' % RDR_DATASET_ID
print 'EHR_DATASET_ID=%s' % EHR_DATASET_ID

# ## Most Recent Bucket Uploads
# _Based on `storage.objects.create` log events of objects named `person.csv` archived in BigQuery_

bq.Query('''
SELECT
  h.hpo_id AS hpo_id,
  m.Site_Name AS site_name,
  protopayload_auditlog.authenticationInfo.principalEmail AS email,
  resource.labels.bucket_name AS bucket_name,
  MAX(SUBSTR(protopayload_auditlog.resourceName, 34)) AS resource_name,
  MAX(timestamp) AS upload_timestamp
FROM
  `lookup_tables.hpo_id_bucket_name` h
  JOIN `lookup_tables.hpo_site_id_mappings` m ON h.hpo_id = m.HPO_ID
  LEFT JOIN `all-of-us-rdr-prod.GcsBucketLogging.cloudaudit_googleapis_com_data_access_{year}*` l
   ON l.resource.labels.bucket_name = h.bucket_name
WHERE
  _TABLE_SUFFIX BETWEEN '0801' AND '{end_suffix}'
  AND protopayload_auditlog.authenticationInfo.principalEmail IS NOT NULL
  AND protopayload_auditlog.authenticationInfo.principalEmail <> 'aou-res-curation-prod@appspot.gserviceaccount.com'
  AND protopayload_auditlog.methodName = 'storage.objects.create'
  AND resource.labels.bucket_name LIKE 'aou%'
  AND protopayload_auditlog.resourceName LIKE '%person.csv'
GROUP BY
  h.hpo_id,
  m.Site_Name,
  resource.labels.bucket_name,
  protopayload_auditlog.authenticationInfo.principalEmail
ORDER BY MAX(timestamp) ASC
'''.format(year=now.year, end_suffix=end_suffix)).execute(output_options=bq.QueryOutput.dataframe(use_cache=False)).result()

# ## EHR Site Submission Counts

bq.Query('''
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
'''.format(EHR_DATASET_ID=EHR_DATASET_ID)).execute(output_options=bq.QueryOutput.dataframe(use_cache=False)).result()

# get list of all hpo_ids
hpo_ids = bq.Query("""
SELECT REPLACE(table_id, '_person', '') AS hpo_id
FROM `{EHR_DATASET_ID}.__TABLES__`
WHERE table_id LIKE '%person' 
AND table_id NOT LIKE '%unioned_ehr_%'
""".format(EHR_DATASET_ID=EHR_DATASET_ID)).execute(output_options=bq.QueryOutput.dataframe(use_cache=False)).result().hpo_id.tolist()

# # Person
# ## Person ID validation

# For each site submission, how many person_ids cannot be found in the latest RDR dump (*not_in_rdr*) or are not valid 9-digit participant identifiers (_invalid_).

subqueries = []
subquery = """
SELECT
 '{h}' AS hpo_id,
 not_in_rdr.n AS not_in_rdr,
 invalid.n AS invalid,
 CAST(T.row_count AS INT64) AS total
FROM {EHR_DATASET_ID}.__TABLES__ T
LEFT JOIN
(SELECT COUNT(1) AS n
 FROM {EHR_DATASET_ID}.{h}_person e
 WHERE NOT EXISTS(
  SELECT 1 
  FROM {RDR_DATASET_ID}.person r
  WHERE r.person_id = e.person_id)) not_in_rdr
 ON TRUE
LEFT JOIN
(SELECT COUNT(1) AS n
 FROM {EHR_DATASET_ID}.{h}_person e
 WHERE NOT person_id BETWEEN 100000000 AND 999999999) invalid
 ON TRUE
WHERE T.table_id = '{h}_person'"""
for hpo_id in hpo_ids:
    subqueries.append(subquery.format(h=hpo_id, EHR_DATASET_ID=EHR_DATASET_ID, RDR_DATASET_ID=RDR_DATASET_ID))
q = '\n\nUNION ALL\n'.join(subqueries)
df = bq.Query(q).execute(output_options=bq.QueryOutput.dataframe(use_cache=False)).result()
df

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

df = bq.Query(cte).execute(output_options=bq.QueryOutput.dataframe(use_cache=False)).result()
df
