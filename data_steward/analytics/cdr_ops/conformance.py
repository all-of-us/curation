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

# # Conformance
# Low level checks on HPO site submissions based on bucket contents
#
# _Note: This requires extra configuration for bucket names_

# +
import yaml
# %matplotlib inline
from google.datalab import storage
from notebooks import parameters

with open ('env.yaml') as f:
    app_env = yaml.safe_load(f)

OUTPUT_DIR = 'output'
KEY_PREFIX = 'BUCKET_NAME_'
DRC_BUCKET_NAME = parameters.DRC_BUCKET_NAME

bucket_keys = [key for key in app_env.keys() if key.startswith(KEY_PREFIX) and not app_env[key].startswith('test') and not app_env[key] == DRC_BUCKET_NAME]

hpo_buckets = dict()
for bucket_key in bucket_keys:
    hpo_id = bucket_key.replace(KEY_PREFIX, '').lower()
    bucket = app_env[bucket_key]
    hpo_buckets[hpo_id] = bucket

drc_bucket = storage.Bucket(name=DRC_BUCKET_NAME)

# +
def hpo_ls(hpo_id, bucket):
    prefix = '%s/%s/' % (hpo_id, bucket)
    objs = list(drc_bucket.objects(prefix))
    return objs

def scan_obj(obj):
    comps = obj.key.split('/')
    if len(comps) != 4:
        return
    hpo_id, bucket, dir_name, file_name = comps
    local_dir = os.path.join(OUTPUT_DIR, hpo_id, bucket, dir_name)
    if file_name in ('result.csv', 'processed.txt'):
        try:
            os.makedirs(local_dir)
        except OSError:
            pass
        local_file_path = os.path.join(local_dir, file_name)
        with open(local_file_path, 'w') as local_fp:
            local_fp.write(obj.download())

def download_output(hpo_id, bucket):
    """
    Download all archived pipeline output for an hpo and save it locally.
    
    Example: ./nyc_cu/2019-01-16/result.csv
    """
    objs = hpo_ls(hpo_id, bucket)
    for obj in objs:
        scan_obj(obj)
# -

for hpo_id, bucket in hpo_buckets.items():
    print 'Processing %s...' % hpo_id
    download_output(hpo_id, bucket)

# +
import fnmatch
import os
import re
import pandas
import sqlite3

DB_NAME = 'conformance.db'
RESULTS_TABLE = 'results_csv'
conn = sqlite3.connect(DB_NAME)

result_csvs = []
for root, dirnames, filenames in os.walk(OUTPUT_DIR):
    for filename in fnmatch.filter(filenames, 'result.csv'):
        result_csvs.append(os.path.join(root, filename))

conn.executescript('drop table if exists %s;' % RESULTS_TABLE)
for result_csv in result_csvs:
    _, hpo_id, bucket, folder, _ = result_csv.split(os.path.sep)
    submit_date = re.findall(r'\d\d\d\d\-\d\d-\d\d', result_csv)
    submit_date = submit_date[0] if submit_date else None
    result_df = pandas.read_csv(result_csv)
    result_df['submit_date'] = submit_date
    result_df['hpo_id'] = hpo_id
    result_df['bucket'] = bucket
    result_df['folder'] = folder
    result_df.to_sql(RESULTS_TABLE, conn, if_exists='append', index=False)
# -

# ## Conformance of latest submissions

q = """
SELECT
  hpo_id,
  MAX(folder) AS folder,
  COUNT(DISTINCT cdm_file_name) table_count
FROM results_csv
WHERE submit_date IS NOT NULL
 AND loaded = 1
GROUP BY hpo_id
ORDER BY hpo_id;
"""
pandas.read_sql_query(q, conn)

# ## Conformance level over time

q = """
SELECT folder,
  hpo_id,
  COUNT(DISTINCT cdm_file_name) table_count
FROM results_csv
WHERE submit_date IS NOT NULL
  AND loaded = 1
GROUP BY folder, hpo_id
ORDER BY hpo_id, submit_date
"""
pandas.read_sql_query(q, conn)
