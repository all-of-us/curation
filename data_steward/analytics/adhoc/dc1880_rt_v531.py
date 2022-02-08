# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.7.1
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# +
import os

from google.cloud import bigquery
from google.auth import default
from IPython.core.display import display, HTML
import pandas as pd

from common import JINJA_ENV
from notebooks import render
import resources
from utils import auth
from tools.snapshot_by_query import BIGQUERY_DATA_TYPES
# -

SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/bigquery",
]

# + tags=["parameters"]
project_id:str = ''
dataset_id:str = ''
compare_id:str = ''
run_as:str = ''

# +
def execute(query,
            print_query=True,
            project_id=project_id,
            runas=run_as,
            **kwargs):
    """
    Execute a bigquery command and return the results in a dataframe
    
    :param query: the query to execute
    """
    credentials = None
    if runas:
        credentials = auth.get_impersonation_credentials(runas, SCOPES)
    client = bigquery.Client(project=project_id, credentials=credentials)
    if print_query:
        print(query)
    df = client.query(query, **kwargs).to_dataframe()
    return df


q = f'''
WITH compare AS
(SELECT
 COALESCE(t1.table_id, t0.table_id) AS table_id
,CAST(t0.row_count AS INT64) t0_row_count
,CAST(t1.row_count AS INT64) t1_row_count
FROM `{project_id}.{dataset_id}.__TABLES__` t1
 FULL OUTER JOIN `{project_id}.{compare_id}.__TABLES__` t0
  USING (table_id))
SELECT * FROM compare
WHERE t1_row_count <> t0_row_count
ORDER BY ABS(t1_row_count-t0_row_count) DESC
'''
execute(q)

# ## Verify schema
# The output dataset must have (at minimum) all the cdm tables.
# For each, all fields must have the expected name, type, mode.
# -

COLUMNS_QUERY_TPL = JINJA_ENV.from_string('''
SELECT 
 table_name
,column_name
,IF(is_nullable='YES', 'nullable', 'required') AS mode
,data_type AS type
FROM `{{ project_id }}.{{ dataset_id }}.INFORMATION_SCHEMA.COLUMNS`
WHERE 1=1
 AND is_system_defined = 'NO'
ORDER BY table_name ASC, ordinal_position ASC
''')
q = COLUMNS_QUERY_TPL.render(project_id=project_id, dataset_id=dataset_id)
df = execute(q)
g = df.groupby(['table_name'])

for root, dirs, files in os.walk(resources.cdm_fields_path):
    for filename in files:
        table_name, _ = filename.split('.')
        print(f'Verifing {table_name} schema...')
        cols_df = g.get_group(table_name)
        rsrc_fields = resources.fields_for(table_name)
        actual_fields = list(cols_df.to_dict('records'))
        expect = dict(table_name=table_name)
        for i in range(0, len(rsrc_fields)):
            rsrc, actual = rsrc_fields[i], actual_fields[i]
            expect['mode'] = rsrc['mode']
            expect['type'] = BIGQUERY_DATA_TYPES.get(rsrc['type'])
            expect['column_name'] = rsrc['name']
            
            if expect != actual:
                print(f'Expected {expect} but actual was {actual}')
