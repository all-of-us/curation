# -*- coding: utf-8 -*-
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

# Purpose: Use this notebook to search for ids in sandbox datasets

# + tags=["parameters"]
project_id = ''
sandbox_dataset_id = '' # Sandbox dataset to search in for the problem ids
search_field = '' # field in the sandbox tables expected to contain the ids. Example: observation_id
run_as = ''

# +
from utils import auth
import pandas as pd
from gcloud.bq import BigQueryClient
from common import JINJA_ENV
from analytics.cdr_ops.notebook_utils import execute, IMPERSONATION_SCOPES, render_message

pd.set_option('display.max_rows', None)
# -

impersonation_creds = auth.get_impersonation_credentials(
    run_as, target_scopes=IMPERSONATION_SCOPES)

client = BigQueryClient(project_id, credentials=impersonation_creds)

# # Create list of ids to search
# Run the following cell to create a list of ids to search for. Recommend using a LIMIT if the list is quite large.<br>
# OR <br>
# Manually create a list of ids called ids_list

# +
tpl = JINJA_ENV.from_string('''
{INSERT QUERY HERE}
''')
query = tpl.render()
ids = execute(client, query)

ids_list = ids[search_field].to_list()


# -

# # Get the tables that contain the search_field, from the sandbox dataset

# +
tpl = JINJA_ENV.from_string('''
    SELECT
      *
    FROM
      `{{project_id}}.{{sandbox_dataset_id}}.INFORMATION_SCHEMA.COLUMNS`
    WHERE
      column_name = '{{search_field}}'
    ORDER BY table_name
    
''')
query = tpl.render(sandbox_dataset_id=sandbox_dataset_id,
                   project_id=project_id,
                   search_field=search_field)
tables_in_dataset = execute(client, query)

tables_list = tables_in_dataset['table_name'].to_list()
tables_list
# -

# # Search in each sandbox table and print results

queries = []
for table in tables_list:
    tpl = JINJA_ENV.from_string('''    
    SELECT 
    '{{table}}' as table,
    COUNT(*) AS n_{{search_field}}s_found
    FROM
    `{{project_id}}.{{sandbox_dataset_id}}.{{table}}`
    WHERE {{search_field}} IN UNNEST ({{ids_list}})
    ''')
    query = tpl.render(sandbox_dataset_id=sandbox_dataset_id,
                       project_id=project_id,
                       table=table,
                       ids_list=ids_list,
                       search_field=search_field)
    queries.append(query)
execute(client, '\nUNION ALL\n'.join(queries))

