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

# + tags=["parameters"]
project_id: str = ""  # identifies the project where datasets are located
fitbit_dataset: str = ""  # identifies the name of the new fitbit dataset
sandbox_dataset: str = ""  # the sandbox dataset where sandbox and lookup tables are located
source_dataset: str = ""  # identifies the name of the rdr dataset
cutoff_date: str = ""  # CDR cutoff date in YYYY--MM-DD format
run_as: str = ""  # service account email to impersonate
# -

from common import JINJA_ENV, FITBIT_TABLES
from utils import auth
from gcloud.bq import BigQueryClient
from analytics.cdr_ops.notebook_utils import execute, IMPERSONATION_SCOPES, render_message

impersonation_creds = auth.get_impersonation_credentials(
    run_as, target_scopes=IMPERSONATION_SCOPES)
client = BigQueryClient(project_id, credentials=impersonation_creds)

date_columns = {
    'activity_summary': 'date',
    'heart_rate_summary': 'date',
    'heart_rate_minute_level': 'datetime',
    'steps_intraday': 'datetime',
    'sleep_level': 'sleep_date',
    'sleep_daily_summary': 'sleep_date'
}

# ## verify all participants have digital health sharing consent

# +

non_existent_pids_check = JINJA_ENV.from_string('''
SELECT
  \'{{table_name}}\' as table,
  COUNT(1) bad_rows
FROM
  `{{project}}.{{dataset}}.{{table_name}}`
WHERE
  person_id NOT IN (
  SELECT
    person_id
  FROM
    `{{project}}.{{source_dataset}}.person`)
''')

queries_list = []
for table in FITBIT_TABLES:
    queries_list.append(
        non_existent_pids_check.render(project=project_id,
                                       dataset=new_fitbit_dataset,
                                       table_name=table,
                                       source_dataset=source_dataset))

union_all_query = '\nUNION ALL\n'.join(queries_list)

execute(client, union_all_query)
# -

# ## Check if any records exist past cutoff date

# +
data_past_cutoff_check = JINJA_ENV.from_string('''
SELECT
   \'{{table_name}}\' as table,
  COUNT(1) bad_rows
FROM
  `{{project}}.{{dataset}}.{{table_name}}`
WHERE
  DATE({{date_column}}) > \'{{cutoff_date}}\'
''')

queries_list = []
for table in FITBIT_TABLES:
    queries_list.append(
        data_past_cutoff_check.render(project=project_id,
                                      dataset=fitbit_dataset,
                                      table_name=table,
                                      cutoff_date=cutoff_date,
                                      date_column=date_columns[table]))
union_all_query = '\nUNION ALL\n'.join(queries_list)

execute(client, union_all_query)
# -

# ## Check if any records exist past deactivation date

# +
past_deactivation_check = JINJA_ENV.from_string('''
SELECT
  \'{{table_name}}\' as table,
  COUNT(1) bad_rows
FROM
  `{{project}}.{{dataset}}.{{table_name}}` t
JOIN
  `{{project}}.{{sandbox_dataset}}._deactivated_participants` d
USING
  (person_id)
WHERE
  DATE(t.{{date_column}}) > DATE(d.deactivated_datetime)
''')

queries_list = []
for table in FITBIT_TABLES:
    queries_list.append(
        past_deactivation_check.render(project=project_id,
                                       dataset=fitbit_dataset,
                                       table_name=table,
                                       sandbox_dataset=sandbox_dataset,
                                       date_column=date_columns[table]))
union_all_query = '\nUNION ALL\n'.join(queries_list)

execute(client, union_all_query)
# -
