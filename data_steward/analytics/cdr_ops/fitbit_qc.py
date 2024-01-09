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
sandbox_dataset: str = ""  # the pipeline tables sandbox
source_dataset: str = ""  # identifies the name of the clean rdr dataset
deid_dataset: str = "" # dataset contains wear_study table
cutoff_date: str = ""  # CDR cutoff date in YYYY--MM-DD format
run_as: str = ""  # service account email to impersonate
# -

from common import JINJA_ENV, FITBIT_TABLES, PIPELINE_TABLES, SITE_MASKING_TABLE_ID
from utils import auth
from gcloud.bq import BigQueryClient
from analytics.cdr_ops.notebook_utils import execute, IMPERSONATION_SCOPES, render_message

impersonation_creds = auth.get_impersonation_credentials(
    run_as, target_scopes=IMPERSONATION_SCOPES)
client = BigQueryClient(project_id, credentials=impersonation_creds)

# +
# All date/datetime fields in the fitbit tables should be represented in one of the following dictionaries.

# The first date type column of the table
date_columns = {
    'activity_summary': 'date',
    'heart_rate_summary': 'date',
    'heart_rate_intraday': 'datetime',
    'steps_intraday': 'datetime',
    'sleep_level': 'sleep_date',
    'sleep_daily_summary': 'sleep_date',
    'device': 'device_date'
}

# For tables that have a second date field that needs to be checked for their cutoff date/deactivation dates
secondary_date_column = {
    'device': 'last_sync_time',
    'sleep_level': 'start_datetime'
}
# -

# Used in the 'Validate fitbit fields' query.
table_fields_values = {
    'device': {
        'battery': ['High', 'Medium', 'Low','Empty']
    },
    'sleep_level': {
        'level': [
            'awake', 'light', 'asleep', 'deep', 'restless', 'wake', 'rem',
            'unknown'
        ]
    },
    'sleep_daily_summary': {
        'is_main_sleep': ['true', 'false']
    },
    'heart_rate_summary': {
        'zone_name': ['Peak', 'Cardio', 'Fat Burn', 'Out of Range']
    }
}

# ## Identify person_ids that are not in the person table
# This check verifies that person_ids are valid. That they exist in the CDM person table and are not null.
# There should be no bad rows.
#
# In case of failure:
# - If the person_id is not in the CDM person table. Check that `RemoveNonExistingPids` was applied.
# - If the person_ids are NULL contact the DST team. It should not be possible for person_id to be null.

# +
non_existent_pids_check = JINJA_ENV.from_string("""
SELECT
  '{{table_name}}' as table,
  COUNT(1) bad_rows
FROM
  `{{project}}.{{dataset}}.{{table_name}}`
WHERE
  person_id NOT IN (
  SELECT
    person_id
  FROM
    `{{project}}.{{source_dataset}}.person`)
  OR person_id IS NULL
""")

queries_list = []
for table in FITBIT_TABLES:
    queries_list.append(
        non_existent_pids_check.render(project=project_id,
                                       dataset=fitbit_dataset,
                                       table_name=table,
                                       source_dataset=source_dataset))

union_all_query = '\nUNION ALL\n'.join(queries_list)

execute(client, union_all_query)
# -

# ## Check if any records exist past cutoff date

# +
data_past_cutoff_check = JINJA_ENV.from_string("""
SELECT
  '{{table_name}}' as table,
  COUNT(1) bad_rows
FROM
  `{{project}}.{{dataset}}.{{table_name}}`
WHERE
  DATE({{date_column}}) > \'{{cutoff_date}}\'
  {% if secondary_date_column -%}
  OR DATE({{secondary_date_column}}) > \'{{cutoff_date}}\'
  {% endif %}
""")

queries_list = []
for table in FITBIT_TABLES:
    queries_list.append(
        data_past_cutoff_check.render(
            project=project_id,
            dataset=fitbit_dataset,
            table_name=table,
            cutoff_date=cutoff_date,
            date_column=date_columns[table],
            secondary_date_column=secondary_date_column.get(table)))
union_all_query = '\nUNION ALL\n'.join(queries_list)

execute(client, union_all_query)
# -

# ## Check if any records exist past deactivation date

# +
past_deactivation_check = JINJA_ENV.from_string("""
SELECT
  '{{table_name}}' as table,
  COUNT(1) bad_rows
FROM
  `{{project}}.{{dataset}}.{{table_name}}` t
JOIN
  `{{project}}.{{dataset}}_sandbox._deactivated_participants` d
USING
  (person_id)
WHERE
  DATE(t.{{date_column}}) > DATE(d.deactivated_datetime)
  {% if secondary_date_column -%}
  OR DATE({{secondary_date_column}}) > DATE(d.deactivated_datetime)
  {% endif %}
""")

queries_list = []
for table in FITBIT_TABLES:
    queries_list.append(
        past_deactivation_check.render(
            project=project_id,
            dataset=fitbit_dataset,
            table_name=table,
            sandbox_dataset=sandbox_dataset,
            date_column=date_columns[table],
            secondary_date_column=secondary_date_column.get(table)))
union_all_query = '\nUNION ALL\n'.join(queries_list)

execute(client, union_all_query)
# -

# ## Check for src_id to exist for all records
# Fitbit tables will require a src_id in future CDRs. Each record should have a defined source.

# +
src_check = JINJA_ENV.from_string("""
SELECT
  '{{table_name}}' as table,
  COUNT(1) bad_rows
FROM
  `{{project}}.{{dataset}}.{{table_name}}` t
WHERE t.src_id IS NULL                          

""")

queries_list = []
for table in FITBIT_TABLES:
    queries_list.append(
        src_check.render(project=project_id,
                         dataset=fitbit_dataset,
                         table_name=table,
                         pipeline_tables=PIPELINE_TABLES,
                         site_maskings=SITE_MASKING_TABLE_ID))
union_all_query = '\nUNION ALL\n'.join(queries_list)

execute(client, union_all_query)
# -

# ## Check for rows without a valid date field
# Fitbit table records must have at least one valid date in order to be deemed valid.
# This is a preleminary check as this circumstance(lacking a date) should not be possible. No CR currently exists to
# remove data of this type.
#
# If bad rows are found a new CR may be required. Notify and recieve guidance from the DST.

# +
date_check = JINJA_ENV.from_string("""
SELECT
  '{{table_name}}' as table,
  COUNT(1) bad_rows
FROM
  `{{project}}.{{dataset}}.{{table_name}}` t
WHERE
  DATE(t.{{date_column}}) IS NULL
  {% if secondary_date_column -%}
  AND DATE(t.{{secondary_date_column}}) IS NULL
  {% endif %}
""")

queries_list = []
for table in FITBIT_TABLES:
    queries_list.append(
        date_check.render(
            project=project_id,
            dataset=fitbit_dataset,
            table_name=table,
            sandbox_dataset=sandbox_dataset,
            date_column=date_columns[table],
            secondary_date_column=secondary_date_column.get(table)))
union_all_query = '\nUNION ALL\n'.join(queries_list)

execute(client, union_all_query)
# -

# ## Validate fitbit fields
# Valdates that fitbit fields are either empty or contain only expected values.

# +
field_value_validation = JINJA_ENV.from_string("""
SELECT
  '{{table_name}}' as table,
  COUNT(1) bad_rows
FROM
  `{{project}}.{{dataset}}.{{table_name}}` t
WHERE
  ({{field_name}} IS NOT NULL)
   AND ({{field_name}} NOT IN UNNEST({{field_values}}))
""")

queries_list = []
for table_name, field_info in table_fields_values.items():
    field_name, field_values = list(field_info.items())[0]
    queries_list.append(
        field_value_validation.render(project=project_id,
                                      dataset=fitbit_dataset,
                                      table_name=table_name,
                                      field_name=field_name,
                                      field_values=field_values))

union_all_query = '\nUNION ALL\n'.join(queries_list)

execute(client, union_all_query)
# -
# # Check percentage of wear_study participants lacking fitbit data
#
# This check requires a deid dataset containing the generated wear_study table. 
#
# If the check fails - If one of the data sources is missing or if the percentage of wear_study participants lacking
# fitbit data is more than 40% for vibrent participants or 10% for ce participants, the data analytics team should be
# notified.
# See DC-3629 for more information.

# +
query = JINJA_ENV.from_string("""
WITH fb_person_ids AS ( -- identify pids with fitbit data --
SELECT DISTINCT person_id
FROM {{project_id}}.{{dataset}}.activity_summary
)
, consenting_ws_ids AS ( -- identify consenting pids --
SELECT person_id,research_id, 
FROM {{project_id}}.{{pipeline}}.primary_pid_rid_mapping dm
WHERE research_id IN (SELECT person_id 
    FROM {{project_id}}.{{deid_dataset}}.wear_study  
    WHERE wear_consent_end_date IS NULL)
)
SELECT 
src_id, 
ROUND(COUNT(CASE WHEN fb.person_id IS NULL THEN 1 ELSE NULL END) * 100 / COUNT(c_ws),1) AS percent_without_fb,
FROM (SELECT * FROM {{project_id}}.{{source_dataset}}.observation WHERE observation_source_concept_id = 2100000010) o
JOIN {{project_id}}.{{source_dataset}}._mapping_observation USING(observation_id) 
JOIN consenting_ws_ids c_ws USING(person_id) 
LEFT JOIN fb_person_ids fb ON o.person_id = fb.person_id
GROUP BY 1
""").render(project_id=project_id,
            dataset=fitbit_dataset,
            source_dataset=source_dataset,
            pipeline=sandbox_dataset,
            deid_dataset=deid_dataset)

df = execute(client, query)

# conditions for a passing check
cond_vibrent_percentage = df.loc[df['src_id'] == 'vibrent', 'percent_without_fb'].iloc[0] < 40
cond_ce_percentage = df.loc[df['src_id'] == 'ce', 'percent_without_fb'].iloc[0] < 10
is_success = cond_vibrent_percentage and cond_ce_percentage

success_msg = "Conditions Pass"
failure_msg = (
    """
    One of the following checks failed. Confirm failure, and notify the proper team(Data Analytics) <br>
    (1) The percentage of wear_study participants lacking fitbit data should be less than than 40% for vibrent. <br>
    (2) The percentage of wear_study participants lacking fitbit data should be less than than 10% for ce. <br>
    """
    )

render_message(df,
              success_msg,
              failure_msg,
              is_success=is_success)
# -




