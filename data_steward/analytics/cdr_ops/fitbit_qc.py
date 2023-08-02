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
source_dataset: str = ""  # identifies the name of the rdr dataset
cutoff_date: str = ""  # CDR cutoff date in YYYY--MM-DD format
run_as: str = ""  # service account email to impersonate
# -

from common import JINJA_ENV, FITBIT_TABLES, PIPELINE_TABLES, SITE_MASKING_TABLE_ID
from utils import auth
from gcloud.bq import BigQueryClient
from analytics.cdr_ops.notebook_utils import execute, IMPERSONATION_SCOPES

impersonation_creds = auth.get_impersonation_credentials(
    run_as, target_scopes=IMPERSONATION_SCOPES)
client = BigQueryClient(project_id, credentials=impersonation_creds)

# +
# All date/datetime fields in the fitbit tables should be represented in one of the following dictionaries.

# The first date type column of the table
date_columns = {
    'activity_summary': 'date',
    'heart_rate_summary': 'date',
    'heart_rate_minute_level': 'datetime',
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
        'battery': ['high', 'medium', 'low']
    },
    'sleep_level': {
        'level': [
            'awake', 'light', 'asleep', 'deep', 'restless', 'wake', 'rem',
            'unknown'
        ]
    },
    'sleep_daily_summary': {
        'is_main_sleep': ['Peak', 'Cardio', 'Fat Burn', 'Out of Range']
    },
    'heart_rate_summary': {
        'zone_name': ['true', 'false']
    }
}

# ## Verify all participants have digital health sharing consent

# +
health_sharing_consent_check = JINJA_ENV.from_string("""
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
    `{{project}}.{{sandbox_dataset}}.digital_health_sharing_status` d
  WHERE
    status = 'YES'
    AND d.wearable = 'fitbit')
""")

queries_list = []
for table in FITBIT_TABLES:
    queries_list.append(
        health_sharing_consent_check.render(project=project_id,
                                            dataset=fitbit_dataset,
                                            table_name=table,
                                            sandbox_dataset=sandbox_dataset))

union_all_query = '\nUNION ALL\n'.join(queries_list)

execute(client, union_all_query)
# -

# ## Identify person_ids that are not in the person table
# This check verifies that person_ids are valid. That they exist in the CDM person table and are not null. There should be no bad rows.
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
  `{{project}}.{{sandbox_dataset}}._deactivated_participants` d
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
WHERE t.src_id not in (
    SELECT 
        hpo_id
    FROM
        `{{project_id}}.{{pipeline_tables}}.{{site_maskings}}`
    WHERE 
        REGEXP_CONTAINS(src_id, r'(?i)Participant Portal')                            
)
OR t.src_id is NULL
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
# This is a preleminary check as this circumstance(lacking a date) should not be possible. No CR currently exists to remove data of this type.
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
