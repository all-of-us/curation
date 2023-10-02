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

# ## Notebook parameters

# + tags=["parameters"]
project_id = ""  # identifies the project containing the datasets
dataset_id = ""  # raw fitbit dataset. Most likely it ends with `_backup`.
run_as = ""  # Service account email for impersonation
# -

# # QC for Raw Fitbit
# Quality checks for raw fitbit data.
# Run this QC notebook as soon as we load the Fitbit tables into the curation project.
# See DC-3444's attachment for the original list of validation criteria.

from common import JINJA_ENV
from utils import auth
from gcloud.bq import BigQueryClient
from analytics.cdr_ops.notebook_utils import execute, IMPERSONATION_SCOPES, render_message
from IPython.display import display, HTML

impersonation_creds = auth.get_impersonation_credentials(
    run_as, target_scopes=IMPERSONATION_SCOPES)

client = BigQueryClient(project_id, credentials=impersonation_creds)

# # STEPS_INTRADAY table

# Validation criteria for steps_intraday is the following:
# - The table includes both PTSC and CE data per the src_id field

# +
query = JINJA_ENV.from_string("""
SELECT src_id, COUNT(*) AS row_count
FROM `{{project_id}}.{{dataset}}.steps_intraday`
GROUP BY src_id ORDER BY src_id
""").render(project_id=project_id, dataset=dataset_id)
df = execute(client, query)

check_status = "Look at the result and see if it meets all the following criteria."
msg = (
    "The result must show that <br>"
    "(1) The table has records from both PTSC and CE, and<br>"
    "(2) all the records' src_ids are either PTSC or CE (= No other src_id in this table) <br>"
    "If any of (1) - (2) does not look good, the source records are not properly prepared. "
    "Bring up the issue to the RDR team so they can fix it.")

display(
    HTML(
        f'''<h3>Check Status: <span style="color: gold">{check_status}</span></h3><p>{msg}</p>'''
    ))

display(df)

# -

# # HEART_RATE_INTRADAY table

# Validation criteria for steps_intraday is the following:
# - The table includes both PTSC and CE data per the src_id field

# +
query = JINJA_ENV.from_string("""
SELECT src_id, COUNT(*) AS row_count
FROM `{{project_id}}.{{dataset}}.heart_rate_intraday`
GROUP BY src_id ORDER BY src_id
""").render(project_id=project_id, dataset=dataset_id)
df = execute(client, query)

check_status = "Look at the result and see if it meets all the following criteria."
msg = (
    "The result must show that <br>"
    "(1) The table has records from both PTSC and CE, and<br>"
    "(2) all the records' src_ids are either PTSC or CE (= No other src_id in this table) <br>"
    "If any of (1) - (2) does not look good, the source records are not properly prepared. "
    "Bring up the issue to the RDR team so they can fix it.")

display(
    HTML(
        f'''<h3>Check Status: <span style="color: gold">{check_status}</span></h3><p>{msg}</p>'''
    ))

display(df)

# -

# # HEART_RATE_SUMMARY table

# Validation criteria for heart_rate_summary is the following:
# - The table includes both PTSC and CE data per the src_id field
# - At least 40% of participants should have at least all 4 zone names for at least one date

# +

src_ids_check = JINJA_ENV.from_string("""
SELECT src_id, COUNT(*) as row_count
FROM `{{project_id}}.{{dataset}}.heart_rate_summary`
GROUP BY src_id ORDER BY src_id
""").render(project_id=project_id, dataset=dataset_id)

zone_names_check = JINJA_ENV.from_string("""
WITH distinct_zones AS (
SELECT 
    DISTINCT zone_name, 
    person_id
FROM 
    `{{project_id}}.{{dataset}}.heart_rate_summary`
),
                                 
at_least_four_zones AS (
    SELECT
        person_id, 
        COUNT(person_id) AS total
    FROM
        distinct_zones
    GROUP BY
        person_id
    HAVING total > 3
),                 

for_at_least_one_date AS (
    SELECT 
        DISTINCT person_id, 
        date, 
        COUNT(*) as total
    FROM 
        `{{project_id}}.{{dataset}}.heart_rate_summary`
    WHERE person_id IN (
        SELECT 
            person_id
        FROM
            at_least_four_zones
    )
    GROUP BY 1,2
    HAVING total > 3
)                             

SELECT 
  ROUND((COUNT(*)/(
    SELECT
        COUNT(DISTINCT person_id)
    FROM 
        distinct_zones
   ))*100,2) AS percentage
FROM
    for_at_least_one_date
""").render(project_id=project_id, dataset=dataset_id)

src_ids_check_results = execute(client, src_ids_check)
zones_check_results = execute(client, zone_names_check)

display(src_ids_check_results)
display(zones_check_results)

check_status = "Look at the result and see if it meets all the following criteria."
msg = (
    "The result must show that <br>"
    "(1) The table has records from both PTSC and CE, and<br>"
    "(2) all the records' src_ids are either PTSC or CE (= No other src_id in this table) <br>"
    "(3) The percentage value returned is equal to or greater than 40. <br>"
    "If any of (1) - (2) does not look good, the source records are not properly prepared. "
    "Bring up the issue to the RDR team so they can fix it.")

display(
    HTML(
        f'''<h3>Check Status: <span style="color: gold">{check_status}</span></h3><p>{msg}</p>'''
    ))
# -

# # ACTIVITY_SUMMARY table

# Validation criteria for activity_summary is the following:
# - The table includes both PTSC and CE data per the src_id field

# +
query = JINJA_ENV.from_string("""
SELECT src_id, COUNT(*) as row_count
FROM `{{project_id}}.{{dataset}}.activity_summary`
GROUP BY src_id ORDER BY src_id
""").render(project_id=project_id, dataset=dataset_id)
df = execute(client, query)

check_status = "Look at the result and see if it meets all the following criteria."
msg = (
    "The result must show that <br>"
    "(1) The table has records from both PTSC and CE, and<br>"
    "(2) all the records' src_ids are either PTSC or CE (= No other src_id in this table) <br>"
    "If any of (1) - (2) does not look good, the source records are not properly prepared. "
    "Bring up the issue to the RDR team so they can fix it.")

display(
    HTML(
        f'''<h3>Check Status: <span style="color: gold">{check_status}</span></h3><p>{msg}</p>'''
    ))

display(df)

# -

# # DEVICE table

# Validation criteria for device is the following:
# - The table includes both PTSC and CE data per the src_id field

# +
query = JINJA_ENV.from_string("""
SELECT src_id, COUNT(*) as row_count
FROM `{{project_id}}.{{dataset}}.device`
GROUP BY src_id ORDER BY src_id
""").render(project_id=project_id, dataset=dataset_id)
df = execute(client, query)

check_status = "Look at the result and see if it meets all the following criteria."
msg = (
    "The result must show that <br>"
    "(1) The table has records from both PTSC and CE, and<br>"
    "(2) all the records' src_ids are either PTSC or CE (= No other src_id in this table) <br>"
    "If any of (1) - (2) does not look good, the source records are not properly prepared. "
    "Bring up the issue to the RDR team so they can fix it.")

display(
    HTML(
        f'''<h3>Check Status: <span style="color: gold">{check_status}</span></h3><p>{msg}</p>'''
    ))

display(df)

# -

# # SLEEP_LEVEL table

# Validation criteria for sleep_level is the following:
# - The table includes both PTSC and CE data per the src_id field

# +
query = JINJA_ENV.from_string("""
SELECT src_id, COUNT(*) as row_count
FROM `{{project_id}}.{{dataset}}.sleep_level`
GROUP BY src_id ORDER BY src_id
""").render(project_id=project_id, dataset=dataset_id)
df = execute(client, query)

check_status = "Look at the result and see if it meets all the following criteria."
msg = (
    "The result must show that <br>"
    "(1) The table has records from both PTSC and CE, and<br>"
    "(2) all the records' src_ids are either PTSC or CE (= No other src_id in this table) <br>"
    "If any of (1) - (2) does not look good, the source records are not properly prepared. "
    "Bring up the issue to the RDR team so they can fix it.")

display(
    HTML(
        f'''<h3>Check Status: <span style="color: gold">{check_status}</span></h3><p>{msg}</p>'''
    ))

display(df)

# -
