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

# # Incremental load validation notebook
# This notebook is for validating 2022q4 "missing basics" hotfix.
# See [DC-3016](https://precisionmedicineinitiative.atlassian.net/browse/DC-3016)
# and its subtasks for details.

# + tags=["parameters"]
run_as: str = ""  # service account email to impersonate
project_id: str = ""  # project where datasets are located
new_dataset: str = ""  # dataset we created during this hotfix
source_dataset: str = ""  # dataset that new_dataset is based off of
aian_lookup_dataset: str = ""  # dataset where we have aian lookup table
aian_lookup_table: str = ""  # table that has PIDs/RIDs of AIAN participants
incremental_dataset: str = ""  # dataset so-called "incremental"
is_deidentified: str = ""  # True if this is DEID stage, False if not
includes_aian: str = ""  # True if AIAN participants' data is in the dataset, False if not
# -

# +
from IPython.display import display, HTML

from common import JINJA_ENV
from analytics.cdr_ops.notebook_utils import (execute, IMPERSONATION_SCOPES,
                                              render_message)
from utils import auth
from gcloud.bq import BigQueryClient
# -

impersonation_creds = auth.get_impersonation_credentials(
    run_as, target_scopes=IMPERSONATION_SCOPES)
client = BigQueryClient(project_id, credentials=impersonation_creds)
pid_or_rid = 'research_id' if is_deidentified == 'True' else 'person_id'

# ## QC 1. (Only for `includes_aian == False`) Confirm no AIAN participants data is included in the new dataset
# `incremental_dataset` contains AIAN participants' records. <br>We must exclude those in `new_dataset`
# when creating a WITHOUT AIAN dataset.

# +
if includes_aian == "True":
    success_msg = "Skipping this check because the new dataset can include AIAN participants.<br><br>"
    render_message('', success_msg=success_msg)

else:
    person_id_tables_query = JINJA_ENV.from_string('''
        SELECT table_name
        FROM `{{project}}.{{new_dataset}}.INFORMATION_SCHEMA.COLUMNS`
        WHERE column_name = "person_id"
        ORDER BY table_name
    ''').render(project=project_id, new_dataset=new_dataset)
    pid_table_list = client.query(person_id_tables_query).to_dataframe().get(
        'table_name').to_list()

    query = JINJA_ENV.from_string('''
        SELECT
            \'{{table}}\' as table_name,
            COUNT(*) AS aian_row_count
        FROM `{{project}}.{{new_dataset}}.{{table}}`
        WHERE person_id IN (
            SELECT {{pid_or_rid}} FROM `{{project}}.{{aian_lookup_dataset}}.{{aian_lookup_table}}`
        )
        HAVING COUNT(*) > 0
    ''')
    queries = [
        query.render(project=project_id,
                     new_dataset=new_dataset,
                     table=table,
                     aian_lookup_dataset=aian_lookup_dataset,
                     aian_lookup_table=aian_lookup_table,
                     pid_or_rid=pid_or_rid) for table in pid_table_list
    ]
    union_all_query = '\nUNION ALL\n'.join(queries)

    df = execute(client, union_all_query)

    success_null_check = f"No AIAN participants' records are found in {new_dataset}.<br><br>"
    failure_null_check = (
        "There are <b>{count}</b> tables that might have "
        "AIAN participants' records. <br>Look at these tables and investigate why AIAN "
        "records are there: <b>{tables}</b><br><br>")

    render_message(df,
                   success_null_check,
                   failure_null_check,
                   failure_msg_args={
                       'count': len(df),
                       'tables': ', '.join(df.table_name)
                   })
# -

# ## QC 2. Confirm there are no duplicate `OBSERVATION_ID`s
# Any observation records from `incremental_dataset` got new `OBSERVATION_ID`s
# assigned in `new_dataset` to avoid ID duplicates. <br>We must confirm all the
# `OBSERVATION_ID`s in `new_dataset` are unique. <br>The same goes to
# `_mapping_observation` and `obsesrvation_ext`.

# +
obs_tables = ['observation', 'observation_ext'
             ] if is_deidentified == 'True' else [
                 'observation', '_mapping_observation', 'observation_ext'
             ]

query = JINJA_ENV.from_string('''
    SELECT
        \'{{table}}\' as table_name,
        observation_id,
        COUNT(*) AS duplicate_row_count
    FROM `{{project}}.{{new_dataset}}.{{table}}`
    GROUP BY table_name, observation_id
    HAVING COUNT(*) > 1
''')

queries = [
    query.render(project=project_id, new_dataset=new_dataset, table=table)
    for table in obs_tables
]

union_all_query = '\nUNION ALL\n'.join(queries)
df = execute(client, union_all_query)

success_null_check = f"No OBSERVATION_ID duplicates are found in {new_dataset}.<br><br>"
failure_null_check = (
    "There are total <b>{count}</b> OBSERVATION_IDs that might have"
    "duplicates. <br>Look at these tables and investigate why there are duplicates: <b>{tables}</b><br><br>"
)

render_message(df,
               success_null_check,
               failure_null_check,
               failure_msg_args={
                   'count': len(df),
                   'tables': ', '.join(df.table_name)
               })
# -

# ## QC 3. Confirm mapping/ext tables are consistent
# This hotfix runs several delete and insert statements to OBSERVATION, SURVEY_CONDUCT, PERSON
# and their mapping/ext tables. <br>We must confirm that mapping/ext tables are consistent with
# their correspondants after the hotfix.

# +
map_ext_tuples = [
    ('observation', 'observation_ext'),
    ('survey_conduct', 'survey_conduct_ext'),
    ('person', 'person_ext'),
] if is_deidentified == 'True' else [
    ('observation', '_mapping_observation'),
    ('observation', 'observation_ext'),
    ('survey_conduct', '_mapping_survey_conduct'),
    ('survey_conduct', 'survey_conduct_ext'),
]

query = JINJA_ENV.from_string('''
    SELECT
        \'{{mapping_table}}\' as table_name,
        COUNT(*) AS unmatched_ids
    FROM `{{project}}.{{new_dataset}}.{{table_name}}` d
    FULL OUTER JOIN `{{project}}.{{new_dataset}}.{{mapping_table}}` mp
    USING ({{table_name}}_id)
    WHERE d.{{table_name}}_id IS NULL OR mp.{{table_name}}_id IS NULL
    HAVING COUNT(*) > 0
''')

queries = []
for (table, mapping_table) in map_ext_tuples:
    queries.append(
        query.render(project=project_id,
                     new_dataset=new_dataset,
                     table_name=table,
                     mapping_table=mapping_table))
union_all_query = '\nUNION ALL\n'.join(queries)

df = execute(client, union_all_query)

success_null_check = (
    "All OBSERVATION, SURVEY_CONDUCT, and PERSON records have "
    "valid corresponding records in their mapping/ext tables. <br>"
    "And there are no invalid records in the mapping/ext tables.")
failure_null_check = (
    "These mapping/ext tables are inconsistent with their "
    "correspondants. <br>Look at these tables and investigate why "
    "they are inconsistent: <b>{tables}</b><br><br>")

render_message(df,
               success_null_check,
               failure_null_check,
               failure_msg_args={'tables': ', '.join(df.table_name)})

# -

# ## QC 4. (Only for `is_deidentified == True`) Confirm `person_ext`'s state related columns come from `source_dataset`
# `state_of_residence_concept_id` and `state_of_residence_source_value` in `incremental_dataset.person_ext` are
# all NULL. This is because these two columns do not originate from the basics. <br>To have a complete `new_dataset.person_ext`,
# we must pull these columns from `source_dataset`, not from `incremental_dataset`.

# +
if is_deidentified == "False":
    success_msg = "Skipping this check person_ext table exists only in DEID datasets.<br>"
    render_message('', success_msg=success_msg)

else:
    query = JINJA_ENV.from_string('''
        SELECT *
        FROM `{{project}}.{{new_dataset}}.person_ext` n
        JOIN `{{project}}.{{source_dataset}}.person_ext` s
        ON n.person_id = s.person_id
        WHERE (
            n.state_of_residence_concept_id != s.state_of_residence_concept_id
            OR n.state_of_residence_source_value != s.state_of_residence_source_value
        )
        OR (
            (n.state_of_residence_concept_id IS NULL AND s.state_of_residence_concept_id IS NOT NULL)
            OR (n.state_of_residence_source_value IS NULL AND s.state_of_residence_source_value IS NOT NULL)
        )
    ''').render(project=project_id,
                new_dataset=new_dataset,
                source_dataset=source_dataset)

    df = execute(client, query)

    success_null_check = (
        f"All state columns in {new_dataset}.person_ext come from {source_dataset}, "
        f"not {incremental_dataset}.<br><br>")
    failure_null_check = (
        f"There are <b>{len(df)}</b> records in {new_dataset}.person_ext that have "
        f"unmatching state columns from {source_dataset}. <br>Look at the table and "
        "investigate why they are inconsistent.<br><br>")

    render_message(df, success_null_check, failure_null_check)

# -

# ## QC 5. Confirm `SURVEY_CONDUCT` references `OBSERVATION` correctly
# This hotfix runs delete and insert on both `SURVEY_CONDUCT` and `OBSERVATION`. <br>
# `survey_conduct_id` must have corresponding `questionnaire_response_id` in `OBSERVATION`.
# This QC confirms all `SURVEY_CONDUCT` records have corresponding records in `OBSERVATION`
# after the hotfix.

# +
query = JINJA_ENV.from_string('''
    SELECT * FROM `{{project}}.{{new_dataset}}.survey_conduct`
    WHERE survey_conduct_id NOT IN (
        SELECT questionnaire_response_id FROM `{{project}}.{{new_dataset}}.observation`
    )
    ''').render(project=project_id, new_dataset=new_dataset)

df = execute(client, query)

success_null_check = (
    f"All records in {new_dataset}.survey_conduct have corresponding records in "
    f"{new_dataset}.observation<br><br>")
failure_null_check = (
    f"There are <b>{len(df)}</b> records in {new_dataset}.survey_conduct that miss "
    f"corresponding records {new_dataset}.observation. <br>Look at the table and "
    "investigate why they are inconsistent.<br><br>")

render_message(df, success_null_check, failure_null_check)

# -

# ## QC 6. Confirm most of "missing basics" issues are remediated
# This hotfix will fix the "missing basics" problem. But not all "missing basics"
# will be fixed. <br>This QC is to see how much of the problem is resolved.
# (We do not have a specific number for this check to succeed/fail.)<br>
# We must re-assess our hotfix if unresolved "missing basics" are still way too many.

# +
query = JINJA_ENV.from_string('''
    WITH new_dataset AS (
        SELECT COUNT(DISTINCT person_id) AS count_missing_basics
        FROM `{{project}}.{{new_dataset}}.person`
        WHERE person_id NOT IN
        (
            SELECT DISTINCT person_id
            FROM `{{project}}.{{new_dataset}}.concept` 
            JOIN `{{project}}.{{new_dataset}}.concept_ancestor`
                ON concept_id = ancestor_concept_id
            JOIN `{{project}}.{{new_dataset}}.observation`
                ON descendant_concept_id = observation_concept_id
            WHERE observation_concept_id NOT IN (40766240, 43528428, 1585389)
            AND concept_class_id = 'Module'
            AND concept_name IN ('The Basics') 
            AND questionnaire_response_id is not null
            AND observation_source_value NOT LIKE 'Second%' 
            AND observation_source_value NOT LIKE 'PersonOne%'
            AND observation_source_value NOT LIKE 'SocialSecurity%'
        )
    ), source_dataset AS (
        SELECT COUNT(DISTINCT person_id) AS count_missing_basics
        FROM `{{project}}.{{source_dataset}}.person`
        WHERE person_id NOT IN
        (
            SELECT DISTINCT person_id
            FROM `{{project}}.{{source_dataset}}.concept` 
            JOIN `{{project}}.{{source_dataset}}.concept_ancestor`
                ON concept_id = ancestor_concept_id
            JOIN `{{project}}.{{source_dataset}}.observation`
                ON descendant_concept_id = observation_concept_id
            WHERE observation_concept_id NOT IN (40766240, 43528428, 1585389)
            AND concept_class_id = 'Module'
            AND concept_name IN ('The Basics') 
            AND questionnaire_response_id is not null
            AND observation_source_value NOT LIKE 'Second%' 
            AND observation_source_value NOT LIKE 'PersonOne%'
            AND observation_source_value NOT LIKE 'SocialSecurity%'
        )
    )
    SELECT 
        new_dataset.count_missing_basics AS remaining_missing_basics,
        source_dataset.count_missing_basics AS original_missing_basics,
        source_dataset.count_missing_basics - new_dataset.count_missing_basics AS resolved_missing_basics
    FROM new_dataset CROSS JOIN source_dataset
''').render(project=project_id,
            new_dataset=new_dataset,
            source_dataset=source_dataset)

df = execute(client, query)
remaining_missing_basics, original_missing_basics, resolved_missing_basics = df.remaining_missing_basics[
    0], df.original_missing_basics[0], df.resolved_missing_basics[0]

check_status = "Cannot tell success or failure. Check the result."
msg = (
    f"Originally there were <b>{original_missing_basics}</b> participants who had missing basics. <br>"
    f"We successfully remediated <b>{resolved_missing_basics}</b> participants with this hotfix. <br>"
    f"There are still <b>{remaining_missing_basics}</b> participants who miss their basics. <br>"
    f"If <b>{remaining_missing_basics}</b> seems too high, re-assess our hotfix and ensure "
    "we are not missing anything.<br><br>")

display(
    HTML(f'''<br>
        <h3>Check Status: <span style="color: gold">{check_status}</span></h3>
        <p>{msg}</p>
    '''))
df

# -
