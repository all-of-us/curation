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

# ## List of tables with person_id column

person_id_tables_query = JINJA_ENV.from_string('''
    SELECT table_name
    FROM `{{project}}.{{new_dataset}}.INFORMATION_SCHEMA.COLUMNS`
    WHERE column_name = "person_id"
    ORDER BY table_name
''').render(project=project_id, new_dataset=new_dataset)
pid_table_list = client.query(person_id_tables_query).to_dataframe().get(
    'table_name').to_list()
pid_table_list

# ## QC 1. (Only for `includes_aian == False`) Confirm no AIAN participants data is included in the new dataset
# `incremental_dataset` contains AIAN participants' records. We must exclude those in `new_dataset`
# when creating a WITHOUT AIAN dataset.

# +
if includes_aian == "True":
    success_msg = "Skipping this check because the new dataset can include AIAN participants."
    render_message('', success_msg=success_msg)

else:
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

    success_null_check = f"No AIAN participants' records are found in {new_dataset}"
    failure_null_check = (
        "There are <b>{count}</b> tables that might have "
        "AIAN participants' records. Look at these tables and investigate why AIAN "
        "records are there: <b>{tables}</b>")

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
# assigned to avoid ID duplicates. We must confirm all the `OBSERVATION_ID`s in
# `new_dataset` are unique. The same goes to `_mapping_observation` and
# `obsesrvation_ext`.

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

success_null_check = f"No OBSERVATION_ID duplicates are found in {new_dataset}"
failure_null_check = (
    "There are total <b>{count}</b> OBSERVATION_IDs that might have"
    "duplicates. Look at these tables and investigate why there are duplicates: <b>{tables}</b>"
)

render_message(df,
               success_null_check,
               failure_null_check,
               failure_msg_args={
                   'count': len(df),
                   'tables': ', '.join(df.table_name)
               })
# -

# ## QC 3. Mapping/ext tables are consistent
# This hotfix runs several delete and insert statements to OBSERVATION, SURVEY_CONDUCT, PERSON
# and their mapping/ext tables. We must confirm that mapping/ext tables are consistent with
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
    FROM `{{project}}.{{dataset}}.{{table_name}}` d
    FULL OUTER JOIN `{{project}}.{{dataset}}.{{mapping_table}}` mp
    USING ({{table_name}}_id)
    WHERE d.{{table_name}}_id IS NULL OR mp.{{table_name}}_id IS NULL
    HAVING COUNT(*) > 0
''')

queries = []
for (table, mapping_table) in map_ext_tuples:
    queries.append(
        query.render(project=project_id,
                     dataset=new_dataset,
                     table_name=table,
                     mapping_table=mapping_table))
union_all_query = '\nUNION ALL\n'.join(queries)

df = execute(client, union_all_query)

success_null_check = (
    "All OBSERVATION, SURVEY_CONDUCT, and PERSON records have "
    "valid corresponding records in their mapping/ext tables. "
    "And all there are no invalid records in the mapping/ext tables.")
failure_null_check = (
    "These mapping/ext tables are inconsistent with their "
    "correspondants. Look at these tables and investigate why "
    "they are inconsistent: <b>{tables}</b>")

render_message(df,
               success_null_check,
               failure_null_check,
               failure_msg_args={'tables': ', '.join(df.table_name)})

# -

# ## QC 4. The number of participants with "missing basics" is smaller (< ~5,000)
# This hotfix will fix "missing basics" problem. But not all "missing basics"
# will be fixed. Our investigation shows "missing basics" participants will be
# about less than 5,000 after the hotfix. We must re-assess our hotfix if we
# do not meet the number.

# +
query = JINJA_ENV.from_string('''
    SELECT COUNT(DISTINCT person_id) AS missing_basics_participants
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
        AND concept_class_id='Module'
        AND concept_name IN ('The Basics') 
        AND questionnaire_response_id is not null
        AND observation_source_value NOT LIKE 'Second%' 
        AND observation_source_value NOT LIKE 'PersonOne%'
        AND observation_source_value NOT LIKE 'SocialSecurity%'
    )
''').render(project=project_id, new_dataset=new_dataset)

df = execute(client, query)
missing_basics_participants = df.missing_basics_participants[0]

if missing_basics_participants < 5000:
    df = ''

success_null_check = (
    f"There are still {missing_basics_participants} participants without "
    "the basics. But it is known that this hotfix will not solve all "
    f"the missing basics, and {missing_basics_participants} is small enough.")
failure_null_check = (
    f"There are still {missing_basics_participants} participants without "
    "the basics. The number is still too big. Investigate and make sure "
    "all the hotfixes are properly applied.")

render_message(df, success_null_check, failure_null_check)

# -
