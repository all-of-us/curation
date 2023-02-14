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
sandbox_dataset: str = ""  # sandbox dataset that is used for this hotfix
sandbox_obs: str = ""  # sandbox table for observation used for this hotfix
aian_lookup_dataset: str = ""  # dataset where we have aian lookup table
aian_lookup_table: str = ""  # table that has PIDs/RIDs of AIAN participants
incremental_dataset: str = ""  # dataset so-called "incremental"
obs_id_lookup_dataset: str = ""  # dataset that has NEW_OBS_ID_LOOKUP table.
includes_aian: str = ""  # True if AIAN participants' data is in the dataset, False if not
# -

# +
from IPython.display import display, HTML

from common import JINJA_ENV, OBSERVATION, PERSON, SURVEY_CONDUCT
from analytics.cdr_ops.notebook_utils import (execute, IMPERSONATION_SCOPES,
                                              render_message)
from utils import auth
from gcloud.bq import BigQueryClient
from resources import ext_table_for, mapping_table_for
from retraction.retract_utils import (is_combined_release_dataset,
                                      is_deid_dataset, is_deid_release_dataset,
                                      is_rdr_dataset)
# -

impersonation_creds = auth.get_impersonation_credentials(
    run_as, target_scopes=IMPERSONATION_SCOPES)
client = BigQueryClient(project_id, credentials=impersonation_creds)
pid_or_rid = 'research_id' if is_deid_dataset(new_dataset) else 'person_id'

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
            WHERE {{pid_or_rid}} IS NOT NULL
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

# ## QC 2-1. Confirm there are no duplicate `OBSERVATION_ID`s
# Any observation records from `incremental_dataset` got new `OBSERVATION_ID`s
# assigned in `new_dataset` to avoid ID duplicates. <br>We must confirm all the
# `OBSERVATION_ID`s in `new_dataset` are unique. <br>The same goes to
# `_mapping_observation` and `obsesrvation_ext`.

# +
if is_deid_dataset(new_dataset):
    obs_tables = [OBSERVATION, ext_table_for(OBSERVATION)]
elif is_combined_release_dataset(new_dataset):
    obs_tables = [
        OBSERVATION,
        mapping_table_for(OBSERVATION),
        ext_table_for(OBSERVATION)
    ]
elif is_rdr_dataset(new_dataset):
    obs_tables = [OBSERVATION]
else:
    obs_tables = [OBSERVATION, mapping_table_for(OBSERVATION)]

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

# ## QC 2-2. Confirm all new records in `OBSERVATION` have mapping info in `_observation_id_map`
# Any observation records from `incremental_dataset` got new `OBSERVATION_ID`s
# assigned in `new_dataset` to avoid ID duplicates. The mapping of the old-new
# `OBSERVATION_ID`s is kept in `obs_id_lookup_dataset._observation_id_map`. <br>We must confirm
# all the new `OBSERVATION_ID`s have their mapping info in `obs_id_lookup_dataset._observation_id_map`<br>.
# NOTE: Not all records in `_observation_id_map` have corresponding records in `new_dataset`
# because `_observation_id_map` is referenced by multiple data releases.

# +
query = JINJA_ENV.from_string('''
    SELECT
        'no mapping record found in obs_id_lookup_dataset._observation_id_map' AS issue,
        COUNT(*) AS no_mapping_row_count
    FROM `{{project}}.{{new_dataset}}.observation`
    WHERE observation_id NOT IN (
        SELECT observation_id FROM `{{project}}.{{obs_id_lookup_dataset}}._observation_id_map`
        WHERE observation_id IS NOT NULL
    )
    HAVING COUNT(*) > 1
''').render(project=project_id,
            new_dataset=new_dataset,
            sandbox_dataset=sandbox_dataset)

df = execute(client, query)
issues = df.issue

success_null_check = (
    f"All records in {new_dataset}.observation have corresponding records in "
    f"{obs_id_lookup_dataset}._observation_id_map and vice versa.<br><br>")
failure_null_check = (
    f"Issue(s) found: {', '.join(issues)}. Look at the result table below. <br>"
    "investigate why they are inconsistent.<br><br>")

render_message(df, success_null_check, failure_null_check)
# -

# ## QC 2-3. Confirm all the sandboxed `OBSERVATION` records have new records populated in `new_dataset`
# We must confirm all the sandboxed `OBSERVATION` records have new records in `new_dataset`.
# We can do so by looking at `OBSERVATION_SOURCE_CONCEPT_ID` and `PERSON_ID`. We cannot use `OBSERVATION_ID`
# here because old-OBSERVATION_ID:new-OBSERVATION_ID can be 1:N, N:1, and N:N. (Not always 1:1)

# +
query = JINJA_ENV.from_string('''
    SELECT COUNT(*) AS unmatched_records
    FROM `{{project}}.{{sandbox_dataset}}.{{sandbox_obs}}` m
    WHERE NOT EXISTS (
        SELECT 1 FROM `{{project}}.{{new_dataset}}.observation` o
        WHERE m.person_id = o.person_id
        AND m.observation_source_concept_id = o.observation_source_concept_id
    )
    HAVING COUNT(*) > 0
''').render(project=project_id,
            new_dataset=new_dataset,
            sandbox_dataset=sandbox_dataset,
            sandbox_obs=sandbox_obs)

df = execute(client, query)
unmatched_records = df.unmatched_records[0]

success_null_check = (
    f"All the sandboxed records in {sandbox_dataset}.{sandbox_obs} have new records "
    f"in {new_dataset}.observation.<br><br>")
failure_null_check = (
    f"There are <b>{unmatched_records}</b> sandboxed observation records that did not "
    f"get populated in {new_dataset}. <br>investigate why they are inconsistent.<br><br>"
)

render_message(df, success_null_check, failure_null_check)
# -

# ## QC 3. Confirm mapping/ext tables are consistent
# This hotfix runs several delete and insert statements to OBSERVATION, SURVEY_CONDUCT, PERSON
# and their mapping/ext tables. <br>We must confirm that mapping/ext tables are consistent with
# their correspondants after the hotfix.

# +
if is_rdr_dataset(new_dataset):
    success_msg = "Skipping this check because RDR dataset does not have mapping/ext tables.<br><br>"
    render_message('', success_msg=success_msg)

else:
    if is_combined_release_dataset(new_dataset):
        map_ext_tuples = [
            (OBSERVATION, mapping_table_for(OBSERVATION)),
            (OBSERVATION, ext_table_for(OBSERVATION)),
            (SURVEY_CONDUCT, mapping_table_for(SURVEY_CONDUCT)),
            (SURVEY_CONDUCT, ext_table_for(SURVEY_CONDUCT)),
        ]
    elif is_deid_dataset(new_dataset):
        map_ext_tuples = [
            (OBSERVATION, ext_table_for(OBSERVATION)),
            (SURVEY_CONDUCT, ext_table_for(SURVEY_CONDUCT)),
            (PERSON, ext_table_for(PERSON)),
        ]
    else:
        map_ext_tuples = [
            (OBSERVATION, mapping_table_for(OBSERVATION)),
            (SURVEY_CONDUCT, mapping_table_for(SURVEY_CONDUCT)),
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

# ## QC 4-1. (Only for deid base/clean) Confirm `person_ext`'s state related columns come from `source_dataset` <br>- Check against entire `new_dataset`
# `state_of_residence_concept_id` and `state_of_residence_source_value` in `incremental_dataset.person_ext` are
# all NULL. This is because these two columns do not originate from the basics. <br>To have a complete `new_dataset.person_ext`,
# we must pull these columns from `source_dataset`, not from `incremental_dataset`.<br>
# If this QC fails, see how QC 4-2 goes.

# +
if not is_deid_release_dataset(new_dataset):
    success_msg = "Skipping this check person_ext table exists only in deid base/clean datasets.<br>"
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
        "investigate why they are inconsistent. <br><b>QC 4-2 will help narrow down "
        "the potential cause of the issue.</b><br><br>")

    render_message(df, success_null_check, failure_null_check)

# -

# ## QC 4-2. (Only for deid base/clean) Confirm `person_ext`'s state related columns come from `source_dataset` <br>- Check against records from `incremental_dataset`
# If QC 4-1 fails, this QC will be helpful for troubleshooting. This QC checks the same thing as 4-1, but the scope is limited to only the participants from `incremental_dataset`.<br>
# If QC 4-1 fails but 4-2 is successful, that means the issue comes from the original dataset, not from the incremental dataset. If both 4-1 and 4-2 fail, that means
# something related to the incremental dataset did not work as expected.

# +
if not is_deid_release_dataset(new_dataset):
    success_msg = "Skipping this check person_ext table exists only in deid base/clean datasets.<br>"
    render_message('', success_msg=success_msg)

else:
    query = JINJA_ENV.from_string('''
        SELECT *
        FROM `{{project}}.{{new_dataset}}.person_ext` n
        JOIN `{{project}}.{{source_dataset}}.person_ext` s
        ON n.person_id = s.person_id
        WHERE n.person_id IN (
            SELECT person_id FROM `{{project}}.{{incremental_dataset}}.person`
            WHERE person_id IS NOT NULL
        )
        AND (
            (
                n.state_of_residence_concept_id != s.state_of_residence_concept_id
                OR n.state_of_residence_source_value != s.state_of_residence_source_value
            ) OR (
                (n.state_of_residence_concept_id IS NULL AND s.state_of_residence_concept_id IS NOT NULL)
                OR (n.state_of_residence_source_value IS NULL AND s.state_of_residence_source_value IS NOT NULL)
            )
        )
    ''').render(project=project_id,
                new_dataset=new_dataset,
                incremental_dataset=incremental_dataset,
                source_dataset=source_dataset)

    df = execute(client, query)

    success_null_check = (
        f"All records in {new_dataset}.person_ext affected by the hotfix have correct state columns.<br> "
        f"If QC 4-1 failed, we can be sure the problem came from the original data in {source_dataset}, "
        f"not by the hotfix.<br><br>")
    failure_null_check = (
        f"There are <b>{len(df)}</b> records in {new_dataset}.person_ext that are affected by the hotfix "
        f"and still have unmatching state columns from {source_dataset}. <br>Look at the table and "
        "investigate why they are inconsistent.<br><br>")

    render_message(df, success_null_check, failure_null_check)

# -

# ## QC 5-1. Confirm `SURVEY_CONDUCT` - `OBSERVATION` relationship is still intact
# This hotfix runs delete and insert on both `SURVEY_CONDUCT` and `OBSERVATION`. <br>
# `survey_conduct_id` must have corresponding `questionnaire_response_id` in `OBSERVATION`
# and vice versa. <br>
# This QC confirms the relationship between `SURVEY_CONDUCT`and `OBSERVATION` are still complete
# after the hotfix.

# +
query = JINJA_ENV.from_string('''
    SELECT 'orphaned survey_conduct_id' AS issue, COUNT(*) 
    FROM `{{project}}.{{new_dataset}}.survey_conduct`
    WHERE survey_conduct_id NOT IN (
        SELECT questionnaire_response_id FROM `{{project}}.{{new_dataset}}.observation`
        WHERE questionnaire_response_id IS NOT NULL
    )
    HAVING COUNT(*) > 0
    UNION ALL
    SELECT 'orphaned questionnaire_response_id' AS issue, COUNT(*) 
    FROM `{{project}}.{{new_dataset}}.observation`
    WHERE questionnaire_response_id NOT IN (
        SELECT survey_conduct_id FROM `{{project}}.{{new_dataset}}.survey_conduct`
        WHERE survey_conduct_id IS NOT NULL
    )
    HAVING COUNT(*) > 0
    ''').render(project=project_id, new_dataset=new_dataset)

df = execute(client, query)
issues = df.issue

success_null_check = (
    f"All records in {new_dataset}.survey_conduct have corresponding records in "
    f"{new_dataset}.observation and vice versa.<br><br>")
failure_null_check = (
    f"Issue(s) found: {', '.join(issues)}. Look at the result table below. <br>"
    "investigate why they are inconsistent.<br><br>")

render_message(df, success_null_check, failure_null_check)

# -

# ## QC 5-2. How many new `questionnaire_response_id`s and `survey_conduct_id`s came from the "incremental"
# This is a supplemental check. There is no success/failure for it. <br>
# This QC shows how many new `questionnaire_response_id`s and `survey_conduct_id`s are introduced
# to `new_dataset` from `incremental_dataset`.
# <b>The new questionnaire_response_ids and survey_conduct_ids should be the same.</b>

# +
query = JINJA_ENV.from_string('''
    WITH qrid AS (
        SELECT 
            COUNT(DISTINCT questionnaire_response_id) AS num_new_id
        FROM `{{project}}.{{new_dataset}}.observation`
        WHERE questionnaire_response_id NOT IN (
            SELECT DISTINCT questionnaire_response_id
            FROM `{{project}}.{{source_dataset}}.observation`
            WHERE questionnaire_response_id IS NOT NULL
        )
    ), scid AS (
        SELECT 
            COUNT(DISTINCT survey_conduct_id) AS num_new_id
        FROM `{{project}}.{{new_dataset}}.survey_conduct`
        WHERE survey_conduct_id NOT IN (
            SELECT DISTINCT survey_conduct_id
            FROM `{{project}}.{{source_dataset}}.survey_conduct`
            WHERE survey_conduct_id IS NOT NULL
        )
    )
    SELECT
        qrid.num_new_id AS new_questionnaire_response_ids,
        scid.num_new_id AS new_survey_conduct_ids
    FROM qrid CROSS JOIN scid
    ''').render(project=project_id,
                new_dataset=new_dataset,
                source_dataset=source_dataset)

df = execute(client, query)
new_questionnaire_response_ids, new_survey_conduct_ids = df.new_questionnaire_response_ids[
    0], df.new_survey_conduct_ids[0]

check_status = "Cannot tell success or failure. Check the result."
msg = (
    f"There are <b>{new_questionnaire_response_ids}</b> questionnaire_response_ids added as a result of the hotfix. <br>"
    f"There are <b>{new_survey_conduct_ids}</b> survey_conducts added as a result of the hotfix. <br>"
    "<b>These numbers should be the same.</b> <br>"
    "If these numbers look off, investigate.<br><br>")

display(
    HTML(f'''<br>
        <h3>Check Status: <span style="color: gold">{check_status}</span></h3>
        <p>{msg}</p>
    '''))
df

# -

# ## QC 6. Confirm most of "missing basics" issues are remediated
# This hotfix will fix the "missing basics" problem. But not all "missing basics"
# will be fixed. <br>This QC is to see how much of the problem is resolved.
# (We do not have a specific number for this check to succeed/fail.)<br>
# We must re-assess our hotfix if unresolved "missing basics" are still way too many.

# +
query = JINJA_ENV.from_string('''
    WITH new_missing AS (
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
    ), source_missing AS (
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
    ), new_all AS (
        SELECT COUNT(*) AS count_new_all
        FROM `{{project}}.{{new_dataset}}.person`
    ), source_all AS (
        SELECT COUNT(*) AS count_source_all
        FROM `{{project}}.{{source_dataset}}.person`        
    )
    SELECT 
        new_missing.count_missing_basics AS remaining_missing_basics,
        source_missing.count_missing_basics AS original_missing_basics,
        new_all.count_new_all AS count_new_all,
        source_all.count_source_all AS count_source_all,
        ROUND(source_missing.count_missing_basics / source_all.count_source_all * 100, 1) AS original_missing_percent,
        ROUND(new_missing.count_missing_basics / new_all.count_new_all * 100, 1) AS remaining_missing_percent,
        source_missing.count_missing_basics - new_missing.count_missing_basics AS remediated_missing_basics
    FROM new_missing 
    CROSS JOIN source_missing
    CROSS JOIN new_all
    CROSS JOIN source_all
''').render(project=project_id,
            new_dataset=new_dataset,
            source_dataset=source_dataset)

df = execute(client, query)
(remaining_missing_basics, original_missing_basics, count_new_all,
 count_source_all, original_missing_percent, remaining_missing_percent,
 remediated_missing_basics) = (df.remaining_missing_basics[0],
                               df.original_missing_basics[0],
                               df.count_new_all[0], df.count_source_all[0],
                               df.original_missing_percent[0],
                               df.remaining_missing_percent[0],
                               df.remediated_missing_basics[0])

check_status = "Cannot tell success or failure. Check the result."
msg = (
    f"Originally there were <b>{original_missing_basics}</b> participants "
    f"(<b>{original_missing_percent}% of {count_source_all} participants</b>) who had missing basics. <br>"
    f"We successfully remediated <b>{remediated_missing_basics}</b> participants with this hotfix. <br>"
    f"There are still <b>{remaining_missing_basics}</b> participants "
    f"(<b>{remaining_missing_percent}% of {count_new_all} participants</b>) who miss their basics. <br>"
    f"If <b>{remaining_missing_basics}</b> seems too high, re-assess our hotfix and ensure "
    "we are not missing anything.<br><br>")

display(
    HTML(f'''<br>
        <h3>Check Status: <span style="color: gold">{check_status}</span></h3>
        <p>{msg}</p>
    '''))
df

# -
