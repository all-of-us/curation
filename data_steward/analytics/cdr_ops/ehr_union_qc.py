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
PROJECT_ID = ""  # identifies the project containing the datasets
PREVIOUS_UNIONED_EHR_DATASET_ID = ""  # Identifies the dataset the snapshot was created from
CURRENT_UNIONED_EHR_DATASET_ID = ""  # Identifies the snapshot dataset
RELEASE_TAG = ""  # Identifies the release tag for current CDR
EHR_CUTOFF_DATE = ""  # CDR cutoff date
RUN_AS = ""  # service account email to impersonate
# -

# +
from cdm import tables_to_map
from common import AOU_DEATH, DEATH, JINJA_ENV, MAPPING_PREFIX
from resources import CDM_TABLES
from utils import auth
from gcloud.bq import BigQueryClient
from analytics.cdr_ops.notebook_utils import execute, IMPERSONATION_SCOPES, render_message

# -

impersonation_creds = auth.get_impersonation_credentials(
    RUN_AS, target_scopes=IMPERSONATION_SCOPES)

client = BigQueryClient(PROJECT_ID, credentials=impersonation_creds)

# ## Check Tags and lables added to Unioned_ehr datasets

stage = 'unioned_ehr'
for suffix in ['_backup', '_staging', '_sandbox', '']:
    dataset_id = f'{RELEASE_TAG}_unioned_ehr{suffix}'
    dataset = client.get_dataset(dataset_id)
    print(f'''{dataset.dataset_id}
  description: {dataset.description}
  labels: {dataset.labels}
  
    ''')

# ## Compare row counts of current unioned_ehr dataset to previous unioned_ehr_dataset

# +
query = f'''
WITH 
  diffs AS (
    SELECT 
      table_id
     ,o.row_count AS o_row_count
     ,n.row_count AS n_row_count
     ,n.row_count - o.row_count AS diff
     ,SAFE_DIVIDE(ABS(n.row_count - o.row_count), CAST(o.row_count AS FLOAT64)) * 100 AS percentage_change
    FROM `{PROJECT_ID}.{CURRENT_UNIONED_EHR_DATASET_ID}.__TABLES__` o
      LEFT JOIN `{PROJECT_ID}.{PREVIOUS_UNIONED_EHR_DATASET_ID}.__TABLES__` n
        USING (table_id)
    WHERE 1=1
      AND NOT (table_id LIKE '%mapping%' OR table_id LIKE '%achilles%' OR table_id LIKE '%note%')
      AND o.row_count - n.row_count != 0
  )
SELECT
 diffs.*
,SAFE_DIVIDE(10, percentage_change) AS diff_factor
FROM diffs
WHERE 1=1
  AND percentage_change > 25
'''

execute(client, query)
# -

# # Confirm all the expected tables are in this dataset
# This QC confirms all the expected tables are present in this dataset. If not,
# our pipeline might not be working as expected. Missing tables will
# break the combined dataset generation. If this check fails, fix the issues before
# proceeding to the next step.
# See DC-3454 for the background.

expected_domain_tables = [
    table_name for table_name in CDM_TABLES if table_name != DEATH
] + [AOU_DEATH]
expected_mapping_tables = [
    f'{MAPPING_PREFIX}{table_name}' for table_name in tables_to_map()
]
expected_tables = expected_domain_tables + expected_mapping_tables

tpl = JINJA_ENV.from_string('''
WITH expected_tables AS (
{% for table in expected_tables %}
    SELECT '{{table}}' AS table_id 
    {% if not loop.last -%} UNION ALL {% endif %}
{% endfor %}
)
SELECT table_id AS missing_table FROM expected_tables
WHERE table_id NOT IN (SELECT table_id FROM `{{project_id}}.{{dataset}}.__TABLES__`)
''')
query = tpl.render(project_id=PROJECT_ID,
                   dataset=CURRENT_UNIONED_EHR_DATASET_ID,
                   expected_tables=expected_tables)
df = execute(client, query)

# +
success_msg = 'All the expected tables are present in this dataset.'
failure_msg = '''
<b>{code_count}</b> tables are missing. Check if the missing tables are important ones.<br>
If it is NOT important (e.g., expected to be empty), simply create an empty table for that
and move on. Create an investigation ticket so we can investigate it later.<br>
If it is an important table, troubleshoot and figure out why the table is missing in the 
dataset before moving on to the next steps.
'''

render_message(df,
               success_msg,
               failure_msg,
               failure_msg_args={'code_count': len(df)})

# -

# ## Participant counts per hpo_site compared to ehr_ops.

# +
query = f'''
WITH
  previous_count AS (
  SELECT
    src_hpo_id,
    COUNT(src_hpo_id) AS previous_unioned_ct
  FROM
    `{PROJECT_ID}.{PREVIOUS_UNIONED_EHR_DATASET_ID}._mapping_person` mp
  LEFT JOIN
    `{PROJECT_ID}.lookup_tables.hpo_site_id_mappings` hsm
  ON
    mp.src_hpo_id=LOWER(hsm.HPO_ID)
  GROUP BY
    src_hpo_id,
    Display_Order
  ORDER BY
    hsm.Display_Order),
  current_count AS (
  SELECT
    src_hpo_id,
    COUNT(src_hpo_id) AS current_unioned_ct
  FROM
    `{PROJECT_ID}.{CURRENT_UNIONED_EHR_DATASET_ID}._mapping_person`
  GROUP BY
    src_hpo_id)
SELECT
  cc.src_hpo_id,
  coalesce(previous_unioned_ct, 0) as previous_unioned_ct,
  coalesce(current_unioned_ct, 0)  AS current_unioned_ct,
  coalesce(cast((current_unioned_ct - previous_unioned_ct)/ current_unioned_ct * 100 as INT64), 100) as percentage_change 
FROM
  previous_count AS pc
FULL OUTER JOIN
  current_count AS cc
USING(src_hpo_id)
order by percentage_change desc
'''

execute(client, query)
# -

# ## Verify Note text data

# +
query = f'''
SELECT 'note_text' AS field, note_text AS field_value, COUNT(note_text) AS row_count,
FROM `{PROJECT_ID}.{CURRENT_UNIONED_EHR_DATASET_ID}.note`
GROUP BY note_text

UNION ALL

SELECT 'note_title' AS field, note_title AS field_value, COUNT(note_title) AS row_count,
FROM `{PROJECT_ID}.{CURRENT_UNIONED_EHR_DATASET_ID}.note`
GROUP BY note_title

UNION ALL

SELECT 'note_source_value' AS field, note_source_value AS field_value, COUNT(note_source_value) AS row_count,
FROM `{PROJECT_ID}.{CURRENT_UNIONED_EHR_DATASET_ID}.note`
GROUP BY note_source_value
'''

execute(client, query)
# -

# ## Verifying no data past cut-off date

# +
query = f'''
SELECT
  'observation' AS TABLE,
  COUNT(*) AS non_clompling_rows
FROM
  `{PROJECT_ID}.{CURRENT_UNIONED_EHR_DATASET_ID}.observation`
WHERE
  observation_date > DATE('{EHR_CUTOFF_DATE}')
UNION ALL
SELECT
  'measurement' AS TABLE,
  COUNT(*) AS non_clompling_rows
FROM
  `{PROJECT_ID}.{CURRENT_UNIONED_EHR_DATASET_ID}.measurement`
WHERE
  measurement_date > DATE('{EHR_CUTOFF_DATE}')
UNION ALL
SELECT
  'visit_occurrence' AS TABLE,
  COUNT(*) AS non_clompling_rows
FROM
  `{PROJECT_ID}.{CURRENT_UNIONED_EHR_DATASET_ID}.visit_occurrence`
WHERE
  visit_end_date > DATE('{EHR_CUTOFF_DATE}')
UNION ALL
SELECT
  'drug_exposure' AS TABLE,
  COUNT(*) AS non_clompling_rows
FROM
  `{PROJECT_ID}.{CURRENT_UNIONED_EHR_DATASET_ID}.drug_exposure`
WHERE
  drug_exposure_end_date > DATE('{EHR_CUTOFF_DATE}')
UNION ALL
SELECT
  'procedure' AS TABLE,
  COUNT(*) AS non_clompling_rows
FROM
  `{PROJECT_ID}.{CURRENT_UNIONED_EHR_DATASET_ID}.procedure_occurrence`
WHERE
  procedure_date > DATE('{EHR_CUTOFF_DATE}')
  UNION ALL
SELECT
  'visit_detail' AS TABLE,
  COUNT(*) AS non_clompling_rows
FROM
  `{PROJECT_ID}.{CURRENT_UNIONED_EHR_DATASET_ID}.visit_detail`
WHERE
  visit_detail_end_date > DATE('{EHR_CUTOFF_DATE}')
'''

execute(client, query)
# -

# ## Verified duplicates in domain tables.

# +
query = f'''
DECLARE
  omop_tables ARRAY<String>;
DECLARE
  i INT64 DEFAULT 0;
SET
  omop_tables = [ 'care_site', 'condition_occurrence', 'device_exposure', 'drug_exposure', 'location',
  'measurement', 'note', 'observation', 'person', 'procedure_occurrence', 'provider', 'specimen',
  'visit_occurrence']; 
CREATE TEMP TABLE result(table_name STRING,
    id_field_value INT64,
    number_of_duplicated_ids INT64);
LOOP
SET i = i + 1;
IF i > ARRAY_LENGTH(omop_tables) THEN
LEAVE ;
END IF ;
EXECUTE IMMEDIATE
  """
INSERT result
SELECT '""" || omop_tables[ORDINAL(i)] || """' as table_name, 
""" || omop_tables[ORDINAL(i)] ||"""_id as id_field_value, 
COUNT(*) as number_of_duplicated_ids 
FROM `{PROJECT_ID}.{CURRENT_UNIONED_EHR_DATASET_ID}.""" ||omop_tables[ORDINAL(i)] || """`
group by 1, 2
having count(*) > 1
  """;
END LOOP
  ;
SELECT
  table_name,
  number_of_duplicated_ids,
  COUNT(*) AS no_of_records
FROM
  result
GROUP BY
  table_name,
  number_of_duplicated_ids;
'''

execute(client, query)
# -

# ## Verified mapping tables represent actual data

# +
query = f"""
DECLARE omop_tables Array<String>;
DECLARE i INT64 DEFAULT 0;

set omop_tables = [
  'care_site', 'condition_occurrence', 'device_exposure', 'drug_exposure', 
  'location', 'measurement', 'note', 'observation', 'person',
  'procedure_occurrence', 'provider', 'specimen', 'visit_occurrence'];

CREATE TEMP TABLE result(table_name STRING, id_field_value INT64);  

LOOP
  SET i = i + 1;
  IF i > ARRAY_LENGTH(omop_tables) THEN 
    LEAVE; 
  END IF;
  EXECUTE IMMEDIATE '''
INSERT result
SELECT "''' || omop_tables[ORDINAL(i)] || '''" as table_name, 
''' || omop_tables[ORDINAL(i)] ||'''_id as id_field_value, 
FROM `{PROJECT_ID}.{CURRENT_UNIONED_EHR_DATASET_ID}.''' ||omop_tables[ORDINAL(i)] || '''` as table
full outer join `{PROJECT_ID}.{CURRENT_UNIONED_EHR_DATASET_ID}._mapping_''' ||omop_tables[ORDINAL(i)] || '''` as map
using(''' ||omop_tables[ORDINAL(i)] || '''_id)
where map.''' ||omop_tables[ORDINAL(i)] || '''_id is null or table.''' ||omop_tables[ORDINAL(i)] || '''_id is null
  ''';

END LOOP; 

SELECT * FROM result;
"""

execute(client, query)
# -

# # QC for AOU_DEATH table

# From CDR V8, Curation generates the AOU_DEATH table in Unioned_EHR. AOU_DEATH allows more than one death record per participant.
# It has the column `primary_death_record` and it flags the primary records for each participant.
# The logic for deciding which is primary comes from the following business requirements:
# - If multiple death records exist from across sources, provide the first date EHR death record in the death table
# - If death_datetime is not available and multiple death records exist for the same death_date, provide the fullest record in the death table
# - Example: Order by HPO site name and insert the first into the death table
# - Death records from HealthPro can have NULL death_date. Such records must be always `primary_death_record=False`.
#
# This QC confirms that the logic for the primary records are applied as expected in the `AOU_DEATH` table.

# +
query = JINJA_ENV.from_string("""
WITH qc_aou_death AS (
    SELECT 
        aou_death_id, 
        CASE WHEN aou_death_id IN (
            SELECT aou_death_id FROM `{{project_id}}.{{dataset_id}}.aou_death`
            WHERE death_date IS NOT NULL -- NULL death_date records must not become primary --
            QUALIFY RANK() OVER (
                PARTITION BY person_id 
                ORDER BY
                    LOWER(src_id) NOT LIKE '%healthpro%' DESC, -- EHR records are chosen over HealthPro ones --
                    death_date ASC, -- Earliest death_date records are chosen over later ones --
                    death_datetime ASC NULLS LAST, -- Earliest non-NULL death_datetime records are chosen over later or NULL ones --
                    src_id ASC -- EHR site that alphabetically comes first is chosen --
            ) = 1   
        ) THEN TRUE ELSE FALSE END AS primary_death_record
    FROM `{{project}}.{{dataset}}.aou_death`    
)
SELECT ad.aou_death_id
FROM `{{project_id}}.{{dataset}}.aou_death` ad
LEFT JOIN qc_aou_death qad
ON ad.aou_death_id = qad.aou_death_id
WHERE ad.primary_death_record != qad.primary_death_record
""").render(project_id=PROJECT_ID, dataset=CURRENT_UNIONED_EHR_DATASET_ID)
df = execute(client, query)

success_msg = 'All death records have the correct `primary_death_record` values.'
failure_msg = '''
    <b>{code_count}</b> records do not have the correct `primary_death_record` values. 
    Investigate and confirm if (a) any logic is incorrect, (b) the requirement has changed, or (c) something else.
'''
render_message(df,
               success_msg,
               failure_msg,
               failure_msg_args={'code_count': len(df)})
# -

# # QC for DEATH table

# From CDR V8, Unioned_EHR data stage will have all the death records in the AOU_DEATH table. The DEATH table must exist but must have no records.
# This QC confirms that the DEATH table is there and is empty.

# +
query_if_empty = JINJA_ENV.from_string("""
SELECT COUNT(*)
FROM `{{project_id}}.{{dataset}}.death`
HAVING COUNT(*) > 0
""").render(project_id=PROJECT_ID, dataset=CURRENT_UNIONED_EHR_DATASET_ID)
df_if_empty = execute(client, query_if_empty)

success_msg_if_empty = 'Death table is empty.'
failure_msg_if_empty = '''
    Death table is NOT empty. We expect DEATH table to be empty in Unioned_EHR. Investigate why DEATH is not empty and fix it.
'''
render_message(df_if_empty, success_msg_if_empty, failure_msg_if_empty)
# -