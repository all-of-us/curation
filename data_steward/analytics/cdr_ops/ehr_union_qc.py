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

# +
import pandas as pd

from utils.bq import get_client
from analytics.cdr_ops.notebook_utils import execute
# -

client = get_client(PROJECT_ID)

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
query = f'''select * from 
(SELECT 
 COALESCE(t0.table_id ,t1.table_id) AS table_id
,t0.row_count AS _{PREVIOUS_UNIONED_EHR_DATASET_ID}
,t1.row_count AS _{CURRENT_UNIONED_EHR_DATASET_ID}
,t1.row_count-t0.row_count AS row_diff 
FROM `{CURRENT_UNIONED_EHR_DATASET_ID}.__TABLES__` t1 
 FULL OUTER JOIN `{PREVIOUS_UNIONED_EHR_DATASET_ID}.__TABLES__` t0 
  USING (table_id) 
ORDER BY ABS(t1.row_count-t0.row_count) DESC)
where _{PREVIOUS_UNIONED_EHR_DATASET_ID} <>0 and _{CURRENT_UNIONED_EHR_DATASET_ID} <>0'''

execute(client, query)
# -

# ## Participant counts per hpo_site compared to ehr_ops.

# +
query = f'''WITH
  subs_count AS (
  SELECT
    src_hpo_id,
    COUNT(src_hpo_id) AS submission_ct
  FROM
    `{PROJECT_ID}.ehr_ops._mapping_person` mp
  JOIN
    `{PROJECT_ID}.lookup_tables.hpo_site_id_mappings` hsm
  ON
    mp.src_hpo_id=LOWER(hsm.HPO_ID)
  GROUP BY
    src_hpo_id,
    Display_Order
  ORDER BY
    hsm.Display_Order),
  unioned_count AS (
  SELECT
    src_hpo_id,
    COUNT(src_hpo_id) AS unioned_ct
  FROM
    `{PROJECT_ID}.{CURRENT_UNIONED_EHR_DATASET_ID}._mapping_person`
  GROUP BY
    src_hpo_id)
SELECT
  sc.src_hpo_id,
  submission_ct,
  CAST(unioned_ct AS INT64)  AS unioned_ct
FROM
  subs_count AS sc
FULL OUTER JOIN
  unioned_count AS uc
USING(src_hpo_id)
order by unioned_ct desc'''

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






