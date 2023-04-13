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

#
# # Purpose:
# This notebook is used to validate new uploads of the OMOP vocabularys prior to implementing in production.<br>
#
#
#  **Link to get the latest custom concepts [HERE](https://github.com/all-of-us/curation/blame/develop/data_steward/resource_files/aou_vocab/CONCEPT.csv).**
#

# # + Import packages
from common import JINJA_ENV
from utils import auth
from gcloud.bq import BigQueryClient
from analytics.cdr_ops.notebook_utils import execute, IMPERSONATION_SCOPES
import pandas as pd

# + tags=["parameters"]
project_id = ''
old_vocabulary = ''
new_vocabulary = ''
run_as = ''
# -

# These are the AoU_Custom and AoU_General concepts. Look for added concepts in the aou_vocab/CONCEPT.csv. Link Above. 
custom_concepts = [2000000000,2000000001,2000000002,2000000003,2000000004,2000000005,
                   2000000006,2000000007,2000000008,2000000009,2000000010,2000000011,
                   2000000012,2000000013,2100000000,2100000001,2100000002,2100000003,
                   2100000004,2100000005,2100000006,2100000007]

vocabulary_dataset_old = f'{project_id}.{old_vocabulary}'
vocabulary_dataset_new = f'{project_id}.{new_vocabulary}'

impersonation_creds = auth.get_impersonation_credentials(
    run_as, target_scopes=IMPERSONATION_SCOPES)

client = BigQueryClient(project_id, credentials=impersonation_creds)

pd.set_option('max_colwidth', None)

# # Validate PPI

# ## Review the new PPI concepts
# The dataframe will contain all new PPI concepts. <br>
# Generally there will be additions to the PPI vocabulary between uploads, but it is also possible that the update contains changes only to the EHR vocabularies.<br>
#
# > - **If the dataframe is not empty** - Manually review the new concepts cross referencing the vocabulary upload Jira ticket which should include the expected PPI updates to look for. If the ticket does not have this information contact the ticket reporter. <br>
# > - **If the dataframe is empty** - There are no new PPI concepts. Verify this by referencing the 'vocabulary upload' Jira ticket which should include the expected PPI updates. If the ticket does not have this information contact the ticket reporter. <br>

tpl = JINJA_ENV.from_string('''
SELECT
c.concept_code 
FROM `{{vocabulary_dataset_new}}.concept` c
WHERE c.concept_code NOT IN (SELECT concept_code 
                            FROM `{{vocabulary_dataset_old}}.concept` sq 
                            WHERE sq.vocabulary_id LIKE "PPI" )
AND c.vocabulary_id LIKE "PPI"
ORDER BY c.concept_code
''')
query = tpl.render(vocabulary_dataset_old=vocabulary_dataset_old,
                   vocabulary_dataset_new=vocabulary_dataset_new)
execute(client, query, max_rows=True)

# # Review the retired or removed concepts 
# The dataframe will contain all PPI concepts that were newly deprecated or removed from Athena. <br>
# Generally none would be deprecated or removed. <br>
#
# **If the dataframe is not empty** - Manually review the retired or removed concepts, cross referencing the vocabulary upload Jira ticket which should include the expected PPI updates to look for. If the ticket does not have this information contact the ticket reporter. <br>
# **If the dataframe is empty** - There are no deprecated or removed PPI concepts. Verify this by referencing the 'vocabulary upload' Jira ticket which should include the expected PPI updates. If the ticket does not have this information contact the ticket reporter. <br>

tpl = JINJA_ENV.from_string('''
SELECT
c.concept_code,
'removed' AS status
FROM `{{vocabulary_dataset_old}}.concept` c
WHERE c.concept_code NOT IN (SELECT concept_code 
                            FROM `{{vocabulary_dataset_new}}.concept`  
                            WHERE vocabulary_id LIKE "PPI")
AND c.vocabulary_id LIKE "PPI"

UNION ALL

SELECT
c.concept_code,
'recently_deprecated' as status
FROM `{{vocabulary_dataset_new}}.concept` c
WHERE c.concept_code NOT IN (SELECT concept_code 
                            FROM `{{vocabulary_dataset_old}}.concept` 
                            WHERE vocabulary_id LIKE "PPI"
                            AND c.valid_end_date < CURRENT_DATE())
AND c.vocabulary_id LIKE "PPI"
AND c.valid_end_date < CURRENT_DATE()
''')
query = tpl.render(vocabulary_dataset_old=vocabulary_dataset_old,
                   vocabulary_dataset_new=vocabulary_dataset_new)
execute(client, query, max_rows=True)

# # Verify the updates made to the vocabulary in the upload process

# ## All AoU_Custom and AoU_General concepts are present in the new vocabulary
# In the process of updating the vocabulary custom concepts are added to the vocabulary. <br>
# The list of concepts should have been updated at the start of this notebook.<br>
#
# If the check fails, investigate. It is important that all of the custom concepts are added to the vocabulary.

# +
tpl = JINJA_ENV.from_string('''
SELECT
COUNT(concept_code) as total
FROM `{{vocabulary_dataset_new}}.concept` c
WHERE vocabulary_id in ('AoU_Custom', 'AoU_General')
OR concept_code IN ('AOU generated') 
''')
query = tpl.render(vocabulary_dataset_old=vocabulary_dataset_old,
                   vocabulary_dataset_new=vocabulary_dataset_new,
                  custom_concepts=custom_concepts)
df = execute(client, query, max_rows=True)

display(df)
print('This check passes if the total is ' + str(len(custom_concepts)))
# -

# # Vocabulary Summary Queries

# ## Tables row count comparison
#
# > - The difference between the number of rows in each table of the datasets.  <br>
# > - **Generally, all 'changes' should increase**, but with the occasional edge case. <br>
# > - **Investigate any negative values**, the release notes are a great starting reference. <br>
# > - [Release notes](https://github.com/OHDSI/Vocabulary-v5.0/releases).   <br>

tpl = JINJA_ENV.from_string('''
WITH new_table_info AS (
   SELECT dataset_id, table_id, row_count
   FROM `{{vocabulary_dataset_new}}.__TABLES__`
),
old_table_info AS (
   SELECT dataset_id, table_id, row_count
   FROM `{{vocabulary_dataset_old}}.__TABLES__`
)
SELECT
n.table_id,
o.row_count AS old_count,
n.row_count AS new_count,
(n.row_count - o.row_count) AS changes
FROM new_table_info AS n
LEFT join old_table_info AS o
USING (table_id)
ORDER BY table_id
''')
query = tpl.render(vocabulary_dataset_old=vocabulary_dataset_old,
                   vocabulary_dataset_new=vocabulary_dataset_new)
execute(client, query, max_rows=True)

# # Vocabulary_id comparison
# > - The table will show the vocabulary_ids that exist in either the new or old datasets but not both. <br>
# > - Generally, the same vocabularies should exist in each dataset, so the table below should be empty. <br>
#
# **If the dataframe is empty** and a new vocabulary was not expected. This check passes.
#
# **If the dataframe is not empty** a vocabulary was either added or removed since the last upload. If this is not expected, investigate. [release notes](https://github.com/OHDSI/Vocabulary-v5.0/releases).  

tpl = JINJA_ENV.from_string('''
WITH new_table_info AS (
   SELECT COUNT(vocabulary_id) AS new_concept_count, vocabulary_id
   FROM `{{vocabulary_dataset_new}}.concept`
   GROUP BY vocabulary_id
),
old_table_info AS (
   SELECT COUNT(vocabulary_id) AS old_concept_count, vocabulary_id
   FROM `{{vocabulary_dataset_old}}.concept`
   GROUP BY vocabulary_id
)
SELECT vocabulary_id,old_concept_count,new_concept_count, new_concept_count - old_concept_count AS diff
FROM new_table_info
FULL OUTER JOIN old_table_info
USING (vocabulary_id)
WHERE new_concept_count IS NULL
OR old_concept_count IS NULL
ORDER BY vocabulary_id
''')
query = tpl.render(vocabulary_dataset_old=vocabulary_dataset_old,
                   vocabulary_dataset_new=vocabulary_dataset_new)
execute(client, query, max_rows=True)

# # Row count comparison by vocabulary_id
# > - Shows the number of individual concepts added or removed from each vocabulary.<br>
# > - **Generally the count should only increase** (when it does change).<br>
# > - Investigate any negative differences.<br>
# > - Changes to AoU_Custom and AoU_General can be validated by looking for Jira issues that affect the<br>
#     data_steward/resource_files/aou_vocab/CONCEPT.csv file.<br>

tpl = JINJA_ENV.from_string('''
WITH new_table_info AS (
   SELECT COUNT(vocabulary_id) AS new_concept_count, vocabulary_id
   FROM `{{vocabulary_dataset_new}}.concept`
   GROUP BY vocabulary_id
),
old_table_info AS (
   SELECT COUNT(vocabulary_id) AS old_concept_count, vocabulary_id
   FROM `{{vocabulary_dataset_old}}.concept`
   GROUP BY vocabulary_id
)
SELECT vocabulary_id,old_concept_count,new_concept_count, new_concept_count - old_concept_count AS diff
FROM new_table_info
FULL OUTER JOIN old_table_info
USING (vocabulary_id)
ORDER BY vocabulary_id
''')
query = tpl.render(vocabulary_dataset_old=vocabulary_dataset_old,
                   vocabulary_dataset_new=vocabulary_dataset_new)
execute(client, query, max_rows=True)

# # Access to the new vocabulary
# > - This query shows the users and the roles who have access to the new vocabulary.
# > - The project_id should be prod here.
# > - Confirm that the PDR service account has BigQuery Data Viewer access to it.
# > - If no access, re-run the step in the [vocabulary playbook](https://docs.google.com/document/d/1U8AIunEVRdJOUGgYJASKcTGkzPDg5MVU_hiIKDwmipk/edit#). <br>

tpl = JINJA_ENV.from_string('''
select * from `{{project_id}}.region-us.INFORMATION_SCHEMA.OBJECT_PRIVILEGES`
where object_name = '{{new_vocabulary}}'
order by grantee
''')
query = tpl.render(project_id=project_id, new_vocabulary=new_vocabulary)
execute(client, query)
