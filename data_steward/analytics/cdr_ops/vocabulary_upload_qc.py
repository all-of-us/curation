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
project_id = ''
old_vocabulary = ''
new_vocabulary = ''
# -

#
# # Description:
#
# > - These queries will ensure the new vocabulary is reliable prior to implementing in production. <br>
#

# # + Import packages
from common import JINJA_ENV
from gcloud.bq import BigQueryClient
from analytics.cdr_ops.notebook_utils import execute
import pandas as pd
# -

vocabulary_dataset_old = f'{project_id}.{old_vocabulary}'
vocabulary_dataset_new = f'{project_id}.{new_vocabulary}'

client = BigQueryClient(project_id)

pd.set_option('max_colwidth', None)

#
# # PPI concept comparison, new concepts
#
# > - The dataframe will contain all new PPI concepts. <br>
# > - Generally there will be additions to the PPI vocabulary between uploads, but not always.<br>
# > - It is also possible that the update contains changes only to the EHR vocabularies.<br>
# > - If the dataframe is empty, get confirmation from the survey team that no new PPI concepts are expected.<br>

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

#
# # PPI concept comparison, retired or removed concepts 
#
# > - The dataframe will contain all PPI concepts that were newly deprecated or removed from Athena. <br>
# > - Generally none would be deprecated or removed. <br>
# > - If the dataframe is not empty contact the survey team to inquire about the concepts.<br>

tpl = JINJA_ENV.from_string('''
SELECT
c.concept_code 
FROM `{{vocabulary_dataset_old}}.concept` c
WHERE c.concept_code NOT IN (SELECT concept_code 
                            FROM `{{vocabulary_dataset_new}}.concept`  
                            WHERE vocabulary_id LIKE "PPI")
AND c.vocabulary_id LIKE "PPI"

UNION ALL

SELECT
c.concept_code
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

# +
# How has relationship changed?

# changed_ppi_relationships=execute(f'''
# SELECT SUM(CASE WHEN r1.is_hierarchical  != r2.is_hierarchical THEN 1 ELSE 0 END) change_hierarchy,
#        SUM(CASE WHEN r1.defines_ancestry != r2.defines_ancestry THEN 1 ELSE 0 END) change_ancestry,
#        SUM(CASE WHEN r1.reverse_relationship_id != r2.reverse_relationship_id THEN 1 ELSE 0 END) change_reverse_rel,
#        SUM(CASE WHEN r1.relationship_concept_id != r2.relationship_concept_id THEN 1 ELSE 0 END) change_rel_concept
# FROM `{vocabulary_dataset_old}.relationship` r1
# JOIN `{vocabulary_dataset_new}.relationship` r2 ON r1.relationship_id = r2.relationship_id
# JOIN `{vocabulary_dataset_new}.concept` c ON r1.relationship_concept_id = c.concept_id
# WHERE c.vocabulary_id LIKE "PPI"
# ''')
# -

#
# # Tables row count comparison
#
# > - The difference between the number of rows in each table of the datasets.  <br>
# > - Generally, all 'changes' should increase, but with the occasional edge case. <br>
# > - Investigate any negative values, the release notes are a great starting reference. <br>
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
# > - Investigate if any are missing or added. [release notes](https://github.com/OHDSI/Vocabulary-v5.0/releases).  <br>

tpl = JINJA_ENV.from_string('''
WITH new_table_info AS (
   SELECT vocabulary_id , count(vocabulary_id) AS c
   FROM `{{vocabulary_dataset_new}}.vocabulary`
   GROUP BY vocabulary_id
),
old_table_info AS (
   SELECT vocabulary_id, count(vocabulary_id) AS c
   FROM `{{vocabulary_dataset_old}}.vocabulary`
   GROUP BY vocabulary_id
)

SELECT
n.vocabulary_id, n.c AS new_count, o.c AS old_count , o.vocabulary_id
FROM new_table_info AS n
FULL OUTER JOIN old_table_info AS o
USING (vocabulary_id)
WHERE n.c != o.c
ORDER BY n.vocabulary_id
''')
query = tpl.render(vocabulary_dataset_old=vocabulary_dataset_old,
                   vocabulary_dataset_new=vocabulary_dataset_new)
execute(client, query, max_rows=True)

# # Row count comparison by vocabulary_id
# > - Shows the number of individual concepts added or removed from each vocabulary.<br>
# > - Generally the count should only increase (when it does change).<br>
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


