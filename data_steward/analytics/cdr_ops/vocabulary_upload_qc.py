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

# # QC of a new vocabulary downloaded from Athena and imported into BigQuery, via the vocabulary playbook.
#

# # + Import packages
import pandas as pd
import numpy as np
from datetime import date
from google.cloud import bigquery
# -

# # + tags=["parameters"]
project_id=''
old_vocabulary=''
new_vocabulary=''
# -

vocabulary_dataset_new = f'{project_id}.{old_vocabulary}'
vocabulary_dataset_old = f'{project_id}.{new_vocabulary}'


# +
# Allow sql queries from bq

CLIENT = bigquery.Client(project=project_id)

def execute(query, print_query=True, **kwargs):
    """
    Execute a bigquery command and return the results in a dataframe
    
    :param query: the query to execute
    """
    if print_query:
        df = CLIENT.query(query, **kwargs).to_dataframe()
        df_fmt = dict((col, athena_link) for col in df.columns.to_list() if col.endswith('concept_id'))
    return df


# -

# # New PPI. Where ppi concepts are not present in the old vocabulary.

execute(f'''
SELECT
c.concept_code 
FROM `{vocabulary_dataset_new}.concept` c
WHERE c.concept_code NOT IN (SELECT concept_code 
                            FROM `{vocabulary_dataset_old}.concept` sq 
                            WHERE sq.vocabulary_id LIKE "PPI" )
AND c.vocabulary_id LIKE "PPI"
ORDER BY c.concept_code
''')


# # Removed PPI. Where ppi concepts are not present in the new vocabulary

execute(f'''
SELECT
c.concept_code 
FROM `{vocabulary_dataset_old}.concept` c
WHERE c.concept_code NOT IN (SELECT concept_code 
                            FROM `{vocabulary_dataset_new}.concept` sq 
                            WHERE sq.vocabulary_id LIKE "PPI" )
 AND c.vocabulary_id LIKE "PPI"
 ORDER BY c.concept_code
''')


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

# # The difference between the number of rows in each table of the dataset. All 'changes' should increase. https://github.com/OHDSI/Vocabulary-v5.0/releases

execute(f'''
WITH new_table_info AS (
   SELECT dataset_id, table_id, row_count
   FROM `{vocabulary_dataset_new}.__TABLES__`
),
old_table_info AS (
   SELECT dataset_id, table_id, row_count
   FROM `{vocabulary_dataset_old}.__TABLES__`
)
SELECT
n.table_id,
n.row_count AS new_count,
o.row_count AS old_count,
(n.row_count - o.row_count) AS changes
FROM new_table_info AS n
LEFT join old_table_info AS o
USING (table_id)
ORDER BY table_id
''')

# # The vocabularies that exist, in each vocab version. Check that none are lost, or any are added. Use the release notes if there is a change.

vocab_exists = execute(f'''
WITH new_table_info AS (
   SELECT vocabulary_id , count(vocabulary_id) AS c
   FROM `{vocabulary_dataset_new}.vocabulary`
   GROUP BY vocabulary_id
),
old_table_info AS (
   SELECT vocabulary_id, count(vocabulary_id) AS c
   FROM `{vocabulary_dataset_old}.vocabulary`
   GROUP BY vocabulary_id
)

SELECT
n.vocabulary_id, n.c AS new_count, o.c AS old_count , o.vocabulary_id
FROM new_table_info AS n
FULL OUTER JOIN old_table_info AS o
USING (vocabulary_id)
ORDER BY n.vocabulary_id
''')

# +
# Shows the number of individual concepts added to each vocab type.The 'diff' should always increase.
# -

vocab_counts = execute(f'''
WITH new_table_info AS (
   SELECT COUNT(vocabulary_id) AS new_concept_count, vocabulary_id
   FROM `{vocabulary_dataset_new}.concept`
   GROUP BY vocabulary_id
),
old_table_info AS (
   SELECT COUNT(vocabulary_id) AS old_concept_count, vocabulary_id
   FROM `{vocabulary_dataset_old}.concept`
   GROUP BY vocabulary_id
)
SELECT vocabulary_id,old_concept_count,new_concept_count, new_concept_count - old_concept_count AS diff
FROM new_table_info
FULL OUTER JOIN old_table_info
USING (vocabulary_id)
ORDER BY vocabulary_id
''')

