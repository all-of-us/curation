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

#import pymysql
import pandas as pd
import numpy as np
from datetime import date
#from helpers import *

VOCABULARY_DATASET_NEW = f'{CURATION_PROJECT_ID}.vocabulary20211116'
VOCABULARY_DATASET_OLD = f'{CURATION_PROJECT_ID}.vocabulary20210601'

one=execute(f'''
WITH 
 vocab_new AS 
  (SELECT * 
  FROM `{VOCABULARY_DATASET_NEW}.__TABLES__`)
 ,vocab_old AS 
( SELECT * 
  FROM `{VOCABULARY_DATASET_OLD}.__TABLES__` )
  
SELECT * FROM
vocab_new FULL OUTER JOIN vocab_old USING (table_id)
''')
# one

# +

old=execute(f'''
SELECT
concept_code 
FROM `{VOCABULARY_DATASET_OLD}.concept` v1
WHERE vocabulary_id LIKE 'PPI'
''')
print(len(old))
# old

# +

only_new_ppi=execute(f'''
SELECT
v0.concept_code 
FROM `{VOCABULARY_DATASET_NEW}.concept` v0
WHERE v0.concept_code NOT IN (SELECT concept_code FROM `{VOCABULARY_DATASET_OLD}.concept` AS vw WHERE vw.vocabulary_id LIKE "PPI" )
 AND v0.vocabulary_id LIKE "PPI"
''')

# -

new_ppi_all=execute(f'''
SELECT
concept_code 
FROM `{VOCABULARY_DATASET_NEW}.concept` v0
WHERE vocabulary_id LIKE 'PPI'
''')


#bq_output=pd.DataFrame(bq_output)
print(len(new_ppi_all))
path='data_storage/new_bq_ppi'+ str(date.today()) + '.csv'
new_ppi_all.to_csv(path,index=False)

row_counts = execute(f'''
with new_table_info as (
   select dataset_id, table_id, row_count
   from `{VOCABULARY_DATASET_NEW}.__TABLES__`
),
old_table_info as (
   select dataset_id, table_id, row_count
   from `{VOCABULARY_DATASET_OLD}.__TABLES__`
)
select
n.table_id,
n.row_count as new_count,
o.row_count as old_count,
(n.row_count - o.row_count) as changes
from new_table_info as n
left join old_table_info as o
using (table_id)
order by table_id
''')

ppi_check = execute(f'''
with new_table_info as (
   select vocabulary_id , count(vocabulary_id) as c
   from `{VOCABULARY_DATASET_NEW}.vocabulary`
   group by vocabulary_id
),
old_table_info as (
   select vocabulary_id, count(vocabulary_id) as c
   from `{VOCABULARY_DATASET_OLD}.vocabulary`
   group by vocabulary_id
)

select
n.vocabulary_id, n.c, o.c , o.vocabulary_id
from new_table_info as n
full outer join old_table_info as o
using (vocabulary_id)
''')

vocab_exists = execute(f'''
with new_table_info as (
   select vocabulary_id , count(vocabulary_id) as c
   from `{VOCABULARY_DATASET_NEW}.vocabulary`
   group by vocabulary_id
),
old_table_info as (
   select vocabulary_id, count(vocabulary_id) as c
   from `{VOCABULARY_DATASET_OLD}.vocabulary`
   group by vocabulary_id
)

select
n.vocabulary_id, n.c, o.c , o.vocabulary_id
from new_table_info as n
full outer join old_table_info as o
using (vocabulary_id)
''')

vocab_counts = execute(f'''
with new_table_info as (
   select count(vocabulary_id) as new_concept_count, vocabulary_id
   from `{VOCABULARY_DATASET_NEW}.concept`
   group by vocabulary_id
),
old_table_info as (
   select count(vocabulary_id) as old_concept_count, vocabulary_id
   from `{VOCABULARY_DATASET_OLD}.concept`
   group by vocabulary_id
)
select vocabulary_id,new_concept_count,old_concept_count, new_concept_count - old_concept_count as diff
from new_table_info
full outer join old_table_info
using (vocabulary_id)
''')


