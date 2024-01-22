# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.3.0
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

import utils.bq
from notebooks import parameters

bigquery_dataset_id = parameters.SUBMISSION_DATASET_ID
print(bigquery_dataset_id)

# ## Tables with Duplicate IDs

# +
# get list of all hpo_ids
query = """
SELECT REPLACE(table_id, '_person', '') AS hpo_id
FROM `{bq_dataset_id}.__TABLES__`
WHERE table_id LIKE '%person'
AND table_id NOT LIKE '%unioned_ehr_%' AND table_id NOT LIKE '\\\_%'
""".format(bq_dataset_id=bigquery_dataset_id)
hpo_ids = utils.bq.query(query).tolist()

domains = [
    'care_site', 'condition_occurrence', 'device_cost', 'device_exposure',
    'drug_exposure', 'location', 'measurement', 'note', 'observation', 'person',
    'procedure_occurrence', 'provider', 'specimen', 'visit_occurrence'
]

# +
import pandas as pd

subquery = """
SELECT
 '{h}' AS hpo_id,
 table_name,
 num_dups
FROM prod_drc_dataset.__TABLES__ T
LEFT JOIN
(select distinct '{h}_{d}' as table_name, count(*) as num_dups
from `{bq_dataset_id}.{h}_{d}`
group by {d}_id
having count(*) > 1
order by num_dups desc
LIMIT 1)
 ON TRUE
WHERE T.table_id = '{h}_{d}'"""
df = pd.core.frame.DataFrame([])
for hpo_id in hpo_ids:
    subqueries = []
    for d in domains:
        subqueries.append(
            subquery.format(h=hpo_id, d=d, bq_dataset_id=bigquery_dataset_id))
    q = '\n\nUNION ALL\n'.join(subqueries)
    x = utils.bq.query(q)
    df = df.append(x.drop_duplicates())
