# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.3'
#       jupytext_version: 0.8.3
#   kernelspec:
#     display_name: Python 2
#     language: python
#     name: python2
#   language_info:
#     codemirror_mode:
#       name: ipython
#       version: 2
#     file_extension: .py
#     mimetype: text/x-python
#     name: python
#     nbconvert_exporter: python
#     pygments_lexer: ipython2
#     version: 2.7.14
# ---

# %matplotlib inline
import google.datalab.bigquery as bq
import matplotlib.pyplot as plt


# ## Tables with Duplicate IDs

# +
# get list of all hpo_ids
hpo_ids = bq.Query("""
SELECT REPLACE(table_id, '_person', '') AS hpo_id
FROM `aou-res-curation-prod.prod_drc_dataset.__TABLES__`
WHERE table_id LIKE '%person' 
AND table_id NOT LIKE '%unioned_ehr_%'
""").execute(output_options=bq.QueryOutput.dataframe(use_cache=False)).result().hpo_id.tolist()

domains = ['care_site', 'condition_occurrence', 'device_cost', 'device_exposure', 'drug_exposure', 'location', 'measurement', 'note', 'observation', 'person', 'procedure_occurrence', 'provider', 'specimen', 'visit_occurrence']

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
from `aou-res-curation-prod.prod_drc_dataset.{h}_{d}` 
group by {d}_id
having count(*) > 1
order by num_dups desc
LIMIT 1)
 ON TRUE
WHERE T.table_id = '{h}_{d}'"""
df = pd.core.frame.DataFrame([])
# i = 0 
for hpo_id in hpo_ids:
    subqueries = []
#     print(hpo_id)
    for d in domains:
        subqueries.append(subquery.format(h=hpo_id, d=d))

    q = '\n\nUNION ALL\n'.join(subqueries)

    x = bq.Query(q).execute(output_options=bq.QueryOutput.dataframe(use_cache=False)).result()
    df = df.append(x.drop_duplicates())

# print(df.drop_duplicates())


# +
z = df.drop_duplicates()
z = z[z['table_name'].notnull()]
z


