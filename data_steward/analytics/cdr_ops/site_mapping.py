# +
import bq
from defaults import DEFAULT_DATASETS
from IPython.display import display, HTML

COMBINED = DEFAULT_DATASETS.latest.combined
DEID = DEFAULT_DATASETS.latest.deid


# -

def display_dataframe(df):
    if len(df) == 0:
        html = HTML('<div class="alert alert-info">There are no records in the dataframe.</div>')
    else:
        html = HTML(df.to_html())
    display(html)


# ## Row counts in combined `_mapping*` and deid `*_ext` tables

ROW_COUNTS_QUERY = """
SELECT dataset_id, 
  REPLACE(REPLACE(table_id, '_mapping_', ''), '_ext', '') mapped_table, 
  table_id, 
  creation_time, 
  last_modified_time,
  row_count
FROM
(SELECT *
 FROM {DEID}.__TABLES__
 WHERE table_id LIKE '%\\\_ext'

 UNION ALL

 SELECT * 
 FROM {COMBINED}.__TABLES__ d1
 WHERE table_id LIKE '\\\_mapping\\\_%')

ORDER BY REPLACE(REPLACE(table_id, '_mapping_', ''), '_ext', ''), dataset_id
"""
q = ROW_COUNTS_QUERY.format(COMBINED=COMBINED, DEID=DEID)
row_counts_df = bq.query(q)
display_dataframe(row_counts_df)

# ## Side by side comparison of row counts

compare_df = row_counts_df.pivot(index='mapped_table', columns='dataset_id', values='row_count')
display_dataframe(compare_df)

# ## Row count differences
# The combined mapping tables and deid ext tables are expected to have the same number of rows. Below we find where the row counts differ.

query_str = '{DEID} <> {COMBINED}'.format(COMBINED=COMBINED, DEID=DEID)
diff_row_counts_df = compare_df.query(query_str)
display_dataframe(diff_row_counts_df)
