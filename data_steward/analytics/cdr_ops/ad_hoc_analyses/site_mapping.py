# +
import utils.bq
from notebooks import render, parameters

COMBINED = parameters.COMBINED_DATASET_ID
DEID = parameters.DEID_DATASET_ID
print("""COMBINED = {COMBINED}
DEID = {DEID}""".format(COMBINED=COMBINED, DEID=DEID))
# -

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
row_counts_df = utils.bq.query(q)
render.dataframe(row_counts_df)

# ## Side by side comparison of row counts

compare_df = row_counts_df.pivot(index='mapped_table',
                                 columns='dataset_id',
                                 values='row_count')
render.dataframe(compare_df)

# ## Row count differences
# The combined mapping tables and deid ext tables are expected to have the same number of rows. Below we find where the row counts differ.

query_str = '{DEID} <> {COMBINED}'.format(COMBINED=COMBINED, DEID=DEID)
diff_row_counts_df = utils.bq.query(query_str)
render.dataframe(diff_row_counts_df)
