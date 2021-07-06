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

# ### This notebook is designed to output the number of ACHILLES errors for each of the sites. The number of ACHILLES errors is triaged into two categories:
# - Number of distinct ACHILLES errors
# - Number of TOTAL ACHILLES errors (meaning the same 'issue' can be reported twice)

from google.cloud import bigquery
# %reload_ext google.cloud.bigquery
client = bigquery.Client()
# %load_ext google.cloud.bigquery

# +
from notebooks import parameters
DATASET = parameters.LATEST_DATASET
LOOKUP_TABLES = parameters.LOOKUP_TABLES

print(f"Dataset to use: {DATASET}")
print(f"Lookup tables: {LOOKUP_TABLES}")

# +
import warnings

warnings.filterwarnings('ignore')
import pandas as pd
import matplotlib.pyplot as plt
import os

plt.style.use('ggplot')
pd.options.display.max_rows = 999
pd.options.display.max_columns = 999
pd.options.display.max_colwidth = 999


def cstr(s, color='black'):
    return "<text style=color:{}>{}</text>".format(color, s)


# -

cwd = os.getcwd()
cwd = str(cwd)
print("Current working directory is: {cwd}".format(cwd=cwd))

# ### Get the list of HPO IDs
#
# ### NOTE: This assumes that all of the relevant HPOs have a person table.

hpo_id_query = f"""
SELECT REPLACE(table_id, '_person', '') AS src_hpo_id
FROM
`{DATASET}.__TABLES__`
WHERE table_id LIKE '%person' 
AND table_id 
NOT LIKE '%unioned_ehr_%' 
AND table_id NOT LIKE '\\\_%'
"""

site_df = pd.io.gbq.read_gbq(hpo_id_query, dialect='standard')

get_full_names = f"""
select * from {LOOKUP_TABLES}.hpo_site_id_mappings
"""

full_names_df = pd.io.gbq.read_gbq(get_full_names, dialect='standard')

# +
full_names_df.columns = ['org_id', 'src_hpo_id', 'site_name', 'display_order']
columns_to_use = ['src_hpo_id', 'site_name']

full_names_df = full_names_df[columns_to_use]
full_names_df['src_hpo_id'] = full_names_df['src_hpo_id'].str.lower()

# +
cols_to_join = ['src_hpo_id']

site_df = pd.merge(site_df, full_names_df, on=['src_hpo_id'], how='left')
# -

site_df

# # Note!!! 
# Sites that have yet made a submission should be suppressed
#
# ## do check again every time before run

site_df = site_df[site_df["src_hpo_id"] != "illinois_near_north"]
site_df

# ### NOTE: This next section looks at distinct achilles_heel_warnings - NOT just ACHILLES Heel IDs. This means that NOTIFICATIONS (those that are not logged with an analysis_id) are also included in the count.

hpo_ids = site_df['src_hpo_id'].tolist()

# +
subqueries = []

subquery = """
SELECT
'{src_hpo_id}' AS src_hpo_id,
COUNT(DISTINCT ahr.achilles_heel_warning) as num_distinct_warnings
FROM
`{DATASET}.{src_hpo_id}_achilles_heel_results` ahr
"""

for src_hpo_id in hpo_ids:
    subqueries.append(subquery.format(DATASET=DATASET, src_hpo_id=src_hpo_id))
    
final_query = '\n\nUNION ALL\n'.join(subqueries)
# -

dataframe_distinct_ahes = """
WITH num_distinct_ahes AS
({final_query})
SELECT
DISTINCT
*
FROM
num_distinct_ahes
ORDER BY num_distinct_warnings DESC
""".format(final_query=final_query)

distinct_ahes_df = pd.io.gbq.read_gbq(dataframe_distinct_ahes, dialect='standard')

distinct_ahes_df

# ### NOTE: This next section looks at distinct achilles_heel_ids. NOTIFICATIONS and WARNINGS are NOT included in this count.

# +
subqueries = []

subquery = """
SELECT
'{hpo_id}' AS src_hpo_id,
COUNT(DISTINCT ahr.analysis_id) as num_distinct_ids
FROM
`{DATASET}.{hpo_id}_achilles_heel_results` ahr
WHERE ahr.achilles_heel_warning LIKE "ERROR%"
"""

for hpo_id in hpo_ids:
    subqueries.append(subquery.format(DATASET=DATASET, hpo_id=hpo_id))

final_query = '\n\nUNION ALL\n'.join(subqueries)
# -

dataframe_distinct_ids = """
WITH num_distinct_ids AS
({final_query})
SELECT
DISTINCT
*
FROM
num_distinct_ids
ORDER BY num_distinct_ids DESC
""".format(final_query=final_query)

dataframe_distinct_ids = pd.io.gbq.read_gbq(dataframe_distinct_ids, dialect='standard')

dataframe_distinct_ids

# ### NOTE: This next section looks at distinct achilles_heel_ids. NOTIFICATIONS are NOT included in this count. This section, however, looks at the TOTAL NUMBER OF AFFECTED ROWS.

# +
subqueries = []

subquery = """
SELECT
'{hpo_id}' AS src_hpo_id,
SUM(ahr.record_count) as rows_with_ah_failure
FROM
`{DATASET}.{hpo_id}_achilles_heel_results` ahr
"""

for hpo_id in hpo_ids:
    subqueries.append(subquery.format(DATASET=DATASET, hpo_id=hpo_id))

final_query = '\n\nUNION ALL\n'.join(subqueries)
# -

dataframe_ahe_failure_count = """
WITH num_rows_with_ah_failure AS
({final_query})
SELECT
DISTINCT
*
FROM
num_rows_with_ah_failure
ORDER BY rows_with_ah_failure DESC
""".format(final_query=final_query)

ahe_id_failure_counts = pd.io.gbq.read_gbq(dataframe_ahe_failure_count, dialect='standard')

ahe_id_failure_counts

final_df = pd.merge(dataframe_distinct_ids, distinct_ahes_df, how='outer', on='src_hpo_id')

final_df = pd.merge(final_df, ahe_id_failure_counts, how='outer', on='src_hpo_id')

final_df.fillna(0)

final_df

final_df.to_csv("{cwd}/achilles_errors.csv".format(cwd = cwd))


