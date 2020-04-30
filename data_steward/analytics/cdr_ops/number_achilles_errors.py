# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.4.2
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

print("Dataset to use: {DATASET}".format(DATASET = DATASET))

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

# ### Get the list of HPO IDs

hpo_id_query = """
SELECT REPLACE(table_id, '_person', '') AS hpo_id
FROM
`{DATASET}.__TABLES__`
WHERE table_id LIKE '%person' 
AND table_id 
NOT LIKE '%unioned_ehr_%' 
AND table_id NOT LIKE '\\\_%'
""".format(DATASET=DATASET)

hpo_ids = pd.io.gbq.read_gbq(hpo_id_query, dialect='standard').hpo_id.tolist()

# ### NOTE: This next section looks at distinct achilles_heel_warnings - NOT just ACHILLES Heel IDs. This means that NOTIFICATIONS (those that are not logged with an analysis_id) are also included in the count.

# +
subqueries = []

subquery = """
SELECT
'{hpo_id}' AS hpo_id,
COUNT(DISTINCT ahr.achilles_heel_warning) as num_distinct_warnings
FROM
`{DATASET}.{hpo_id}_achilles_heel_results` ahr
"""

for hpo_id in hpo_ids:
    subqueries.append(subquery.format(DATASET=DATASET, hpo_id=hpo_id))
    
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

# ### NOTE: This next section looks at distinct achilles_heel_ids. NOTIFICATIONS are NOT included in this count.

# +
subqueries = []

subquery = """
SELECT
'{hpo_id}' AS hpo_id,
COUNT(DISTINCT ahr.analysis_id) as num_distinct_id
FROM
`{DATASET}.{hpo_id}_achilles_heel_results` ahr
"""

for hpo_id in hpo_ids:
    subqueries.append(subquery.format(DATASET=DATASET, hpo_id=hpo_id))
    
final_query = '\n\nUNION ALL\n'.join(subqueries)
# -

dataframe_distinct_ahes = """
WITH num_distinct_ids AS
({final_query})

SELECT
DISTINCT
*
FROM
num_distinct_ids
ORDER BY num_distinct_ids DESC
""".format(final_query=final_query)

distinct_ahe_ids = pd.io.gbq.read_gbq(dataframe_distinct_ahes, dialect='standard')

distinct_ahe_ids
