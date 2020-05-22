# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
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

from google.cloud import bigquery
# %reload_ext google.cloud.bigquery
client = bigquery.Client()
# %load_ext google.cloud.bigquery

# +
from notebooks import parameters
DATASET = parameters.UNIONED_Q4_2018
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

get_full_names = f"""
select * from {LOOKUP_TABLES}
"""

full_names_df = pd.io.gbq.read_gbq(get_full_names, dialect='standard')

# +
full_names_df.columns = ['org_id', 'src_hpo_id', 'site_name', 'display_order']
columns_to_use = ['src_hpo_id', 'site_name']

full_names_df = full_names_df[columns_to_use]
full_names_df['src_hpo_id'] = full_names_df['src_hpo_id'].str.lower()

# +
cols_to_join = ['src_hpo_id']

site_df = full_names_df
# -

# #  Integration of Routes for Select Drugs:
# #### This is required for all drugs according to the [AoU EHR Operations Page](https://sites.google.com/view/ehrupload/omop-tables/drug_exposure?authuser=0) 

# #### Getting the numbers for all of the route concept IDs by site

# +
route_concept_ids_by_site_query = """
CREATE TABLE `{DATASET}.sites_route_counts`
OPTIONS (
expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 3 MINUTE)
)
AS
SELECT
DISTINCT
mde.src_hpo_id, COUNT(de.drug_exposure_id) as number_total_routes
FROM
`{DATASET}.drug_exposure` de
JOIN
`{DATASET}._mapping_drug_exposure` mde
ON
de.drug_exposure_id = mde.drug_exposure_id
WHERE
LOWER(mde.src_hpo_id) NOT LIKE '%rdr%'
GROUP BY 1
ORDER BY number_total_routes DESC
""".format(DATASET = DATASET)

route_concept_ids_by_site = pd.io.gbq.read_gbq(route_concept_ids_by_site_query, dialect='standard')

# +
route_concept_ids_by_site_query = """
SELECT
*
FROM
`{DATASET}.sites_route_counts`
""".format(DATASET = DATASET)

route_concept_ids_by_site = pd.io.gbq.read_gbq(route_concept_ids_by_site_query, dialect='standard')
# -

route_concept_ids_by_site

# #### Below are the "successful" route concept IDs

# +
successful_route_concept_ids_by_site_query = """
CREATE TABLE `{DATASET}.sites_successful_route_counts`
OPTIONS (
expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 3 MINUTE)
)
AS
SELECT
DISTINCT
mde.src_hpo_id, COUNT(de.drug_exposure_id) as number_valid_routes
FROM
`{DATASET}.drug_exposure` de
JOIN
`{DATASET}._mapping_drug_exposure` mde
ON
de.drug_exposure_id = mde.drug_exposure_id 
JOIN
`{DATASET}.concept` c
ON
de.route_concept_id = c.concept_id
WHERE
c.standard_concept IN ('S')
AND
LOWER(c.domain_id) LIKE '%route%'
AND
LOWER(mde.src_hpo_id) NOT LIKE '%rdr%'
GROUP BY 1
ORDER BY number_valid_routes DESC
""".format(DATASET = DATASET)

successful_route_concept_ids_by_site = pd.io.gbq.read_gbq(successful_route_concept_ids_by_site_query, dialect='standard')

# +
successful_route_concept_ids_by_site_query = """
SELECT
*
FROM
`{DATASET}.sites_successful_route_counts`
""".format(DATASET = DATASET)

successful_route_concept_ids_by_site = pd.io.gbq.read_gbq(successful_route_concept_ids_by_site_query, dialect='standard')
# -

successful_route_concept_ids_by_site

final_all_routes_df = pd.merge(site_df, route_concept_ids_by_site, on = 'src_hpo_id', how = 'left')

final_all_routes_df = pd.merge(final_all_routes_df, successful_route_concept_ids_by_site, on = 'src_hpo_id', how = 'left')

final_all_routes_df['total_route_success_rate'] = round(final_all_routes_df['number_valid_routes'] / final_all_routes_df['number_total_routes'] * 100, 2)

final_all_routes_df = final_all_routes_df.fillna(0)

final_all_routes_df = final_all_routes_df.sort_values(by='total_route_success_rate', ascending = False)

final_all_routes_df

final_all_routes_df.to_csv("{cwd}/drug_routes.csv".format(cwd = cwd))
