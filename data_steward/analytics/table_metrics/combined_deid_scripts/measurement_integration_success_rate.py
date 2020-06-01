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

Lipid = (40782589, 40795800, 40772572)

CBC = (40789356, 40789120, 40789179, 40772748, 40782735,
40789182, 40786033, 40779159)

CBCwDiff = (40785788, 40785796, 40779195, 40795733, 40795725,
40772531, 40779190, 40785793, 40779191, 40782561, 40789266)

CMP = (3049187, 3053283, 40775801, 40779224, 40782562, 40782579, 40785850,
40785861, 40785869, 40789180, 40789190, 40789527, 40791227, 40792413, 40792440,
40795730, 40795740, 40795754)

Physical_Measurement = (40654163, 40655804, 40654162, 40655805, 40654167, 40654164)

all_measurements = Lipid + CBC + CBCwDiff + CMP + Physical_Measurement

# # Improve the Definitions of Measurement Integration

# ## Lipid

len(Lipid)

num_lipids = len(set(Lipid))

df = pd.io.gbq.read_gbq('''
SELECT
    a.src_hpo_id, 
    round(COUNT(a.src_hpo_id) / {num_lipids} * 100, 2) perc_ancestors
FROM
     (
     SELECT
         DISTINCT mm.src_hpo_id, ca.ancestor_concept_id -- logs an ancestor_concept if it is found
     FROM
         `{DATASET}.measurement` m
     JOIN -- to get the site info
         `{DATASET}._mapping_measurement` mm
     ON
         m.measurement_id = mm.measurement_id
     JOIN
         `{DATASET}.concept` c
     ON
         c.concept_id = m.measurement_concept_id
     JOIN -- ensuring you 'navigate up' the hierarchy
         `{DATASET}.concept_ancestor` ca
     ON
         m.measurement_concept_id = ca.descendant_concept_id
     WHERE
         ca.ancestor_concept_id IN {Lipid}
     ) a
 WHERE LOWER(a.src_hpo_id) NOT LIKE '%rdr%'
 GROUP BY 1
 ORDER BY perc_ancestors DESC, a.src_hpo_id
    '''.format(DATASET=DATASET, num_lipids=num_lipids,Lipid=Lipid),
                        dialect='standard')
df.shape

df_Lipid = df.rename(columns={"perc_ancestors": 'Lipid'})

df_Lipid.head(100)

# ## CBC

len(CBC)

num_cbc = len(set(CBC))

df = pd.io.gbq.read_gbq('''
SELECT
    a.src_hpo_id, 
    round(COUNT(a.src_hpo_id) / {num_cbc} * 100, 2) perc_ancestors
FROM
     (
     SELECT
         DISTINCT mm.src_hpo_id, ca.ancestor_concept_id -- logs an ancestor_concept if it is found
     FROM
         `{DATASET}.measurement` m
     JOIN -- to get the site info
         `{DATASET}._mapping_measurement` mm
     ON
         m.measurement_id = mm.measurement_id
     JOIN
         `{DATASET}.concept` c
     ON
         c.concept_id = m.measurement_concept_id
     JOIN -- ensuring you 'navigate up' the hierarchy
         `{DATASET}.concept_ancestor` ca
     ON
         m.measurement_concept_id = ca.descendant_concept_id
     WHERE
         ca.ancestor_concept_id IN {CBC}
     ) a
 WHERE LOWER(a.src_hpo_id) NOT LIKE '%rdr%'
 GROUP BY 1
 ORDER BY perc_ancestors DESC, a.src_hpo_id
    '''.format(num_cbc=num_cbc, DATASET=DATASET, CBC=CBC),
                        dialect='standard')
df.shape

df_CBC = df.rename(columns={"perc_ancestors": 'CBC'})

df_CBC.head(100)

# ## CBCwDiff

len(CBCwDiff)

num_cbc_w_diff = len(set(CBCwDiff))

df = pd.io.gbq.read_gbq('''
SELECT
    a.src_hpo_id, 
    round(COUNT(a.src_hpo_id) / {num_cbc_w_diff} * 100, 2) perc_ancestors
FROM
     (
     SELECT
         DISTINCT mm.src_hpo_id, ca.ancestor_concept_id -- logs an ancestor_concept if it is found
     FROM
         `{DATASET}.measurement` m
     JOIN -- to get the site info
         `{DATASET}._mapping_measurement` mm
     ON
         m.measurement_id = mm.measurement_id
     JOIN
         `{DATASET}.concept` c
     ON
         c.concept_id = m.measurement_concept_id
     JOIN -- ensuring you 'navigate up' the hierarchy
         `{DATASET}.concept_ancestor` ca
     ON
         m.measurement_concept_id = ca.descendant_concept_id
     WHERE
         ca.ancestor_concept_id IN {CBCwDiff}
     ) a
 WHERE LOWER(a.src_hpo_id) NOT LIKE '%rdr%'
 GROUP BY 1
 ORDER BY perc_ancestors DESC, a.src_hpo_id
    '''.format(num_cbc_w_diff=num_cbc_w_diff, DATASET=DATASET, 
               CBCwDiff=CBCwDiff),
                        dialect='standard')
df.shape

df_CBCwDiff = df.rename(columns={"perc_ancestors": 'CBCwDiff'})

df_CBCwDiff.head(100)

# ## CMP

len(CMP)

num_cmp = len(set(CMP))

df = pd.io.gbq.read_gbq('''
SELECT
    a.src_hpo_id, 
    round(COUNT(a.src_hpo_id) / {num_cmp} * 100, 2) perc_ancestors
FROM
     (
     SELECT
         DISTINCT mm.src_hpo_id, ca.ancestor_concept_id -- logs an ancestor_concept if it is found
     FROM
         `{DATASET}.measurement` m
     JOIN -- to get the site info
         `{DATASET}._mapping_measurement` mm
     ON
         m.measurement_id = mm.measurement_id
     JOIN
         `{DATASET}.concept` c
     ON
         c.concept_id = m.measurement_concept_id
     JOIN -- ensuring you 'navigate up' the hierarchy
         `{DATASET}.concept_ancestor` ca
     ON
         m.measurement_concept_id = ca.descendant_concept_id
     WHERE
         ca.ancestor_concept_id IN {CMP}
     ) a
 WHERE LOWER(a.src_hpo_id) NOT LIKE '%rdr%'
 GROUP BY 1
 ORDER BY perc_ancestors DESC, a.src_hpo_id
    '''.format(num_cmp=num_cmp, DATASET=DATASET, CMP=CMP),
                        dialect='standard')
df.shape

df_CMP = df.rename(columns={"perc_ancestors": 'CMP'})

df_CMP.head(100)

# ## Physical_Measurement

len(Physical_Measurement)

num_pms = len(set(Physical_Measurement))

df = pd.io.gbq.read_gbq('''
SELECT
    a.src_hpo_id, 
    round(COUNT(a.src_hpo_id) / {num_pms} * 100, 2) perc_ancestors
FROM
     (
     SELECT
         DISTINCT mm.src_hpo_id, ca.ancestor_concept_id -- logs an ancestor_concept if it is found
     FROM
         `{DATASET}.measurement` m
     JOIN -- to get the site info
         `{DATASET}._mapping_measurement` mm
     ON
         m.measurement_id = mm.measurement_id
     JOIN
         `{DATASET}.concept` c
     ON
         c.concept_id = m.measurement_concept_id
     JOIN -- ensuring you 'navigate up' the hierarchy
         `{DATASET}.concept_ancestor` ca
     ON
         m.measurement_concept_id = ca.descendant_concept_id
     WHERE
         ca.ancestor_concept_id IN {Physical_Measurement}
     ) a
 WHERE LOWER(a.src_hpo_id) NOT LIKE '%rdr%'
 GROUP BY 1
 ORDER BY perc_ancestors DESC, a.src_hpo_id
    '''.format(num_pms=num_pms, DATASET=DATASET, Physical_Measurement=Physical_Measurement),
                        dialect='standard')
df.shape

df_Physical_Measurement = df.rename(
    columns={"perc_ancestors": 'Physical_Measurement'})

df_Physical_Measurement.head(100)

# ## All Measurements

len(all_measurements)

num_all_measurements = len(set(all_measurements))

df = pd.io.gbq.read_gbq('''
SELECT
    a.src_hpo_id, 
    round(COUNT(a.src_hpo_id) / {num_all_measurements} * 100, 2) perc_ancestors
FROM
     (
     SELECT
         DISTINCT mm.src_hpo_id, ca.ancestor_concept_id -- logs an ancestor_concept if it is found
     FROM
         `{DATASET}.measurement` m
     JOIN -- to get the site info
         `{DATASET}._mapping_measurement` mm
     ON
         m.measurement_id = mm.measurement_id
     JOIN
         `{DATASET}.concept` c
     ON
         c.concept_id = m.measurement_concept_id
     JOIN -- ensuring you 'navigate up' the hierarchy
         `{DATASET}.concept_ancestor` ca
     ON
         m.measurement_concept_id = ca.descendant_concept_id
     WHERE
         ca.ancestor_concept_id IN {all_measurements}
     ) a
 WHERE LOWER(a.src_hpo_id) NOT LIKE '%rdr%'
 GROUP BY 1
 ORDER BY perc_ancestors DESC, a.src_hpo_id
    '''.format(num_all_measurements=num_all_measurements, DATASET=DATASET,
              all_measurements=all_measurements),
                        dialect='standard')
df.shape

df_all_measurements = df.rename(columns={"perc_ancestors": 'All_Measurements'})

df_all_measurements.head(100)

# ## Sites combined

sites_measurement = pd.merge(df_Physical_Measurement,
                             df_CMP,
                             how='outer',
                             on='src_hpo_id')
sites_measurement = pd.merge(sites_measurement,
                             df_CBCwDiff,
                             how='outer',
                             on='src_hpo_id')
sites_measurement = pd.merge(sites_measurement,
                             df_CBC,
                             how='outer',
                             on='src_hpo_id')
sites_measurement = pd.merge(sites_measurement,
                             df_Lipid,
                             how='outer',
                             on='src_hpo_id')
sites_measurement = pd.merge(sites_measurement,
                             df_all_measurements,
                             how='outer',
                             on='src_hpo_id')

sites_measurement[["Physical_Measurement","CMP","CBCwDiff","CBC","Lipid","All_Measurements"]]\
    =sites_measurement[["Physical_Measurement","CMP","CBCwDiff","CBC","Lipid","All_Measurements"]]

sites_measurement = pd.merge(sites_measurement,
                             site_df,
                             how='outer',
                             on='src_hpo_id')
sites_measurement = sites_measurement.fillna(0)

sites_measurement

sites_measurement = sites_measurement.sort_values(by='All_Measurements', ascending = False)

sites_measurement

sites_measurement.to_csv("{cwd}/sites_measurement.csv".format(cwd = cwd))


