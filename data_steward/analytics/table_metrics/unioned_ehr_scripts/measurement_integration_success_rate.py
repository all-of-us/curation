# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
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

# + tags=["parameters"]
PROJECT_ID = ""
DATASET = ""
LOOKUP_TABLES = ""
RUN_AS = ""
# -

# +
import warnings

warnings.filterwarnings('ignore')
import pandas as pd
import matplotlib.pyplot as plt
import os
from utils import auth
from analytics.cdr_ops.notebook_utils import execute, IMPERSONATION_SCOPES
from gcloud.bq import BigQueryClient

impersonation_creds = auth.get_impersonation_credentials(
    RUN_AS, target_scopes=IMPERSONATION_SCOPES)

client = BigQueryClient(PROJECT_ID, credentials=impersonation_creds)

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

site_df = execute(client, hpo_id_query)

get_full_names = f"""
select * from {LOOKUP_TABLES}.hpo_site_id_mappings
"""

full_names_df = execute(client, get_full_names)

# +
full_names_df.columns = ['org_id', 'src_hpo_id', 'site_name', 'display_order']
columns_to_use = ['src_hpo_id', 'site_name']

full_names_df = full_names_df[columns_to_use]
full_names_df['src_hpo_id'] = full_names_df['src_hpo_id'].str.lower()

# +
cols_to_join = ['src_hpo_id']

site_df = pd.merge(site_df, full_names_df, on=['src_hpo_id'], how='left')
# -

Lipid = (40782589, 40795800, 40772572)

CBC = (40789356, 40789120, 40789179, 40772748, 40782735, 40789182, 40786033,
       40779159)

CBCwDiff = (40785788, 40785796, 40779195, 40795733, 40795725, 40772531,
            40779190, 40785793, 40779191, 40782561, 40789266)

CMP = (3049187, 3053283, 40775801, 40779224, 40782562, 40782579, 40785850,
       40785861, 40785869, 40789180, 40789190, 40789527, 40791227, 40792413,
       40792440, 40795730, 40795740, 40795754, 40771922)

Physical_Measurement = (40654163, 40655804, 40654162, 40655805, 40654167,
                        40654164)

all_measurements = Lipid + CBC + CBCwDiff + CMP + Physical_Measurement

# # Improve the Definitions of Measurement Integration

# ## Lipid

len(Lipid)

num_lipids = len(set(Lipid))

lipid_query = '''
SELECT
    a.src_hpo_id, 
    round(COUNT(a.src_hpo_id) / {num_lipids} * 100, 2) perc_ancestors -- does not make sense
FROM
     (
     SELECT
         DISTINCT mm.src_hpo_id, ca.ancestor_concept_id -- logs an ancestor_concept if it is found
     FROM
         `{DATASET}.unioned_ehr_measurement` m
     JOIN -- to get the site info
         `{DATASET}._mapping_measurement` mm
     ON
         m.measurement_id = mm.measurement_id
     JOIN
         `{DATASET}.concept` c
     ON
         c.concept_id = m.measurement_concept_id
     JOIN -- ensuring you 'navigate up' the hierarchy
         `{DATASET}.union_concept_ancestor` ca
     ON
         m.measurement_concept_id = ca.descendant_concept_id
     WHERE
         ca.ancestor_concept_id IN {Lipid}
     ) a
 GROUP BY 1
 ORDER BY perc_ancestors DESC, a.src_hpo_id
    '''.format(DATASET=DATASET, num_lipids=num_lipids, Lipid=Lipid)

df_Lipid = execute(client, lipid_query)

df_Lipid = df_Lipid.rename(columns={"perc_ancestors": 'Lipid'})

df_Lipid.head(100)

# ## CBC

len(CBC)

num_cbc = len(set(CBC))

CBC_query = '''
SELECT
    a.src_hpo_id, 
    round(COUNT(a.src_hpo_id) / {num_cbc} * 100, 2) perc_ancestors
FROM
     (
     SELECT
         DISTINCT mm.src_hpo_id, ca.ancestor_concept_id -- logs an ancestor_concept if it is found
     FROM
         `{DATASET}.unioned_ehr_measurement` m
     JOIN -- to get the site info
         `{DATASET}._mapping_measurement` mm
     ON
         m.measurement_id = mm.measurement_id
     JOIN
         `{DATASET}.concept` c
     ON
         c.concept_id = m.measurement_concept_id
     JOIN -- ensuring you 'navigate up' the hierarchy
         `{DATASET}.union_concept_ancestor` ca
     ON
         m.measurement_concept_id = ca.descendant_concept_id
     WHERE
         ca.ancestor_concept_id IN {CBC}
     ) a
 GROUP BY 1
 ORDER BY perc_ancestors DESC, a.src_hpo_id
    '''.format(num_cbc=num_cbc, DATASET=DATASET, CBC=CBC)

df_CBC = execute(client, CBC_query)

df_CBC = df_CBC.rename(columns={"perc_ancestors": 'CBC'})

df_CBC.head(100)

# ## CBCwDiff

len(CBCwDiff)

num_cbc_w_diff = len(set(CBCwDiff))

CBCwDiff_query = '''
SELECT
    a.src_hpo_id, 
    round(COUNT(a.src_hpo_id) / {num_cbc_w_diff} * 100, 2) perc_ancestors
FROM
     (
     SELECT
         DISTINCT mm.src_hpo_id, ca.ancestor_concept_id -- logs an ancestor_concept if it is found
     FROM
         `{DATASET}.unioned_ehr_measurement` m
     JOIN -- to get the site info
         `{DATASET}._mapping_measurement` mm
     ON
         m.measurement_id = mm.measurement_id
     JOIN
         `{DATASET}.concept` c
     ON
         c.concept_id = m.measurement_concept_id
     JOIN -- ensuring you 'navigate up' the hierarchy
         `{DATASET}.union_concept_ancestor` ca
     ON
         m.measurement_concept_id = ca.descendant_concept_id
     WHERE
         ca.ancestor_concept_id IN {CBCwDiff}
     ) a
 GROUP BY 1
 ORDER BY perc_ancestors DESC, a.src_hpo_id
    '''.format(num_cbc_w_diff=num_cbc_w_diff,
               DATASET=DATASET,
               CBCwDiff=CBCwDiff)
df_CBCwDiff = execute(client, CBCwDiff_query)

df_CBCwDiff = df_CBCwDiff.rename(columns={"perc_ancestors": 'CBCwDiff'})

df_CBCwDiff.head(100)

# ## CMP

len(CMP)

num_cmp = len(set(CMP))

CMP_query = '''
SELECT
    a.src_hpo_id, 
    round(COUNT(a.src_hpo_id) / {num_cmp} * 100, 2) perc_ancestors
FROM
     (
     SELECT
         DISTINCT mm.src_hpo_id, ca.ancestor_concept_id -- logs an ancestor_concept if it is found
     FROM
         `{DATASET}.unioned_ehr_measurement` m
     JOIN -- to get the site info
         `{DATASET}._mapping_measurement` mm
     ON
         m.measurement_id = mm.measurement_id
     JOIN
         `{DATASET}.concept` c
     ON
         c.concept_id = m.measurement_concept_id
     JOIN -- ensuring you 'navigate up' the hierarchy
         `{DATASET}.union_concept_ancestor` ca
     ON
         m.measurement_concept_id = ca.descendant_concept_id
     WHERE
         ca.ancestor_concept_id IN {CMP}
     ) a
 GROUP BY 1
 ORDER BY perc_ancestors DESC, a.src_hpo_id
    '''.format(num_cmp=num_cmp, DATASET=DATASET, CMP=CMP)
df_CMP = execute(client, CMP_query)

df_CMP = df_CMP.rename(columns={"perc_ancestors": 'CMP'})

df_CMP.head(100)

# ## Physical_Measurement

len(Physical_Measurement)

num_pms = len(set(Physical_Measurement))

Physical_Measurement_query = '''
SELECT
    a.src_hpo_id, 
    round(COUNT(a.src_hpo_id) / {num_pms} * 100, 2) perc_ancestors
FROM
     (
     SELECT
         DISTINCT mm.src_hpo_id, ca.ancestor_concept_id -- logs an ancestor_concept if it is found
     FROM
         `{DATASET}.unioned_ehr_measurement` m
     JOIN -- to get the site info
         `{DATASET}._mapping_measurement` mm
     ON
         m.measurement_id = mm.measurement_id
     JOIN
         `{DATASET}.concept` c
     ON
         c.concept_id = m.measurement_concept_id
     JOIN -- ensuring you 'navigate up' the hierarchy
         `{DATASET}.union_concept_ancestor` ca
     ON
         m.measurement_concept_id = ca.descendant_concept_id
     WHERE
         ca.ancestor_concept_id IN {Physical_Measurement}
     ) a
 GROUP BY 1
 ORDER BY perc_ancestors DESC, a.src_hpo_id
    '''.format(num_pms=num_pms,
               DATASET=DATASET,
               Physical_Measurement=Physical_Measurement)
df_Physical_Measurement = execute(client, Physical_Measurement_query)

df_Physical_Measurement = df_Physical_Measurement.rename(
    columns={"perc_ancestors": 'Physical_Measurement'})

df_Physical_Measurement.head(100)

# ## All Measurements

len(all_measurements)

num_all_measurements = len(set(all_measurements))

all_measurements_query = '''
SELECT
    a.src_hpo_id, 
    round(COUNT(a.src_hpo_id) / {num_all_measurements} * 100, 2) perc_ancestors
FROM
     (
     SELECT
         DISTINCT mm.src_hpo_id, ca.ancestor_concept_id -- logs an ancestor_concept if it is found
     FROM
         `{DATASET}.unioned_ehr_measurement` m
     JOIN -- to get the site info
         `{DATASET}._mapping_measurement` mm
     ON
         m.measurement_id = mm.measurement_id
     JOIN
         `{DATASET}.concept` c
     ON
         c.concept_id = m.measurement_concept_id
     JOIN -- ensuring you 'navigate up' the hierarchy
         `{DATASET}.union_concept_ancestor` ca
     ON
         m.measurement_concept_id = ca.descendant_concept_id
     WHERE
         ca.ancestor_concept_id IN {all_measurements}
     ) a
 GROUP BY 1
 ORDER BY perc_ancestors DESC, a.src_hpo_id
    '''.format(num_all_measurements=num_all_measurements,
               DATASET=DATASET,
               all_measurements=all_measurements)
df_all_measurements = execute(client, all_measurements_query)

df_all_measurements = df_all_measurements.rename(
    columns={"perc_ancestors": 'All_Measurements'})

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

sites_measurement[["Physical_Measurement", "CMP", "CBCwDiff", "CBC", "Lipid", "All_Measurements"]] \
    = sites_measurement[["Physical_Measurement", "CMP", "CBCwDiff", "CBC", "Lipid", "All_Measurements"]]

sites_measurement = pd.merge(sites_measurement,
                             site_df,
                             how='outer',
                             on='src_hpo_id')
sites_measurement = sites_measurement.fillna(0)

sites_measurement

sites_measurement = sites_measurement.sort_values(by='All_Measurements',
                                                  ascending=False)

sites_measurement

sites_measurement.to_csv("{cwd}/sites_measurement.csv".format(cwd=cwd))
