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

# # Improve the Definitions of Drug Ingredient

diuretics = (974166, 956874, 970250, 1395058, 904542, 942350, 932745,
            907013, 978555, 991382, 1309799)

ccb = (1332418, 1328165, 1318853, 1307863, 1353776, 1318137)

vaccine = (45637323, 529411, 529303, 42800027, 45658522, 45628027, 529218, 36212685, 40163692,
           528323, 528986, 792777, 596876)

oralhypoglycemics = (1503297, 1560171, 1580747, 1559684, 1525215, 1597756, 45774751,
                    40239216, 40166035, 1516766, 1529331)

opioids = (1124957, 1103314, 1201620, 1174888, 1126658, 1110410, 1154029, 1103640, 1102527)

antibiotics = (1734104, 1836430, 1713332, 1797513, 1705674, 1786621,
1742253, 997881, 1707164, 1738521, 1759842, 1746940, 902722, 45892419,
1717327, 1777806, 1836948, 1746114, 1775741)

statins = (1551860, 1545958, 1539403, 1510813, 1592085, 1549686, 40165636)

msknsaids = (1115008, 1177480, 1124300, 1178663, 1136980, 1118084, 1150345, 1236607, 1395573, 1146810)

painnsaids = (1177480, 1125315, 1112807, 1115008, 45660697, 45787568, 36156482, 45696636, 45696805)

ace_inhibitors = (1308216, 1341927, 1335471, 1331235, 1334456, 1340128, 1363749)

all_drugs = diuretics + ccb + vaccine + oralhypoglycemics + opioids + antibiotics + statins + msknsaids + painnsaids + ace_inhibitors

# ## Diuretics

print(len(all_drugs))

len(diuretics)

num_diuretics = len(set(diuretics))

df_diuretics = pd.io.gbq.read_gbq('''
SELECT
     mde.src_hpo_id, 
     round(COUNT(DISTINCT ca.ancestor_concept_id) / {num_diuretics} * 100, 0) as ancestor_usage
 FROM
     `{DATASET}.drug_exposure` de
 JOIN
     `{DATASET}.concept_ancestor` ca
 ON
     de.drug_concept_id = ca.descendant_concept_id
 JOIN
     `{DATASET}._mapping_drug_exposure` mde
 ON
     de.drug_exposure_id = mde.drug_exposure_id
 WHERE
     ca.ancestor_concept_id IN {diuretics}
 AND
     LOWER(mde.src_hpo_id) NOT LIKE '%rdr%'
 GROUP BY 
     1
 ORDER BY 
     ancestor_usage DESC, 
     mde.src_hpo_id
    '''.format(num_diuretics = num_diuretics, DATASET=DATASET, diuretics=diuretics),
                                  dialect='standard')
df_diuretics.shape

df_diuretics = df_diuretics.rename(columns={"ancestor_usage": 'diuretics'})

df_diuretics.head(100)

# ## CCB

len(ccb)

num_ccbs = len(set(ccb))

df_ccb = pd.io.gbq.read_gbq('''
SELECT
     mde.src_hpo_id, 
     round(COUNT(DISTINCT ca.ancestor_concept_id) / {num_ccbs} * 100, 0) as ancestor_usage
 FROM
     `{DATASET}.drug_exposure` de
 JOIN
     `{DATASET}.concept_ancestor` ca
 ON
     de.drug_concept_id = ca.descendant_concept_id
 JOIN
     `{DATASET}._mapping_drug_exposure` mde
 ON
     de.drug_exposure_id = mde.drug_exposure_id
 WHERE
     ca.ancestor_concept_id IN {ccbs}
  AND
     LOWER(mde.src_hpo_id) NOT LIKE '%rdr%'
 GROUP BY 
     1
 ORDER BY 
     ancestor_usage DESC, 
     mde.src_hpo_id
    '''.format(num_ccbs=num_ccbs, ccbs=ccb, DATASET=DATASET),
                            dialect='standard')
df_ccb.shape

df_ccb = df_ccb.rename(columns={"ancestor_usage": 'ccb'})

df_ccb.head(100)

# ## Vaccine

len(vaccine)

num_vaccines = len(set(vaccine))

df_vaccine = pd.io.gbq.read_gbq('''
SELECT
     mde.src_hpo_id, 
     round(COUNT(DISTINCT ca.ancestor_concept_id) / {num_vaccines} * 100, 0) as ancestor_usage
 FROM
     `{DATASET}.drug_exposure` de
 JOIN
     `{DATASET}.concept_ancestor` ca
 ON
     de.drug_concept_id = ca.descendant_concept_id
 JOIN
     `{DATASET}._mapping_drug_exposure` mde
 ON
     de.drug_exposure_id = mde.drug_exposure_id
 WHERE
     ca.ancestor_concept_id IN {vaccine}
  AND
     LOWER(mde.src_hpo_id) NOT LIKE '%rdr%'
 GROUP BY 
     1
 ORDER BY 
     ancestor_usage DESC, 
     mde.src_hpo_id
    '''.format(num_vaccines = num_vaccines, DATASET=DATASET, vaccine=vaccine),
                                dialect='standard')
df_vaccine.shape

df_vaccine = df_vaccine.rename(columns={"ancestor_usage": 'vaccine'})

df_vaccine.head(100)

# ## oralHypoglycemics

len(oralhypoglycemics)

num_oralhypoglycemics = len(set(oralhypoglycemics))

df_oralhypoglycemics = pd.io.gbq.read_gbq('''
SELECT
     mde.src_hpo_id, 
     round(COUNT(DISTINCT ca.ancestor_concept_id) / {num_oralhypoglycemics} * 100, 0) as ancestor_usage
 FROM
     `{DATASET}.drug_exposure` de
 JOIN
     `{DATASET}.concept_ancestor` ca
 ON
     de.drug_concept_id = ca.descendant_concept_id
 JOIN
     `{DATASET}._mapping_drug_exposure` mde
 ON
     de.drug_exposure_id = mde.drug_exposure_id
 WHERE
     ca.ancestor_concept_id IN {oralhypoglycemics}
  AND
     LOWER(mde.src_hpo_id) NOT LIKE '%rdr%'
 GROUP BY 
     1
 ORDER BY 
     ancestor_usage DESC, 
     mde.src_hpo_id
    '''.format(num_oralhypoglycemics=num_oralhypoglycemics,
              DATASET=DATASET, oralhypoglycemics=oralhypoglycemics),
              dialect='standard')
df_oralhypoglycemics.shape

df_oralhypoglycemics = df_oralhypoglycemics.rename(
    columns={"ancestor_usage": 'oralhypoglycemics'})

df_oralhypoglycemics.head(100)

# ## opioids

len(opioids)

num_opioids = len(set(opioids))

df_opioids = pd.io.gbq.read_gbq('''
SELECT
     mde.src_hpo_id, 
     round(COUNT(DISTINCT ca.ancestor_concept_id) / {num_opioids} * 100, 0) as ancestor_usage
 FROM
     `{DATASET}.drug_exposure` de
 JOIN
     `{DATASET}.concept_ancestor` ca
 ON
     de.drug_concept_id = ca.descendant_concept_id
 JOIN
     `{DATASET}._mapping_drug_exposure` mde
 ON
     de.drug_exposure_id = mde.drug_exposure_id
 WHERE
     ca.ancestor_concept_id IN {opioids}
 AND
     LOWER(mde.src_hpo_id) NOT LIKE '%rdr%'
 GROUP BY 
     1
 ORDER BY 
     ancestor_usage DESC, 
     mde.src_hpo_id
    '''.format(num_opioids=num_opioids, DATASET=DATASET, opioids=opioids),
                                dialect='standard')
df_opioids.shape

df_opioids = df_opioids.rename(columns={"ancestor_usage": 'opioids'})

df_opioids.head(100)

# ## Antibiotics

len(antibiotics)

num_antibiotics = len(set(antibiotics))

# +
df_antibiotics = pd.io.gbq.read_gbq('''
SELECT
     mde.src_hpo_id, 
     round(COUNT(DISTINCT ca.ancestor_concept_id) / {num_antibiotics} * 100, 0) as ancestor_usage
 FROM
     `{DATASET}.drug_exposure` de
 JOIN
     `{DATASET}.concept_ancestor` ca
 ON
     de.drug_concept_id = ca.descendant_concept_id
 JOIN
     `{DATASET}._mapping_drug_exposure` mde
 ON
     de.drug_exposure_id = mde.drug_exposure_id
 WHERE
     ca.ancestor_concept_id IN {antibiotics}
 AND
     LOWER(mde.src_hpo_id) NOT LIKE '%rdr%'
 GROUP BY 
     1
 ORDER BY 
     ancestor_usage DESC, 
     mde.src_hpo_id
    '''.format(num_antibiotics=num_antibiotics, DATASET=DATASET, antibiotics=antibiotics),
                dialect='standard')

df_antibiotics.shape
# -

df_antibiotics = df_antibiotics.rename(
    columns={"ancestor_usage": 'antibiotics'})

df_antibiotics.head(100)

# ## Statins

len(statins)

num_statins = len(set(statins))

df_statins = pd.io.gbq.read_gbq('''
SELECT
     mde.src_hpo_id, 
     round(COUNT(DISTINCT ca.ancestor_concept_id) / {num_statins} * 100, 0) as ancestor_usage
 FROM
     `{DATASET}.drug_exposure` de
 JOIN
     `{DATASET}.concept_ancestor` ca
 ON
     de.drug_concept_id = ca.descendant_concept_id
 JOIN
     `{DATASET}._mapping_drug_exposure` mde
 ON
     de.drug_exposure_id = mde.drug_exposure_id
 WHERE
     ca.ancestor_concept_id IN {statins}
 AND
     LOWER(mde.src_hpo_id) NOT LIKE '%rdr%'
 GROUP BY 
     1
 ORDER BY 
     ancestor_usage DESC, 
     mde.src_hpo_id
    '''.format(num_statins=num_statins, DATASET=DATASET, statins=statins),
               dialect='standard')
df_statins.shape

df_statins = df_statins.rename(columns={"ancestor_usage": 'statins'})

df_statins.head(100)

# ## msknsaids

len(msknsaids)

num_msknsaids = len(set(msknsaids))

df_msknsaids = pd.io.gbq.read_gbq('''
SELECT
     mde.src_hpo_id, 
     round(COUNT(DISTINCT ca.ancestor_concept_id) / {num_msknsaids} * 100, 0) as ancestor_usage
 FROM
     `{DATASET}.drug_exposure` de
 JOIN
     `{DATASET}.concept_ancestor` ca
 ON
     de.drug_concept_id = ca.descendant_concept_id
 JOIN
     `{DATASET}._mapping_drug_exposure` mde
 ON
     de.drug_exposure_id = mde.drug_exposure_id
 WHERE
     ca.ancestor_concept_id IN {msknsaids}
 AND
     LOWER(mde.src_hpo_id) NOT LIKE '%rdr%'
 GROUP BY 
     1
 ORDER BY 
     ancestor_usage DESC, 
     mde.src_hpo_id
    '''.format(num_msknsaids=num_msknsaids, DATASET=DATASET, msknsaids=msknsaids),
                dialect='standard')
df_msknsaids.shape

df_msknsaids = df_msknsaids.rename(columns={"ancestor_usage": 'msknsaids'})

df_msknsaids.head(100)

# ## painnsaids

len(painnsaids)

num_painnsaids = len(set(painnsaids))

df_painnsaids = pd.io.gbq.read_gbq('''
SELECT
     mde.src_hpo_id, 
     round(COUNT(DISTINCT ca.ancestor_concept_id) / {num_painnsaids} * 100, 0) as ancestor_usage
 FROM
     `{DATASET}.drug_exposure` de
 JOIN
     `{DATASET}.concept_ancestor` ca
 ON
     de.drug_concept_id = ca.descendant_concept_id
 JOIN
     `{DATASET}._mapping_drug_exposure` mde
 ON
     de.drug_exposure_id = mde.drug_exposure_id
 WHERE
     ca.ancestor_concept_id IN {painnsaids}
 AND
     LOWER(mde.src_hpo_id) NOT LIKE '%rdr%'
 GROUP BY 
     1
 ORDER BY 
     ancestor_usage DESC, 
     mde.src_hpo_id
    '''.format(DATASET=DATASET, num_painnsaids=num_painnsaids,
              painnsaids=painnsaids), dialect='standard')
df_painnsaids.shape

df_painnsaids = df_painnsaids.rename(columns={"ancestor_usage": 'painnsaids'})

df_painnsaids.head(100)

# ## ace_inhibitors

len(ace_inhibitors)

num_ace_inhib = len(set(ace_inhibitors))

df_ace_inhibitors = pd.io.gbq.read_gbq('''
SELECT
     mde.src_hpo_id, 
     round(COUNT(DISTINCT ca.ancestor_concept_id) / {num_ace_inhib} * 100, 0) as ancestor_usage
 FROM
     `{DATASET}.drug_exposure` de
 JOIN
     `{DATASET}.concept_ancestor` ca
 ON
     de.drug_concept_id = ca.descendant_concept_id
 JOIN
     `{DATASET}._mapping_drug_exposure` mde
 ON
     de.drug_exposure_id = mde.drug_exposure_id
 WHERE
     ca.ancestor_concept_id IN {ace_inhibitors}
 AND
     LOWER(mde.src_hpo_id) NOT LIKE '%rdr%'
 GROUP BY 
     1
 ORDER BY 
     ancestor_usage DESC, 
     mde.src_hpo_id
    '''.format(num_ace_inhib=num_ace_inhib, DATASET=DATASET,
              ace_inhibitors=ace_inhibitors),
                                       dialect='standard')
df_ace_inhibitors.shape

df_ace_inhibitors = df_ace_inhibitors.rename(
    columns={"ancestor_usage": 'ace_inhibitors'})

df_ace_inhibitors.head(100)

# ## all_drugs

len(all_drugs)

num_drugs = len(set(all_drugs))

df_all_drugs = pd.io.gbq.read_gbq('''
SELECT
     mde.src_hpo_id, 
     round(COUNT(DISTINCT ca.ancestor_concept_id) / {num_drugs} * 100, 0) as ancestor_usage
 FROM
     `{DATASET}.drug_exposure` de
 JOIN
     `{DATASET}.concept_ancestor` ca
 ON
     de.drug_concept_id = ca.descendant_concept_id
 JOIN
     `{DATASET}._mapping_drug_exposure` mde
 ON
     de.drug_exposure_id = mde.drug_exposure_id
 WHERE
     ca.ancestor_concept_id IN {all_drugs}
 AND
     LOWER(mde.src_hpo_id) NOT LIKE '%rdr%'
 GROUP BY 
     1
 ORDER BY 
     ancestor_usage DESC, 
     mde.src_hpo_id
    '''.format(num_drugs=num_drugs, DATASET=DATASET, all_drugs=all_drugs),
                                  dialect='standard')
df_all_drugs.shape

df_all_drugs = df_all_drugs.rename(columns={"ancestor_usage": 'all_drugs'})

df_all_drugs.head(100)

# ## Sites combined

sites_drug_success = pd.merge(df_ace_inhibitors,
                              df_painnsaids,
                              how='outer',
                              on='src_hpo_id')
sites_drug_success = pd.merge(sites_drug_success,
                              df_msknsaids,
                              how='outer',
                              on='src_hpo_id')
sites_drug_success = pd.merge(sites_drug_success,
                              df_statins,
                              how='outer',
                              on='src_hpo_id')
sites_drug_success = pd.merge(sites_drug_success,
                              df_antibiotics,
                              how='outer',
                              on='src_hpo_id')
sites_drug_success = pd.merge(sites_drug_success,
                              df_opioids,
                              how='outer',
                              on='src_hpo_id')
sites_drug_success = pd.merge(sites_drug_success,
                              df_oralhypoglycemics,
                              how='outer',
                              on='src_hpo_id')
sites_drug_success = pd.merge(sites_drug_success,
                              df_vaccine,
                              how='outer',
                              on='src_hpo_id')
sites_drug_success = pd.merge(sites_drug_success,
                              df_ccb,
                              how='outer',
                              on='src_hpo_id')
sites_drug_success = pd.merge(sites_drug_success,
                              df_diuretics,
                              how='outer',
                              on='src_hpo_id')
sites_drug_success = pd.merge(sites_drug_success,
                              df_all_drugs,
                              how='outer',
                              on='src_hpo_id')

sites_drug_success = sites_drug_success.fillna(0)
sites_drug_success

sites_drug_success[["ace_inhibitors","painnsaids","msknsaids","statins","antibiotics","opioids","oralhypoglycemics","vaccine","ccb","diuretics","all_drugs"]]\
    =sites_drug_success[["ace_inhibitors","painnsaids","msknsaids","statins","antibiotics","opioids","oralhypoglycemics","vaccine","ccb","diuretics","all_drugs"]]
sites_drug_success

sites_drug_success = pd.merge(sites_drug_success,
                              site_df,
                              how='outer',
                              on='src_hpo_id')
sites_drug_success = sites_drug_success.fillna(0)

sites_drug_success = sites_drug_success.sort_values(by='all_drugs', ascending = False)

sites_drug_success

sites_drug_success.to_csv("{cwd}/drug_success.csv".format(cwd = cwd))
