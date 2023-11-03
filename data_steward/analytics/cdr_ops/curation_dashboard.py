# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.7.1
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# +
# %matplotlib inline
import warnings

import seaborn as sns

from common import PIPELINE_TABLES
from utils import bq

warnings.filterwarnings('ignore')
sns.set()

RDR = ''
UNIONED = ''
VOCABULARY = ''
COMBINED = ''
RT_DATASET = ''
CT_DATASET = ''

ALL_RDR = []
ALL_UNIONED = []
ALL_COMBINED = []
ALL_RT_DATASET = []
ALL_CT_DATASET = []
# -


def row_counts(dataset_ids):
    sq = "SELECT '{dataset_id}' dataset_id, table_id, row_count FROM `{dataset_id}.__TABLES__`"
    sqs = [sq.format(dataset_id=d) for d in dataset_ids]
    iq = "\nUNION ALL\n".join(sqs)
    q = """
    SELECT dataset_id, table_id, row_count 
    FROM ({iq})
    WHERE table_id NOT LIKE '%union%' 
      AND table_id NOT LIKE '%ipmc%'
    ORDER BY table_id, dataset_id""".format(iq=iq)
    df = bq.query(q)
    df['load_date'] = df.dataset_id.str[-8:]
    df['load_date'] = df['load_date'].astype('category')
    df['dataset_id'] = df['dataset_id'].astype('category')
    df['table_id'] = df['table_id'].astype('category')
    g = sns.FacetGrid(df, col='table_id', sharey=False, col_wrap=5)
    g.map(sns.barplot, 'dataset_id', 'row_count', ci=None)
    g.set_xticklabels(rotation=45, ha='right')
    return df


# # RDR data volume over time

rdr_df = row_counts(ALL_RDR + [RDR])
rdr_df = rdr_df.pivot(index='table_id',
                      columns='dataset_id',
                      values='row_count')
rdr_df.to_csv('%s.csv' % 'rdr_diff')

# # EHR data volume over time

unioned_df = row_counts(ALL_UNIONED + [UNIONED])
unioned_df = unioned_df.pivot(index='table_id',
                              columns='dataset_id',
                              values='row_count')
unioned_df.to_csv('%s.csv' % 'unioned_diff')

# ## Combined data volume over time

combined_df = row_counts(ALL_COMBINED + [COMBINED])
combined_df = combined_df.pivot(index='table_id',
                                columns='dataset_id',
                                values='row_count')
combined_df.to_csv('%s.csv' % 'combined_diff')

ct_df = row_counts(ALL_CT_DATASET + [CT_DATASET])
ct_df = ct_df.pivot(index='table_id',
                                columns='dataset_id',
                                values='row_count')
ct_df.to_csv('%s.csv' % 'ct_diff')

# # Characterization of EHR data

q = f"""
SELECT 
  (2018 - r.year_of_birth) AS age,
  gc.concept_name AS gender,
  rc.concept_name AS race,
  ec.concept_name AS ethnicity,
  CASE WHEN e.person_id IS NULL THEN 'no' ELSE 'yes' END AS has_ehr_data
FROM `{RDR}.person` r
  LEFT JOIN `{UNIONED}.person` e 
    ON r.person_id = e.person_id
JOIN `{VOCABULARY}.concept` gc 
  ON r.gender_concept_id = gc.concept_id
JOIN `{VOCABULARY}.concept` rc
  ON r.race_concept_id = rc.concept_id
JOIN `{VOCABULARY}.concept` ec
  ON r.ethnicity_concept_id = ec.concept_id
ORDER BY age, gender, race
"""
df = bq.query(q)

# ## Presence of EHR data by race

# +
df['race'] = df['race'].astype('category')
df['ethnicity'] = df['ethnicity'].astype('category')
df['has_ehr_data'] = df['has_ehr_data'].astype('category')

# exclude anomalous records where age<18 or age>100
f = df[(df.age > 17) & (df.age < 100)]
g = sns.factorplot('race',
                   data=f,
                   aspect=4,
                   size=3.25,
                   kind='count',
                   order=f.race.value_counts().index,
                   hue='has_ehr_data')
g.set_xticklabels(rotation=45, ha='right')
# -

# ## Presence of EHR data by ethnicity

g = sns.factorplot('ethnicity',
                   data=f,
                   kind='count',
                   order=f.ethnicity.value_counts().index,
                   hue='has_ehr_data')

# # Characterization of CDR data
# The following statistics describe the candidate CDR dataset. This dataset is formed by combining the unioned EHR data submitted by HPOs with the PPI data we receive from the RDR.

df = bq.query(f'''
SELECT 
  {PIPELINE_TABLES}.calculate_age(CURRENT_DATE, EXTRACT(DATE FROM p.birth_datetime)) AS age,
  gc.concept_name AS gender,
  rc.concept_name AS race,
  ec.concept_name AS ethnicity
FROM `{UNIONED}.person` p
JOIN `{VOCABULARY}.concept` gc 
  ON p.gender_concept_id = gc.concept_id
JOIN `{VOCABULARY}.concept` rc
  ON p.race_concept_id = rc.concept_id
JOIN `{VOCABULARY}.concept` ec
  ON p.ethnicity_concept_id = ec.concept_id
WHERE p.birth_datetime IS NOT NULL
ORDER BY age, gender, race
''')

# ## Distribution of participant age stratified by gender

# +
df['race'] = df['race'].astype('category')
df['gender'] = df['gender'].astype('category')

# exclude anomalous records where age<18 or age>100
f = df[(df.age > 17) & (df.age < 100)]
g = sns.factorplot('age',
                   data=f,
                   aspect=4,
                   size=3.25,
                   kind='count',
                   hue='gender',
                   order=range(15, 100))
g.set_xticklabels(step=5)
# -

# ## Distribution of participant race

g = sns.factorplot(x='race',
                   data=f,
                   aspect=5,
                   size=2.5,
                   kind='count',
                   order=f.race.value_counts().index)
g.set_xticklabels(rotation=45, ha='right')

# # Gender By Race


def gender_by_race(dataset_id):
    df = bq.query(f'''
    SELECT 
     c1.concept_name AS gender,
     c2.concept_name AS race,
     COUNT(1) AS `count`
    FROM `{dataset_id}.person` p
    JOIN `{VOCABULARY}.concept` c1 
      ON p.gender_concept_id = c1.concept_id
    JOIN `{VOCABULARY}.concept` c2
      ON p.race_concept_id = c2.concept_id
    GROUP BY c2.concept_name, c1.concept_name
    ''')
    df['race'] = df['race'].astype('category')
    df['gender'] = df['gender'].astype('category')
    g = sns.FacetGrid(df, col='race', sharey=False, hue='gender', col_wrap=5)
    g.map(sns.barplot, 'gender', 'count', ci=None)
    g.set_xticklabels([])
    g.set_axis_labels('', '')
    g.add_legend()


# ## RDR

gender_by_race(RDR)

# ## EHR

gender_by_race(UNIONED)

# ## CDR

gender_by_race(COMBINED)

