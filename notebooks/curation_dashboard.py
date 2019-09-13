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
#     version: 2.7.16
# ---

import warnings

# +
# %matplotlib inline
import google.datalab.bigquery as bq
import seaborn as sns

warnings.filterwarnings('ignore')
sns.set()
# -

def row_counts(dataset_ids):
    sq = "SELECT '{dataset_id}' dataset_id, table_id, row_count FROM {dataset_id}.__TABLES__"
    sqs = map(lambda d: sq.format(dataset_id=d), dataset_ids)
    iq = "\nUNION ALL\n".join(sqs)
    q = """ 
    SELECT dataset_id, table_id, row_count 
    FROM ({iq})
    WHERE table_id NOT LIKE '%union%' 
      AND table_id NOT LIKE '%ipmc%'
    ORDER BY table_id, dataset_id""".format(iq=iq)
    df = bq.Query(q).execute(output_options=bq.QueryOutput.dataframe(use_cache=False)).result()
    df['load_date'] = df.dataset_id.str[-8:]
    df['load_date'] = df['load_date'].astype('category')
    df['dataset_id'] = df['dataset_id'].astype('category')
    df['table_id'] = df['table_id'].astype('category')
    g = sns.FacetGrid(df, col='table_id', sharey=False, col_wrap=5)
    g.map(sns.barplot, 'load_date', 'row_count', ci=None)
    g.set_xticklabels(rotation=45, ha='right')
    return df

# # RDR data volume over time

rdr_datasets = ['', '', '']
rdr_df = row_counts(rdr_datasets)
rdr_df.pivot(index='table_id', columns='dataset_id', values='row_count')
rdr_df.to_csv('rdr.csv')

# # EHR data volume over time

dataset_ids = ['', '', '']
unioned_df = row_counts(dataset_ids)
unioned_df.pivot(index='table_id', columns='dataset_id', values='row_count')
unioned_df.to_csv('unioned.csv')

# ## Combined data volume over time

dataset_ids = ['', '']
combined_df = row_counts(dataset_ids)
combined_df.pivot(index='table_id', columns='dataset_id', values='row_count')
combined_df.to_csv('combined.csv')

# # Characterization of EHR data

dataset_id = ''
q = """
SELECT 
  (2018 - r.year_of_birth) AS age,
  gc.concept_name AS gender,
  rc.concept_name AS race,
  ec.concept_name AS ethnicity,
  CASE WHEN e.person_id IS NULL THEN 'no' ELSE 'yes' END AS has_ehr_data
FROM {dataset}.person r
  LEFT JOIN `unioned_ehr20181114.person` e 
    ON r.person_id = e.person_id
JOIN `vocabulary20180104.concept` gc 
  ON r.gender_concept_id = gc.concept_id
JOIN `vocabulary20180104.concept` rc
  ON r.race_concept_id = rc.concept_id
JOIN `vocabulary20180104.concept` ec
  ON r.ethnicity_concept_id = ec.concept_id
ORDER BY age, gender, race
""".format(dataset=dataset_id)
df = bq.Query(q).execute(output_options=bq.QueryOutput.dataframe(use_cache=False)).result()

# ## Presence of EHR data by race

# +
df['race'] = df['race'].astype('category')
df['ethnicity'] = df['ethnicity'].astype('category')
df['has_ehr_data'] = df['has_ehr_data'].astype('category')

# exclude anomalous records where age<18 or age>100
f = df[(df.age > 17) & (df.age < 100)]
g = sns.factorplot('race', data=f, aspect=4, size=3.25, kind='count', order=f.race.value_counts().index, hue='has_ehr_data')
g.set_xticklabels(rotation=45, ha='right')
# -

# ## Presence of EHR data by ethnicity

g = sns.factorplot('ethnicity', data=f, kind='count', order=f.ethnicity.value_counts().index, hue='has_ehr_data')

# # Characterization of CDR data
# The following statistics describe the candidate CDR dataset `combined20181114`. This dataset is formed by combining the unioned EHR data submitted by HPOs with the PPI data we receive from the RDR.

dataset_id = ''
q = bq.Query('''
SELECT 
  (2018 - p.year_of_birth) AS age,
  gc.concept_name AS gender,
  rc.concept_name AS race,
  ec.concept_name AS ethnicity
FROM `{dataset}.person` p
JOIN `vocabulary20180104.concept` gc 
  ON p.gender_concept_id = gc.concept_id
JOIN `vocabulary20180104.concept` rc
  ON p.race_concept_id = rc.concept_id
JOIN `vocabulary20180104.concept` ec
  ON p.ethnicity_concept_id = ec.concept_id
ORDER BY age, gender, race
''').formar(dataset=dataset_id)
df = q.execute(output_options=bq.QueryOutput.dataframe(use_cache=False)).result()

# ## Distribution of participant age stratified by gender

# +
df['race'] = df['race'].astype('category')
df['gender'] = df['gender'].astype('category')

# exclude anomalous records where age<18 or age>100
f = df[(df.age > 17) & (df.age < 100)]
g = sns.factorplot('age', data=f, aspect=4, size=3.25, kind='count', hue='gender', order=range(15,100))
g.set_xticklabels(step=5)
# -

# ## Distribution of participant race

g = sns.factorplot(x='race', data=f, aspect=5, size=2.5, kind='count', order=f.race.value_counts().index)
g.set_xticklabels(rotation=45, ha='right')

# # Gender By Race

def gender_by_race(dataset_id):
    q = bq.Query('''
    SELECT 
     c1.concept_name AS gender,
     c2.concept_name AS race,
     COUNT(1) AS `count`
    FROM `{dataset_id}.person` p
    JOIN `vocabulary20180104.concept` c1 
      ON p.gender_concept_id = c1.concept_id
    JOIN `vocabulary20180104.concept` c2
      ON p.race_concept_id = c2.concept_id
    GROUP BY c2.concept_name, c1.concept_name
    '''.format(dataset_id=dataset_id))
    df = q.execute(output_options=bq.QueryOutput.dataframe()).result()
    df['race'] = df['race'].astype('category')
    df['gender'] = df['gender'].astype('category')
    g = sns.FacetGrid(df, col='race', hue='gender', col_wrap=5)
    g.map(sns.barplot, 'gender', 'count', ci=None)
    g.set_xticklabels([])
    g.set_axis_labels('', '')
    g.add_legend()

# ## RDR

gender_by_race('')

# ## EHR

gender_by_race('')

# ## CDR

gender_by_race('')
