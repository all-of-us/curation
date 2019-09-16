# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.4'
#       jupytext_version: 1.2.3
#   kernelspec:
#     display_name: Python 2
#     language: python
#     name: python2
# ---

# +
# %matplotlib inline
import warnings
import google.datalab.bigquery as bq
import re
import seaborn as sns

VOCABULARY_DATASET_RE = re.compile('^vocabulary\d{8}$')
RDR_DATASET_RE = re.compile('^rdr\d{8}$')
EHR_DATASET_RE = re.compile('^unioned_ehr\d{8}$')
COMBINED_DATASET_RE = re.compile('^combined\d{8}$')
DEID_DATASET_RE = re.compile('^combined\d{8}_deid$')
TREND_N = 3

warnings.filterwarnings('ignore')
sns.set()
# -

DATASETS = list(bq.Datasets())

dataset_ids = []
for dataset in DATASETS:
    dataset_ids.append(dataset.name.dataset_id)
dataset_ids.sort(reverse=True)

vocabulary_dataset_ids = []
rdr_dataset_ids = []
ehr_dataset_ids = []
combined_dataset_ids = []
deid_dataset_ids = []
for dataset_id in dataset_ids:
    if re.match(VOCABULARY_DATASET_RE, dataset_id):
        vocabulary_dataset_ids.append(dataset_id)
    elif re.match(RDR_DATASET_RE, dataset_id):
        rdr_dataset_ids.append(dataset_id)
    elif re.match(EHR_DATASET_RE, dataset_id):
        ehr_dataset_ids.append(dataset_id)
    elif re.match(COMBINED_DATASET_RE, dataset_id):
        combined_dataset_ids.append(dataset_id)
    elif re.match(DEID_DATASET_RE, dataset_id):
        deid_dataset_ids.append(dataset_id)

vocabulary_dataset_id = vocabulary_dataset_ids[0]
trend_rdr_datasets = rdr_dataset_ids[0:TREND_N]
trend_ehr_datasets = ehr_dataset_ids[0:TREND_N]
trend_combined_datasets = combined_dataset_ids[0:TREND_N]
trend_deid_datasets = deid_dataset_ids[0:TREND_N]


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

rdr_df = row_counts(trend_rdr_datasets)
rdr_df.pivot(index='table_id', columns='dataset_id', values='row_count')
rdr_df.to_csv('rdr.csv')

# # EHR data volume over time

unioned_df = row_counts(trend_ehr_datasets)
unioned_df.pivot(index='table_id', columns='dataset_id', values='row_count')
unioned_df.to_csv('unioned.csv')

# ## Combined data volume over time

combined_df = row_counts(trend_combined_datasets)
combined_df.pivot(index='table_id', columns='dataset_id', values='row_count')
combined_df.to_csv('combined.csv')

# # Characterization of EHR data

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
JOIN `{vocabulary_dataset_id}.concept` gc 
  ON r.gender_concept_id = gc.concept_id
JOIN `{vocabulary_dataset_id}.concept` rc
  ON r.race_concept_id = rc.concept_id
JOIN `{vocabulary_dataset_id}.concept` ec
  ON r.ethnicity_concept_id = ec.concept_id
ORDER BY age, gender, race
""".format(dataset=ehr_dataset_ids[0], vocabulary_dataset_id=vocabulary_dataset_id)
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
# The following statistics describe the candidate CDR dataset. This dataset is formed by combining the unioned EHR data submitted by HPOs with the PPI data we receive from the RDR.

q = bq.Query('''
SELECT 
  (EXTRACT(YEAR FROM CURRENT_DATE()) - p.year_of_birth) AS age,
  gc.concept_name AS gender,
  rc.concept_name AS race,
  ec.concept_name AS ethnicity
FROM `{dataset}.person` p
JOIN `{vocabulary_dataset_id}.concept` gc 
  ON p.gender_concept_id = gc.concept_id
JOIN `{vocabulary_dataset_id}.concept` rc
  ON p.race_concept_id = rc.concept_id
JOIN `{vocabulary_dataset_id}.concept` ec
  ON p.ethnicity_concept_id = ec.concept_id
ORDER BY age, gender, race
'''.format(dataset=ehr_dataset_ids[0], vocabulary_dataset_id=vocabulary_dataset_id))
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
    JOIN `{vocabulary_dataset_id}.concept` c1 
      ON p.gender_concept_id = c1.concept_id
    JOIN `{vocabulary_dataset_id}.concept` c2
      ON p.race_concept_id = c2.concept_id
    GROUP BY c2.concept_name, c1.concept_name
    '''.format(dataset_id=dataset_id, vocabulary_dataset_id=vocabulary_dataset_id))
    df = q.execute(output_options=bq.QueryOutput.dataframe()).result()
    df['race'] = df['race'].astype('category')
    df['gender'] = df['gender'].astype('category')
    g = sns.FacetGrid(df, col='race', hue='gender', col_wrap=5)
    g.map(sns.barplot, 'gender', 'count', ci=None)
    g.set_xticklabels([])
    g.set_axis_labels('', '')
    g.add_legend()

# ## RDR

gender_by_race(rdr_dataset_ids[0])

# ## EHR

gender_by_race(ehr_dataset_ids[0])

# ## CDR

gender_by_race(deid_dataset_ids[0])
