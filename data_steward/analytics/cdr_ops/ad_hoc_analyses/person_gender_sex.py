# Regenerate the deid person table with the changes below.
# 1. Update gender fields using responses to the question [Gender_GenderIdentity](http://athena.ohdsi.org/search-terms/terms/1585838)
#  * `gender_concept_id` contains the associated `value_as_concept_id`
#  * `gender_source_concept_id` contains the associated `value_source_concept_id`
#  * `gender_source_value` contains the `concept_code` associated with the `gender_source_concept_id`
# 1. Add new `sex_at_birth_*` fields populated using responses to the question [BiologicalSexAtBirth_SexAtBirth](http://athena.ohdsi.org/search-terms/terms/1585845)
#   * `sex_at_birth_concept_id` contains the associated `value_as_concept_id`
#   * `sex_at_birth_source_concept_id` contains the associated `value_source_concept_id`
#   * `sex_at_birth_source_value` contains the `concept_code` associated with `sex_at_birth_source_concept_id`
import utils.bq
from notebooks import render
from notebooks.parameters import SANDBOX, DEID_DATASET_ID
import resources
print("""
DEID={DEID}
SANDBOX={SANDBOX}
""".format(DEID=DEID_DATASET_ID, SANDBOX=SANDBOX))

SEX_AT_BIRTH_QUERY = """
WITH sex_at_birth AS
(SELECT
    p.person_id,
    COALESCE(o.value_as_concept_id, 0)     AS sex_at_birth_concept_id,
    COALESCE(o.value_source_concept_id, 0) AS sex_at_birth_source_concept_id
  FROM
    `{DATASET}.person` p
    LEFT JOIN `{DATASET}.observation` o
     ON p.person_id = o.person_id AND observation_source_concept_id = 1585845)

SELECT s.person_id,
  s.sex_at_birth_concept_id,
  s.sex_at_birth_source_concept_id,
  c.concept_code AS sex_at_birth_source_value
FROM sex_at_birth s
JOIN `{DATASET}.concept` c
  ON s.sex_at_birth_source_concept_id = c.concept_id
"""
GENDER_QUERY = """
WITH gender AS
(SELECT
    p.person_id,
    COALESCE(o.value_as_concept_id, 0)     AS gender_concept_id,
    COALESCE(o.value_source_concept_id, 0) AS gender_source_concept_id
  FROM
    `{DATASET}.person` p
    LEFT JOIN `{DATASET}.observation` o
     ON p.person_id = o.person_id AND observation_source_concept_id = 1585838)

SELECT g.person_id,
  g.gender_concept_id,
  g.gender_source_concept_id,
  c.concept_code AS gender_source_value
FROM gender g
JOIN `{DATASET}.concept` c
  ON g.gender_source_concept_id = c.concept_id
"""

q = SEX_AT_BIRTH_QUERY.format(DATASET=DEID_DATASET_ID)
sex_at_birth_df = utils.bq.query(q)
render.dataframe(sex_at_birth_df)

q = GENDER_QUERY.format(DATASET=DEID_DATASET_ID)
gender_df = utils.bq.query(q)
render.dataframe(gender_df)

# +
from pandas_gbq.gbq import TableCreationError


def df_to_gbq(df, destination_table, table_schema=None):
    try:
        df.to_gbq(destination_table=destination_table,
                  if_exists='fail',
                  table_schema=table_schema)
    except TableCreationError as table_creation_error:
        print('Using existing {} table'.format(destination_table))


# +
sex_at_birth_log_table = '{SANDBOX}.{DATASET}_dc540_sex_at_birth'.format(
    SANDBOX=SANDBOX, DATASET=DEID_DATASET_ID)
df_to_gbq(sex_at_birth_df, destination_table=sex_at_birth_log_table)

gender_log_table = '{SANDBOX}.{DATASET}_dc540_gender'.format(
    SANDBOX=SANDBOX, DATASET=DEID_DATASET_ID)
df_to_gbq(gender_df, destination_table=gender_log_table)
# -

UPDATED_PERSON_QUERY = """
SELECT
  p.person_id,
  g.gender_concept_id,
  p.year_of_birth,
  p.month_of_birth,
  p.day_of_birth,
  p.birth_datetime,
  p.race_concept_id,
  p.ethnicity_concept_id,
  p.location_id,
  p.provider_id,
  p.care_site_id,
  p.person_source_value,
  g.gender_source_value,
  g.gender_source_concept_id,
  p.race_source_value,
  p.race_source_concept_id,
  p.ethnicity_source_value,
  p.ethnicity_source_concept_id,
  s.sex_at_birth_concept_id,
  s.sex_at_birth_source_value,
  s.sex_at_birth_source_concept_id
FROM {DATASET}.person p
JOIN `{GENDER_LOG}` g       ON p.person_id = g.person_id
JOIN `{SEX_AT_BIRTH_LOG}` s ON p.person_id = s.person_id
""".format(DATASET=DEID_DATASET_ID,
           GENDER_LOG=gender_log_table,
           SEX_AT_BIRTH_LOG=sex_at_birth_log_table)

person_schema = resources.fields_for('person')

person_df = utils.bq.query(UPDATED_PERSON_QUERY)
person_log_table = '{SANDBOX}.{DATASET}_dc540_person'.format(
    SANDBOX=SANDBOX, DATASET=DEID_DATASET_ID)
df_to_gbq(person_df,
          destination_table=person_log_table,
          table_schema=person_schema)

PERSON_HIST_QUERY = """
SELECT
 p.gender_concept_id,
 p.gender_source_value,
 p.gender_source_concept_id,
 p.sex_at_birth_concept_id,
 p.sex_at_birth_source_value,
 p.sex_at_birth_source_concept_id,
 COUNT(person_id) AS person_count
FROM `{PERSON_LOG}` p
GROUP BY
 p.gender_concept_id,
 p.gender_source_value,
 p.gender_source_concept_id,
 p.sex_at_birth_concept_id,
 p.sex_at_birth_source_value,
 p.sex_at_birth_source_concept_id
ORDER BY
 person_count DESC,
 p.gender_concept_id,
 p.gender_source_value,
 p.gender_source_concept_id,
 p.sex_at_birth_concept_id,
 p.sex_at_birth_source_value,
 p.sex_at_birth_source_concept_id
""".format(PERSON_LOG=person_log_table)
person_hist_df = utils.bq.query(PERSON_HIST_QUERY)
render.dataframe(person_hist_df)
