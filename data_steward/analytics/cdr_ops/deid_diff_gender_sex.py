from defaults import DEFAULT_DATASETS
import bq

DEID = DEFAULT_DATASETS.latest.deid
RDR = DEFAULT_DATASETS.latest.rdr

# Privacy requirements indicate that in survey responses where reported gender identity (`value_source_concept_id` in observation rows where `observation_source_concept_id` is [1585838](http://athena.ohdsi.org/search-terms/terms/1585838)) differs from sex assigned at birth (`value_source_concept_id` in observation rows where `observation_source_concept_id` is [1585845](http://athena.ohdsi.org/search-terms/terms/1585845)) the gender identity should be generalized using the concept ID [2000000002](https://github.com/all-of-us/curation/blob/develop/data_steward/resources/aou_general/concept.csv#L4).

# Determine the number of rows that meet criteria for gender generalization
GENDER_SEX_DIFF_QUERY = """
SELECT 
 'GenderIdentity_Woman' AS rdr_gender_identity,
 'SexAtBirth_Male'      AS rdr_sex,
 COUNT(1) row_count
FROM `{DATASET}.observation`
WHERE
  observation_source_concept_id = 1585838 -- Gender_GenderIdentity
  AND value_source_concept_id = 1585840   -- GenderIdentity_Woman
  AND person_id IN (
    SELECT
      person_id
    FROM
      `{DATASET}.observation`
    WHERE
      observation_source_concept_id = 1585845 -- BiologicalSexAtBirth_SexAtBirth
        AND value_source_concept_id = 1585846 -- SexAtBirth_Male
 )

UNION ALL

SELECT 
 'GenderIdentity_Man' AS rdr_gender_identity,
 'SexAtBirth_Female'  AS rdr_sex,
 COUNT(1) row_count
FROM `{DATASET}.observation`
WHERE
  observation_source_concept_id = 1585838 -- Gender_GenderIdentity
  AND value_source_concept_id = 1585839   -- GenderIdentity_Man
  AND person_id IN (
  SELECT
    person_id
  FROM
    `{DATASET}.observation`
  WHERE
    observation_source_concept_id = 1585845  -- BiologicalSexAtBirth_SexAtBirth
      AND value_source_concept_id = 1585847) -- SexAtBirth_Female
"""

# ## Deid dataset
# How many rows in the deid dataset **do not** have the appropriate generalization applied for each scenario?

q = GENDER_SEX_DIFF_QUERY.format(DATASET=DEID)
deid_gender_sex_diff_df = bq.query(q)
deid_has_diff_obs = len(deid_gender_sex_diff_df.query('row_count > 0').index) > 0
deid_gender_sex_diff_df

# ## RDR dataset
# How many rows in the rdr dataset are described by each scenario?

q = GENDER_SEX_DIFF_QUERY.format(DATASET=RDR)
rdr_gender_sex_diff_df = bq.query(q)
rdr_gender_sex_diff_df
