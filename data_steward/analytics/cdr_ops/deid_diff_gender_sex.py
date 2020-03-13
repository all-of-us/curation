import bq_utils
import utils.bq
from notebooks import render, parameters

DEID = parameters.DEID_DATASET_ID
RDR = parameters.RDR_DATASET_ID

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
deid_gender_sex_diff_df = utils.bq.query(q)
deid_has_diff_obs = len(utils.bq.query('row_count > 0').index) > 0
render.dataframe(deid_gender_sex_diff_df)

# Show number of person_ids associated with each combination (person.gender, ppi)
GENDER_SEX_HIST_QUERY = """
WITH sex_at_birth AS (
  SELECT
    person_id,
    value_source_concept_id AS sex_at_birth_concept_id 
  FROM
    `{DATASET}.observation`
  WHERE
    observation_source_concept_id = 1585845) -- BiologicalSexAtBirth_SexAtBirth
,

gender_identity AS (
  SELECT
    person_id,
    value_source_concept_id gender_identity_concept_id
  FROM
    `{DATASET}.observation`
  WHERE
    observation_source_concept_id = 1585838) -- Gender_GenderIdentity
,

gender_sex_hist AS (
 SELECT 
  p.gender_concept_id,
  p.gender_source_concept_id,
  p.gender_source_value,
  s.sex_at_birth_concept_id, 
  g.gender_identity_concept_id,
  COUNT(DISTINCT p.person_id) person_count
 FROM `{DATASET}.person` p
  LEFT JOIN sex_at_birth s USING (person_id)
  LEFT JOIN gender_identity g USING (person_id)
 GROUP BY p.gender_concept_id,
  p.gender_source_concept_id,
  p.gender_source_value,
  s.sex_at_birth_concept_id, 
  g.gender_identity_concept_id)

SELECT 
 pgc.concept_code      AS person_gender_concept,
 pgsc.concept_code     AS person_gender_source_concept,
 h.gender_source_value AS person_gender_source_value,
 sc.concept_code       AS ppi_sex_at_birth_concept,
 gc.concept_code       AS ppi_gender_concept,
 h.person_count
FROM gender_sex_hist h
 JOIN `{DATASET}.concept` pgc ON h.gender_concept_id = pgc.concept_id
 JOIN `{DATASET}.concept` pgsc ON h.gender_source_concept_id  = pgsc.concept_id
 JOIN `{DATASET}.concept` sc ON h.sex_at_birth_concept_id = sc.concept_id
 JOIN `{DATASET}.concept` gc ON h.gender_identity_concept_id = gc.concept_id
ORDER BY person_count DESC, 
  ppi_gender_concept, 
  ppi_sex_at_birth_concept, 
  person_gender_concept, 
  person_gender_source_concept, 
  person_gender_source_value
"""
q = GENDER_SEX_HIST_QUERY.format(DATASET=DEID)
deid_gender_sex_hist_df = utils.bq.query(q)
render.dataframe(deid_gender_sex_hist_df)

# ## RDR dataset
# How many rows in the rdr dataset are described by each scenario?

q = GENDER_SEX_DIFF_QUERY.format(DATASET=RDR)
rdr_gender_sex_diff_df = utils.bq.query(q)
render.dataframe(rdr_gender_sex_diff_df)
