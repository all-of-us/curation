# +
import utils.bq
from notebooks import parameters

DEID = parameters.DEID_DATASET_ID
# -

# Derive combined dataset associated with this deid dataset
deid_index = DEID.index('_deid')
COMBINED = DEID[:deid_index]

# ## How many generalized responses are duplicated?

DUPLICATE_GEN_RACE_QUERY = """
SELECT
  COUNT(*)
FROM
  `{DEID}.observation` AS o
JOIN
(
  SELECT
    observation_id
  FROM (
    SELECT
      DENSE_RANK() OVER(PARTITION BY person_id,
        observation_source_concept_id,
        value_source_concept_id
      ORDER BY
        observation_datetime DESC,
        observation_id DESC) AS rank_order,
      observation_id
    FROM
      `{DEID}.observation`
    JOIN
      `{COMBINED}._mapping_observation` as map
    USING
    (observation_id)
      WHERE observation_source_concept_id IN (1586140, 1585838, 1585952) -- race, gender, employment status
      AND value_source_concept_id IN (2000000008, 2000000005, 2000000004, 2000000002)
      AND map.src_hpo_id like "rdr"
    ) o
  WHERE
    o.rank_order <> 1
) unique_observation_ids
ON o.observation_id = unique_observation_ids.observation_id
"""
q = DUPLICATE_GEN_RACE_QUERY.format(DEID=DEID, COMBINED=COMBINED)
df = utils.bq.query(q)
df
