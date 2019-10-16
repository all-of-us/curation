from defaults import DEFAULT_DATASETS
import bq
import pandas as pd
import render
from parameters import *

pd.set_option('display.max_colwidth', -1)
VOCAB = DEFAULT_DATASETS.latest.vocabulary

print("""DATASET_BEFORE_CONVERSION = {DATASET_BEFORE_CONVERSION}
DATASET_AFTER_CONVERSION = {DATASET_AFTER_CONVERSION}
TABLE_BEFORE_CONVERSION = {TABLE_BEFORE_CONVERSION}
TABLE_AFTER_CONVERSION = {TABLE_AFTER_CONVERSION}
UNIT_MAPPING = {UNIT_MAPPING}""".format(
    DATASET_BEFORE_CONVERSION=DATASET_BEFORE_CONVERSION,
    DATASET_AFTER_CONVERSION=DATASET_AFTER_CONVERSION,
    TABLE_BEFORE_CONVERSION=TABLE_BEFORE_CONVERSION, 
    TABLE_AFTER_CONVERSION=TABLE_AFTER_CONVERSION,
    UNIT_MAPPING=UNIT_MAPPING))

# # Query for counting the number of units converted

UNIT_CONVERSION_COUNT_TEMPLATE = """
SELECT
  before.measurement_concept_id,
  c.concept_name AS measurement_concept_name,
  u1.concept_name AS before_unit,
  u2.concept_name AS after_unit,
  before.mea_count AS before_mea_count,
  after.mea_count AS after_mea_count
FROM (
  SELECT
    measurement_concept_id,
    unit_concept_id,
    COUNT(*) AS mea_count
  FROM
    `{DATASET_BEFORE_CONVERSION}.{TABLE_BEFORE_CONVERSION}` AS m
  JOIN (
    SELECT DISTINCT 
      measurement_concept_id,
      unit_concept_id
    FROM
      `{DATASET_BEFORE_CONVERSION}.{UNIT_MAPPING}`) AS u
  USING
    (measurement_concept_id,
      unit_concept_id)
  GROUP BY
    measurement_concept_id,
    unit_concept_id ) before
JOIN
  `{DATASET_BEFORE_CONVERSION}.{UNIT_MAPPING}` AS um
ON
  before.measurement_concept_id = um.measurement_concept_id
  AND before.unit_concept_id = um.unit_concept_id
JOIN (
  SELECT
    m.measurement_concept_id,
    m.unit_concept_id,
    COUNT(*) AS mea_count
  FROM
    `{DATASET_AFTER_CONVERSION}.{TABLE_AFTER_CONVERSION}` AS m
  JOIN (
    SELECT DISTINCT 
      measurement_concept_id,
      set_unit_concept_id
    FROM
      `{DATASET_BEFORE_CONVERSION}.{UNIT_MAPPING}`) AS u
  ON
    m.measurement_concept_id = u.measurement_concept_id
    AND m.unit_concept_id = u.set_unit_concept_id
  GROUP BY
    m.measurement_concept_id,
    m.unit_concept_id ) after
ON
  after.measurement_concept_id = um.measurement_concept_id
  AND after.unit_concept_id = um.set_unit_concept_id
JOIN
  `{VOCAB}.concept` AS c
ON
  before.measurement_concept_id = c.concept_id
JOIN
  `{VOCAB}.concept` AS u1
ON
  before.unit_concept_id = u1.concept_id
JOIN
  `{VOCAB}.concept` AS u2
ON
  after.unit_concept_id = u2.concept_id
ORDER BY
  after.measurement_concept_id,
  after.unit_concept_id
"""

# # Query for computing the basic stats for the units converted

UNIT_CONVERSION_STATS_TEMPLATE = """
SELECT
  before.measurement_concept_id,
  c.concept_name AS measurement_concept_name,
  u1.concept_name  AS before_unit_name,
  before.first_quartile_value_as_number AS before_first_quartile,
  before.median_value_as_number AS before_median,
  before.third_quartile_value_as_number AS before_third_quartile,
  CONCAT(CAST(ROUND((before.number_records / after.number_records) * 100, 3) AS STRING), '%') AS percentage,
  um.transform_value_as_number,
  u2.concept_name AS after_unit_name,
  after.first_quartile_value_as_number AS after_first_quartile,
  after.median_value_as_number AS after_median,
  after.third_quartile_value_as_number AS after_third_quartile
FROM
(
  SELECT DISTINCT
    measurement_concept_id,
    unit_concept_id,
    COUNT(*) OVER (PARTITION BY measurement_concept_id, unit_concept_id) AS number_records,
    percentile_cont(value_as_number,.25) over (partition by measurement_concept_id, unit_concept_id) AS first_quartile_value_as_number,
    percentile_cont(value_as_number,.5) over (partition by measurement_concept_id, unit_concept_id) AS median_value_as_number,
    percentile_cont(value_as_number,.75) over (partition by measurement_concept_id, unit_concept_id) AS third_quartile_value_as_number
  FROM
    `{DATASET_BEFORE_CONVERSION}.{TABLE_BEFORE_CONVERSION}` AS m
  JOIN
    (
    SELECT
      DISTINCT measurement_concept_id,
      unit_concept_id
    FROM
      `{DATASET_BEFORE_CONVERSION}.{UNIT_MAPPING}`) AS u
  USING
    (measurement_concept_id, unit_concept_id)
    
) before JOIN `{DATASET_BEFORE_CONVERSION}.{UNIT_MAPPING}` AS um
  ON before.measurement_concept_id = um.measurement_concept_id
      AND before.unit_concept_id = um.unit_concept_id
JOIN (
  SELECT DISTINCT
    m.measurement_concept_id,
    m.unit_concept_id,
    u.set_unit_concept_id,
    COUNT(*) OVER (PARTITION BY m.measurement_concept_id, m.unit_concept_id) AS number_records,
    percentile_cont(value_as_number,.25) over (partition by m.measurement_concept_id, m.unit_concept_id) AS first_quartile_value_as_number,
    percentile_cont(value_as_number,.5) over (partition by m.measurement_concept_id, m.unit_concept_id) AS median_value_as_number,
    percentile_cont(value_as_number,.75) over (partition by m.measurement_concept_id, m.unit_concept_id) AS third_quartile_value_as_number
  FROM
    `{DATASET_AFTER_CONVERSION}.{TABLE_AFTER_CONVERSION}` AS m
  JOIN
    (
    SELECT
      DISTINCT measurement_concept_id,
      set_unit_concept_id
    FROM
      `{DATASET_BEFORE_CONVERSION}.{UNIT_MAPPING}`) AS u
    ON m.measurement_concept_id = u.measurement_concept_id 
      AND m.unit_concept_id  = u.set_unit_concept_id 
) after
  ON after.measurement_concept_id = um.measurement_concept_id
    AND after.unit_concept_id = um.set_unit_concept_id
JOIN `{VOCAB}.concept` AS c
  ON before.measurement_concept_id  = c.concept_id
JOIN `{VOCAB}.concept` AS u1
  ON before.unit_concept_id  = u1.concept_id
JOIN `{VOCAB}.concept` AS u2
  ON after.unit_concept_id  = u2.concept_id
ORDER BY after.measurement_concept_id, after.unit_concept_id
"""

UNIT_DISTRIBUTION_QUERY = """
SELECT
  m2.measurement_concept_id, m2.unit_concept_id, m2.bin, COUNT(*) AS bin_freq
FROM
(
  SELECT
    measurement_concept_id,
    unit_concept_id,
    value_as_number,
    first_quartile_value_as_number,
    third_quartile_value_as_number,
    CASE
      WHEN value_as_number < first_quartile_value_as_number THEN 1
      WHEN value_as_number > third_quartile_value_as_number THEN 10
      WHEN third_quartile_value_as_number - first_quartile_value_as_number = 0 THEN -1
      ELSE CAST(TRUNC((value_as_number -first_quartile_value_as_number) * 8 / (third_quartile_value_as_number - first_quartile_value_as_number) + 2) AS INT64)
    END AS bin
  FROM
  (
    SELECT
      m.measurement_concept_id,
      m.unit_concept_id,
      value_as_number,
      percentile_cont(value_as_number,.05) over (partition by m.measurement_concept_id, m.unit_concept_id) AS first_quartile_value_as_number,
      percentile_cont(value_as_number,.95) over (partition by m.measurement_concept_id, m.unit_concept_id) AS third_quartile_value_as_number
    FROM
      `{DATASET}.{TABLE}` AS m
    JOIN
      (
      SELECT
        DISTINCT measurement_concept_id,
        {UNIT_CONCEPT_ID_COLUMN}
      FROM
        `{DATASET}.{UNIT_MAPPING}`) AS u
    ON m.measurement_concept_id = u.measurement_concept_id
      AND m.unit_concept_id = u.{UNIT_CONCEPT_ID_COLUMN}
    WHERE m.value_as_number IS NOT NULL
  ) m1
) m2
GROUP BY m2.measurement_concept_id, m2.unit_concept_id, m2.bin
ORDER BY m2.measurement_concept_id, m2.unit_concept_id, m2.bin
"""

# Check the number of records associated with the units before and after the unit transformation. Theoretically the number of records units should be same as before after the unit transformation.

unit_conversion_count_query = UNIT_CONVERSION_COUNT_TEMPLATE.format(
                            DATASET_BEFORE_CONVERSION=DATASET_BEFORE_CONVERSION, 
                            DATASET_AFTER_CONVERSION=DATASET_AFTER_CONVERSION, 
                            TABLE_BEFORE_CONVERSION=TABLE_BEFORE_CONVERSION,
                            TABLE_AFTER_CONVERSION=TABLE_AFTER_CONVERSION,
                            UNIT_MAPPING=UNIT_MAPPING,
                            VOCAB=VOCAB)
unit_conversion_count = bq.query(unit_conversion_count_query)
render.dataframe(unit_conversion_count)

# Compute the first, median and third quartiles before and after the unit transformation

unit_conversion_stats_query = UNIT_CONVERSION_STATS_TEMPLATE.format(
                            DATASET_BEFORE_CONVERSION=DATASET_BEFORE_CONVERSION, 
                            DATASET_AFTER_CONVERSION=DATASET_AFTER_CONVERSION, 
                            TABLE_BEFORE_CONVERSION=TABLE_BEFORE_CONVERSION,
                            TABLE_AFTER_CONVERSION=TABLE_AFTER_CONVERSION,
                            UNIT_MAPPING=UNIT_MAPPING,
                            VOCAB=VOCAB)
unit_conversion_stats = bq.query(unit_conversion_stats_query)
render.dataframe(unit_conversion_stats)

# +
before_unit_conversion_dist_query = UNIT_DISTRIBUTION_QUERY.format(
                            DATASET=DATASET_AFTER_CONVERSION, 
                            TABLE=TABLE_AFTER_CONVERSION,
                            UNIT_MAPPING=UNIT_MAPPING,
                            UNIT_CONCEPT_ID_COLUMN='unit_concept_id')


before_unit_conversion_dist = bq.query(before_unit_conversion_dist_query)
render.dataframe(before_unit_conversion_dist)

# +
after_unit_conversion_dist_query = UNIT_DISTRIBUTION_QUERY.format(
                            DATASET=DATASET_AFTER_CONVERSION, 
                            TABLE=TABLE_AFTER_CONVERSION,
                            UNIT_MAPPING=UNIT_MAPPING,
                            UNIT_CONCEPT_ID_COLUMN='set_unit_concept_id')


after_unit_conversion_dist = bq.query(after_unit_conversion_dist_query)
render.dataframe(after_unit_conversion_dist)
# -


