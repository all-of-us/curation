import json
import pandas as pd

from notebooks import render
from notebooks.parameters import SANDBOX, VOCAB_DATASET_ID
from notebooks.measurement_hist_utils import *

pd.set_option('display.max_colwidth', -1)
VOCAB = VOCAB_DATASET_ID

# Fully qualified tables
TABLE_BEFORE_CONVERSION = ''  # e.g. deid.measurement
TABLE_AFTER_CONVERSION = ''  # e.g. deid_clean.measurement
UNIT_MAPPING = '{SANDBOX}.unit_mapping'.format(SANDBOX=SANDBOX)
print("""TABLE_BEFORE_CONVERSION = {TABLE_BEFORE_CONVERSION}
TABLE_AFTER_CONVERSION = {TABLE_AFTER_CONVERSION}
UNIT_MAPPING = {UNIT_MAPPING}""".format(
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
    `{TABLE_BEFORE_CONVERSION}` AS m
  JOIN (
    SELECT DISTINCT 
      measurement_concept_id,
      unit_concept_id
    FROM
      `{UNIT_MAPPING}`) AS u
  USING
    (measurement_concept_id,
      unit_concept_id)
  GROUP BY
    measurement_concept_id,
    unit_concept_id ) before
JOIN
  `{UNIT_MAPPING}` AS um
ON
  before.measurement_concept_id = um.measurement_concept_id
  AND before.unit_concept_id = um.unit_concept_id
JOIN (
  SELECT
    m.measurement_concept_id,
    m.unit_concept_id,
    COUNT(*) AS mea_count
  FROM
    `{TABLE_AFTER_CONVERSION}` AS m
  JOIN (
    SELECT DISTINCT 
      measurement_concept_id,
      set_unit_concept_id
    FROM
      `{UNIT_MAPPING}`) AS u
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
    `{TABLE_BEFORE_CONVERSION}` AS m
  JOIN
    (
    SELECT
      DISTINCT measurement_concept_id,
      unit_concept_id
    FROM
      `{UNIT_MAPPING}`) AS u
  USING
    (measurement_concept_id, unit_concept_id)
    
) before JOIN `{UNIT_MAPPING}` AS um
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
    `{TABLE_AFTER_CONVERSION}` AS m
  JOIN
    (
    SELECT
      DISTINCT measurement_concept_id,
      set_unit_concept_id
    FROM
      `{UNIT_MAPPING}`) AS u
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
  CAST(m3.measurement_concept_id AS STRING) AS measurement_concept_id,
  CAST(m3.unit_concept_id AS STRING) AS unit_concept_id,
  m3.measurement_concept_name,
  m3.unit_concept_name,
  CAST(m3.bin AS INT64) AS bin,
  m3.bin_width,
  m3.bin_centroid,
  m3.bin_lower_bound,
  m3.bin_upper_bound,
  COUNT(*) AS bin_count
FROM (
  SELECT
    m2.*,
    (m2.bin - 2) * m2.bin_width + m2.first_quartile_value_as_number AS bin_lower_bound,
    (m2.bin - 1) * m2.bin_width + m2.first_quartile_value_as_number AS bin_upper_bound,
    ((m2.bin - 2) * m2.bin_width + (m2.bin - 1) * m2.bin_width) / 2 + m2.first_quartile_value_as_number AS bin_centroid
  FROM (
    SELECT
      measurement_concept_id,
      measurement_concept_name,
      unit_concept_id,
      unit_concept_name,
      value_as_number,
      first_quartile_value_as_number,
      third_quartile_value_as_number,
      (third_quartile_value_as_number - first_quartile_value_as_number) / 18 AS bin_width,
      CASE
        WHEN value_as_number < first_quartile_value_as_number THEN 1
        WHEN value_as_number > third_quartile_value_as_number THEN 20
        WHEN third_quartile_value_as_number - first_quartile_value_as_number = 0 THEN NULL
      ELSE
      CAST(TRUNC((value_as_number -first_quartile_value_as_number) * 18 / (third_quartile_value_as_number - first_quartile_value_as_number) + 2) AS INT64)
    END
      AS bin
    FROM (
      SELECT
        m.measurement_concept_id,
        mea.concept_name AS measurement_concept_name,
        m.unit_concept_id,
        u.concept_name AS unit_concept_name,
        um.transform_value_as_number,
        value_as_number,
        percentile_cont(value_as_number,
          .01) OVER (PARTITION BY m.measurement_concept_id, m.unit_concept_id) AS first_quartile_value_as_number,
        percentile_cont(value_as_number,
          .99) OVER (PARTITION BY m.measurement_concept_id, m.unit_concept_id) AS third_quartile_value_as_number
      FROM
        `{TABLE}` AS m
      JOIN (
        SELECT
          DISTINCT measurement_concept_id,
          transform_value_as_number,
          {UNIT_CONCEPT_ID_COLUMN}
        FROM
          `{UNIT_MAPPING}`) AS um
          ON
            m.measurement_concept_id = um.measurement_concept_id
            AND m.unit_concept_id = um.{UNIT_CONCEPT_ID_COLUMN}
      JOIN {VOCAB}.concept AS mea
          ON m.measurement_concept_id = mea.concept_id
      JOIN {VOCAB}.concept AS u
          ON m.unit_concept_id = u.concept_id
      WHERE
        m.value_as_number IS NOT NULL ) m1 ) m2 ) m3
WHERE m3.bin IS NOT NULL
GROUP BY
  m3.measurement_concept_id,
  m3.measurement_concept_name,
  m3.unit_concept_id,
  m3.unit_concept_name,
  m3.bin,
  m3.bin_width,
  m3.bin_centroid,
  m3.bin_lower_bound,
  m3.bin_upper_bound
ORDER BY
  m3.measurement_concept_id,
  m3.unit_concept_id,
  m3.bin
"""

# Check the number of records associated with the units before and after the unit transformation. Theoretically the number of records units should be same as before after the unit transformation.

unit_conversion_count_query = UNIT_CONVERSION_COUNT_TEMPLATE.format(
    TABLE_BEFORE_CONVERSION=TABLE_BEFORE_CONVERSION,
    TABLE_AFTER_CONVERSION=TABLE_AFTER_CONVERSION,
    UNIT_MAPPING=UNIT_MAPPING,
    VOCAB=VOCAB)
unit_conversion_count = pd.io.gbq.read_gbq(unit_conversion_count_query,
                                           dialect='standard')
render.dataframe(unit_conversion_count)

# Compute the first, median and third quartiles before and after the unit transformation

unit_conversion_stats_query = UNIT_CONVERSION_STATS_TEMPLATE.format(
    TABLE_BEFORE_CONVERSION=TABLE_BEFORE_CONVERSION,
    TABLE_AFTER_CONVERSION=TABLE_AFTER_CONVERSION,
    UNIT_MAPPING=UNIT_MAPPING,
    VOCAB=VOCAB)
unit_conversion_stats = pd.io.gbq.read_gbq(unit_conversion_stats_query,
                                           dialect='standard')
unit_conversion_stats.measurement_concept_id = unit_conversion_stats.measurement_concept_id.apply(
    str)
render.dataframe(unit_conversion_stats)

before_unit_conversion_dist_query = UNIT_DISTRIBUTION_QUERY.format(
    TABLE=TABLE_BEFORE_CONVERSION,
    VOCAB=VOCAB,
    UNIT_MAPPING=UNIT_MAPPING,
    UNIT_CONCEPT_ID_COLUMN='unit_concept_id')
before_unit_conversion_dist = pd.io.gbq.read_gbq(
    before_unit_conversion_dist_query, dialect='standard')
render.dataframe(before_unit_conversion_dist)

# +
after_unit_conversion_dist_query = UNIT_DISTRIBUTION_QUERY.format(
    TABLE=TABLE_AFTER_CONVERSION,
    VOCAB=VOCAB,
    UNIT_MAPPING=UNIT_MAPPING,
    UNIT_CONCEPT_ID_COLUMN='set_unit_concept_id')

after_unit_conversion_dist = pd.io.gbq.read_gbq(
    after_unit_conversion_dist_query, dialect='standard')
render.dataframe(after_unit_conversion_dist)

# -

# ### Generate the dictionaries for plotting

measurement_concept_ids = get_measurement_concept_ids(
    before_unit_conversion_dist)
measurement_concept_dict = get_measurement_concept_dict(
    before_unit_conversion_dist)
before_unit_dict = get_unit_concept_id_dict(before_unit_conversion_dist)
after_unit_dict = get_unit_concept_id_dict(after_unit_conversion_dist)

print(json.dumps(measurement_concept_dict, indent=1))

# ### Distribution comparison for Systolic blood pressure (measurement_concept_id = 3004249)

generate_plot(3004249, measurement_concept_dict, before_unit_conversion_dist,
              after_unit_conversion_dist, before_unit_dict, after_unit_dict,
              False, False)

# ### Distribution comparison for Heart rate (measurement_concept_id = 3027018)

generate_plot(3027018, measurement_concept_dict, before_unit_conversion_dist,
              after_unit_conversion_dist, before_unit_dict, after_unit_dict,
              False, False)

# ### Distribution comparison for the first 20 measurement_concept_ids in the entire measurement_concept_ids
# The retreived 20 measurement_concept_ids are in random order. Only 20 plots are printed in this example because there is a limition of 20 plots being printed at the same time

for measurement_concept_id in measurement_concept_ids[0:20]:
    generate_plot(measurement_concept_id, measurement_concept_dict,
                  before_unit_conversion_dist, after_unit_conversion_dist,
                  before_unit_dict, after_unit_dict, False, False)
