import pandas as pd

from notebooks import render
from notebooks.measurement_hist_utils import *
from notebooks.parameters import VOCAB_DATASET_ID

pd.set_option('display.max_colwidth', -1)
VOCAB = VOCAB_DATASET_ID

# Fully qualified tables
TABLE_BEFORE_CONVERSION = ''  # e.g. deid.measurement
TABLE_AFTER_CONVERSION = ''  # e.g. deid_clean.measurement
print("""TABLE_BEFORE_CONVERSION = {TABLE_BEFORE_CONVERSION}
TABLE_AFTER_CONVERSION = {TABLE_AFTER_CONVERSION}""".format(
    TABLE_BEFORE_CONVERSION=TABLE_BEFORE_CONVERSION,
    TABLE_AFTER_CONVERSION=TABLE_AFTER_CONVERSION))

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
        value_as_number,
        percentile_cont(value_as_number,
          .01) OVER (PARTITION BY m.measurement_concept_id, m.unit_concept_id) AS first_quartile_value_as_number,
        percentile_cont(value_as_number,
          .99) OVER (PARTITION BY m.measurement_concept_id, m.unit_concept_id) AS third_quartile_value_as_number
      FROM
        `{TABLE}` AS m
      JOIN {VOCAB}.concept AS mea
          ON m.measurement_concept_id = mea.concept_id
      JOIN {VOCAB}.concept AS u
          ON m.unit_concept_id = u.concept_id
      WHERE
        m.value_as_number IS NOT NULL AND m.measurement_concept_id IN {CONCEPT_IDS}) m1 ) m2 ) m3
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

HEIGHT_CONCEPT_IDS = [3036277, 3023540, 3019171]
WEIGHT_CONCEPT_IDS = [3025315, 3013762, 3023166]

# ## Compute distributes for heights

# +
height_before_query = UNIT_DISTRIBUTION_QUERY.format(
    TABLE=TABLE_BEFORE_CONVERSION,
    VOCAB=VOCAB,
    CONCEPT_IDS=convert_to_sql_list(HEIGHT_CONCEPT_IDS))

height_before_dist = pd.io.gbq.read_gbq(height_before_query, dialect='standard')
render.dataframe(height_before_dist)

# +
height_after_query = UNIT_DISTRIBUTION_QUERY.format(
    TABLE=TABLE_AFTER_CONVERSION,
    VOCAB=VOCAB,
    CONCEPT_IDS=convert_to_sql_list(HEIGHT_CONCEPT_IDS))

height_after_dist = pd.io.gbq.read_gbq(height_after_query, dialect='standard')
render.dataframe(height_after_dist)
# -
# ## Generate distributions for height measurements

# +
# measurement_concept_ids = get_measurement_concept_ids(height_before_dist)
height_concept_dict = {
    **get_measurement_concept_dict(height_before_dist),
    **get_measurement_concept_dict(height_after_dist)
}
height_before_unit_dict = get_unit_concept_id_dict(height_before_dist)
height_after_unit_dict = get_unit_concept_id_dict(height_after_dist)

for height_concept_id in HEIGHT_CONCEPT_IDS:
    generate_plot(height_concept_id, height_concept_dict, height_before_dist,
                  height_after_dist, height_before_unit_dict,
                  height_after_unit_dict, False, False)
# -

# ## Compute distributes for weights

# +
weight_before_query = UNIT_DISTRIBUTION_QUERY.format(
    TABLE=TABLE_BEFORE_CONVERSION,
    VOCAB=VOCAB,
    CONCEPT_IDS=convert_to_sql_list(WEIGHT_CONCEPT_IDS))

weight_before_dist = pd.io.gbq.read_gbq(weight_before_query, dialect='standard')
render.dataframe(weight_before_dist)

# +
weight_after_query = UNIT_DISTRIBUTION_QUERY.format(
    TABLE=TABLE_AFTER_CONVERSION,
    VOCAB=VOCAB,
    CONCEPT_IDS=convert_to_sql_list(WEIGHT_CONCEPT_IDS))

weight_after_dist = pd.io.gbq.read_gbq(weight_after_query, dialect='standard')
render.dataframe(weight_after_dist)
# -

# ## Generate distributions for weight measurements

# +
weight_concept_dict = {
    **get_measurement_concept_dict(weight_before_dist),
    **get_measurement_concept_dict(weight_after_dist)
}
weight_before_unit_dict = get_unit_concept_id_dict(weight_before_dist)
weight_after_unit_dict = get_unit_concept_id_dict(weight_after_dist)

for weight_concept_id in WEIGHT_CONCEPT_IDS:
    generate_plot(weight_concept_id, weight_concept_dict, weight_before_dist,
                  weight_after_dist, weight_before_unit_dict,
                  weight_after_unit_dict, False, False)
# -


