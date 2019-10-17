from defaults import DEFAULT_DATASETS
from parameters import *
import bq
import pandas as pd
import render
import matplotlib.pyplot as plt
import numpy as np

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
  m3.measurement_concept_id,
  m3.unit_concept_id,
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
      unit_concept_id,
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
        m.unit_concept_id,
        value_as_number,
        percentile_cont(value_as_number,
          .01) OVER (PARTITION BY m.measurement_concept_id, m.unit_concept_id) AS first_quartile_value_as_number,
        percentile_cont(value_as_number,
          .99) OVER (PARTITION BY m.measurement_concept_id, m.unit_concept_id) AS third_quartile_value_as_number
      FROM
        `{DATASET}.{TABLE}` AS m
      JOIN (
        SELECT
          DISTINCT measurement_concept_id,
          {UNIT_CONCEPT_ID_COLUMN}
        FROM
          `{DATASET}.{UNIT_MAPPING}`) AS u
      ON
        m.measurement_concept_id = u.measurement_concept_id
        AND m.unit_concept_id = u.{UNIT_CONCEPT_ID_COLUMN}
      WHERE
        m.value_as_number IS NOT NULL ) m1 ) m2 ) m3
GROUP BY
  m3.measurement_concept_id,
  m3.unit_concept_id,
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
                            DATASET=DATASET_BEFORE_CONVERSION, 
                            TABLE=TABLE_BEFORE_CONVERSION,
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
# + {}
def init_histogram(axis, sub_dataframe):
    centroids = sub_dataframe['bin_centroid']
    bins = len(sub_dataframe)
    weights = sub_dataframe['bin_count']
    min_bin = sub_dataframe['bin_lower_bound'].min()
    max_bin = sub_dataframe['bin_upper_bound'].max()
    counts_, bins_, _ = axis.hist(centroids, bins=bins,
                             weights=weights, range=(min_bin, max_bin))
    
def get_measurement_concept_ids(df):
    return df['measurement_concept_id'].unique()


def get_unit_concept_ids(df, measurement_concept_id=None):
    
    unit_concept_ids = []
    if measurement_concept_id is None:
        unit_concept_ids = df['unit_concept_id'].unique()
    else:
        unit_concept_ids = df.loc[df['measurement_concept_id'] == measurement_concept_id, 'unit_concept_id'].unique()
    return unit_concept_ids

def get_sub_dataframe(df, measurement_concept_id, unit_concept_id):
    indexes = (df['measurement_concept_id'] == measurement_concept_id) \
        & (df['unit_concept_id'] == unit_concept_id)
    return df[indexes]


# +
measurement_concept_ids = get_measurement_concept_ids(before_unit_conversion_dist)

for measurement_concept_id in measurement_concept_ids[0:10]:
    units_before = get_unit_concept_ids(before_unit_conversion_dist, measurement_concept_id)
    units_after = get_unit_concept_ids(after_unit_conversion_dist, measurement_concept_id)
    for unit_after in units_after:

        fig, axs = plt.subplots(len(units_before), 2, sharex=True, sharey=True)
        fig.suptitle('Measurement {}, standard unit {}'.format(measurement_concept_id, unit_after))
        counter = 0
        sub_df_after = get_sub_dataframe(after_unit_conversion_dist, measurement_concept_id, unit_after)

        for unit_before in units_before:
            sub_df_before = get_sub_dataframe(before_unit_conversion_dist, measurement_concept_id, unit_before)
            if len(units_before) == 1:
                init_histogram(axs[0], sub_df_before)
                axs[0].set_title('before unit {}'.format(unit_before))

                init_histogram(axs[1], sub_df_after)
                axs[1].set_title('after unit {}'.format(unit_after))
            else:
                init_histogram(axs[counter][0], sub_df_before)
                axs[counter][0].set_title('before unit {}'.format(unit_before))

                init_histogram(axs[counter][1], sub_df_after)
                axs[counter][1].set_title('after unit {}'.format(unit_after))

            counter += 1
# -

plt.rcParams['figure.figsize'] = [18, 18]
#_, axs = plt.subplots(1, 1, squeeze=False)


