# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.4.2
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# ## This notebook is intended to gather information about the different sites for selected height and weight concept_IDs. This may allow us to better identify cleaning rules that could be implemented or sites with which we must correspond

from google.cloud import bigquery
client = bigquery.Client()
# %load_ext google.cloud.bigquery

# %matplotlib inline
import pandas as pd

from notebooks import parameters
DATASET = parameters.LATEST_DATASET

# +
import os

cwd = os.getcwd()
cwd = str(cwd)
print("Current working directory is: {cwd}".format(cwd=cwd))

# +
to_print = f"Dataset to use: {DATASET}"

print(to_print)
# -

# ## First - let's see what unit_concept_ids sites are using for each of the concept_ids

height_unit_distribution_query = f"""
SELECT
DISTINCT
m.measurement_concept_id, c.concept_name as measurement_concept, 
m.unit_concept_id, c2.concept_name as unit_concept, c2.standard_concept as unit_standard_concept,
COUNT(*) as count
FROM
`{DATASET}.unioned_ehr_measurement` m
JOIN
`{DATASET}.concept` c
ON
m.measurement_concept_id = c.concept_id 
JOIN
`{DATASET}.concept` c2
ON
m.unit_concept_id = c2.concept_id
WHERE
m.measurement_concept_id IN (3036277, 3023540, 3019171)
GROUP BY 1, 2, 3, 4, 5
ORDER BY count DESC
"""

height_unit_distribution = pd.io.gbq.read_gbq(height_unit_distribution_query, dialect='standard')

height_unit_distribution

weight_unit_distribution_query = f"""
SELECT
DISTINCT
m.measurement_concept_id, c.concept_name as measurement_concept, 
m.unit_concept_id, c2.concept_name as unit_concept, c2.standard_concept as unit_standard_concept,
COUNT(*) as count
FROM
`{DATASET}.unioned_ehr_measurement` m
JOIN
`{DATASET}.concept` c
ON
m.measurement_concept_id = c.concept_id 
JOIN
`{DATASET}.concept` c2
ON
m.unit_concept_id = c2.concept_id
WHERE
m.measurement_concept_id IN (3025315, 3013762, 3023166)
GROUP BY 1, 2, 3, 4, 5
ORDER BY count DESC
"""

weight_unit_distribution = pd.io.gbq.read_gbq(weight_unit_distribution_query, dialect='standard')

weight_unit_distribution

# ### Want to see if any site uses > 1 unit_concept_id for the same measurement_concept_id

units_used_per_site_query = f"""
SELECT
DISTINCT
mm.src_hpo_id,
m.measurement_concept_id, c.concept_name as measurement_concept,
COUNT(DISTINCT m.unit_concept_id) as num_units_used

FROM
`{DATASET}.unioned_ehr_measurement` m
JOIN
`{DATASET}._mapping_measurement` mm
ON
m.measurement_id = mm.measurement_id

JOIN
`{DATASET}.concept` c
ON
m.measurement_concept_id = c.concept_id 

WHERE
m.measurement_concept_id IN (3025315, 3013762, 3023166, 3036277, 3023540, 3019171)
GROUP BY 1, 2, 3
ORDER BY num_units_used DESC
"""

units_used_per_site = pd.io.gbq.read_gbq(units_used_per_site_query, dialect='standard')

units_used_per_site

# ## Now ascertaining the data for each site - will include the number of unit_concept_ids used as well just to show any potential reasons for a large range

height_distribution_by_site_query = f"""
SELECT
DISTINCT
mm.src_hpo_id,
m.measurement_concept_id, c.concept_name as measurement_concept,

ROUND(MIN(m.value_as_number) OVER (PARTITION BY mm.src_hpo_id), 2) as min,
ROUND(MAX(m.value_as_number) OVER (PARTITION BY mm.src_hpo_id), 2) as max,

ROUND(AVG(m.value_as_number) OVER (PARTITION BY mm.src_hpo_id), 2) as mean,

ROUND(STDDEV_POP(m.value_as_number) OVER (PARTITION BY mm.src_hpo_id), 2) as stdev,

FROM
`{DATASET}.unioned_ehr_measurement` m
JOIN
`{DATASET}._mapping_measurement` mm
ON
m.measurement_id = mm.measurement_id

JOIN
`{DATASET}.concept` c
ON
m.measurement_concept_id = c.concept_id 

WHERE
m.measurement_concept_id IN (3036277, 3023540, 3019171)
GROUP BY mm.src_hpo_id, m.measurement_concept_id, c.concept_name, m.value_as_number

ORDER BY measurement_concept_id DESC, mean DESC
"""

height_distribution_by_site = pd.io.gbq.read_gbq(height_distribution_by_site_query, dialect='standard')

# +
temp_df = units_used_per_site[['src_hpo_id', 'measurement_concept_id', 'num_units_used']]
matching_cols = ['src_hpo_id', 'measurement_concept_id']


height_distribution_by_site = height_distribution_by_site.merge(temp_df, on=matching_cols)
# -

height_distribution_by_site

height_distribution_by_site.to_csv(f"{cwd}/height_analysis_by_site.csv")

weight_distribution_by_site_query = f"""
SELECT
DISTINCT
mm.src_hpo_id,
m.measurement_concept_id, c.concept_name as measurement_concept,

ROUND(MIN(m.value_as_number) OVER (PARTITION BY mm.src_hpo_id), 2) as min,
ROUND(MAX(m.value_as_number) OVER (PARTITION BY mm.src_hpo_id), 2) as max,

ROUND(AVG(m.value_as_number) OVER (PARTITION BY mm.src_hpo_id), 2) as mean,

ROUND(STDDEV_POP(m.value_as_number) OVER (PARTITION BY mm.src_hpo_id), 2) as stdev,

FROM
`{DATASET}.unioned_ehr_measurement` m
JOIN
`{DATASET}._mapping_measurement` mm
ON
m.measurement_id = mm.measurement_id

JOIN
`{DATASET}.concept` c
ON
m.measurement_concept_id = c.concept_id 

WHERE
m.measurement_concept_id IN (3025315, 3013762, 3023166)
GROUP BY mm.src_hpo_id, m.measurement_concept_id, c.concept_name, m.value_as_number

ORDER BY measurement_concept_id DESC, mean DESC
"""

weight_distribution_by_site = pd.io.gbq.read_gbq(weight_distribution_by_site_query, dialect='standard')

# +
temp_df = units_used_per_site[['src_hpo_id', 'measurement_concept_id', 'num_units_used']]
matching_cols = ['src_hpo_id', 'measurement_concept_id']


weight_distribution_by_site = weight_distribution_by_site.merge(temp_df, on=matching_cols)
# -

weight_distribution_by_site.to_csv(f"{cwd}/weight_analysis_by_site.csv")

weight_distribution_by_site

# ## Want to determine the number of sites / unit

all_unit_df = weight_unit_distribution.append(height_unit_distribution)

all_units = set(all_unit_df['unit_concept_id'].tolist())
all_units = list(all_units)

unit_concept_ids_as_str = str(all_units).strip('[]')

sites_per_unit_query = f"""
SELECT
DISTINCT
m.unit_concept_id, c.concept_name as unit_concept_name,
COUNT(DISTINCT mm.src_hpo_id) as num_sites

FROM
`{DATASET}.unioned_ehr_measurement` m
JOIN
`{DATASET}.concept` c
ON
m.unit_concept_id = c.concept_id

JOIN
`{DATASET}._mapping_measurement` mm
ON
m.measurement_id = mm.measurement_id

WHERE
m.unit_concept_id IN ({unit_concept_ids_as_str})

GROUP BY m.unit_concept_id, c.concept_name
ORDER BY num_sites DESC
"""

sites_per_unit = pd.io.gbq.read_gbq(sites_per_unit_query, dialect='standard')

sites_per_unit

# ## Now let's look at the variance for the height/weight based on the unit_concept_id

height_distribution_by_unit_query = f"""
SELECT
DISTINCT
m.unit_concept_id, c2.concept_name as unit_name, c2.standard_concept as unit_standard_concept,

m.measurement_concept_id, c.concept_name as measurement_concept,

ROUND(MIN(m.value_as_number) OVER (PARTITION BY m.unit_concept_id), 2) as min,
ROUND(MAX(m.value_as_number) OVER (PARTITION BY m.unit_concept_id), 2) as max,

ROUND(AVG(m.value_as_number) OVER (PARTITION BY m.unit_concept_id), 2) as mean,

ROUND(STDDEV_POP(m.value_as_number) OVER (PARTITION BY m.unit_concept_id), 2) as stdev

FROM
`{DATASET}.unioned_ehr_measurement` m

JOIN
`{DATASET}.concept` c
ON
m.measurement_concept_id = c.concept_id 

JOIN
`{DATASET}.concept` c2
ON
m.unit_concept_id = c2.concept_id

JOIN
`{DATASET}._mapping_measurement` mm
ON
m.measurement_id = mm.measurement_id

WHERE
m.measurement_concept_id IN (3036277, 3023540, 3019171)

GROUP BY m.unit_concept_id, c2.concept_name, c2.standard_concept,
m.measurement_concept_id, c.concept_name, m.value_as_number

ORDER BY unit_concept_id DESC, mean DESC
"""

height_distribution_by_unit = pd.io.gbq.read_gbq(height_distribution_by_unit_query, dialect='standard')

# +
temp_df = sites_per_unit[['unit_concept_id', 'num_sites']]
matching_cols = ['unit_concept_id']


height_distribution_by_unit = height_distribution_by_unit.merge(temp_df, on=matching_cols)
# -

height_distribution_by_unit.to_csv(f"{cwd}/height_analysis_by_unit.csv")

height_distribution_by_unit

weight_distribution_by_unit_query = f"""
SELECT
DISTINCT
m.unit_concept_id, c2.concept_name as unit_name, c2.standard_concept as unit_standard_concept,

m.measurement_concept_id, c.concept_name as measurement_concept,

ROUND(MIN(m.value_as_number) OVER (PARTITION BY m.unit_concept_id), 2) as min,
ROUND(MAX(m.value_as_number) OVER (PARTITION BY m.unit_concept_id), 2) as max,

ROUND(AVG(m.value_as_number) OVER (PARTITION BY m.unit_concept_id), 2) as mean,

ROUND(STDDEV_POP(m.value_as_number) OVER (PARTITION BY m.unit_concept_id), 2) as stdev

FROM
`{DATASET}.unioned_ehr_measurement` m

JOIN
`{DATASET}.concept` c
ON
m.measurement_concept_id = c.concept_id 

JOIN
`{DATASET}.concept` c2
ON
m.unit_concept_id = c2.concept_id

JOIN
`{DATASET}._mapping_measurement` mm
ON
m.measurement_id = mm.measurement_id

WHERE
m.measurement_concept_id IN (3025315, 3013762, 3023166)

GROUP BY m.unit_concept_id, c2.concept_name, c2.standard_concept,
m.measurement_concept_id, c.concept_name, m.value_as_number

ORDER BY unit_concept_id DESC, mean DESC
"""

weight_distribution_by_unit = pd.io.gbq.read_gbq(weight_distribution_by_unit_query, dialect='standard')

# +
temp_df = sites_per_unit[['unit_concept_id', 'num_sites']]
matching_cols = ['unit_concept_id']


weight_distribution_by_unit = weight_distribution_by_unit.merge(temp_df, on=matching_cols)
# -

weight_distribution_by_unit.to_csv(f"{cwd}/weight_analysis_by_unit.csv")

weight_distribution_by_unit
