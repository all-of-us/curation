# -*- coding: utf-8 -*-
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

# ## Function is used to create the models as designated by [EDQ-456](https://precisionmedicineinitiative.atlassian.net/browse/EDQ-456?focusedCommentId=61806&page=com.atlassian.jira.plugin.system.issuetabpanels%3Acomment-tabpanel#comment-61806). 
#
# ### These models will include the following:
# - Leverage the concept_ancestor table in an attempt to get a larger ‘scope’ of potential concept_ids associated with height and weight
#
# - Create visualizations for the height, weight, and calculated BMI after excluding set amounts of data
#
# - 1 stdev, 2 stdev, 3 stdev, etc.
#
# - The calculated BMI would perhaps be useful to see if erroneous heights are almost always associated with erroneous weights
#
# - This could also be an interesting contrast to the BMI provided by its own concept_id.

from google.cloud import bigquery
# %reload_ext google.cloud.bigquery
client = bigquery.Client()
# %load_ext google.cloud.bigquery
import numpy as np
from matplotlib import pyplot as plt

# +
from notebooks import parameters
DATASET = parameters.LATEST_DATASET
rounding_val = 2

print(f"Dataset to use: {DATASET}")

# +
import warnings

warnings.filterwarnings('ignore')
import pandas as pd
import matplotlib.pyplot as plt
import os

plt.style.use('ggplot')
pd.options.display.max_rows = 999
pd.options.display.max_columns = 999
pd.options.display.max_colwidth = 999


def cstr(s, color='black'):
    return "<text style=color:{}>{}</text>".format(color, s)


# -

cwd = os.getcwd()
cwd = str(cwd)
print(f"Current working directory is: {cwd}")

# # Part I: Settting up the foundations
# - Queries that will be used to exclude
#     - out of scope conditions
#     - persons to not include in the analysis
# - Seeing the distribution of units for the applicable measurements
# - Distribution of the concepts used for height and weight

# ## The strings below are the ancestor concept IDs that we will use as 'ancestors'

# +
height_concepts = "1003912, 45876161, 1003116, 1003232, 40655804, 45876162"

weight_concepts = "1002670, 45876171, 1003383, 1004141, 40655805, 45876172, 3042378"
# -

# ## The string below will be used to exclude certain concept_ids

caveat_string = """
  AND LOWER(c.domain_id) LIKE '%measurement%'
  AND LOWER(c.concept_name) NOT LIKE '%birth%' -- excluding as outliers
  AND LOWER(c.concept_name) NOT LIKE '%fetal%'
  AND LOWER(c.concept_name) NOT LIKE '%fetus%'
  AND LOWER(c.concept_name) NOT LIKE '%bmi%'
  AND LOWER(c.concept_name) NOT LIKE '%lower segment%'
  AND LOWER(c.concept_name) NOT LIKE '%upper segment%'
  AND LOWER(c.concept_name) NOT LIKE '%body fat%'
  AND LOWER(c.concept_name) NOT LIKE '%muscle mass%'
  AND LOWER(c.concept_name) NOT LIKE '%percentile%'
  AND LOWER(c.concept_name) NOT LIKE '%weight-for-length%'
  AND LOWER(c.concept_name) NOT LIKE '%difference%'
"""

# ## We would also want to exclude persons with certain conditions, such as dwarfism, that would explain their height/weight that may deviate specifically from the means/medians/etc.

outlier_pts_height_query = f"""
SELECT
DISTINCT
co.person_id as outlier_patient
FROM
`{DATASET}.unioned_ehr_condition_occurrence` co
WHERE
co.condition_concept_id IN (80502, 77079, 4078547, 4165513,
      4027406, 2766929, 4263905, 4192565, 4210444, 4219032, 4058850, 4023190, 4205235, 2767168,
      4239226, 40482475, 4162099, 4239226, 2767165, 2859839, 2777592, 4105090, 45766159, 321661,
      2799062, 2777583, 2784242, 4070317, 4203318, 37017415, 2767188, 4069953, 4242396, 2777581,
      2105109, 2102534, 4078713, 2777590, 2767167, 4261484, 4196391, 2767152, 2000074, 4260962,
      4000503, 2767153, 2784251, 4108565, 2824338, 2886800, 2865747, 2824346, 2105449, 2806603,
      2824349, 2104872, 2865752, 2104874, 2105088, 2105091, 2819470, 2824342, 2859836, 2812154,
      2891857, 2819466, 2806610, 2847074, 2847082, 2878890, 2832426, 2891855, 2871198, 2799069,
      2852300, 2836961, 2819467, 2878892, 2767175, 2767405, 2767166, 2767179, 2767150, 2766943,
      2766933, 2767145, 2767202, 2767174, 2767141, 2767399, 2767156, 2767152, 2767194, 2767159,
      2767143, 4119910, 4061286, 4002134, 4194335, 2760553, 4087704, 4338528, 2777584, 2806605,
      2767163, 2784248, 4076601, 4167353, 2102532, 2000076, 2767189, 4210148, 2784249, 2000075,
      2897708, 2005882, 2005888, 2767161, 2762965, 4078579, 2006247, 2777596, 2777585, 44783110,
      4321732, 4012842, 2777587, 2834834, 2819468, 4179666, 2897701, 4003481, 2824336, 2886812,
      2891852, 2837088, 2832429, 2859828, 2824344, 2865756, 2105447, 2806606, 2859830, 2836954,
      2812148, 2886808, 2799066, 2824341, 2824354, 2865755, 2865750, 2799060, 2897699, 2812147,
      2847079, 2847075, 2871203, 2812156, 2812153, 2865746, 2891854, 2878905, 2767163, 2767407,
      2767157, 2767173, 2767185, 2767410, 2767167, 2110709, 4152592, 2832428, 2806609, 2767146,
      4249895, 77365, 2760549, 2767151, 4034835, 2767157, 35622506, 4197307, 2767148, 2767185,
      4274954, 2767147, 4234682, 37118455, 2105349, 2762967, 2832436, 4079521, 4173335, 4078586,
      4290776, 4305518, 4323766, 2767181, 2784245, 2812150, 2760552, 2784246, 4136754, 2105110,
      4034297, 4078713, 2767183, 4146312, 2819471, 2106032, 2878897, 2886811, 2886805, 2104875,
      2104872, 2871204, 2897706, 2871197, 2886799, 2832433, 2878895, 2865757, 2891862, 2891856,
      2859827, 2859834, 2878906, 2836959, 2897705, 2812155, 2878888, 2865748, 2859837, 2837090,
      2897707, 2824353, 2819474, 2847073, 2799061, 2799059, 2824337, 2767409, 2767135, 2767168,
      2767148, 2767182, 2767140, 2767395, 2767406, 2767176, 2766936, 2767161, 2767403, 2767136,
      2766932, 2767199, 2767200, 2767160, 2767196, 2767147, 4076471, 2762966, 4220063, 4076600,
      81390, 2104841, 2784244, 2760550, 4267431, 4338257, 4034717, 43531648, 4083671, 2784250,
      2105087, 43531147, 2760551, 2777591, 2886803, 2819473, 4034313, 2767179, 4145255, 4234406,
      760563, 2834833, 2871202, 2767178, 2777595, 4204008, 4138869, 4001860, 4067885, 4231436,
      2105343, 4076602, 4231436, 4012670, 4206405, 4003054, 2836956, 2859831, 2891859, 2104873,
      2832435, 2105223, 2859835, 2832437, 2878891, 2837091, 2836955, 2852295, 2102520, 2886801,
      2836962, 2897709, 2871200, 2878889, 2865751, 2859842, 2104875, 2865758, 2832432, 2832431,
      2824356, 2897710, 2832438, 2819472, 2852299, 2865749, 2799057, 2806602, 2812149, 2832424,
      2852296, 2767186, 2103436, 2767197, 2767138, 2767183, 2767397, 2767195, 2767169, 2767153,
      2767142, 44515922, 2767186, 40482065, 2105065, 2767150, 4069651, 2767162, 4034298, 2799055,
      2760554, 2897694, 4142364, 2832440, 4343904, 4239961, 2784247, 2767155, 2767149, 4147645,
      2777586, 4271851, 4031127, 4186318, 2897703, 2897698, 2847078, 2859829, 2847080, 2106033,
      2105448, 2799068, 2105210, 2897696, 2878899, 2852298, 2886809, 2859840, 2891858, 2819464,
      2105089, 2824339, 2836960, 2847076, 2105209, 2852301, 2824340, 2886810, 2819476, 2897700,
      2847083, 2886807, 2837092, 2897695, 2897704, 2806607, 2847085, 2799067, 2767402, 2766938,
      2767162, 2767184, 2767177, 2767164, 2767178, 2766939, 2766931, 2767144, 2767396, 2767181,
      2766930, 2767139, 2767155, 2110701, 2836958, 760846, 2105085, 4101249, 760545, 4069652,
      2777589, 2005278, 4306618, 4249875, 4299395, 4194335, 2104833, 4324317, 4070274, 2006228,
      2767145, 2891851, 4002021, 44783435, 2102519, 2878903, 763898, 4264289, 4133304, 136788,
      2762969, 2777582, 42539022, 4203771, 2878900, 2886806, 2799058, 2105202, 2852294, 2824351,
      2865754, 2824352, 2859832, 2859841, 2105211, 2799063, 2871199, 2105222, 2799064, 2832427,
      2891861, 2897697, 2847081, 2832430, 2871201, 2824345, 2852297, 2824348, 2819465, 2865759,
      2878896, 2806608, 2105451, 2767165, 2767193, 2767400, 2766935, 2767401, 2767189, 2766937,
      2767171, 2767201, 2767190, 2767188, 2767398, 2767191, 2767149, 2767154, 2766942, 2767158,
      2766940, 2766934, 2104835, 4284397, 4143795, 37115743, 2104950, 2104841, 2767177, 2799056,
      2897702, 2767182, 2832425, 2767180, 4343455, 765177, 2837089, 4236762, 2000073, 4142079,
      2762964, 2784243, 2105446, 4143771, 436785, 4225800, 2777594, 2102536, 2769725, 4259567,
      2784241, 2105343, 4149785, 2777593, 2767184, 4195136, 2006242, 2847084, 2865753, 2836953,
      2859838, 2104873, 2878898, 2105111, 2105450, 2891853, 2832439, 2806600, 2006243, 2878894,
      2824355, 2832434, 2878902, 2819469, 2824343, 2859833, 2878901, 2886804, 2873707, 2806604,
      2799065, 2824347, 2836957, 2812151, 2878904, 2824350, 2891860, 2886802, 2812152, 2878893,
      2847077, 2806601, 2819463, 2819475, 2767192, 2767146, 2767172, 2766941, 2767408, 2767411,
      2767170, 2767404, 2767198, 2767151, 2767137, 2767180, 2767203, 2767187)
"""

outlier_pts_weight_query = f"""
SELECT
DISTINCT
co.person_id as outlier_patient
FROM
`{DATASET}.unioned_ehr_condition_occurrence` co
WHERE
co.condition_concept_id IN (440076, 436675, 4204347, 4229881, 36714252, 37116399, 36676691,
      37395978, 35622248, 36714182, 4033951, 44784528, 36716141, 4214302, 37111630, 44783252, 36674971,
      36675008, 36716779, 37016351, 36713802, 36717597, 36713801, 4330231, 4052869, 4111319, 4082503,
      4238810, 36714112, 4006979, 4287262, 36713764, 37396322, 4096098, 4269485, 36674974, 37117816,
      36716442, 37110064, 4025818, 4216214, 4331598, 36715406, 37396271, 36674412, 4300305, 36713653,
      36674995, 37396500, 36717398, 36714025, 506557, 4204347, 4325860, 36676625, 35608003, 36675641,
      4282075, 4314386, 36715303, 36674411, 4028945, 4134010, 37116379, 37116374, 4192645, 37116394,
      36717098, 4058570, 4045573, 4007583, 36675180, 35622957, 36714634, 36716390, 35622371, 4143827,
      36715092, 35624153, 37117186, 37116377, 4243014, 35622011, 35623139, 25780, 35622777, 35622394,
      36715123, 37396246, 36715404, 36714301, 36714238, 37396250, 36717531, 36676516, 435928, 37398922,
      37109890, 36713543, 37118645, 36676673, 36715405, 36714074, 35622761, 4245014, 37111590, 4164551,
      37115760, 36675025, 35622802, 36713991, 4208782, 36712855, 36714113, 36713523, 37111668, 37110071,
      4326901, 506558, 35624205, 37110104, 4059420, 4119133, 36714111, 35622250, 36675640, 4223757,
      35622390, 36716445, 42536694, 36716030, 35621870, 35622774, 36676860, 36675005, 35622260, 36716032,
      4102312, 4060981, 36716110, 36717441)
"""

# ## Want to see what the unit distribution is for the measurements that we are looking at

# ### Weight

# +
persons_to_exclude_weight_df = pd.io.gbq.read_gbq(outlier_pts_weight_query, dialect='standard')

persons_to_exclude = persons_to_exclude_weight_df['outlier_patient'].tolist()
num_persons_excluded_by_condition = len(persons_to_exclude)

persons_to_exclude = list(map(str, persons_to_exclude))
persons_to_exclude_as_str = ', '.join(persons_to_exclude)

print(f"""
There are {num_persons_excluded_by_condition} excluded from this weight analysis based on their pre-existing conditions.""")
# -

CONCEPT_ID_STRINGS = weight_concepts

get_all_descendant_concepts_as_table = f"""
WITH
height_and_weight_concepts AS
(SELECT
  DISTINCT ca.descendant_concept_id,
  c.concept_name AS descendant_name,
  c.domain_id AS domain
FROM
  `{DATASET}.concept_ancestor` ca
JOIN
  `{DATASET}.concept` c
ON
  ca.descendant_concept_id = c.concept_id
WHERE
  ca.ancestor_concept_id IN 
  ({CONCEPT_ID_STRINGS})
  {caveat_string})
"""

see_weight_unit_distrib = f"""
SELECT
m.unit_concept_id, c.concept_name, COUNT(*) as num_measurements
FROM
`{DATASET}.unioned_ehr_measurement` m
JOIN
`{DATASET}.concept` c
ON
m.unit_concept_id = c.concept_id
WHERE
m.measurement_concept_id IN
  (SELECT DISTINCT
  h_w.descendant_concept_id
  FROM
  height_and_weight_concepts AS h_w)
AND
m.value_as_number IS NOT NULL
AND
m.value_as_number <> 9999999  -- issue with one site that heavily skews data
AND
m.value_as_number <> 0.0  -- not something we expect; appears for a site
AND
m.person_id NOT IN ({persons_to_exclude_as_str})
GROUP BY 1, 2
ORDER BY num_measurements DESC
"""

see_weight_unit_distrib = get_all_descendant_concepts_as_table + see_weight_unit_distrib

unit_df_for_weights = pd.io.gbq.read_gbq(see_weight_unit_distrib, dialect='standard')

unit_df_for_weights

# ### Height

# +
persons_to_exclude_height_df = pd.io.gbq.read_gbq(outlier_pts_height_query, dialect='standard')

persons_to_exclude = persons_to_exclude_height_df['outlier_patient'].tolist()
num_persons_excluded_by_condition = len(persons_to_exclude)

persons_to_exclude = list(map(str, persons_to_exclude))
persons_to_exclude_as_str = ', '.join(persons_to_exclude)

print(f"""
There are {num_persons_excluded_by_condition} excluded from this height analysis based on their pre-existing conditions.""")
# -

CONCEPT_ID_STRINGS = height_concepts

get_all_descendant_concepts_as_table = f"""
WITH
height_and_weight_concepts AS
(SELECT
  DISTINCT ca.descendant_concept_id,
  c.concept_name AS descendant_name,
  c.domain_id AS domain
FROM
  `{DATASET}.concept_ancestor` ca
JOIN
  `{DATASET}.concept` c
ON
  ca.descendant_concept_id = c.concept_id
WHERE
  ca.ancestor_concept_id IN 
  ({CONCEPT_ID_STRINGS})
  {caveat_string})
"""

see_height_unit_distrib = f"""
SELECT
m.unit_concept_id, c.concept_name, COUNT(*) as num_measurements
FROM
`{DATASET}.unioned_ehr_measurement` m
JOIN
`{DATASET}.concept` c
ON
m.unit_concept_id = c.concept_id
WHERE
m.measurement_concept_id IN
  (SELECT DISTINCT
  h_w.descendant_concept_id
  FROM
  height_and_weight_concepts AS h_w)
AND
m.value_as_number IS NOT NULL
AND
m.value_as_number <> 9999999  -- issue with one site that heavily skews data
AND
m.value_as_number <> 0.0  -- not something we expect; appears for a site
AND
m.person_id NOT IN ({persons_to_exclude_as_str})
GROUP BY 1, 2
ORDER BY num_measurements DESC
"""

see_height_unit_distrib = get_all_descendant_concepts_as_table + see_height_unit_distrib

unit_df_for_heights = pd.io.gbq.read_gbq(see_height_unit_distrib, dialect='standard')

unit_df_for_heights

# ## See the concept usage for weight

CONCEPT_ID_STRINGS = weight_concepts

get_all_descendant_concepts_as_table = f"""
WITH
height_and_weight_concepts AS
(SELECT
  DISTINCT ca.descendant_concept_id,
  c.concept_name AS descendant_name,
  c.domain_id AS domain
FROM
  `{DATASET}.concept_ancestor` ca
JOIN
  `{DATASET}.concept` c
ON
  ca.descendant_concept_id = c.concept_id
WHERE
  ca.ancestor_concept_id IN 
  ({CONCEPT_ID_STRINGS})
  {caveat_string})
"""

bulk_of_query = f"""
SELECT
DISTINCT
m.measurement_concept_id, c.concept_name, 
COUNT(DISTINCT mm.src_hpo_id) as num_sites,
COUNT(*) as num_rows

FROM
`{DATASET}.unioned_ehr_measurement` m
JOIN
`{DATASET}.concept` c
ON
m.measurement_concept_id = c.concept_id

JOIN
`{DATASET}._mapping_measurement` mm
ON
m.measurement_id = mm.measurement_id 

WHERE m.measurement_concept_id IN
  (SELECT DISTINCT
  h_w.descendant_concept_id
  FROM
  height_and_weight_concepts AS h_w)
  
AND m.person_id NOT IN ({persons_to_exclude_as_str})

GROUP BY 1, 2
ORDER BY num_rows DESC
"""

# +
persons_to_exclude_weight_df = pd.io.gbq.read_gbq(outlier_pts_weight_query, dialect='standard')

persons_to_exclude = persons_to_exclude_weight_df['outlier_patient'].tolist()
num_persons_excluded_by_condition = len(persons_to_exclude)

persons_to_exclude = list(map(str, persons_to_exclude))
persons_to_exclude_as_str = ', '.join(persons_to_exclude)

print(f"""
There are {num_persons_excluded_by_condition} excluded from this weight analysis based on their pre-existing conditions.""")
# -

full_weight_query = get_all_descendant_concepts_as_table + bulk_of_query

weight_concept_usage = pd.io.gbq.read_gbq(full_weight_query, dialect='standard')

weight_concept_usage

# ## See the concept usage for height

CONCEPT_ID_STRINGS = height_concepts

get_all_descendant_concepts_as_table = f"""
WITH
height_and_weight_concepts AS
(SELECT
  DISTINCT ca.descendant_concept_id,
  c.concept_name AS descendant_name,
  c.domain_id AS domain
FROM
  `{DATASET}.concept_ancestor` ca
JOIN
  `{DATASET}.concept` c
ON
  ca.descendant_concept_id = c.concept_id
WHERE
  ca.ancestor_concept_id IN 
  ({CONCEPT_ID_STRINGS})
  {caveat_string})
"""

# +
persons_to_exclude_height_df = pd.io.gbq.read_gbq(outlier_pts_height_query, dialect='standard')

persons_to_exclude = persons_to_exclude_height_df['outlier_patient'].tolist()
num_persons_excluded_by_condition = len(persons_to_exclude)

persons_to_exclude = list(map(str, persons_to_exclude))
persons_to_exclude_as_str = ', '.join(persons_to_exclude)

print(f"""
There are {num_persons_excluded_by_condition} excluded from this height analysis based on their pre-existing conditions.""")
# -

full_height_query = get_all_descendant_concepts_as_table + bulk_of_query

height_concept_usage = pd.io.gbq.read_gbq(full_height_query, dialect='standard')

height_concept_usage

# # Part II: Investigating the distribution of weights
#
# The rules of converting units will be as follows (saying that weights between 100 and 325lbs are what we expect):
# - Between 45.36 and 100: assume in kg. Therefore *2.20462
# - Between 136.078 and 325: assume already in lbs. Therefore *1
# - Between 1600 and 5200: assume in oz. Therefore *0.0625
# - Between 45350 and 147418: assume in grams. Therefore *0.00220462
#
# - NOTE: Between 100 and 136.078 - check the unit_concept_id to determine if a multiplier should be used. Whether this is kg or lbs is somewhat ambiguous. Unit IDs can be defined by looking earlier in this script to see which ones are used by the sites.

weight_concept_ids = weight_concept_usage['measurement_concept_id'].tolist()

weight_concept_ids = list(map(str, weight_concept_ids))

weight_concepts_as_str = ', '.join(weight_concept_ids)

weight_concepts_as_str

# +
persons_to_exclude_weight_df = pd.io.gbq.read_gbq(outlier_pts_weight_query, dialect='standard')

persons_to_exclude = persons_to_exclude_weight_df['outlier_patient'].tolist()
num_persons_excluded_by_condition = len(persons_to_exclude)

persons_to_exclude = list(map(str, persons_to_exclude))
persons_to_exclude_as_str = ', '.join(persons_to_exclude)

print(f"""
There are {num_persons_excluded_by_condition} persons excluded from this weight analysis based on their pre-existing conditions.""")
# -

weight_conversion_rules_str = """
CASE WHEN
-- kg
(m.value_as_number BETWEEN 45.36 AND 100) THEN m.value_as_number * 2.20462 WHEN
(m.value_as_number BETWEEN 100 AND 136.078 AND m.unit_concept_id IN (9529)) THEN m.value_as_number * 2.20462 WHEN

-- lb
(m.value_as_number BETWEEN 100 AND 136.078 AND m.unit_concept_id IN (4124425, 8739)) THEN m.value_as_number * 1 WHEN
(m.value_as_number BETWEEN 136.078 AND 325) THEN m.value_as_number * 1 WHEN

-- oz
(m.value_as_number BETWEEN 1600 AND 5200) THEN m.value_as_number * 0.0625 WHEN

-- grams
(m.value_as_number BETWEEN 45350 and 147418) THEN m.value_as_number * 0.00220462

ELSE value_as_number  -- outliers. leave as-is.
END
"""

query_all_weights = f"""
SELECT
m.value_as_number, {weight_conversion_rules_str}
FROM
`{DATASET}.unioned_ehr_measurement` m
WHERE
m.measurement_concept_id IN ({weight_concepts_as_str})
AND
m.value_as_number IS NOT NULL
AND
m.value_as_number <> 9999999  -- issue with one site that heavily skews data
AND
m.value_as_number <> 0.0  -- not something we expect; appears for a site
AND
m.person_id NOT IN ({persons_to_exclude_as_str})
ORDER BY value_as_number DESC
"""

weight_df = pd.io.gbq.read_gbq(query_all_weights, dialect='standard')

# # Now for the true analysis 

# +
weights = weight_df['value_as_number'].tolist()

number_records = str(len(weights))
mean = str(round(np.mean(weights), rounding_val))

decile1 = str(round(np.percentile(weights, 10), rounding_val))

quartile1 = str(round(np.percentile(weights, 25), rounding_val))
median = str(round(np.percentile(weights, 50), rounding_val))
quartile3 = str(round(np.percentile(weights, 75), rounding_val))

decile9 = str(round(np.percentile(weights, 90), rounding_val))

stdev = str(round(np.std(np.asarray(weights)), rounding_val))

min_weight = min(weights)
max_weight = max(weights)
# -

general_weight_attributes = f"""
Number of weights: {number_records}

Minimum: {min_weight}
maximum: {max_weight}

Mean Weight: {mean}
Standard Devidation: {stdev}

10th Percentile: {decile1}
25th Percentile: {quartile1}
Median: {median}
75th Percentile: {quartile3}
90th Percentile: {decile9}
"""

print(general_weight_attributes)

# ### Interesting distribution below; shows the bin borders so that each bucket gets 10% of data

# +
n_bins = 10
bin_borders = [np.amin(weights)] + [weights[(len(weights) // n_bins) * i] for i in range(1, n_bins)] + [np.amax(weights)]

bin_borders.sort()
print(bin_borders)
# -

# ### Let's try to see what the distribution would look like with excluding those outside the stdev

# +
stdev_val = round(np.std(np.asarray(weights)), rounding_val)
mean_val = round(np.mean(weights), rounding_val)

within_one_stdev = [x for x in weights if x > (mean_val - stdev_val) and x < (stdev_val + mean_val)]
within_one_stdev.sort()

print(max(within_one_stdev))
# -

bin_borders = [-200, 0, 100, 200, 300, 400, 500, 4000, 8000, 13000]

# +
plt.hist(within_one_stdev, bins=bin_borders, alpha= 0.5)
plt.title('Weight Distribution Across All Sites - Within 1 Stdev')
plt.xlabel('Weight (in lbs)')
plt.ylabel('Count')

plt.show()
# -
# ### Let's try to use something other than stdev - those within the middle 70% to start

fifteenth_perc = round(np.percentile(weights, 15), rounding_val)
eighy_fifth_perc = round(np.percentile(weights, 85), rounding_val)


# +
within_mid_seventy = [x for x in weights if x > fifteenth_perc and x < eighy_fifth_perc]
within_mid_seventy.sort()

print(max(within_mid_seventy))
print(min(within_mid_seventy))
# -

bin_borders = [80, 100, 120, 140, 160, 180, 200, 220, 240, 260, 280, 300, 320, 340, 360, 400, 3000]

# +
plt.hist(within_mid_seventy, bins=bin_borders, alpha= 0.5)
plt.title('Weight Distribution Across All Sites - Mid 70%')
plt.xlabel('Weight (in lbs)')
plt.ylabel('Count')

plt.show()
# -

# ### Make the analysis smaller - mid 50%

quartile1 = round(np.percentile(weights, 25), rounding_val)
quartile3 = round(np.percentile(weights, 75), rounding_val)

# +
within_mid_fifty = [x for x in weights if x > quartile1 and x < quartile3]
within_mid_fifty.sort()

print(max(within_mid_fifty))
print(min(within_mid_fifty))
# -

bin_borders = [80, 100, 120, 140, 160, 180, 200, 220, 240, 260, 280, 300, 320, 340, 360, 400, 3000]

# +
plt.hist(within_mid_fifty, bins=bin_borders, alpha= 0.5)
plt.title('Weight Distribution Across All Sites - Mid 50%')
plt.xlabel('Weight (in lbs)')
plt.ylabel('Count')

plt.show()
# -

# ### Let's see where these crazy measurements are coming from and what units/sites are associated

weight_conversion_rules_str = """
CASE WHEN
-- kg
(m.value_as_number BETWEEN 45.36 AND 100) THEN m.value_as_number * 2.20462 WHEN
(m.value_as_number BETWEEN 100 AND 136.078 AND m.unit_concept_id IN (9529)) THEN m.value_as_number * 2.20462 WHEN

-- lb
(m.value_as_number BETWEEN 100 AND 136.078 AND m.unit_concept_id IN (4124425, 8739)) THEN m.value_as_number * 1 WHEN
(m.value_as_number BETWEEN 136.078 AND 325) THEN m.value_as_number * 1 WHEN

-- oz
(m.value_as_number BETWEEN 1600 AND 5200) THEN m.value_as_number * 0.0625 WHEN

-- grams
(m.value_as_number BETWEEN 45350 and 147418) THEN m.value_as_number * 0.00220462

ELSE value_as_number  -- outliers. leave as-is.
END
"""

query_all_weights = f"""
SELECT
DISTINCT

mm.src_hpo_id,
-- c1.concept_name as measurement_name,
-- c2.concept_name as unit_name, 
COUNT(*) as cnt

FROM
`{DATASET}.unioned_ehr_measurement` m
JOIN
`{DATASET}.concept` c1
ON
m.measurement_concept_id = c1.concept_id
JOIN
`{DATASET}.concept` c2
ON
m.unit_concept_id = c2.concept_id
JOIN
`{DATASET}._mapping_measurement` mm
ON
m.measurement_id = mm.measurement_id

WHERE
m.measurement_concept_id IN ({weight_concepts_as_str})
AND
m.value_as_number IS NOT NULL
AND
m.value_as_number <> 9999999  -- issue with one site that heavily skews data
AND
m.value_as_number <> 0.0  -- not something we expect; appears for a site
AND
m.person_id NOT IN ({persons_to_exclude_as_str})

AND

-- between pounds and ounces
(m.value_as_number > 325 AND
m.value_as_number < 1600) 

OR

-- between ounces and grams
(m.value_as_number > 5200 AND
m.value_as_number < 45350) 

OR

-- beyond grams
(m.value_as_number > 147418)

GROUP BY 1
ORDER BY cnt DESC
"""

weight_df = pd.io.gbq.read_gbq(query_all_weights, dialect='standard')

weight_df

# #### Sanity check - what weight concepts are we using again?

check_the_concepts_used = f"""
SELECT
DISTINCT
c.concept_id, c.concept_name
FROM
`{DATASET}.unioned_ehr_measurement` m
JOIN
`{DATASET}.concept` c
ON
m.measurement_concept_id = c.concept_id
WHERE
m.measurement_concept_id IN ({weight_concepts_as_str})
"""

weight_concepts = pd.io.gbq.read_gbq(check_the_concepts_used, dialect='standard')

weight_concepts

# # Part III: Investigating the distribution of heights
#
# The rules of converting units will be as follows (saying that heights between 36 (3ft) and 90in (7ft 6in) are what we expect):
# - Between 36 and 90: assume already in inches. Therefore *1
# - Between 91.44 and 228.6: assume in cm. Therefore *0.393701

# +
height_concept_ids = height_concept_usage['measurement_concept_id'].tolist()

height_concept_ids = list(map(str, height_concept_ids))

height_concepts_as_str = ', '.join(height_concept_ids)

height_concepts_as_str

# +
persons_to_exclude_height_df = pd.io.gbq.read_gbq(outlier_pts_height_query, dialect='standard')

persons_to_exclude = persons_to_exclude_height_df['outlier_patient'].tolist()
num_persons_excluded_by_condition = len(persons_to_exclude)

persons_to_exclude = list(map(str, persons_to_exclude))
persons_to_exclude_as_str = ', '.join(persons_to_exclude)

print(f"""
There are {num_persons_excluded_by_condition} persons excluded from this weight analysis based on their pre-existing conditions.""")
# -

height_conversion_rules_str = """
CASE WHEN
-- cm
(m.value_as_number BETWEEN 91.44 AND 228.6) THEN m.value_as_number * 0.0393701 
ELSE value_as_number  -- outliers. leave as-is. inches (or something that we do not know about)
END
"""

query_all_heights = f"""
SELECT
m.value_as_number, {height_conversion_rules_str}
FROM
`{DATASET}.unioned_ehr_measurement` m
WHERE
m.measurement_concept_id IN ({height_concepts_as_str})
AND
m.value_as_number IS NOT NULL
AND
m.value_as_number <> 9999999  -- issue with one site that heavily skews data
AND
m.value_as_number <> 0.0  -- not something we expect; appears for a site
AND
m.person_id NOT IN ({persons_to_exclude_as_str})
ORDER BY value_as_number DESC
"""

height_df = pd.io.gbq.read_gbq(query_all_heights, dialect='standard')

# # Now for the true analysis 

# +
heights = height_df['value_as_number'].tolist()

number_records = str(len(heights))
mean = str(round(np.mean(heights), rounding_val))

decile1 = str(round(np.percentile(heights, 10), rounding_val))

quartile1 = str(round(np.percentile(heights, 25), rounding_val))
median = str(round(np.percentile(heights, 50), rounding_val))
quartile3 = str(round(np.percentile(heights, 75), rounding_val))

decile9 = str(round(np.percentile(heights, 90), rounding_val))

stdev = str(round(np.std(np.asarray(heights)), rounding_val))

min_weight = min(heights)
max_weight = max(heights)

# +
general_height_attributes = f"""
Number of heights: {number_records}

Minimum: {min_weight}
maximum: {max_weight}

Mean Height: {mean}
Standard Devidation: {stdev}

10th Percentile: {decile1}
25th Percentile: {quartile1}
Median: {median}
75th Percentile: {quartile3}
90th Percentile: {decile9}
"""

print(general_height_attributes)
# -

# ### Interesting distribution below; shows the bin borders so that each bucket gets 10% of data

# +
n_bins = 10
bin_borders = [np.amin(heights)] + [heights[(len(heights) // n_bins) * i] for i in range(1, n_bins)] + [np.amax(heights)]

bin_borders.sort()
print(bin_borders)
# -

# ### Let's try to see what the distribution would look like with excluding those outside the stdev

# +
stdev_val = round(np.std(np.asarray(heights)), rounding_val)
mean_val = round(np.mean(heights), rounding_val)

within_one_stdev = [x for x in heights if x > (mean_val - stdev_val) and x < (stdev_val + mean_val)]
within_one_stdev.sort()

print(max(within_one_stdev))

# +
bin_borders = [36, 39, 42, 45, 48, 51, 54, 57, 60, 63, 66, 69, 72, 75, 78, 81, 84, 87, 90, 100]

plt.hist(within_one_stdev, bins=bin_borders, alpha= 0.5)
plt.title('Height Distribution Across All Sites - Within 1 Stdev')
plt.xlabel('Height (in inches)')
plt.ylabel('Count')

plt.show()
# -

# ### Let's try to use something other than stdev - those within the middle 70% to start

# +
fifteenth_perc = round(np.percentile(heights, 15), rounding_val)
eighy_fifth_perc = round(np.percentile(heights, 85), rounding_val)

within_mid_seventy = [x for x in heights if x > fifteenth_perc and x < eighy_fifth_perc]
within_mid_seventy.sort()

print(max(within_mid_seventy))
print(min(within_mid_seventy))

bin_borders = [36, 39, 42, 45, 48, 51, 54, 57, 60, 63, 66, 69, 72, 75, 78, 81, 84, 87, 90, 100]

plt.hist(within_mid_seventy, bins=bin_borders, alpha= 0.5)
plt.title('Height Distribution Across All Sites - Mid 70%')
plt.xlabel('Height (in inches)')
plt.ylabel('Count')

plt.show()
# -

# ### Make the analysis larger - mid 90%

# +
fifth_perc = round(np.percentile(heights, 5), rounding_val)
ninety_fifth_perc = round(np.percentile(heights, 95), rounding_val)

within_mid_ninety = [x for x in heights if x > fifth_perc and x < ninety_fifth_perc]
within_mid_ninety.sort()

print(max(within_mid_ninety))
print(min(within_mid_ninety))

bin_borders = [36, 39, 42, 45, 48, 51, 54, 57, 60, 63, 66, 69, 72, 75, 78, 81, 84, 87, 90, 100]

plt.hist(within_mid_ninety, bins=bin_borders, alpha= 0.5)
plt.title('Height Distribution Across All Sites - Mid 90%')
plt.xlabel('Height (in inches)')
plt.ylabel('Count')

plt.show()
# -

query_all_heights = f"""
SELECT
DISTINCT

mm.src_hpo_id,
-- c1.concept_name as measurement_name,
-- c2.concept_name as unit_name, 
COUNT(*) as cnt

FROM
`{DATASET}.unioned_ehr_measurement` m
JOIN
`{DATASET}.concept` c1
ON
m.measurement_concept_id = c1.concept_id
JOIN
`{DATASET}.concept` c2
ON
m.unit_concept_id = c2.concept_id
JOIN
`{DATASET}._mapping_measurement` mm
ON
m.measurement_id = mm.measurement_id

WHERE
m.measurement_concept_id IN ({height_concepts_as_str})
AND
m.value_as_number IS NOT NULL
AND
m.value_as_number <> 9999999  -- issue with one site that heavily skews data
AND
m.value_as_number <> 0.0  -- not something we expect; appears for a site
AND
m.person_id NOT IN ({persons_to_exclude_as_str})

AND

-- too small to be inches
-- too large to be cm
(m.value_as_number < 36 OR
m.value_as_number > 228.6) 

GROUP BY 1
ORDER BY cnt DESC
"""

height_df = pd.io.gbq.read_gbq(query_all_heights, dialect='standard')

height_df


