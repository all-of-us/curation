"""
Normalizes all height and weight data into cm and kg and removes invalid/implausible data points (rows)

Original Issues: DC-416, DC-701

The intent of this cleaning rule is to delete zero/null/implausible height/weight rows
and inserting normalized rows (cm and kg). This cleaning rule also expects `measurement_ext` table to be
present in the dataset
"""

# Python imports
import logging

from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule
from cdr_cleaner.cleaning_rules.measurement_table_suppression import (
    MeasurementRecordsSuppression)
# Project imports
from common import MEASUREMENT, JINJA_ENV, PIPELINE_TABLES
from constants.bq_utils import WRITE_APPEND
from constants.cdr_cleaner import clean_cdr as cdr_consts
from resources import fields_for

LOGGER = logging.getLogger(__name__)

ISSUE_NUMBERS = ['DC-416', 'DC-701', 'DC2456']

HEIGHT_TABLE = 'height_table'
WEIGHT_TABLE = 'weight_table'
NEW_HEIGHT_ROWS = 'new_height_rows'
NEW_WEIGHT_ROWS = 'new_weight_rows'

WEIGHT_CONCEPT_IDS = [3025315, 3013762, 3023166]
HEIGHT_CONCEPT_IDS = [3036277, 3023540, 3019171]

MEASUREMENT_FIELDS = [field.get('name') for field in fields_for(MEASUREMENT)]

# height queries
CREATE_HEIGHT_SANDBOX_QUERY = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project_id}}.{{sandbox_dataset_id}}.{{height_table}}` AS
WITH
  concepts AS (
    SELECT
      *
    FROM `{{project_id}}.{{dataset_id}}.concept`
  ),
  persons AS (
    SELECT
      person_id,
      birth_datetime
    FROM `{{project_id}}.{{dataset_id}}.person`
  ),
  sites AS (
    SELECT
      measurement_id,
      src_id
    FROM `{{project_id}}.{{dataset_id}}.measurement_ext`
  ),
  height_measurements AS (
    SELECT
      m.person_id,
      m.measurement_id,
      m.measurement_concept_id,
      m.measurement_date,
      m.measurement_datetime,
      m.measurement_type_concept_id,
      m.operator_concept_id,
      m.value_as_number,
      m.value_as_concept_id,
      m.unit_concept_id,
      m.measurement_time,
      m.visit_detail_id
    FROM `{{project_id}}.{{dataset_id}}.measurement` m
    LEFT JOIN sites s
    USING (measurement_id)
    WHERE (m.measurement_concept_id IN ({{measurement_concept_ids}}))
      AND m.value_as_number IS NOT NULL
      AND m.value_as_number != 0
      AND s.src_id NOT IN (
        SELECT 
          src_id
        FROM
          `{{project_id}}.{{pipeline_tables}}.site_maskings`
        WHERE NOT
          REGEXP_CONTAINS(src_id, r'(?i)(PPI/PM)|(EHR site)')
      ) -- site could use measurement_source_concept_id 903133 but we still need to include them --
  ),
  condition_occ AS (
    SELECT
      person_id,
      condition_concept_id,
      condition_start_date,
      condition_start_datetime,
      condition_end_date,
      condition_end_datetime,
      condition_type_concept_id
    FROM `{{project_id}}.{{dataset_id}}.condition_occurrence`
  ),
  concept_ancestor AS (
    SELECT
      *
    FROM `{{project_id}}.{{dataset_id}}.concept_ancestor`
  ),
  -- Outlier patients if they have certain diagnosis codes --
  outlierHt_pts AS (
    SELECT DISTINCT
      person_id,
      1 AS f_outlierDx_Ht
    FROM condition_occ
    WHERE condition_concept_id IN (80502, 77079, 4078547, 4165513,
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
  ),
  -- Easier to reference this table for later --
  phys_df_ht AS (
    SELECT
      *,
      CASE
        WHEN (value_as_number BETWEEN 0.9 AND 2.3) THEN (value_as_number * 100)
        WHEN (value_as_number BETWEEN 3 AND 7.5) THEN (value_as_number * (12 * 2.54))
        WHEN (value_as_number BETWEEN 36 AND 89.9) THEN (value_as_number * 2.54)
        ELSE (value_as_number)
      END AS adj_Ht,
      CASE
        WHEN (value_as_number BETWEEN 0.9 AND 2.3) THEN 8582
        WHEN (value_as_number BETWEEN 3 AND 7.5) THEN 8582
        WHEN (value_as_number BETWEEN 36 AND 89.9) THEN 8582
        WHEN (value_as_number BETWEEN 90 AND 230) THEN 8582
        ELSE (unit_concept_id)
      END AS adj_unit,
      {{pipeline_tables}}.calculate_age(measurement_date, EXTRACT(DATE FROM birth_datetime)) as age
    FROM height_measurements
    LEFT JOIN persons USING (person_id)
    LEFT JOIN sites USING (measurement_id)
    LEFT JOIN outlierHt_pts USING (person_id)
    WHERE {{pipeline_tables}}.calculate_age(measurement_date, EXTRACT(DATE FROM birth_datetime)) >= 18
  ),
  -- Height disagreement: count == 2, sd > 10 --
  height_disagreement_pts AS (
    SELECT DISTINCT
      person_id,
      1 AS f_disagree2_Ht
    FROM (
      SELECT DISTINCT
        person_id,
        STDDEV_SAMP(adj_Ht) OVER (PARTITION BY person_id) AS sd_height,
        COUNT(*) OVER (PARTITION BY person_id) AS n
      FROM phys_df_ht
      WHERE  value_as_number IS NOT NULL
    )
    WHERE (n = 2 AND sd_height > 10)
  )

    -- All height stuff in a table --
    SELECT DISTINCT
      a.*,
      CASE
        WHEN ((f_outlierDx_Ht IS NULL)
          AND (adj_Ht > median_Ht + 0.03 * median_Ht OR adj_Ht < median_Ht - 0.03 * median_Ht)) THEN (1)
        ELSE (0)
      END AS f_outlier_Ht,
      COALESCE(f_disagree2_Ht,0) AS f_disagree2_Ht
    FROM (
      SELECT
        *,
        PERCENTILE_CONT(ABS(adj_Ht),.5) OVER (PARTITION BY person_id) AS median_Ht
      FROM phys_df_ht
    ) a
    LEFT JOIN
      height_disagreement_pts USING (person_id)
    """)

NEW_HEIGHT_ROWS_QUERY = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project_id}}.{{sandbox_dataset_id}}.{{new_height_rows}}` AS
SELECT *
  REPLACE(adj_Ht AS value_as_number,
  adj_unit AS unit_concept_id,
  COALESCE(u_c.concept_code, unit_source_value) AS unit_source_value)
FROM (
  SELECT
    measurement_id,
    adj_Ht,
    adj_unit
  FROM `{{project_id}}.{{sandbox_dataset_id}}.{{height_table}}`
  WHERE f_outlier_Ht=0
    AND f_disagree2_Ht=0
    AND adj_Ht IS NOT NULL
    AND adj_unit=8582
)
JOIN `{{project_id}}.{{dataset_id}}.measurement` m USING (measurement_id)
LEFT JOIN `{{project_id}}.{{dataset_id}}.concept` u_c ON (adj_unit=concept_id)
""")

# weight queries
CREATE_WEIGHT_SANDBOX_QUERY = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project_id}}.{{sandbox_dataset_id}}.{{weight_table}}` AS
WITH
  concepts AS (
    SELECT
      *
    FROM `{{project_id}}.{{dataset_id}}.concept`
  ),
  persons AS (
    SELECT
      person_id,
      birth_datetime
    FROM `{{project_id}}.{{dataset_id}}.person`
  ),
  sites AS (
    SELECT
      measurement_id,
      src_id
    FROM `{{project_id}}.{{dataset_id}}.measurement_ext`
  ),
  weight_measurements AS (
    SELECT
      m.person_id,
      m.measurement_id,
      m.measurement_concept_id,
      m.measurement_date,
      m.measurement_datetime,
      m.measurement_type_concept_id,
      m.operator_concept_id,
      m.value_as_number,
      m.value_as_concept_id,
      m.unit_concept_id,
      m.measurement_time,
      m.visit_detail_id
    FROM `{{project_id}}.{{dataset_id}}.measurement` m
    LEFT JOIN sites s
    USING (measurement_id)
    WHERE (m.measurement_concept_id IN ({{measurement_concept_ids}}))
      AND m.value_as_number IS NOT NULL
      AND m.value_as_number != 0
      AND s.src_id NOT IN (
        SELECT 
          src_id
        FROM
          `{{project_id}}.{{pipeline_tables}}.site_maskings`
        WHERE NOT
          REGEXP_CONTAINS(src_id, r'(?i)(PPI/PM)|(EHR site)')
      ) -- site could use measurement_source_concept_id 903121 but we still need to include them --
  ),
  condition_occ AS (
    SELECT
      person_id,
      condition_concept_id,
      condition_start_date,
      condition_start_datetime,
      condition_end_date,
      condition_end_datetime,
      condition_type_concept_id
    FROM `{{project_id}}.{{dataset_id}}.condition_occurrence`
  ),
  concept_ancestor AS (
    SELECT
      *
    FROM `{{project_id}}.{{dataset_id}}.concept_ancestor`
  ),
  /* Outlier patients if they have certain diagnosis codes */
  outlierWt_pts_low AS (
    SELECT DISTINCT
      person_id,
      1 AS f_outlierDx_Wt_low
    FROM condition_occ
    WHERE condition_concept_id IN (440076, 436675, 4204347, 4229881, 36714252, 37116399, 36676691,
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
  ),
  outlierWt_pts_high AS (
    SELECT DISTINCT
      person_id,
      1 AS f_outlierDx_Wt_high
    FROM condition_occ
    WHERE condition_concept_id IN (434005, 37018860, 439141, 4074313, 45771307, 4100857, 42539192)
  ),
  -- Easier to reference this table for later --
  -- add some basic cleaning in here --
  -- Drop anything over 16000 --
  -- calculate min, mean and max value_as_number for measurement_concept_id = 3025315 over each unit_concept_id, --
  -- including NULL. If there are discrepancies, plot the distribution of value_as_number --
  -- if multiple peaks exist, it is presumably due to multiple units being used with the same unit_concept_id/NULL --
  -- In such cases, --
  --  1. convert values that do not appear to use kg into kg using the conversion used below in phys_df_wt_adj_site --
  --     and add it as a step in the CASE statement to null out values above 16000 --
  --  2. ensure that only one peak exists in the distribution for each unit for the measurement_concept_id 3025315 --
  phys_df_wt AS (
    SELECT
      person_id,
      measurement_id,
      measurement_concept_id,
      measurement_date,
      measurement_datetime,
      measurement_type_concept_id,
      operator_concept_id,
      value_as_number,
      value_as_concept_id,
      COALESCE(unit_concept_id,-2) AS unit_concept_id,
      measurement_time,
      visit_detail_id,
      src_id,
      birth_datetime,
      f_outlierDx_Wt_high,
      f_outlierDx_Wt_low,
      CASE
        WHEN ABS(value_as_number)>16000 THEN NULL
        ELSE ABS(value_as_number)
    END AS value_as_number_adj,
    {{pipeline_tables}}.calculate_age(measurement_date, EXTRACT(DATE FROM birth_datetime)) AS age
    FROM weight_measurements
    LEFT JOIN persons USING (person_id)
    LEFT JOIN sites USING (measurement_id)
    LEFT JOIN outlierWt_pts_high USING (person_id)
    LEFT JOIN outlierWt_pts_low USING (person_id)
    WHERE {{pipeline_tables}}.calculate_age(measurement_date, EXTRACT(DATE FROM birth_datetime)) >= 18
  ),
  --3) Check medians by site, concept_id, unit_concept_id --
  --Note that there are 'grams' recorded, but essentially nothing that can feasibly be grams. --
  median_Wt_by_site AS (
    SELECT DISTINCT
      src_id,
      measurement_concept_id,
      COALESCE(unit_concept_id,-2) AS unit_concept_id,
      PERCENTILE_CONT(ABS(value_as_number_adj),.5) OVER (PARTITION BY src_id, unit_concept_id) AS median_Wt_site
    FROM phys_df_wt
  ),
  -- 4) Tweak again based on 3, ie site medians --
  phys_df_wt_adj_site AS (
    SELECT
      *,
      CASE
      WHEN (median_Wt_site BETWEEN 60 AND 110) THEN value_as_number_adj
      WHEN (median_Wt_site BETWEEN 120 AND 300) THEN value_as_number_adj / 2.2046
      WHEN (median_Wt_site > 1200) THEN value_as_number_adj / 35.274
      ELSE value_as_number_adj
    END AS adj_Wt_site,
    CASE
      WHEN (median_Wt_site BETWEEN 60 AND 110) THEN 9529
      WHEN (median_Wt_site BETWEEN 120 AND 300) THEN 9529
      WHEN (median_Wt_site > 1200) THEN 9529
      ELSE unit_concept_id
    END AS adj_unit_site
    FROM phys_df_wt
    LEFT JOIN median_Wt_by_site USING (measurement_concept_id, unit_concept_id, src_id)
  ),
  -- List of potential pregnant times. 1 month before and after preg-related conditions. --
  preg_pts AS (
    SELECT DISTINCT
      person_id,
      DATE_SUB(condition_start_date, INTERVAL 1 MONTH) AS preg_start,
      DATE_ADD(condition_start_date, INTERVAL 1 MONTH) AS preg_end
    FROM condition_occ
    JOIN concept_ancestor ON (condition_concept_id=descendant_concept_id)
    WHERE ancestor_concept_id = 4299535
  ),
  --5) Check individual medians and add possible pregnancy flag --
  --Go ahead and adjust again based on the algorithm --
  -- Any weight greater than 1.5x and less than 3x the median weight is assumed to be in lbs and not kgs - do conversion. --
  -- Any weight greater than 24x and less than 50x the median for that individual is assumed to be in oz and not kg, convert. --
  -- This window leaves out the "x10" that results from an extra digit/misplaced decimal point. --
  phys_df_wt_adj AS (
    SELECT
      *,
      CASE
        WHEN (adj_Wt_site/median_Wt BETWEEN 1.5 AND 3) THEN adj_Wt_site/2.2046
        WHEN (adj_Wt_site/median_Wt BETWEEN 24 AND 50) THEN adj_Wt_site/35.274
        ELSE adj_Wt_site
      END AS adj_Wt,
      CASE
        WHEN (adj_Wt_site/median_Wt BETWEEN 1.5 AND 3) THEN 9529
        WHEN (adj_Wt_site/median_Wt BETWEEN 24 AND 50) THEN 9529
        ELSE adj_unit_site
      END AS adj_unit,
      measurement_id IN (
        SELECT DISTINCT
          measurement_id
        FROM phys_df_wt_adj_site
        JOIN preg_pts USING (person_id)
        WHERE measurement_date BETWEEN preg_start AND preg_end
      ) AS possible_pregnancy
    FROM (
      SELECT
        *,
        PERCENTILE_CONT(adj_Wt_site,.5) OVER (PARTITION BY person_id) AS median_Wt
      FROM phys_df_wt_adj_site
    )
  ),
  -- flag any remaining differences >33% of median over 2 years (where >=3 values occur in that window) --
  -- Ignoring pregnancy weights --
  wt_variance_windows as (
    SELECT
      measurement_id,
      CASE MIN(adj_Wt)/MIN(window_median) BETWEEN .7 AND 1.3
        WHEN TRUE THEN 0
        WHEN FALSE THEN 1
      ELSE 0
      END AS window_flag
    FROM (
      SELECT
        parent.measurement_id AS measurement_id,
        parent.adj_Wt AS adj_Wt,
        child.measurement_id AS child_id,
        PERCENTILE_CONT(child.adj_Wt,.5) OVER (PARTITION BY parent.measurement_id) AS window_median
      FROM phys_df_wt_adj AS parent
      JOIN
        phys_df_wt_adj AS child USING (person_id)
      WHERE ABS(DATE_DIFF(parent.measurement_date, child.measurement_date, MONTH)) <= 12
        AND NOT parent.possible_pregnancy
        AND NOT child.possible_pregnancy
    )
    GROUP BY measurement_id
    HAVING COUNT(DISTINCT child_id) >= 3
  ),
  -- Patients with 3+ weights > 250 kg --
  gt250kg_pts AS (
    SELECT DISTINCT
      person_id,
      1 AS f_gt250kg
    FROM phys_df_wt_adj
    WHERE adj_Wt > 250
    GROUP BY person_id
    HAVING (COUNT(DISTINCT measurement_id) >= 3)
  ),
  weight_disagreement_pts AS (
    SELECT DISTINCT
      person_id,
      1 AS f_disagree2_Wt
    FROM (
      SELECT DISTINCT
        person_id,
        STDDEV_SAMP(adj_Wt) OVER (PARTITION BY person_id) AS sd_weight,
        COUNT(*) OVER (PARTITION BY person_id) AS n
      FROM phys_df_wt_adj
      WHERE value_as_number IS NOT NULL
        AND NOT possible_pregnancy
    )
    WHERE (n = 2 AND sd_weight > 10)
  )

    -- All the weight stuff in a table --
    SELECT
      ht.*,
      CASE
        WHEN adj_Wt>IF(f_outlierDx_Wt_high = 1 OR f_gt250kg=1,450,250) THEN 1
        WHEN adj_Wt<IF(f_outlierDx_Wt_low = 1, 20, 30) THEN 1
        ELSE 0
      END AS f_outlier_Wt,
      COALESCE(f_disagree2_Wt,0) AS f_disagree2_Wt,
      COALESCE(window_flag,0) AS window_flag
    FROM phys_df_wt_adj ht
    LEFT JOIN wt_variance_windows USING (measurement_id)
    LEFT JOIN gt250kg_pts USING (person_id)
    LEFT JOIN weight_disagreement_pts USING (person_id)
""")

NEW_WEIGHT_ROWS_QUERY = JINJA_ENV.from_string("""
CREATE OR REPLACE TABLE `{{project_id}}.{{sandbox_dataset_id}}.{{new_weight_rows}}` AS
SELECT
  *
  REPLACE(adj_Wt AS value_as_number,
  adj_unit AS unit_concept_id,
  COALESCE(u_c.concept_code, unit_source_value) AS unit_source_value)
FROM (
  SELECT
    measurement_id,
    adj_Wt,
    adj_unit
  FROM `{{project_id}}.{{sandbox_dataset_id}}.{{weight_table}}`
  WHERE f_outlier_Wt=0
    AND f_disagree2_Wt=0
    AND window_flag=0
    AND adj_Wt IS NOT NULL
    AND adj_unit=9529
)
JOIN `{{project_id}}.{{dataset_id}}.measurement` m USING (measurement_id)
LEFT JOIN `{{project_id}}.{{dataset_id}}.concept` u_c ON (adj_unit=concept_id)
""")

DROP_ROWS_QUERY = JINJA_ENV.from_string("""
  DELETE
  FROM `{{project_id}}.{{dataset_id}}.measurement`
  WHERE measurement_id IN (
    SELECT measurement_id
    FROM `{{project_id}}.{{dataset_id}}.measurement` AS m
    LEFT JOIN `{{project_id}}.{{dataset_id}}.measurement_ext` AS me
    USING (measurement_id)
    WHERE m.measurement_concept_id IN ({{ids_to_drop}})
    AND me.src_id NOT IN (
        SELECT 
          src_id
        FROM
          `{{project_id}}.pipeline_tables.site_maskings`
        WHERE NOT
          REGEXP_CONTAINS(src_id, r'(?i)(PPI/PM)|(EHR site)')
    )
  )
""")

INSERT_NEW_ROWS_QUERY = JINJA_ENV.from_string("""
SELECT {{fields}}
-- the lookup table contains joined table fields, so we must specify which fields to include --
FROM `{{project_id}}.{{sandbox_dataset_id}}.{{new_rows}}`
""")


class CleanHeightAndWeight(BaseCleaningRule):
    """
    Normalizes all height and weight data into cm and kg
    and removes invalid/implausible data points (rows)
    """

    def __init__(self,
                 project_id,
                 dataset_id,
                 sandbox_dataset_id,
                 table_namer=None):
        """
        Initializes the class with the proper information.

        Set the issue numbers, description and affected datasets. As other tickets
        may affect this SQL, append them to the list of Jira Issues.
        DO NOT REMOVE ORIGINAL JIRA ISSUE NUMBERS!
        """
        desc = (f'Normalizes all height and weight data into cm and kg '
                f'and removes invalid/implausible data points')
        super().__init__(issue_numbers=ISSUE_NUMBERS,
                         description=desc,
                         affected_datasets=[
                             cdr_consts.REGISTERED_TIER_DEID_CLEAN,
                             cdr_consts.CONTROLLED_TIER_DEID_CLEAN
                         ],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         affected_tables=[MEASUREMENT],
                         depends_on=[MeasurementRecordsSuppression],
                         table_namer=table_namer)

    def get_query_specs(self):
        """
        Returns a list of dictionary query specifications.

        :return: A list of dictionaries. Each dictionary contains a single query
            and a specification for how to execute that query. The specifications
            are optional but the query is required.
        """

        # height
        save_height_table_query = {
            cdr_consts.QUERY:
                CREATE_HEIGHT_SANDBOX_QUERY.render(
                    project_id=self.project_id,
                    dataset_id=self.dataset_id,
                    sandbox_dataset_id=self.sandbox_dataset_id,
                    height_table=self.sandbox_table_for(HEIGHT_TABLE),
                    measurement_concept_ids=','.join(
                        [str(con) for con in HEIGHT_CONCEPT_IDS]),
                    pipeline_tables=PIPELINE_TABLES),
        }

        save_new_height_rows_query = {
            cdr_consts.QUERY:
                NEW_HEIGHT_ROWS_QUERY.render(
                    project_id=self.project_id,
                    dataset_id=self.dataset_id,
                    sandbox_dataset_id=self.sandbox_dataset_id,
                    new_height_rows=self.sandbox_table_for(NEW_HEIGHT_ROWS),
                    height_table=self.sandbox_table_for(HEIGHT_TABLE)),
        }

        drop_height_rows_query = {
            cdr_consts.QUERY:
                DROP_ROWS_QUERY.render(
                    project_id=self.project_id,
                    dataset_id=self.dataset_id,
                    ids_to_drop=','.join(
                        [str(con) for con in HEIGHT_CONCEPT_IDS]))
        }

        insert_new_height_rows_query = {
            cdr_consts.QUERY:
                INSERT_NEW_ROWS_QUERY.render(
                    project_id=self.project_id,
                    sandbox_dataset_id=self.sandbox_dataset_id,
                    new_rows=self.sandbox_table_for(NEW_HEIGHT_ROWS),
                    fields=','.join(MEASUREMENT_FIELDS)),
            cdr_consts.DESTINATION_TABLE:
                MEASUREMENT,
            cdr_consts.DESTINATION_DATASET:
                self.dataset_id,
            cdr_consts.DISPOSITION:
                WRITE_APPEND
        }

        # weight
        save_weight_table_query = {
            cdr_consts.QUERY:
                CREATE_WEIGHT_SANDBOX_QUERY.render(
                    project_id=self.project_id,
                    sandbox_dataset_id=self.sandbox_dataset_id,
                    weight_table=self.sandbox_table_for(WEIGHT_TABLE),
                    dataset_id=self.dataset_id,
                    measurement_concept_ids=','.join(
                        [str(con) for con in WEIGHT_CONCEPT_IDS]),
                    pipeline_tables=PIPELINE_TABLES),
        }

        save_new_weight_rows_query = {
            cdr_consts.QUERY:
                NEW_WEIGHT_ROWS_QUERY.render(
                    project_id=self.project_id,
                    sandbox_dataset_id=self.sandbox_dataset_id,
                    new_weight_rows=self.sandbox_table_for(NEW_WEIGHT_ROWS),
                    weight_table=self.sandbox_table_for(WEIGHT_TABLE),
                    dataset_id=self.dataset_id),
        }

        drop_weight_rows_query = {
            cdr_consts.QUERY:
                DROP_ROWS_QUERY.render(
                    project_id=self.project_id,
                    dataset_id=self.dataset_id,
                    ids_to_drop=','.join(
                        [str(con) for con in WEIGHT_CONCEPT_IDS]))
        }

        insert_new_weight_rows_query = {
            cdr_consts.QUERY:
                INSERT_NEW_ROWS_QUERY.render(
                    project_id=self.project_id,
                    sandbox_dataset_id=self.sandbox_dataset_id,
                    new_rows=self.sandbox_table_for(NEW_WEIGHT_ROWS),
                    fields=','.join(MEASUREMENT_FIELDS)),
            cdr_consts.DESTINATION_TABLE:
                MEASUREMENT,
            cdr_consts.DESTINATION_DATASET:
                self.dataset_id,
            cdr_consts.DISPOSITION:
                WRITE_APPEND
        }

        return [
            save_height_table_query, save_new_height_rows_query,
            drop_height_rows_query, insert_new_height_rows_query,
            save_weight_table_query, save_new_weight_rows_query,
            drop_weight_rows_query, insert_new_weight_rows_query
        ]

    def setup_rule(self, client):
        """
        Function to run any data upload options before executing a query.
        """
        pass

    def setup_validation(self, client):
        """
        Run required steps for validation setup.
        """
        pass

    def validate_rule(self):
        """
        Validate the cleaning rule which deletes or updates the data from the tables
        """

        raise NotImplementedError("Please fix me.")

    def get_sandbox_tablenames(self):
        return [
            self.sandbox_table_for(table) for table in
            [HEIGHT_TABLE, WEIGHT_TABLE, NEW_HEIGHT_ROWS, NEW_WEIGHT_ROWS]
        ]


if __name__ == '__main__':
    import cdr_cleaner.clean_cdr_engine as clean_engine
    import cdr_cleaner.args_parser as parser

    ARGS = parser.parse_args()

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(ARGS.project_id,
                                                 ARGS.dataset_id,
                                                 ARGS.sandbox_dataset_id,
                                                 [(CleanHeightAndWeight,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(ARGS.project_id, ARGS.dataset_id,
                                   ARGS.sandbox_dataset_id,
                                   [(CleanHeightAndWeight,)])
