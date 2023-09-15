from jinja2 import Template

# """
# Each field with STRING type and is_nullablle == 'YES' should be null
# EXCEPT for:
#   -observation.value_as_string with zip codes observation_source_concept_id in (1585966, 1585914, 1585930, 1585250)
# """
QUERY_SUPPRESSED_NULLABLE_FIELD_NOT_NULL = """
SELECT
    '{{ table_name }}' AS table_name,
    '{{ column_name }}' AS column_name,
    COUNT(*) AS n_row_violation
FROM `{{ project_id }}.{{ post_deid_dataset }}.{{ table_name }}`
WHERE {{ column_name }} IS NOT NULL
{% if table_name == 'observation' and column_name == 'value_as_string' %}
AND observation_source_concept_id NOT IN (1585250, 715711)
{% endif %}
"""

# """
# Each REQUIRED (Not nullable) field with STRING type should be an empty string
# """
QUERY_SUPPRESSED_REQUIRED_FIELD_NOT_EMPTY = """
SELECT
    '{{ table_name }}' AS table_name,
    '{{ column_name }}' AS column_name,
    COUNT(*) AS n_row_violation
FROM `{{ project_id }}.{{ post_deid_dataset }}.{{ table_name }}`
WHERE {{ column_name }} != ''
"""

# """
# Any numeric field should be 0 or NULL
# """
QUERY_SUPPRESSED_NUMERIC_NOT_ZERO = """
SELECT
    '{{ table_name }}' AS table_name,
    '{{ column_name }}' AS column_name,
    COUNT(*) AS n_row_violation
FROM `{{ project_id }}.{{ post_deid_dataset }}.{{ table_name }}`
WHERE {{ column_name }} != 0 AND {{ column_name }} IS NOT NULL
"""

# """
# Records with specific concept id or concept code must be suppressed
# """
QUERY_SUPPRESSED_CONCEPT = """
SELECT
    '{{ table_name }}' AS table_name,
    '{{ column_name }}' AS column_name,
    {% if concept_id|int(-999) != -999 %}
    {{ concept_id }} AS concept_id,
    {% else %}
    '{{ concept_code }}' AS concept_code,
    {% endif %}
    COUNT(*) AS n_row_violation
FROM `{{ project_id }}.{{ post_deid_dataset }}.{{ table_name }}`
{% if concept_id|int(-999) != -999 %}
    WHERE {{ column_name }} IN ({{ concept_id|int }})
{% else %}
    WHERE {{ column_name }} IN ('{{ concept_code|string }}')
{% endif %}
"""

# """
# Suppressed tables must be empty
# """
QUERY_SUPPRESSED_TABLE = """
SELECT
    '{{ table_name }}' AS table_name,
    COUNT(*) AS n_row_violation
FROM `{{ project_id }}.{{ post_deid_dataset }}.{{ table_name }}`
"""

# """
# person_ids should all be int
# Questionnaire_response_id should be int
# """
QUERY_ID_NOT_OF_CORRECT_TYPE = """
SELECT
    table_name,
    CASE WHEN data_type != '{{ data_type }}' THEN 1 ELSE 0 END AS n_row_violation
FROM `{{ project_id }}.{{ post_deid_dataset }}.INFORMATION_SCHEMA.COLUMNS`
WHERE column_name = '{{ column_name }}'
AND table_name = '{{ table_name }}'
"""

# """
# person_id post de-id should not be the same as the person_id pre de-id in tables other than person
# questionnaire_id post de-id should not be the same as questionnaire_id pre de-id
# """
QUERY_ID_NOT_CHANGED_BY_DEID = """
SELECT
    '{{ table_name }}' AS table_name,
    IFNULL(SUM(CASE WHEN input.{{ column_name }} = output.{{ column_name }} THEN 1 ELSE 0 END), 0) AS n_row_violation
FROM `{{ project_id }}.{{ post_deid_dataset }}.{{ table_name }}` output
JOIN `{{ project_id }}.{{ pre_deid_dataset }}.{{ table_name }}` input USING({{ primary_key }})
"""

# """
# person_id post deid should be the same as research_id from the mapping table
# questionnaire_id post deid should be the same as research_response_id from the mapping table
# """
# TODO (Francis R.): change pre_deid_dataset to mapping_dataset for the map, add that to run_check_by_row in helpers
QUERY_ID_NOT_IN_MAPPING = """
SELECT
    '{{ table_name }}' AS table_name,
    COUNT(*) AS n_row_violation
FROM `{{ project_id }}.{{ post_deid_dataset }}.{{ table_name }}`
{% if column_name == 'survey_source_identifier' %}
WHERE CAST({{ column_name }} AS INT64)
{% else %}
WHERE {{ column_name }}
{% endif %}
 NOT IN (
    SELECT {{ new_id }}
    {% if mapping_table == 'site_maskings' %}
    FROM `{{ project_id }}.{{ pipeline_dataset }}.{{ mapping_table }}`
    {% else %}
    FROM `{{ project_id }}.{{ mapping_dataset }}.{{ mapping_table }}`
    {% endif %}
)
"""

# """
# person_id post-deid should be mapped correctly to person_id pre-deid if we use the mapping table
# questionnaire_id post-deid should be mapped correctly to response_research_id pre-deid if we use the mapping table
# """
QUERY_ID_NOT_MAPPED_PROPERLY = """
WITH data AS (
SELECT
    map.{{ new_id }} AS expected_pid,
    CAST(post_deid.{{ column_name }} AS INT64) AS output_pid
FROM `{{ project_id }}.{{ post_deid_dataset }}.{{ table_name }}` post_deid
LEFT JOIN `{{ project_id }}.{{ pre_deid_dataset }}.{{ table_name }}` pre_deid USING({{ primary_key }})
{% if mapping_table == 'site_maskings' %}
LEFT JOIN `{{ project_id }}.{{ pipeline_dataset }}.{{ mapping_table }}` map
ON pre_deid.{{ column_name }} = map.{{ column_name }}
{% elif mapping_table == '_deid_map' %}
LEFT JOIN `{{ project_id }}.{{ mapping_dataset }}.{{ mapping_table }}` map
ON pre_deid.{{ column_name }} = map.{{ column_name }}
{% else %}
LEFT JOIN `{{ project_id }}.{{ questionnaire_response_dataset }}.{{ mapping_table }}` map
ON CAST(pre_deid.{{ column_name }} AS INT64) = map.questionnaire_response_id
{% endif %}
)
SELECT
    '{{ table_name }}' AS table_name,
    IFNULL(SUM(CASE WHEN output_pid != expected_pid THEN 1 ELSE 0 END), 0) AS n_row_violation
FROM data
"""

QUERY_SITE_ID_NOT_MAPPED_PROPERLY = """
WITH data AS (
SELECT
    map.{{ new_id }} AS expected_pid,
    post_deid.{{ column_name }} AS output_pid
FROM `{{ project_id }}.{{ post_deid_dataset }}.{{ table_name }}` post_deid
LEFT JOIN `{{ project_id }}.{{ pre_deid_dataset }}._mapping_{{ table_name|replace("_ext","") }}` pre_deid USING({{ primary_key }})
LEFT JOIN `{{ project_id }}.{{ pipeline_dataset }}.{{ mapping_table }}` map ON pre_deid.src_hpo_id = map.hpo_id
)
SELECT
    '{{ table_name }}' AS table_name,
    IFNULL(SUM(CASE WHEN output_pid != expected_pid THEN 1 ELSE 0 END), 0) AS n_row_violation
FROM data

"""

QUERY_VEHICLE_ACCIDENT_SUPPRESSION_ICD9 = """
SELECT
    '{{ table_name }}' AS table_name,
    '{{ column_name }}' AS column_name,
    COUNT(*) AS n_row_violation
FROM `{{ project_id }}.{{ post_deid_dataset }}.{{ table_name }}`
WHERE {{ column_name }} IN (
    SELECT concept_id
    FROM `{{ project_id }}.{{ post_deid_dataset }}.concept`
    WHERE REGEXP_CONTAINS(concept_code, r"^E8[0-4][0-9]")
    AND NOT REGEXP_CONTAINS(concept_code, r"E8[0-4][0-9][\d]")
)
"""

QUERY_VEHICLE_ACCIDENT_SUPPRESSION_ICD10 = """
SELECT
    '{{ table_name }}' AS table_name,
    '{{ column_name }}' AS column_name,
    COUNT(*) AS n_row_violation
FROM `{{ project_id }}.{{ post_deid_dataset }}.{{ table_name }}`
WHERE {{ column_name }} IN (
    SELECT concept_id
    FROM `{{ project_id }}.{{ post_deid_dataset }}.concept`
    WHERE REGEXP_CONTAINS(concept_code, r"^V")
    AND REGEXP_CONTAINS(vocabulary_id, r"^ICD10")
)
"""

QUERY_CANCER_CONCEPT_SUPPRESSION = """
SELECT
    '{{ table_name }}' AS table_name,
    '{{ column_name }}' AS column_name,
    COUNT(*) AS n_row_violation
FROM `{{ project_id }}.{{ post_deid_dataset }}.{{ table_name }}`
WHERE {{ column_name }} IN (
    SELECT concept_id
    FROM `{{ project_id }}.{{ post_deid_dataset }}.concept`
    WHERE REGEXP_CONTAINS(concept_code, r'(History_WhichConditions)|(Condition_OtherCancer)|(History_AdditionalDiagnosis)|(OutsideTravel6MonthsWhere)')
)
"""

QUERY_ZIP_CODE_GENERALIZATION = """
WITH zips AS (
  SELECT
    LEFT(CONCAT(LEFT(pre_deid.{{ column_name }}, 3), REPEAT('*',
        LENGTH(pre_deid.{{ column_name }}) - 3)), 5) AS expected_zip,
    LENGTH(pre_deid.{{ column_name }}) AS n_actual_zip,
    post_deid.{{ column_name }} AS output_zip
  FROM `{{ project_id }}.{{ post_deid_dataset }}.{{ table_name }}` post_deid
  LEFT JOIN `{{ project_id }}.{{ pre_deid_dataset }}.{{ table_name }}` pre_deid USING({{ primary_key }})
  WHERE post_deid.observation_source_concept_id IN (1585250)
), transformed_zips AS (
  SELECT
    CASE
      WHEN expected_zip = zip_code_3 THEN transformed_zip_code_3
      ELSE expected_zip
      END
      AS expected_zip,
    n_actual_zip,
    output_zip
  FROM zips
  LEFT JOIN `{{ project_id }}.{{ pipeline_dataset }}.{{ zip_table_name }}` zip_agg ON (zips.expected_zip=zip_agg.zip_code_3)
)
SELECT
  '{{ table_name }}' AS table_name,
  IFNULL(SUM(CASE WHEN output_zip != expected_zip THEN 1 ELSE 0 END), 0) AS n_row_violation
FROM transformed_zips
"""

QUERY_SUPPRESSED_FREE_TEXT_RESPONSE = """
SELECT
    '{{ table_name }}' AS table_name,
    '{{ column_name }}' AS column_name,
    COUNT(*) AS n_row_violation
FROM `{{ project_id }}.{{ post_deid_dataset }}.{{ table_name }}`
WHERE {{ column_name }} IN (
    SELECT concept_id
    FROM `{{ project_id }}.{{ post_deid_dataset }}.concept`
    WHERE REGEXP_CONTAINS(concept_code, r"(FreeText)|(TextBox)")
    OR concept_code = 'notes'
)
"""

QUERY_GEOLOCATION_SUPPRESSION = """
SELECT
    '{{ table_name }}' AS table_name,
    '{{ column_name }}' AS column_name,
    COUNT(*) AS n_row_violation
FROM `{{ project_id }}.{{ post_deid_dataset }}.{{ table_name }}`
WHERE {{ column_name }} IN (
    SELECT concept_id
    FROM `{{ project_id }}.{{ post_deid_dataset }}.concept`
    WHERE REGEXP_CONTAINS(concept_code, r"(SitePairing)|(City)|(ArizonaSpecific)|(Michigan)|(_Country)|(ExtraConsent_[A-Za-z]+((Care)|(Registered)))")
    AND (concept_class_id = 'Question' OR concept_class_id = 'Topic')
    AND vocabulary_id = 'PPI'
)
"""

CONTROLLED_TIER_RACE_SUBCATEGORY = """
SELECT value_as_concept_id, COUNT AS Count
FROM (
  SELECT
    SUM( CASE WHEN value_as_concept_id = 1585605 THEN 1 ELSE 0 END ) AS cambodian,
    SUM( CASE WHEN value_as_concept_id = 1585606 THEN 1 ELSE 0 END ) AS chinese,
    SUM( CASE WHEN value_as_concept_id = 1585607 THEN 1 ELSE 0 END ) AS filipino,
    SUM( CASE WHEN value_as_concept_id = 1585608 THEN 1 ELSE 0 END ) AS hmong,
    SUM( CASE WHEN value_as_concept_id = 1585609 THEN 1 ELSE 0 END ) AS indian,
    SUM( CASE WHEN value_as_concept_id = 1585610 THEN 1 ELSE 0 END ) AS pakistani,
    SUM( CASE WHEN value_as_concept_id = 1585611 THEN 1 ELSE 0 END ) AS vietnamese,
    SUM( CASE WHEN value_as_concept_id = 1585612 THEN 1 ELSE 0 END ) AS korean,
    SUM( CASE WHEN value_as_concept_id = 1585613 THEN 1 ELSE 0 END ) AS japanese,
    SUM( CASE WHEN value_as_concept_id = 1585614 THEN 1 ELSE 0 END ) AS asian_none_of_these,
    SUM( CASE WHEN value_as_concept_id = 1585616 THEN 1 ELSE 0 END ) AS barbadian,
    SUM( CASE WHEN value_as_concept_id = 1585617 THEN 1 ELSE 0 END ) AS caribbean,
    SUM( CASE WHEN value_as_concept_id = 1585618 THEN 1 ELSE 0 END ) AS liberian,
    SUM( CASE WHEN value_as_concept_id = 1585619 THEN 1 ELSE 0 END ) AS somali,
    SUM( CASE WHEN value_as_concept_id = 1585620 THEN 1 ELSE 0 END ) AS south_african,
    SUM( CASE WHEN value_as_concept_id = 1585621 THEN 1 ELSE 0 END ) AS african_american,
    SUM( CASE WHEN value_as_concept_id = 1585622 THEN 1 ELSE 0 END ) AS jamaican,
    SUM( CASE WHEN value_as_concept_id = 1585623 THEN 1 ELSE 0 END ) AS haitian,
    SUM( CASE WHEN value_as_concept_id = 1585624 THEN 1 ELSE 0 END ) AS nigerian,
    SUM( CASE WHEN value_as_concept_id = 1585625 THEN 1 ELSE 0 END ) AS ethiopian,
    SUM( CASE WHEN value_as_concept_id = 1585626 THEN 1 ELSE 0 END ) AS ghanaian,
    SUM( CASE WHEN value_as_concept_id = 1585627 THEN 1 ELSE 0 END ) AS black_none_of_these,
    SUM( CASE WHEN value_as_concept_id = 1585345 THEN 1 ELSE 0 END ) AS mexican,
    SUM( CASE WHEN value_as_concept_id = 1585346 THEN 1 ELSE 0 END ) AS puerto_rican,
    SUM( CASE WHEN value_as_concept_id = 1586086 THEN 1 ELSE 0 END ) AS cuban,
    SUM( CASE WHEN value_as_concept_id = 1586087 THEN 1 ELSE 0 END ) AS dominican,
    SUM( CASE WHEN value_as_concept_id = 1586088 THEN 1 ELSE 0 END ) AS honduran,
    SUM( CASE WHEN value_as_concept_id = 1586089 THEN 1 ELSE 0 END ) AS salvadoran,
    SUM( CASE WHEN value_as_concept_id = 1586090 THEN 1 ELSE 0 END ) AS colombian,
    SUM( CASE WHEN value_as_concept_id = 1586091 THEN 1 ELSE 0 END ) AS ecuadorian,
    SUM( CASE WHEN value_as_concept_id = 1586092 THEN 1 ELSE 0 END ) AS hispanic_spanish,
    SUM( CASE WHEN value_as_concept_id = 1586093 THEN 1 ELSE 0 END ) AS hispanic_none_of_these,
    SUM( CASE WHEN value_as_concept_id = 1585316 THEN 1 ELSE 0 END ) AS egyptian,
    SUM( CASE WHEN value_as_concept_id = 1585633 THEN 1 ELSE 0 END ) AS iranian,
    SUM( CASE WHEN value_as_concept_id = 1585630 THEN 1 ELSE 0 END ) AS iraqi,
    SUM( CASE WHEN value_as_concept_id = 1585631 THEN 1 ELSE 0 END ) AS israeli,
    SUM( CASE WHEN value_as_concept_id = 1585629 THEN 1 ELSE 0 END ) AS labanese,
    SUM( CASE WHEN value_as_concept_id = 1585319 THEN 1 ELSE 0 END ) AS middle_eastern_none_of_these,
    SUM( CASE WHEN value_as_concept_id = 1585318 THEN 1 ELSE 0 END ) AS moroccan,
    SUM( CASE WHEN value_as_concept_id = 1585317 THEN 1 ELSE 0 END ) AS syrian,
    SUM( CASE WHEN value_as_concept_id = 1585632 THEN 1 ELSE 0 END ) AS tunisian,
    SUM( CASE WHEN value_as_concept_id = 1585321 THEN 1 ELSE 0 END ) AS native_hawailan,
    SUM( CASE WHEN value_as_concept_id = 1585322 THEN 1 ELSE 0 END ) AS samoan,
    SUM( CASE WHEN value_as_concept_id = 1585323 THEN 1 ELSE 0 END ) AS chuukese,
    SUM( CASE WHEN value_as_concept_id = 1585324 THEN 1 ELSE 0 END ) AS palauan,
    SUM( CASE WHEN value_as_concept_id = 1585325 THEN 1 ELSE 0 END ) AS tahitian,
    SUM( CASE WHEN value_as_concept_id = 1585326 THEN 1 ELSE 0 END ) AS chamorro,
    SUM( CASE WHEN value_as_concept_id = 1585327 THEN 1 ELSE 0 END ) AS tongan,
    SUM( CASE WHEN value_as_concept_id = 1585837 THEN 1 ELSE 0 END ) AS afghan,
    SUM( CASE WHEN value_as_concept_id = 1585836 THEN 1 ELSE 0 END ) AS algerian,
    SUM( CASE WHEN value_as_concept_id = 1585328 THEN 1 ELSE 0 END ) AS fijian,
    SUM( CASE WHEN value_as_concept_id = 1585329 THEN 1 ELSE 0 END ) AS marshallese,
    SUM( CASE WHEN value_as_concept_id = 1585330 THEN 1 ELSE 0 END ) AS pacific_islander_none_of_these,
    SUM( CASE WHEN value_as_concept_id = 1585339 THEN 1 ELSE 0 END ) AS english,
    SUM( CASE WHEN value_as_concept_id = 1585332 THEN 1 ELSE 0 END ) AS german,
    SUM( CASE WHEN value_as_concept_id = 1585333 THEN 1 ELSE 0 END ) AS dutch,
    SUM( CASE WHEN value_as_concept_id = 1585334 THEN 1 ELSE 0 END ) AS norwegian,
    SUM( CASE WHEN value_as_concept_id = 1585335 THEN 1 ELSE 0 END ) AS scottish,
    SUM( CASE WHEN value_as_concept_id = 1585336 THEN 1 ELSE 0 END ) AS white_spanish,
    SUM( CASE WHEN value_as_concept_id = 1585337 THEN 1 ELSE 0 END ) AS european,
    SUM( CASE WHEN value_as_concept_id = 1585338 THEN 1 ELSE 0 END ) AS irish,
    SUM( CASE WHEN value_as_concept_id = 1585340 THEN 1 ELSE 0 END ) AS italian,
    SUM( CASE WHEN value_as_concept_id = 1585341 THEN 1 ELSE 0 END ) AS polish,
    SUM( CASE WHEN value_as_concept_id = 1585342 THEN 1 ELSE 0 END ) AS french,
    SUM( CASE WHEN value_as_concept_id = 1585343 THEN 1 ELSE 0 END ) AS white_none_of_these,
    SUM( CASE WHEN value_as_concept_id = 1586146 THEN 1 ELSE 0 END ) AS white,
    SUM( CASE WHEN value_as_concept_id = 1586143 THEN 1 ELSE 0 END ) AS black,
    SUM( CASE WHEN value_as_concept_id = 1586147 THEN 1 ELSE 0 END ) AS hispanic,
    SUM( CASE WHEN value_as_concept_id = 1586142 THEN 1 ELSE 0 END ) AS asian,
    SUM( CASE WHEN value_as_concept_id = 1586145 THEN 1 ELSE 0 END ) AS pacific_islander,
    SUM( CASE WHEN value_as_concept_id = 1586144 THEN 1 ELSE 0 END ) AS middle_eastern
  FROM `{{ project_id }}.{{ post_deid_dataset }}.observation`
) AS PivotQuery
UNPIVOT (
  COUNT FOR value_as_concept_id IN (
    chinese,
    filipino,
    hmong,
    indian,
    pakistani,
    vietnamese,
    korean,
    japanese,
    asian_none_of_these,
    barbadian,
    caribbean,
    liberian,
    somali,
    south_african,
    african_american,
    jamaican,
    haitian,
    nigerian,
    ethiopian,
    ghanaian,
    black_none_of_these,
    mexican,
    puerto_rican,
    cuban,
    dominican,
    honduran,
    salvadoran,
    colombian,
    ecuadorian,
    hispanic_spanish,
    hispanic_none_of_these,
    egyptian,
    iranian,
    iraqi,
    israeli,
    labanese,
    middle_eastern_none_of_these,
    moroccan,
    syrian,
    tunisian,
    native_hawailan,
    samoan,
    chuukese,
    palauan,
    tahitian,
    chamorro,
    tongan,
    afghan,
    algerian,
    fijian,
    marshallese,
    pacific_islander_none_of_these,
    english,
    german,
    dutch,
    norwegian,
    scottish,
    white_spanish,
    european,
    irish,
    italian,
    polish,
    french,
    white_none_of_these,
    white,
    black,
    hispanic,
    asian,
    pacific_islander,
    middle_eastern
  )) AS UnpivotedData;
"""