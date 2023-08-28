import cdm
from common import (DEATH, JINJA_ENV, PERSON, SURVEY_CONDUCT)

SOURCE_VALUE_EHR_CONSENT = 'EHRConsentPII_ConsentPermission'
CONCEPT_ID_CONSENT_PERMISSION_YES = 1586100  # ConsentPermission_Yes
EHR_CONSENT_TABLE_ID = '_ehr_consent'
VISIT_OCCURRENCE_ID = 'visit_occurrence_id'
PERSON_ID = 'person_id'
FOREIGN_KEYS_FIELDS = [
    'visit_occurrence_id', 'location_id', 'care_site_id', 'provider_id',
    'visit_detail_id'
]
RDR_TABLES_TO_COPY = [PERSON, SURVEY_CONDUCT]
DOMAIN_TABLES = list(
    set(cdm.tables_to_map()) - set(RDR_TABLES_TO_COPY) - set([DEATH]))
TABLES_TO_PROCESS = RDR_TABLES_TO_COPY + DOMAIN_TABLES
LEFT_JOIN = JINJA_ENV.from_string("""
LEFT JOIN
  (
    SELECT *
    FROM (
      SELECT
        *,
        row_number() OVER (PARTITION BY {{prefix}}.{{field}},{{prefix}}.src_hpo_id) AS row_num
      FROM `{{dataset_id}}.{{table}}` AS {{prefix}}
    )
    WHERE row_num = 1
  ) {{prefix}}
ON t.{{field}} = {{prefix}}.src_{{field}}
AND m.src_dataset_id = {{prefix}}.src_dataset_id
""")

JOIN_VISIT = JINJA_ENV.from_string("""
JOIN
  (
    SELECT *
    FROM (
      SELECT
        *,
        row_number() OVER (PARTITION BY {{prefix}}.{{field}}, {{prefix}}.src_hpo_id) AS row_num
      FROM `{{dataset_id}}.{{table}}` AS {{prefix}}
    )
    WHERE row_num = 1
  ) {{prefix}}
ON t.{{field}} = {{prefix}}.src_{{field}}
AND m.src_dataset_id = {{prefix}}.src_dataset_id
""")

LEFT_JOIN_PERSON = JINJA_ENV.from_string("""
LEFT JOIN
  (
    SELECT *
    FROM (
      SELECT
        *,
        row_number() OVER (PARTITION BY {{prefix}}.{{field}}, {{prefix}}.src_hpo_id) AS row_num
      FROM `{{dataset_id}}.{{table}}` AS {{prefix}}
    )
    WHERE row_num = 1
  ) {{prefix}}
ON t.{{field}} = {{prefix}}.src_{{field}}
""")

EHR_CONSENT_QUERY = JINJA_ENV.from_string("""
WITH ordered_response AS
  (
    SELECT
      person_id,
      value_source_concept_id,
      observation_datetime,
      ROW_NUMBER() OVER(
        PARTITION BY person_id ORDER BY observation_datetime DESC,
        value_source_concept_id ASC
        ) AS rn
    FROM `{{dataset_id}}.observation`
    WHERE observation_source_value = '{{source_value_ehr_consent}}'
  )
SELECT person_id
FROM ordered_response
WHERE rn = 1
AND value_source_concept_id = {{concept_id_consent_permission_yes}}
""")

COPY_RDR_QUERY = JINJA_ENV.from_string(
    """SELECT * FROM `{{rdr_dataset_id}}.{{table}}`""")

MAPPING_QUERY = JINJA_ENV.from_string("""
SELECT DISTINCT
    '{{rdr_dataset_id}}' AS src_dataset_id,
    {{domain_table}}_id AS src_{{domain_table}}_id,
    v.src_id as src_hpo_id,
    {% if domain_table in ['survey_conduct', 'person'] %}
    {{domain_table}}_id AS {{domain_table}}_id,
    {% else %}
    {{domain_table}}_id + {{mapping_constant}} AS {{domain_table}}_id,
    {% endif %}
    '{{domain_table}}' as src_table_id
FROM `{{rdr_dataset_id}}.{{domain_table}}` AS t
JOIN `{{rdr_dataset_id}}._mapping_{{domain_table}}` AS v
ON t.{{domain_table}}_id = v.{{domain_table}}_id
{% if domain_table not in ['survey_conduct', 'person'] %}
UNION ALL
SELECT DISTINCT
    '{{ehr_dataset_id}}' AS src_dataset_id,
    t.{{domain_table}}_id AS src_{{domain_table}}_id,
    v.src_hpo_id AS src_hpo_id,
    t.{{domain_table}}_id  AS {{domain_table}}_id,
    '{{domain_table}}' as src_table_id
FROM `{{ehr_dataset_id}}.{{domain_table}}` AS t
JOIN `{{ehr_dataset_id}}._mapping_{{domain_table}}` AS v
ON t.{{domain_table}}_id = v.{{domain_table}}_id
{% if person_id_flag %}
WHERE EXISTS
    (SELECT 1 FROM `{{combined_sandbox_dataset_id}}.{{ehr_consent_table_id}}` AS c
     WHERE t.person_id = c.person_id)
{% endif %}
{% endif %}
""")

LOAD_QUERY = JINJA_ENV.from_string("""
SELECT {{cols}}
FROM `{{rdr_dataset_id}}.{{domain_table}}` AS t
JOIN
(
    SELECT *
    FROM (
      SELECT
          *,
          row_number() OVER (PARTITION BY m.src_{{domain_table}}_id, m.src_hpo_id ) AS row_num
      FROM `{{combined_backup_dataset_id}}.{{mapping_table}}` AS m
    )
    WHERE row_num = 1
) m ON t.{{domain_table}}_id = m.src_{{domain_table}}_id
   {{join_expr}}
WHERE m.src_dataset_id = '{{rdr_dataset_id}}'

UNION ALL

SELECT {{cols}}
FROM
(
    SELECT *
    FROM (
      SELECT
          *,
          row_number() OVER (PARTITION BY m.{{domain_table}}_id) AS row_num
      FROM `{{ehr_dataset_id}}.{{domain_table}}` AS m
    )
    WHERE row_num = 1
) t
JOIN
(
    SELECT *
    FROM (
      SELECT
          *,
          row_number() OVER (PARTITION BY m.src_{{domain_table}}_id, m.src_hpo_id) AS row_num
      FROM `{{combined_backup_dataset_id}}.{{mapping_table}}` AS m
    )
    WHERE row_num = 1
) m
    ON t.{{domain_table}}_id = m.src_{{domain_table}}_id
   {{join_expr}}
WHERE m.src_dataset_id = '{{ehr_dataset_id}}'
""")

MAPPED_PERSON_QUERY = JINJA_ENV.from_string("""
select {{cols}}
from `{{dataset}}.{{table}}` AS t
{{join_expr}}
""")

FACT_RELATIONSHIP_QUERY = JINJA_ENV.from_string("""
SELECT *
FROM (
  SELECT
    fr.domain_concept_id_1 AS domain_concept_id_1,
    CASE
        WHEN domain_concept_id_1 = {{measurement_domain_concept_id}}
          THEN m1.measurement_id
        WHEN domain_concept_id_1 = {{observation_domain_concept_id}}
          THEN o1.observation_id
    END AS fact_id_1,
    fr.domain_concept_id_2,
    CASE
        WHEN domain_concept_id_2 = {{measurement_domain_concept_id}}
          THEN m2.measurement_id
        WHEN domain_concept_id_2 = {{observation_domain_concept_id}}
          THEN o2.observation_id
    END AS fact_id_2,
    fr.relationship_concept_id AS relationship_concept_id
  FROM `{{rdr_dataset_id}}.fact_relationship` AS fr
    LEFT JOIN `{{combined_backup_dataset_id}}.{{mapping_measurement}}` AS m1
      ON m1.src_measurement_id = fr.fact_id_1 AND fr.domain_concept_id_1={{measurement_domain_concept_id}}
    LEFT JOIN `{{combined_backup_dataset_id}}.{{mapping_observation}}` AS o1
      ON o1.src_observation_id = fr.fact_id_1 AND fr.domain_concept_id_1={{observation_domain_concept_id}}
    LEFT JOIN `{{combined_backup_dataset_id}}.{{mapping_measurement}}` AS m2
      ON m2.src_measurement_id = fr.fact_id_2 AND fr.domain_concept_id_2={{measurement_domain_concept_id}}
    LEFT JOIN `{{combined_backup_dataset_id}}.{{mapping_observation}}` AS o2
      ON o2.src_observation_id = fr.fact_id_2 AND fr.domain_concept_id_2={{observation_domain_concept_id}}

 UNION ALL

 SELECT * from `{{ehr_dataset}}.fact_relationship`)
WHERE fact_id_1 IS NOT NULL
AND fact_id_2 IS NOT NULL
""")

LOAD_AOU_DEATH = JINJA_ENV.from_string("""
CREATE TABLE `{{project}}.{{combined_backup}}.{{aou_death}}`
AS
SELECT
    aou_death_id,
    person_id,
    death_date,
    death_datetime,
    death_type_concept_id,
    cause_concept_id,
    cause_source_value,
    cause_source_concept_id,
    s.src_id,
    FALSE AS primary_death_record -- this value is re-calculated at CalculatePrimaryDeathRecord --
FROM `{{project}}.{{unioned_ehr_dataset}}.{{aou_death}}` ad
JOIN `{{project}}.{{combined_sandbox}}.{{site_masking}}` s
ON ad.src_id = s.hpo_id
WHERE EXISTS
   (SELECT 1 FROM `{{project}}.{{combined_sandbox}}.{{ehr_consent}}` AS ec
    WHERE ad.person_id = ec.person_id)
UNION ALL
SELECT
    aou_death_id,
    person_id,
    death_date,
    death_datetime,
    death_type_concept_id,
    cause_concept_id,
    cause_source_value,
    cause_source_concept_id,
    s.src_id,
    FALSE AS primary_death_record -- this value is re-calculated at CalculatePrimaryDeathRecord --
FROM `{{project}}.{{rdr_dataset}}.{{aou_death}}` ad
JOIN `{{project}}.{{combined_sandbox}}.{{site_masking}}` s
ON ad.src_id = s.hpo_id
""")
