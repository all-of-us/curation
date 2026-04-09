# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.7.1
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

#
# # Purpose:
# This notebook is used to validate new uploads of the OMOP vocabularys prior to implementing in production.<br>

# +
# # + Import packages
from common import JINJA_ENV
from gcloud.bq import BigQueryClient
from analytics.cdr_ops.notebook_utils import execute, display_row_level_check_results
import pandas as pd

# + tags=["parameters"]
test_project = ''
prod_project = ''
old_vocabulary = ''
new_vocabulary = ''
unioned_ehr_dataset = ''
find_prevalences = True
row_level_preview_rows = 200
# -

vocabulary_dataset_old = f'{test_project}.{old_vocabulary}'
vocabulary_dataset_new = f'{test_project}.{new_vocabulary}'
unioned_ehr_dataset_name = (
    unioned_ehr_dataset.split('.')[-1] if unioned_ehr_dataset else '')
unioned_ehr_dataset_ref = (
    unioned_ehr_dataset if unioned_ehr_dataset and '.' in unioned_ehr_dataset
    else f'{prod_project}.{unioned_ehr_dataset_name}'
    if unioned_ehr_dataset_name else '')

client = BigQueryClient(test_project)

pd.set_option('max_colwidth', None)

# # Privacy-Only Vocabulary QC Checks
# This notebook intentionally contains only privacy-specific checks not in vocabulary_upload_qc:
# 1) Privacy mapping concept coverage meta queries
# 2) Row-level parity checks enriched with prevalence values


def run_tantalus_check(check_name, sql, max_rows=True):
    """Run a Jinja-templated SQL check and return dataframe results."""
    print(f'\n### {check_name}\n')
    tpl = JINJA_ENV.from_string(sql)
    query = tpl.render(vocabulary_dataset_old=vocabulary_dataset_old,
                       vocabulary_dataset_new=vocabulary_dataset_new,
                       unioned_ehr_dataset=unioned_ehr_dataset_name,
                       unioned_ehr_dataset_name=unioned_ehr_dataset_name,
                       unioned_ehr_dataset_ref=unioned_ehr_dataset_ref,
                       prod_project=prod_project)
    return execute(client, query, max_rows=max_rows)


# ## ROW-LEVEL TESTS BASE CTEs
# This base SQL is shared by the prevalence-enriched row-level check below.
tantalus_row_level_base_sql = '''
WITH source_to_standard_new AS (
  SELECT
    CONCAT(CAST(c.concept_id AS STRING), '-', CAST(c1.concept_id AS STRING)) AS unique_id,
    c.concept_code AS source_code,
    c.concept_id AS source_concept_id,
    c.concept_name AS source_code_description,
    c.vocabulary_id AS source_vocabulary_id,
    c.domain_id AS source_domain_id,
    c.concept_class_id AS source_concept_class_id,
    c.valid_start_date AS source_valid_start_date,
    c.valid_end_date AS source_valid_end_date,
    c.invalid_reason AS source_invalid_reason,
    c1.concept_id AS target_concept_id,
    c1.concept_name AS target_concept_name,
    c1.vocabulary_id AS target_vocabulary_id,
    c1.domain_id AS target_domain_id,
    c1.concept_class_id AS target_concept_class_id,
    c1.invalid_reason AS target_invalid_reason,
    c1.standard_concept AS target_standard_concept
  FROM `{{vocabulary_dataset_new}}.concept` c
  JOIN `{{vocabulary_dataset_new}}.concept_relationship` cr
    ON c.concept_id = cr.concept_id_1
   AND cr.invalid_reason IS NULL
   AND LOWER(cr.relationship_id) = 'maps to'
  JOIN `{{vocabulary_dataset_new}}.concept` c1
    ON cr.concept_id_2 = c1.concept_id
   AND c1.invalid_reason IS NULL

  UNION DISTINCT

  SELECT
    CONCAT(CAST(stcm.source_concept_id AS STRING), '-', CAST(stcm.target_concept_id AS STRING)) AS unique_id,
    stcm.source_code AS source_code,
    stcm.source_concept_id AS source_concept_id,
    stcm.source_code_description AS source_code_description,
    stcm.source_vocabulary_id AS source_vocabulary_id,
    c1.domain_id AS source_domain_id,
    c2.concept_class_id AS source_concept_class_id,
    c1.valid_start_date AS source_valid_start_date,
    c1.valid_end_date AS source_valid_end_date,
    stcm.invalid_reason AS source_invalid_reason,
    stcm.target_concept_id AS target_concept_id,
    c2.concept_name AS target_concept_name,
    stcm.target_vocabulary_id AS target_vocabulary_id,
    c2.domain_id AS target_domain_id,
    c2.concept_class_id AS target_concept_class_id,
    c2.invalid_reason AS target_invalid_reason,
    c2.standard_concept AS target_standard_concept
  FROM `{{vocabulary_dataset_new}}.source_to_concept_map` stcm
  LEFT JOIN `{{vocabulary_dataset_new}}.concept` c1
    ON c1.concept_id = stcm.source_concept_id
  LEFT JOIN `{{vocabulary_dataset_new}}.concept` c2
    ON c2.concept_id = stcm.target_concept_id
  WHERE stcm.invalid_reason IS NULL
),
source_to_standard_old AS (
  SELECT
    CONCAT(CAST(c.concept_id AS STRING), '-', CAST(c1.concept_id AS STRING)) AS unique_id,
    c.concept_code AS source_code,
    c.concept_id AS source_concept_id,
    c.concept_name AS source_code_description,
    c.vocabulary_id AS source_vocabulary_id,
    c.domain_id AS source_domain_id,
    c.concept_class_id AS source_concept_class_id,
    c.valid_start_date AS source_valid_start_date,
    c.valid_end_date AS source_valid_end_date,
    c.invalid_reason AS source_invalid_reason,
    c1.concept_id AS target_concept_id,
    c1.concept_name AS target_concept_name,
    c1.vocabulary_id AS target_vocabulary_id,
    c1.domain_id AS target_domain_id,
    c1.concept_class_id AS target_concept_class_id,
    c1.invalid_reason AS target_invalid_reason,
    c1.standard_concept AS target_standard_concept
  FROM `{{vocabulary_dataset_old}}.concept` c
  JOIN `{{vocabulary_dataset_old}}.concept_relationship` cr
    ON c.concept_id = cr.concept_id_1
   AND cr.invalid_reason IS NULL
   AND LOWER(cr.relationship_id) = 'maps to'
  JOIN `{{vocabulary_dataset_old}}.concept` c1
    ON cr.concept_id_2 = c1.concept_id
   AND c1.invalid_reason IS NULL

  UNION DISTINCT

  SELECT
    CONCAT(CAST(stcm.source_concept_id AS STRING), '-', CAST(stcm.target_concept_id AS STRING)) AS unique_id,
    stcm.source_code AS source_code,
    stcm.source_concept_id AS source_concept_id,
    stcm.source_code_description AS source_code_description,
    stcm.source_vocabulary_id AS source_vocabulary_id,
    c1.domain_id AS source_domain_id,
    c2.concept_class_id AS source_concept_class_id,
    c1.valid_start_date AS source_valid_start_date,
    c1.valid_end_date AS source_valid_end_date,
    stcm.invalid_reason AS source_invalid_reason,
    stcm.target_concept_id AS target_concept_id,
    c2.concept_name AS target_concept_name,
    stcm.target_vocabulary_id AS target_vocabulary_id,
    c2.domain_id AS target_domain_id,
    c2.concept_class_id AS target_concept_class_id,
    c2.invalid_reason AS target_invalid_reason,
    c2.standard_concept AS target_standard_concept
  FROM `{{vocabulary_dataset_old}}.source_to_concept_map` stcm
  LEFT JOIN `{{vocabulary_dataset_old}}.concept` c1
    ON c1.concept_id = stcm.source_concept_id
  LEFT JOIN `{{vocabulary_dataset_old}}.concept` c2
    ON c2.concept_id = stcm.target_concept_id
  WHERE stcm.invalid_reason IS NULL
),
test_vocab_version AS (
  SELECT
    '1.01' AS test_id,
    'Old and new vocabulary version' AS test_description,
    'vocabulary' AS vocabulary_table,
    0 AS concept_id,
    '' AS concept_name,
    new_vocabulary.vocabulary_id AS vocabulary_id,
    'vocabulary' AS domain_id,
    old_vocabulary.vocabulary_version AS old_value,
    new_vocabulary.vocabulary_version AS new_value
  FROM `{{vocabulary_dataset_old}}.vocabulary` old_vocabulary
  JOIN `{{vocabulary_dataset_new}}.vocabulary` new_vocabulary
    ON old_vocabulary.vocabulary_id = new_vocabulary.vocabulary_id
  WHERE LOWER(old_vocabulary.vocabulary_id) = 'none'
),
test_concept_name AS (
  SELECT
    '9.01' AS test_id,
    'Concept name changed' AS test_description,
    'concept' AS vocabulary_table,
    new_concept.concept_id AS concept_id,
    new_concept.concept_name AS concept_name,
    new_concept.vocabulary_id AS vocabulary_id,
    new_concept.domain_id AS domain_id,
    old_concept.concept_name AS old_value,
    new_concept.concept_name AS new_value
  FROM `{{vocabulary_dataset_old}}.concept` old_concept
  JOIN `{{vocabulary_dataset_new}}.concept` new_concept
    ON old_concept.concept_id = new_concept.concept_id
  WHERE TRIM(old_concept.concept_name) != TRIM(new_concept.concept_name)
),
test_domain_change AS (
  SELECT
    '10.01' AS test_id,
    'Concept domain changed' AS test_description,
    'concept' AS vocabulary_table,
    new_concept.concept_id AS concept_id,
    new_concept.concept_name AS concept_name,
    new_concept.vocabulary_id AS vocabulary_id,
    new_concept.domain_id AS domain_id,
    old_concept.domain_id AS old_value,
    new_concept.domain_id AS new_value
  FROM `{{vocabulary_dataset_old}}.concept` old_concept
  JOIN `{{vocabulary_dataset_new}}.concept` new_concept
    ON old_concept.concept_id = new_concept.concept_id
  WHERE old_concept.domain_id != new_concept.domain_id
),
test_deleted_concepts AS (
  SELECT
    '14.01' AS test_id,
    'Concept_id missing between old and new vocab' AS test_description,
    'concept' AS vocabulary_table,
    old_concept.concept_id AS concept_id,
    old_concept.concept_name AS concept_name,
    old_concept.vocabulary_id AS vocabulary_id,
    old_concept.domain_id AS domain_id,
    CONCAT(CAST(old_concept.concept_id AS STRING), ' (', old_concept.concept_name, ')') AS old_value,
    '' AS new_value
  FROM `{{vocabulary_dataset_old}}.concept` old_concept
  LEFT JOIN `{{vocabulary_dataset_new}}.concept` new_concept
    ON old_concept.concept_id = new_concept.concept_id
  WHERE new_concept.concept_id IS NULL
),
test_new_standard_maps AS (
  SELECT
    '99.01' AS test_id,
    'Source concepts with new standard mappings, including 0' AS test_description,
    'concept_relationship' AS vocabulary_table,
    source_concept_id AS concept_id,
    source_code_description AS concept_name,
    source_vocabulary_id AS vocabulary_id,
    source_domain_id AS domain_id,
    CAST(old_target_concept_id AS STRING) AS old_value,
    CAST(new_target_concept_id AS STRING) AS new_value
  FROM (
    SELECT
      o.unique_id,
      o.source_code,
      o.source_code_description,
      o.source_concept_id,
      o.source_vocabulary_id,
      o.source_domain_id,
      o.old_target_concept_id,
      o.map_exists_in_new_vocab,
      CASE
        WHEN o.source_concept_id IN (
          SELECT concept_id
          FROM `{{vocabulary_dataset_new}}.concept`
          WHERE invalid_reason IS NULL
        ) THEN 1
        ELSE 0
      END AS source_code_in_new_vocab,
      CASE
        WHEN o.old_target_concept_id IN (
          SELECT concept_id
          FROM `{{vocabulary_dataset_new}}.concept`
          WHERE invalid_reason IS NOT NULL
        ) THEN 1
        ELSE 0
      END AS old_target_deprecated,
      CASE
        WHEN n.target_concept_id IS NULL THEN 0
        ELSE n.target_concept_id
      END AS new_target_concept_id,
      n.target_domain_id AS new_target_domain_id,
      CASE
        WHEN n.unique_id IN (SELECT unique_id FROM source_to_standard_old) THEN 1
        ELSE 0
      END AS new_map_exists_in_old_vocab,
      ROW_NUMBER() OVER (PARTITION BY o.unique_id ORDER BY o.unique_id) AS rn
    FROM (
      SELECT
        o.unique_id,
        o.source_code,
        o.source_code_description,
        o.source_concept_id,
        o.source_vocabulary_id,
        o.source_domain_id,
        o.target_concept_id AS old_target_concept_id,
        CASE
          WHEN o.unique_id IN (SELECT unique_id FROM source_to_standard_new) THEN 1
          ELSE 0
        END AS map_exists_in_new_vocab
      FROM source_to_standard_old o
      WHERE o.source_vocabulary_id NOT LIKE 'JNJ%'
        AND o.source_concept_id != 0
    ) o
    LEFT JOIN source_to_standard_new n
      ON o.source_concept_id = n.source_concept_id
    WHERE o.map_exists_in_new_vocab = 0
  ) a
),
all_results AS (
  SELECT * FROM test_vocab_version
  UNION ALL
  SELECT * FROM test_concept_name
  UNION ALL
  SELECT * FROM test_domain_change
  UNION ALL
  SELECT * FROM test_deleted_concepts
  UNION ALL
  SELECT * FROM test_new_standard_maps
),
row_level_with_numbers AS (
  SELECT
    ar.*,
    COALESCE(
      (
        SELECT ARRAY_AGG(CAST(num AS FLOAT64) ORDER BY CAST(num AS FLOAT64))
        FROM UNNEST(
          REGEXP_EXTRACT_ALL(COALESCE(ar.old_value, ''), r'[0-9]*[.]{0,1}[0-9]+')
        ) AS num
      ),
      ARRAY<FLOAT64>[]
    ) AS old_numbers,
    COALESCE(
      (
        SELECT ARRAY_AGG(CAST(num AS FLOAT64) ORDER BY CAST(num AS FLOAT64))
        FROM UNNEST(
          REGEXP_EXTRACT_ALL(COALESCE(ar.new_value, ''), r'[0-9]*[.]{0,1}[0-9]+')
        ) AS num
      ),
      ARRAY<FLOAT64>[]
    ) AS new_numbers
  FROM all_results ar
),
enriched_results AS (
  SELECT
    rwn.* EXCEPT (old_numbers, new_numbers),
    EDIT_DISTANCE(COALESCE(rwn.old_value, ''), COALESCE(rwn.new_value, '')) AS string_dist,
    CASE
      WHEN ARRAY_LENGTH(rwn.old_numbers) != ARRAY_LENGTH(rwn.new_numbers) THEN TRUE
      WHEN ARRAY_LENGTH(rwn.old_numbers) = 0
       AND ARRAY_LENGTH(rwn.new_numbers) = 0 THEN FALSE
      ELSE TO_JSON_STRING(rwn.old_numbers) != TO_JSON_STRING(rwn.new_numbers)
    END AS num_check
  FROM row_level_with_numbers rwn
)
'''

# ## Privacy Mapping Concept Coverage Check
# This check surfaces concept_ids present in mapped ehr_ops tables but absent from unioned_ehr_dataset.

if not unioned_ehr_dataset_ref:
    print(
        'unioned_ehr_dataset is blank. Skipping privacy mapping concept coverage check.'
    )
else:
    unioned_ehr_dataset_all_concepts_meta_sql = '''
WITH cols AS (
  SELECT table_name, column_name
  FROM `{{unioned_ehr_dataset_ref}}.INFORMATION_SCHEMA.COLUMNS`
  WHERE column_name LIKE '%concept_id%'
    AND table_name NOT LIKE '%concept%'
    AND table_name NOT IN ('domain', 'drug_strength', 'relationship', 'source_to_concept_map', 'vocabulary')
),
query AS (
  SELECT STRING_AGG(
    FORMAT(
      'SELECT DISTINCT %s concept_id\\nFROM `{{unioned_ehr_dataset_ref}}.%s` t',
      column_name, table_name),
    '\\nUNION DISTINCT\\n'
    ORDER BY table_name, column_name
  ) AS q
  FROM cols
)
SELECT q
FROM query
'''
    unioned_ehr_dataset_all_concepts_query_df = run_tantalus_check(
        'GenerateUnionedEhrDatasetAllConceptsQuery',
        unioned_ehr_dataset_all_concepts_meta_sql,
        max_rows=False)
    cte_unioned_ehr_dataset_all_concepts = ''
    if ('q' in unioned_ehr_dataset_all_concepts_query_df.columns and
            not unioned_ehr_dataset_all_concepts_query_df.empty):
        q_values = unioned_ehr_dataset_all_concepts_query_df['q'].dropna()
        if not q_values.empty:
            cte_unioned_ehr_dataset_all_concepts = q_values.iloc[0].strip()

    if not cte_unioned_ehr_dataset_all_concepts:
        print('No concept_id query was generated for unioned_ehr_dataset.')
    else:
        privacy_gap_meta_sql = '''
WITH cols AS (
  SELECT column_name, table_name
  FROM `{{unioned_ehr_dataset_ref}}.INFORMATION_SCHEMA.COLUMNS`
  WHERE TRUE
    AND column_name LIKE '%concept_id%'
    AND table_name NOT IN ('fact_relationship', 'metadata', 'drug_strength')
    AND table_name NOT LIKE '%concept%'
    AND table_name NOT LIKE '%cohort%'
    AND table_name NOT LIKE '%domain%'
    AND table_name NOT LIKE '%death%'
    AND table_name NOT LIKE '%attribute%'
    AND table_name NOT LIKE '%achilles%'
    AND table_name NOT LIKE '%definition%'
    AND table_name NOT LIKE '%relationship%'
    AND table_name NOT LIKE '%vocabulary%'
),
non_ext_cols AS (
  SELECT column_name, table_name
  FROM `{{unioned_ehr_dataset_ref}}.INFORMATION_SCHEMA.COLUMNS`
  WHERE TRUE
    AND column_name LIKE '%concept_id%'
    AND table_name LIKE '%aou%'
),
query AS (
  SELECT STRING_AGG(
    FORMAT(
      'SELECT DISTINCT %s concept_id\\n' ||
      ',"%s" column_name\\n' ||
      ',"%s" table_name\\n' ||
      'FROM `{{prod_project}}.ehr_ops.unioned_ehr_%s` t\\n' ||
      'JOIN `{{prod_project}}.ehr_ops._mapping_%s` e USING (%s_id)\\n' ||
      'WHERE TRUE',
      column_name,
      column_name,
      table_name,
      table_name,
      table_name,
      table_name),
    '\\nUNION DISTINCT\\n'
    ORDER BY table_name, column_name
  ) AS q
  FROM cols
),
non_ext_query AS (
  SELECT STRING_AGG(
    FORMAT(
      'SELECT DISTINCT %s concept_id\\n' ||
      ',"%s" column_name\\n' ||
      ',"%s" table_name\\n' ||
      'FROM `{{prod_project}}.ehr_ops.unioned_ehr_%s` t\\n' ||
      'WHERE TRUE',
      column_name,
      column_name,
      table_name,
      table_name),
    '\\nUNION DISTINCT\\n'
    ORDER BY table_name, column_name
  ) AS nq
  FROM non_ext_cols
),
combined AS (
  SELECT CONCAT(
    COALESCE(q, ''),
    CASE WHEN q IS NOT NULL AND nq IS NOT NULL THEN '\\nUNION DISTINCT\\n' ELSE '' END,
    COALESCE(nq, '')
  ) AS q
  FROM query, non_ext_query
)
SELECT CASE
  WHEN q IS NULL OR q = '' THEN ''
  ELSE CONCAT(
    ',candidate_concepts AS (\\n',
    q,
    '\\n)\\n',
    'SELECT DISTINCT concept_id, column_name, table_name\\n',
    'FROM candidate_concepts\\n',
    'WHERE concept_id NOT IN (SELECT concept_id FROM cte_unioned_ehr_dataset_all_concepts)'
  )
END AS q
FROM combined
'''
        privacy_gap_query_df = run_tantalus_check(
            'GeneratePrivacyConceptGapQuery',
            privacy_gap_meta_sql,
            max_rows=False)
        privacy_gap_query = ''
        if 'q' in privacy_gap_query_df.columns and not privacy_gap_query_df.empty:
            q_values = privacy_gap_query_df['q'].dropna()
            if not q_values.empty:
                privacy_gap_query = q_values.iloc[0].strip()

        if not privacy_gap_query:
            print('No privacy concept gap query was generated.')
        else:
            privacy_gap_sql = f'''
WITH cte_unioned_ehr_dataset_all_concepts AS (
{cte_unioned_ehr_dataset_all_concepts}
)
{privacy_gap_query}
'''
            print('\n### PrivacyConceptGapResults\n')
            privacy_gap_results = execute(client,
                                          privacy_gap_sql,
                                          max_rows=True)
            if privacy_gap_results.empty:
                print(
                    '\n PASS, no concept_id gaps were found between mapped ehr_ops tables and unioned_ehr_dataset.'
                )
            else:
                print(
                    '\n FAILING! concept_id values exist in mapped ehr_ops tables but are absent from unioned_ehr_dataset.'
                )
            display(privacy_gap_results)

# ## OPTIONAL PREVALENCE ENRICHMENT (FetchPrevalences parity)
# Set find_prevalences=True and unioned_ehr_dataset='<project.dataset>' in the parameter cell to run this.

if find_prevalences and not unioned_ehr_dataset_ref:
    print(
        'find_prevalences is True but unioned_ehr_dataset is blank. Skipping prevalence check.'
    )
elif find_prevalences:
    tantalus_row_level_with_prevalence_sql = tantalus_row_level_base_sql + '''
,concept_ids AS (
  SELECT DISTINCT concept_id
  FROM enriched_results
  WHERE concept_id != 0
),
prevalences AS (
  SELECT person_count / (1.0 * population_size) AS prevalence, concept_id
  FROM (
    SELECT condition_concept_id AS concept_id, COUNT(DISTINCT person_id) AS person_count
    FROM `{{prod_project}}.{{unioned_ehr_dataset_name}}.condition_occurrence`
    WHERE condition_concept_id IN (SELECT concept_id FROM concept_ids)
    GROUP BY condition_concept_id

    UNION ALL

    SELECT procedure_concept_id AS concept_id, COUNT(DISTINCT person_id) AS person_count
    FROM `{{prod_project}}.{{unioned_ehr_dataset_name}}.procedure_occurrence`
    WHERE procedure_concept_id IN (SELECT concept_id FROM concept_ids)
    GROUP BY procedure_concept_id

    UNION ALL

    SELECT drug_concept_id AS concept_id, COUNT(DISTINCT person_id) AS person_count
    FROM `{{prod_project}}.{{unioned_ehr_dataset_name}}.drug_exposure`
    WHERE drug_concept_id IN (SELECT concept_id FROM concept_ids)
    GROUP BY drug_concept_id

    UNION ALL

    SELECT device_concept_id AS concept_id, COUNT(DISTINCT person_id) AS person_count
    FROM `{{prod_project}}.{{unioned_ehr_dataset_name}}.device_exposure`
    WHERE device_concept_id IN (SELECT concept_id FROM concept_ids)
    GROUP BY device_concept_id

    UNION ALL

    SELECT cause_concept_id AS concept_id, COUNT(DISTINCT person_id) AS person_count
    FROM `{{prod_project}}.{{unioned_ehr_dataset_name}}.death`
    WHERE cause_concept_id IN (SELECT concept_id FROM concept_ids)
    GROUP BY cause_concept_id

    UNION ALL

    SELECT measurement_concept_id AS concept_id, COUNT(DISTINCT person_id) AS person_count
    FROM `{{prod_project}}.{{unioned_ehr_dataset_name}}.measurement`
    WHERE measurement_concept_id IN (SELECT concept_id FROM concept_ids)
    GROUP BY measurement_concept_id

    UNION ALL

    SELECT observation_concept_id AS concept_id, COUNT(DISTINCT person_id) AS person_count
    FROM `{{prod_project}}.{{unioned_ehr_dataset_name}}.observation`
    WHERE observation_concept_id IN (SELECT concept_id FROM concept_ids)
    GROUP BY observation_concept_id

    UNION ALL

    SELECT specimen_concept_id AS concept_id, COUNT(DISTINCT person_id) AS person_count
    FROM `{{prod_project}}.{{unioned_ehr_dataset_name}}.specimen`
    WHERE specimen_concept_id IN (SELECT concept_id FROM concept_ids)
    GROUP BY specimen_concept_id

    UNION ALL

    SELECT visit_concept_id AS concept_id, COUNT(DISTINCT person_id) AS person_count
    FROM `{{prod_project}}.{{unioned_ehr_dataset_name}}.visit_occurrence`
    WHERE visit_concept_id IN (SELECT concept_id FROM concept_ids)
    GROUP BY visit_concept_id
  ) unions
  CROSS JOIN (
    SELECT COUNT(*) AS population_size
    FROM `{{prod_project}}.{{unioned_ehr_dataset_name}}.person`
  ) overall
)
SELECT
  results.*,
  prevalences.prevalence
FROM prevalences
RIGHT JOIN enriched_results results
  ON prevalences.concept_id = results.concept_id
ORDER BY results.test_id, results.concept_id
'''
    row_level_results_with_prevalence = run_tantalus_check(
        'RowLevelTestsWithPrevalence',
        tantalus_row_level_with_prevalence_sql,
        max_rows=False)
    display_row_level_check_results(row_level_results_with_prevalence,
                                    preview_rows=row_level_preview_rows)
else:
    print('Set find_prevalences=True to run FetchPrevalences parity checks.')
