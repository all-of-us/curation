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
from utils import auth
from gcloud.bq import BigQueryClient
from analytics.cdr_ops.notebook_utils import execute, IMPERSONATION_SCOPES
from resources import AOU_VOCAB_CONCEPT_CSV_PATH

import pandas as pd

# get current custom concepts
custom_concepts = pd.read_csv(AOU_VOCAB_CONCEPT_CSV_PATH, delimiter='\t')

# + tags=["parameters"]
project_id = ''
old_vocabulary = ''
new_vocabulary = ''
run_as = ''
row_level_preview_rows = 200
# -

# These are the AoU_Custom and AoU_General concepts.
custom_concept_ids = custom_concepts.iloc[:, 0].tolist()

vocabulary_dataset_old = f'{project_id}.{old_vocabulary}'
vocabulary_dataset_new = f'{project_id}.{new_vocabulary}'

impersonation_creds = auth.get_impersonation_credentials(
    run_as, target_scopes=IMPERSONATION_SCOPES)

client = BigQueryClient(project_id, credentials=impersonation_creds)

pd.set_option('max_colwidth', None)

# # Validate PPI

# ## Review the new PPI concepts
# The dataframe will contain all new PPI concepts. <br>
# Generally there will be additions to the PPI vocabulary between uploads, but it is also possible that the update contains changes only to the EHR vocabularies.<br>
#
# > - **If the dataframe is not empty** - Manually review the new concepts cross referencing the vocabulary upload Jira ticket which should include the expected PPI updates to look for. If the ticket does not have this information contact the ticket reporter. <br>
# > - **If the dataframe is empty** - There are no new PPI concepts. Verify this by referencing the 'vocabulary upload' Jira ticket which should include the expected PPI updates. If the ticket does not have this information contact the ticket reporter. <br>

tpl = JINJA_ENV.from_string('''
SELECT
c.concept_code 
FROM `{{vocabulary_dataset_new}}.concept` c
WHERE c.concept_code NOT IN (SELECT concept_code 
                            FROM `{{vocabulary_dataset_old}}.concept` sq 
                            WHERE sq.vocabulary_id LIKE "PPI" )
AND c.vocabulary_id LIKE "PPI"
ORDER BY c.concept_code
''')
query = tpl.render(vocabulary_dataset_old=vocabulary_dataset_old,
                   vocabulary_dataset_new=vocabulary_dataset_new)
execute(client, query, max_rows=True)

# # Review the retired or removed concepts
# The dataframe will contain all PPI concepts that were newly deprecated or removed from Athena. <br>
# Generally none would be deprecated or removed. <br>
#
# **If the dataframe is not empty** - Manually review the retired or removed concepts, cross referencing the vocabulary upload Jira ticket which should include the expected PPI updates to look for. If the ticket does not have this information contact the ticket reporter. <br>
# **If the dataframe is empty** - There are no deprecated or removed PPI concepts. Verify this by referencing the 'vocabulary upload' Jira ticket which should include the expected PPI updates. If the ticket does not have this information contact the ticket reporter. <br>

tpl = JINJA_ENV.from_string('''
SELECT
c.concept_code,
'removed' AS status
FROM `{{vocabulary_dataset_old}}.concept` c
WHERE c.concept_code NOT IN (SELECT concept_code 
                            FROM `{{vocabulary_dataset_new}}.concept`  
                            WHERE vocabulary_id LIKE "PPI")
AND c.vocabulary_id LIKE "PPI"

UNION ALL

SELECT
c.concept_code,
'recently_deprecated' as status
FROM `{{vocabulary_dataset_new}}.concept` c
WHERE c.concept_code NOT IN (SELECT concept_code 
                            FROM `{{vocabulary_dataset_old}}.concept` 
                            WHERE vocabulary_id LIKE "PPI"
                            AND c.valid_end_date < CURRENT_DATE())
AND c.vocabulary_id LIKE "PPI"
AND c.valid_end_date < CURRENT_DATE()
''')
query = tpl.render(vocabulary_dataset_old=vocabulary_dataset_old,
                   vocabulary_dataset_new=vocabulary_dataset_new)
execute(client, query, max_rows=True)

# # Verify the updates made to the vocabulary in the upload process

# ## All AoU_Custom and AoU_General concepts are present in the new vocabulary
# In the process of updating the vocabulary custom concepts are added to the vocabulary. <br>
# The list of concepts should have been updated at the start of this notebook.<br>
#
# **If the check fails**, investigate. It is important that all of the custom concepts are added to the vocabulary. To troubleshoot: Check that all concepts in the aou_vocab/CONCEPT.csv are accounted for in the custom_concepts list. Check the printed dataframe against the custom_concepts list

# +
tpl = JINJA_ENV.from_string('''
SELECT
concept_id
FROM `{{vocabulary_dataset_new}}.concept` c
WHERE vocabulary_id in ('AoU_Custom', 'AoU_General')
OR concept_code IN ('AOU generated') 
ORDER BY concept_id
''')
query = tpl.render(vocabulary_dataset_old=vocabulary_dataset_old,
                   vocabulary_dataset_new=vocabulary_dataset_new)
df = execute(client, query, max_rows=True)

missing_items = [
    item for item in custom_concept_ids if item not in df['concept_id'].values
]

if missing_items:
    print(' \n FAILING! Look in the check description for more.')
    display(missing_items)
else:
    print(' \n PASS, All custom concepts accounted for.')
    display(missing_items)

# +

missing_items = [
    item for item in custom_concept_ids if item not in df['concept_id'].values
]

missing_items
# -

# # Vocabulary Summary Queries

# ## Tables row count comparison
#
# > - The difference between the number of rows in each table of the datasets.  <br>
# > - **Generally, all 'changes' should increase**, but with the occasional edge case. <br>
# > - **Investigate any negative values**, the release notes are a great starting reference. <br>
# > - [Release notes](https://github.com/OHDSI/Vocabulary-v5.0/releases).   <br>

tpl = JINJA_ENV.from_string('''
WITH new_table_info AS (
   SELECT dataset_id, table_id, row_count
   FROM `{{vocabulary_dataset_new}}.__TABLES__`
),
old_table_info AS (
   SELECT dataset_id, table_id, row_count
   FROM `{{vocabulary_dataset_old}}.__TABLES__`
)
SELECT
n.table_id,
o.row_count AS old_count,
n.row_count AS new_count,
(n.row_count - o.row_count) AS changes
FROM new_table_info AS n
LEFT join old_table_info AS o
USING (table_id)
ORDER BY table_id
''')
query = tpl.render(vocabulary_dataset_old=vocabulary_dataset_old,
                   vocabulary_dataset_new=vocabulary_dataset_new)
execute(client, query, max_rows=True)

# # Vocabulary_id comparison
# > - The table will show the vocabulary_ids that exist in either the new or old datasets but not both. <br>
# > - Generally, the same vocabularies should exist in each dataset, so the table below should be empty. <br>
#
# **If the dataframe is empty** and a new vocabulary was not expected. This check passes.
#
# **If the dataframe is not empty** a vocabulary was either added or removed since the last upload. If this is not expected, investigate. [release notes](https://github.com/OHDSI/Vocabulary-v5.0/releases).

tpl = JINJA_ENV.from_string('''
WITH new_table_info AS (
   SELECT COUNT(vocabulary_id) AS new_concept_count, vocabulary_id
   FROM `{{vocabulary_dataset_new}}.concept`
   GROUP BY vocabulary_id
),
old_table_info AS (
   SELECT COUNT(vocabulary_id) AS old_concept_count, vocabulary_id
   FROM `{{vocabulary_dataset_old}}.concept`
   GROUP BY vocabulary_id
)
SELECT vocabulary_id,old_concept_count,new_concept_count, new_concept_count - old_concept_count AS diff
FROM new_table_info
FULL OUTER JOIN old_table_info
USING (vocabulary_id)
WHERE new_concept_count IS NULL
OR old_concept_count IS NULL
ORDER BY vocabulary_id
''')
query = tpl.render(vocabulary_dataset_old=vocabulary_dataset_old,
                   vocabulary_dataset_new=vocabulary_dataset_new)
execute(client, query, max_rows=True)

# # Row count comparison by vocabulary_id
# > - Shows the number of individual concepts added or removed from each vocabulary.<br>
# > - **Generally the count should only increase** (when it does change).<br>
# > - Investigate any negative differences.<br>
# > - Changes to AoU_Custom and AoU_General can be validated by looking for Jira issues that affect the<br>
#     data_steward/resource_files/aou_vocab/CONCEPT.csv file.<br>

tpl = JINJA_ENV.from_string('''
WITH new_table_info AS (
   SELECT COUNT(vocabulary_id) AS new_concept_count, vocabulary_id
   FROM `{{vocabulary_dataset_new}}.concept`
   GROUP BY vocabulary_id
),
old_table_info AS (
   SELECT COUNT(vocabulary_id) AS old_concept_count, vocabulary_id
   FROM `{{vocabulary_dataset_old}}.concept`
   GROUP BY vocabulary_id
)
SELECT vocabulary_id,old_concept_count,new_concept_count, new_concept_count - old_concept_count AS diff
FROM new_table_info
FULL OUTER JOIN old_table_info
USING (vocabulary_id)
ORDER BY vocabulary_id
''')
query = tpl.render(vocabulary_dataset_old=vocabulary_dataset_old,
                   vocabulary_dataset_new=vocabulary_dataset_new)
execute(client, query, max_rows=True)

# # Access to the new vocabulary
# > - This query shows the users and the roles who have access to the new vocabulary.
# > - The project_id should be prod here.
# > - Confirm that the PDR service account has BigQuery Data Viewer access to it.
# > - If no access, re-run the step in the [vocabulary playbook](https://docs.google.com/document/d/1U8AIunEVRdJOUGgYJASKcTGkzPDg5MVU_hiIKDwmipk/edit#). <br>

tpl = JINJA_ENV.from_string('''
select * from `{{project_id}}.region-us.INFORMATION_SCHEMA.OBJECT_PRIVILEGES`
where object_name = '{{new_vocabulary}}'
order by grantee
''')
query = tpl.render(project_id=project_id, new_vocabulary=new_vocabulary)
execute(client, query)

# # Tantalus Full Parity Checks (BigQuery)
# This section ports all Tantalus vocabulary checks (`Count*`, `Test*`, and map support)
# into BigQuery so notebook output covers the same validation surface.


def run_tantalus_check(check_name, sql, max_rows=True):
    """Run a Jinja-templated SQL check and return dataframe results."""
    print(f'\n### {check_name}\n')
    tpl = JINJA_ENV.from_string(sql)
    query = tpl.render(vocabulary_dataset_old=vocabulary_dataset_old,
                       vocabulary_dataset_new=vocabulary_dataset_new)
    return execute(client, query, max_rows=max_rows)


def display_row_level_check_results(df, preview_rows=200):
    """Display summary + bounded preview while retaining the full result dataframe."""
    preview_rows = max(1, int(preview_rows))
    total_rows = len(df)
    print(f'Total row-level results: {total_rows}')

    if total_rows == 0:
        print('No row-level differences found.')
        return

    if 'test_id' in df.columns:
        print('\nCounts by test_id')
        display(
            df.groupby('test_id', dropna=False).size().reset_index(
                name='row_count').sort_values('test_id'))

    shown_rows = min(preview_rows, total_rows)
    print(f'\nPreview (first {shown_rows} rows)')
    display(df.head(preview_rows))

    if total_rows > preview_rows:
        print(
            f'\nShowing first {preview_rows} of {total_rows} rows. Full results are retained in memory in this dataframe.'
        )


# ## BASIC SUMMARY

tantalus_summary_checks = [('GetVocabVersion', '''
SELECT
  (SELECT REPLACE(vocabulary_version, ' ', '-')
   FROM `{{vocabulary_dataset_new}}.vocabulary`
   WHERE LOWER(vocabulary_id) = 'none') AS current_vocab,
  (SELECT REPLACE(vocabulary_version, ' ', '-')
   FROM `{{vocabulary_dataset_old}}.vocabulary`
   WHERE LOWER(vocabulary_id) = 'none') AS prior_vocab
'''),
                           ('CountSummaryDiff', '''
WITH counts AS (
  SELECT
    'CONCEPT' AS table_name,
    (SELECT COUNT(*) FROM `{{vocabulary_dataset_old}}.concept`) AS old_count,
    (SELECT COUNT(*) FROM `{{vocabulary_dataset_new}}.concept`) AS new_count
  UNION ALL
  SELECT
    'CONCEPT_ANCESTOR',
    (SELECT COUNT(*) FROM `{{vocabulary_dataset_old}}.concept_ancestor`),
    (SELECT COUNT(*) FROM `{{vocabulary_dataset_new}}.concept_ancestor`)
  UNION ALL
  SELECT
    'CONCEPT_CLASS',
    (SELECT COUNT(*) FROM `{{vocabulary_dataset_old}}.concept_class`),
    (SELECT COUNT(*) FROM `{{vocabulary_dataset_new}}.concept_class`)
  UNION ALL
  SELECT
    'CONCEPT_RELATIONSHIP',
    (SELECT COUNT(*) FROM `{{vocabulary_dataset_old}}.concept_relationship`),
    (SELECT COUNT(*) FROM `{{vocabulary_dataset_new}}.concept_relationship`)
  UNION ALL
  SELECT
    'CONCEPT_SYNONYM',
    (SELECT COUNT(*) FROM `{{vocabulary_dataset_old}}.concept_synonym`),
    (SELECT COUNT(*) FROM `{{vocabulary_dataset_new}}.concept_synonym`)
  UNION ALL
  SELECT
    'DOMAIN',
    (SELECT COUNT(*) FROM `{{vocabulary_dataset_old}}.domain`),
    (SELECT COUNT(*) FROM `{{vocabulary_dataset_new}}.domain`)
  UNION ALL
  SELECT
    'RELATIONSHIP',
    (SELECT COUNT(*) FROM `{{vocabulary_dataset_old}}.relationship`),
    (SELECT COUNT(*) FROM `{{vocabulary_dataset_new}}.relationship`)
  UNION ALL
  SELECT
    'VOCABULARY',
    (SELECT COUNT(*) FROM `{{vocabulary_dataset_old}}.vocabulary`),
    (SELECT COUNT(*) FROM `{{vocabulary_dataset_new}}.vocabulary`)
)
SELECT table_name, old_count, new_count, new_count - old_count AS diff
FROM counts
''')]

for check_name, sql in tantalus_summary_checks:
    display(run_tantalus_check(check_name, sql))

# ## CONCEPT

tantalus_concept_checks = [('CountConceptDomainChanges', '''
SELECT COUNT(*) AS cnt
FROM `{{vocabulary_dataset_old}}.concept` c1
JOIN `{{vocabulary_dataset_new}}.concept` c2
  ON c1.concept_id = c2.concept_id
WHERE TRIM(LOWER(c1.domain_id)) != TRIM(LOWER(c2.domain_id))
'''),
                           ('CountConceptNameChanges', '''
SELECT COUNT(*) AS cnt
FROM `{{vocabulary_dataset_old}}.concept` c1
JOIN `{{vocabulary_dataset_new}}.concept` c2
  ON c1.concept_id = c2.concept_id
WHERE TRIM(LOWER(c1.concept_name)) != TRIM(LOWER(c2.concept_name))
'''),
                           ('CountConceptClassChanges', '''
SELECT COUNT(*) AS cnt
FROM `{{vocabulary_dataset_old}}.concept` c1
JOIN `{{vocabulary_dataset_new}}.concept` c2
  ON c1.concept_id = c2.concept_id
WHERE TRIM(LOWER(c1.concept_class_id)) != TRIM(LOWER(c2.concept_class_id))
'''),
                           ('CountConceptInvalidChanges', '''
SELECT COUNT(*) AS cnt
FROM `{{vocabulary_dataset_old}}.concept` c1
JOIN `{{vocabulary_dataset_new}}.concept` c2
  ON c1.concept_id = c2.concept_id
WHERE c1.invalid_reason IS NULL
  AND c2.invalid_reason IS NOT NULL
'''),
                           ('CountConceptDiffsByDandV', '''
WITH concept1 AS (
  SELECT vocabulary_id, domain_id, COUNT(*) AS cnt
  FROM `{{vocabulary_dataset_old}}.concept`
  GROUP BY vocabulary_id, domain_id
),
concept2 AS (
  SELECT vocabulary_id, domain_id, COUNT(*) AS cnt
  FROM `{{vocabulary_dataset_new}}.concept`
  GROUP BY vocabulary_id, domain_id
)
SELECT
  c2.vocabulary_id,
  c2.domain_id,
  c1.cnt AS old_count,
  c2.cnt AS new_count,
  c2.cnt - c1.cnt AS diff
FROM concept1 c1
JOIN concept2 c2
  ON c1.vocabulary_id = c2.vocabulary_id
 AND c1.domain_id = c2.domain_id
WHERE c1.cnt - c2.cnt != 0
ORDER BY diff DESC
'''),
                           ('CountConceptDiffsByClass', '''
WITH concept1 AS (
  SELECT concept_class_id, COUNT(*) AS cnt
  FROM `{{vocabulary_dataset_old}}.concept`
  GROUP BY concept_class_id
),
concept2 AS (
  SELECT concept_class_id, COUNT(*) AS cnt
  FROM `{{vocabulary_dataset_new}}.concept`
  GROUP BY concept_class_id
)
SELECT
  c2.concept_class_id,
  c1.cnt AS old_count,
  c2.cnt AS new_count,
  c2.cnt - c1.cnt AS diff
FROM concept1 c1
JOIN concept2 c2
  ON c1.concept_class_id = c2.concept_class_id
WHERE c1.cnt - c2.cnt != 0
ORDER BY diff DESC
'''),
                           ('CountConceptDiffsByDomain', '''
WITH concept1 AS (
  SELECT domain_id, COUNT(*) AS cnt
  FROM `{{vocabulary_dataset_old}}.concept`
  GROUP BY domain_id
),
concept2 AS (
  SELECT domain_id, COUNT(*) AS cnt
  FROM `{{vocabulary_dataset_new}}.concept`
  GROUP BY domain_id
)
SELECT
  c2.domain_id,
  c1.cnt AS old_count,
  c2.cnt AS new_count,
  c2.cnt - c1.cnt AS diff
FROM concept1 c1
JOIN concept2 c2
  ON c1.domain_id = c2.domain_id
WHERE c1.cnt - c2.cnt != 0
ORDER BY diff DESC
'''),
                           ('CountConceptDiffsByInvRsn', '''
WITH concept1 AS (
  SELECT invalid_reason, COUNT(*) AS cnt
  FROM `{{vocabulary_dataset_old}}.concept`
  GROUP BY invalid_reason
),
concept2 AS (
  SELECT invalid_reason, COUNT(*) AS cnt
  FROM `{{vocabulary_dataset_new}}.concept`
  GROUP BY invalid_reason
)
SELECT
  CASE WHEN c2.invalid_reason = 'D' THEN 'Deleted' ELSE 'Updated' END AS reason,
  c1.cnt AS old_count,
  c2.cnt AS new_count,
  c2.cnt - c1.cnt AS diff
FROM concept1 c1
JOIN concept2 c2
  ON c1.invalid_reason = c2.invalid_reason
WHERE c1.cnt - c2.cnt != 0
ORDER BY diff DESC
'''),
                           ('CountConceptDiffsByStdConcept', '''
WITH concept1 AS (
  SELECT standard_concept, COUNT(*) AS cnt
  FROM `{{vocabulary_dataset_old}}.concept`
  GROUP BY standard_concept
),
concept2 AS (
  SELECT standard_concept, COUNT(*) AS cnt
  FROM `{{vocabulary_dataset_new}}.concept`
  GROUP BY standard_concept
)
SELECT
  CASE
    WHEN c2.standard_concept = 'S' THEN 'Standard'
    WHEN c2.standard_concept = 'C' THEN 'Classification'
    WHEN c2.standard_concept IS NULL THEN 'Non-Standard'
  END AS standard_concept,
  c1.cnt AS old_count,
  c2.cnt AS new_count,
  c2.cnt - c1.cnt AS diff
FROM concept1 c1
JOIN concept2 c2
  ON c1.standard_concept = c2.standard_concept
WHERE c1.cnt - c2.cnt != 0
ORDER BY diff DESC
'''),
                           ('CountConceptDiffsByVocab', '''
WITH concept1 AS (
  SELECT vocabulary_id, COUNT(*) AS cnt
  FROM `{{vocabulary_dataset_old}}.concept`
  GROUP BY vocabulary_id
),
concept2 AS (
  SELECT vocabulary_id, COUNT(*) AS cnt
  FROM `{{vocabulary_dataset_new}}.concept`
  GROUP BY vocabulary_id
)
SELECT
  c2.vocabulary_id,
  c1.cnt AS old_count,
  c2.cnt AS new_count,
  c2.cnt - c1.cnt AS diff
FROM concept1 c1
JOIN concept2 c2
  ON c1.vocabulary_id = c2.vocabulary_id
WHERE c1.cnt - c2.cnt != 0
ORDER BY diff DESC
''')]

for check_name, sql in tantalus_concept_checks:
    display(run_tantalus_check(check_name, sql))

# ## CONCEPT_SYNONYM

tantalus_synonym_checks = [('CountSynNameDiffs', '''
WITH current_cs AS (
  SELECT cs.concept_id, COUNT(cs.concept_synonym_name) AS cnt
  FROM `{{vocabulary_dataset_new}}.concept` c
  JOIN `{{vocabulary_dataset_new}}.concept_synonym` cs
    ON c.concept_id = cs.concept_id
  GROUP BY cs.concept_id
),
prior_cs AS (
  SELECT cs.concept_id, COUNT(cs.concept_synonym_name) AS cnt
  FROM `{{vocabulary_dataset_old}}.concept` c
  JOIN `{{vocabulary_dataset_old}}.concept_synonym` cs
    ON c.concept_id = cs.concept_id
  GROUP BY cs.concept_id
)
SELECT COUNT(*) AS cnt
FROM prior_cs pcs
JOIN current_cs ccs
  ON pcs.concept_id = ccs.concept_id
WHERE pcs.cnt > ccs.cnt
''')]

for check_name, sql in tantalus_synonym_checks:
    display(run_tantalus_check(check_name, sql))

# ## CONCEPT_ANCESTOR

tantalus_concept_ancestor_checks = [('CountOrphanedDrugs', '''
WITH current_orphans AS (
  SELECT COUNT(*) AS cnt
  FROM (
    SELECT ca.descendant_concept_id
    FROM `{{vocabulary_dataset_new}}.concept` c
    JOIN `{{vocabulary_dataset_new}}.concept_ancestor` ca
      ON ca.descendant_concept_id = c.concept_id
    JOIN `{{vocabulary_dataset_new}}.concept` c2
      ON c2.concept_id = ca.ancestor_concept_id
    WHERE LOWER(c.domain_id) = 'drug'
    GROUP BY ca.descendant_concept_id
    HAVING MAX(CASE WHEN LOWER(c2.concept_class_id) = 'ingredient' THEN 1 ELSE 0 END) = 0
  ) tmp
),
prior_orphans AS (
  SELECT COUNT(*) AS cnt
  FROM (
    SELECT ca.descendant_concept_id
    FROM `{{vocabulary_dataset_old}}.concept` c
    JOIN `{{vocabulary_dataset_old}}.concept_ancestor` ca
      ON ca.descendant_concept_id = c.concept_id
    JOIN `{{vocabulary_dataset_old}}.concept` c2
      ON c2.concept_id = ca.ancestor_concept_id
    WHERE LOWER(c.domain_id) = 'drug'
    GROUP BY ca.descendant_concept_id
    HAVING MAX(CASE WHEN LOWER(c2.concept_class_id) = 'ingredient' THEN 1 ELSE 0 END) = 0
  ) tmp
)
SELECT po.cnt AS prior, co.cnt AS current_count, co.cnt - po.cnt AS diff
FROM current_orphans co
CROSS JOIN prior_orphans po
'''),
                                    ('CountLostCA', '''
SELECT COUNT(*) AS cnt
FROM (
  SELECT ca.ancestor_concept_id, ca.descendant_concept_id
  FROM `{{vocabulary_dataset_old}}.concept_ancestor` ca
  JOIN `{{vocabulary_dataset_old}}.concept` c1
    ON c1.concept_id = ca.descendant_concept_id
  JOIN `{{vocabulary_dataset_old}}.concept` c2
    ON c2.concept_id = ca.ancestor_concept_id

  EXCEPT DISTINCT

  SELECT ca.ancestor_concept_id, ca.descendant_concept_id
  FROM `{{vocabulary_dataset_new}}.concept_ancestor` ca
  JOIN `{{vocabulary_dataset_new}}.concept` c1
    ON c1.concept_id = ca.descendant_concept_id
  JOIN `{{vocabulary_dataset_new}}.concept` c2
    ON c2.concept_id = ca.ancestor_concept_id
) tmp
''')]

for check_name, sql in tantalus_concept_ancestor_checks:
    display(run_tantalus_check(check_name, sql))

# ## CONCEPT_RELATIONSHIP

tantalus_concept_relationship_checks = [
    ('CountMissingCR', '''
SELECT COUNT(*) AS cnt
FROM (
  SELECT cr.concept_id_1, cr.concept_id_2
  FROM `{{vocabulary_dataset_old}}.concept_relationship` cr
  JOIN `{{vocabulary_dataset_old}}.concept` c1
    ON cr.concept_id_1 = c1.concept_id
  JOIN `{{vocabulary_dataset_old}}.concept` c2
    ON cr.concept_id_2 = c2.concept_id

  EXCEPT DISTINCT

  SELECT cr.concept_id_1, cr.concept_id_2
  FROM `{{vocabulary_dataset_new}}.concept_relationship` cr
  JOIN `{{vocabulary_dataset_new}}.concept` c1
    ON cr.concept_id_1 = c1.concept_id
  JOIN `{{vocabulary_dataset_new}}.concept` c2
    ON cr.concept_id_2 = c2.concept_id
) tmp
'''),
    ('CountValidInvalidCR', '''
WITH current_count AS (
  SELECT COUNT(*) AS cnt
  FROM `{{vocabulary_dataset_new}}.concept_relationship` cr
  JOIN `{{vocabulary_dataset_new}}.concept` c1
    ON cr.concept_id_1 = c1.concept_id
  JOIN `{{vocabulary_dataset_new}}.concept` c2
    ON cr.concept_id_2 = c2.concept_id
  WHERE cr.invalid_reason IS NULL
    AND c1.standard_concept = 'S'
    AND c2.standard_concept = 'S'
    AND (c1.invalid_reason IS NOT NULL OR c2.invalid_reason IS NOT NULL)
),
prior_count AS (
  SELECT COUNT(*) AS cnt
  FROM `{{vocabulary_dataset_old}}.concept_relationship` cr
  JOIN `{{vocabulary_dataset_old}}.concept` c1
    ON cr.concept_id_1 = c1.concept_id
  JOIN `{{vocabulary_dataset_old}}.concept` c2
    ON cr.concept_id_2 = c2.concept_id
  WHERE cr.invalid_reason IS NULL
    AND c1.standard_concept = 'S'
    AND c2.standard_concept = 'S'
    AND (c1.invalid_reason IS NOT NULL OR c2.invalid_reason IS NOT NULL)
)
SELECT pc.cnt AS prior, cc.cnt AS current_count, cc.cnt - pc.cnt AS diff
FROM prior_count pc
CROSS JOIN current_count cc
'''),
    ('CountMissingDescendants', '''
WITH missing_descendants AS (
  SELECT descendant_concept_id
  FROM `{{vocabulary_dataset_old}}.concept_ancestor`

  EXCEPT DISTINCT

  SELECT descendant_concept_id
  FROM `{{vocabulary_dataset_new}}.concept_ancestor`
)
SELECT COUNT(*) AS cnt
FROM missing_descendants md
JOIN `{{vocabulary_dataset_old}}.concept` c
  ON c.concept_id = md.descendant_concept_id
'''),
    ('CountMissingDescendantsByDomVocabClass', '''
WITH missing_descendants AS (
  SELECT descendant_concept_id
  FROM `{{vocabulary_dataset_old}}.concept_ancestor`

  EXCEPT DISTINCT

  SELECT descendant_concept_id
  FROM `{{vocabulary_dataset_new}}.concept_ancestor`
),
stacked AS (
  SELECT 'domain' AS flag, c.domain_id AS col_name, COUNT(*) AS cnt
  FROM `{{vocabulary_dataset_old}}.concept` c
  WHERE c.concept_id IN (SELECT descendant_concept_id FROM missing_descendants)
  GROUP BY c.domain_id

  UNION ALL

  SELECT 'vocab' AS flag, c.vocabulary_id AS col_name, COUNT(*) AS cnt
  FROM `{{vocabulary_dataset_old}}.concept` c
  WHERE c.concept_id IN (SELECT descendant_concept_id FROM missing_descendants)
  GROUP BY c.vocabulary_id

  UNION ALL

  SELECT 'class' AS flag, c.concept_class_id AS col_name, COUNT(*) AS cnt
  FROM `{{vocabulary_dataset_old}}.concept` c
  WHERE c.concept_id IN (SELECT descendant_concept_id FROM missing_descendants)
  GROUP BY c.concept_class_id
),
sparse AS (
  SELECT
    CASE WHEN flag = 'vocab' THEN col_name END AS vocabulary_id,
    CASE WHEN flag = 'vocab' THEN cnt END AS vocab_cnt,
    CASE WHEN flag = 'domain' THEN col_name END AS domain_id,
    CASE WHEN flag = 'domain' THEN cnt END AS domain_cnt,
    CASE WHEN flag = 'class' THEN col_name END AS concept_class_id,
    CASE WHEN flag = 'class' THEN cnt END AS concept_class_cnt,
    ROW_NUMBER() OVER (PARTITION BY flag ORDER BY cnt) AS rn
  FROM stacked
)
SELECT
  MAX(vocabulary_id) AS vocabulary_id,
  MAX(vocab_cnt) AS vocab_count,
  MAX(domain_id) AS domain_id,
  MAX(domain_cnt) AS domain_count,
  MAX(concept_class_id) AS concept_class_id,
  MAX(concept_class_cnt) AS concept_class_count
FROM sparse
GROUP BY rn
''')
]

for check_name, sql in tantalus_concept_relationship_checks:
    display(run_tantalus_check(check_name, sql))

# ## CONCEPT_CLASS

tantalus_concept_class_checks = [('CountCCNDiffs', '''
SELECT COUNT(*) AS cnt
FROM `{{vocabulary_dataset_old}}.concept_class` cc1
JOIN `{{vocabulary_dataset_new}}.concept_class` cc2
  ON cc1.concept_class_id = cc2.concept_class_id
WHERE TRIM(LOWER(cc1.concept_class_name)) != TRIM(LOWER(cc2.concept_class_name))
''')]

for check_name, sql in tantalus_concept_class_checks:
    display(run_tantalus_check(check_name, sql))

# ## DOMAIN

tantalus_domain_checks = [('CountDomainNameDiffs', '''
SELECT COUNT(*) AS cnt
FROM `{{vocabulary_dataset_old}}.domain` d1
JOIN `{{vocabulary_dataset_new}}.domain` d2
  ON d1.domain_id = d2.domain_id
WHERE TRIM(LOWER(d1.domain_name)) != TRIM(LOWER(d2.domain_name))
''')]

for check_name, sql in tantalus_domain_checks:
    display(run_tantalus_check(check_name, sql))

# ## RELATIONSHIP

tantalus_relationship_checks = [('CountRelDiffs', '''
SELECT
  SUM(CASE WHEN r1.is_hierarchical != r2.is_hierarchical THEN 1 ELSE 0 END) AS change_hierarchy,
  SUM(CASE WHEN r1.defines_ancestry != r2.defines_ancestry THEN 1 ELSE 0 END) AS change_ancestry,
  SUM(CASE WHEN r1.reverse_relationship_id != r2.reverse_relationship_id THEN 1 ELSE 0 END) AS change_reverse_rel,
  SUM(CASE WHEN r1.relationship_concept_id != r2.relationship_concept_id THEN 1 ELSE 0 END) AS change_rel_concept
FROM `{{vocabulary_dataset_old}}.relationship` r1
JOIN `{{vocabulary_dataset_new}}.relationship` r2
  ON r1.relationship_id = r2.relationship_id
''')]

for check_name, sql in tantalus_relationship_checks:
    display(run_tantalus_check(check_name, sql))

# ## ROW-LEVEL TESTS (Test*.sql parity + string/numeric comparison checks)
# This reproduces the row-level result set produced by compareVocabData() with TestVocabVersion,
# TestConceptName, TestDomainChange, TestDeletedConcepts, and TestNewStandardMaps.

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

tantalus_row_level_sql = tantalus_row_level_base_sql + '''
SELECT *
FROM enriched_results
ORDER BY test_id, concept_id
'''

row_level_results = run_tantalus_check('RowLevelTestsCombined',
                                       tantalus_row_level_sql,
                                       max_rows=False)
display_row_level_check_results(row_level_results,
                                preview_rows=row_level_preview_rows)
