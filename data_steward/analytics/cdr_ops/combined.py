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

# ## Notebook parameters

# + tags=["parameters"]
PROJECT_ID = ''  # identifies the project containing the datasets
DATASET_ID = ''  # the dataset to evaluate
BASELINE_DATASET_ID = ''  # a baseline dataset for metrics comparison (ex: a prior combined dataset)

# +
import pandas as pd

from common import JINJA_ENV
from cdr_cleaner.cleaning_rules.negative_ages import date_fields
from gcloud.bq import BigQueryClient
from analytics.cdr_ops.notebook_utils import execute

client = BigQueryClient(PROJECT_ID)

pd.options.display.max_rows = 1000
pd.options.display.max_colwidth = 0
pd.options.display.max_columns = None
pd.options.display.width = None

# -

# ## Check for duplicates in observation
# In the past duplicate `observation_id`s were introduced in observation due
# to multiple sites submitting data for the same participant (see
# [DC-1512](https://precisionmedicineinitiative.atlassian.net/browse/DC-1512)).
# If any duplicates are found there may be a bug in the pipeline-
# particularly in `ehr_union.move_ehr_person_to_observation`.

query = f'''
WITH 
 dupe AS
 (SELECT 
    observation_id
   ,COUNT(1) row_count
  FROM `{DATASET_ID}.observation`
  GROUP BY 1)
SELECT * 
FROM `{DATASET_ID}.observation` o
WHERE EXISTS
(SELECT 1 
 FROM dupe d 
 WHERE 1=1
  AND row_count > 1
  AND d.observation_id = o.observation_id)
'''
execute(client, query)

# ## No records where participant age <0 or >150
# Any records where participant age would be <=0 or >=150 are
# not valid and may indicate a bug in the cleaning rule `negative_ages` (see [DC-393](https://precisionmedicineinitiative.atlassian.net/browse/DC-393)).
#
# Note that this differs from the minimum age at enrollment ([DC-1724](https://precisionmedicineinitiative.atlassian.net/browse/DC-1724)). Participants may
# contribute historical EHR data that precede the enrollment date.

tpl = JINJA_ENV.from_string('''
{% for table_name, date_field in date_fields.items() %}
SELECT 
 "{{table_name}}"     AS table_name
,"{{date_field}}"     AS date_field
,t.{{date_field}}     AS date_value
,p.birth_datetime     AS birth_datetime
FROM `{{dataset_id}}.{{table_name}}` t
 JOIN `{{dataset_id}}.person` p
  USING (person_id)
WHERE 
(
 -- age <= 0y --
 t.{{date_field}} < DATE(p.birth_datetime)

 -- age >= 150y --
 OR DATE_DIFF(t.{{date_field}}, DATE(p.birth_datetime), YEAR) >= 150 
)
{% if not loop.last -%}
   UNION ALL
{% endif %}
{% endfor %}
''')
query = tpl.render(dataset_id=DATASET_ID, date_fields=date_fields)
execute(client, query)

# ## Participants must have basics data
# Identify any participants who have don't have any responses
# to questions in the basics survey module (see [DC-706](https://precisionmedicineinitiative.atlassian.net/browse/DC-706)). These should be
# reported to the RDR as they are supposed to be filtered out
# from the RDR export.

# +
BASICS_MODULE_CONCEPT_ID = 1586134

# Note: This assumes that concept_ancestor sufficiently
# represents the hierarchy
query = f'''
WITH 

 -- all PPI question concepts in the basics survey module
 basics_concept AS
 (SELECT
   c.concept_id
  ,c.concept_name
  ,c.concept_code
  FROM `{DATASET_ID}.concept_ancestor` ca
  JOIN `{DATASET_ID}.concept` c
   ON ca.descendant_concept_id = c.concept_id
  WHERE 1=1
    AND ancestor_concept_id={BASICS_MODULE_CONCEPT_ID}
    AND c.vocabulary_id='PPI'
    AND c.concept_class_id='Question')

 -- maps pids to all their associated basics questions in the rdr
,pid_basics AS
 (SELECT
   person_id 
  ,ARRAY_AGG(DISTINCT c.concept_code IGNORE NULLS) basics_codes
  FROM `{DATASET_ID}.observation` o
  JOIN `{DATASET_ID}._mapping_observation` m
   USING (observation_id)
  JOIN basics_concept c
   ON o.observation_concept_id = c.concept_id
  WHERE 1=1
    AND src_hpo_id = 'rdr'
  GROUP BY 1)

 -- list all pids for whom no basics questions are found
SELECT * 
FROM pid_basics
WHERE ARRAY_LENGTH(basics_codes) = 0
'''
execute(client, query)
# -

# ## PPI records should never follow death date
# Make sure no one could die before the program began or have PPI records after their death.

query = f'''
WITH 

 ppi_concept AS
 (SELECT concept_id
  FROM `{DATASET_ID}.concept` c
  WHERE vocabulary_id = 'PPI')

,pid_ppi AS
 (SELECT
   person_id
   
  -- use latest ppi record for most rigid check --
  ,MAX(o.observation_date) AS max_ppi_date
  
  -- in case of multiple death rows, use earliest for most rigid check --
  ,MIN(d.death_date)       AS min_death_date

  FROM `{DATASET_ID}.death` d
   JOIN `{DATASET_ID}.observation` o
    USING (person_id)
   JOIN ppi_concept c
    ON o.observation_source_concept_id = c.concept_id
  GROUP BY person_id)

SELECT * 
FROM pid_ppi
WHERE min_death_date < max_ppi_date
'''
execute(client, query)

# ## Consent required for EHR Data
# If EHR records are found for participants who have not consented this may
# indicate a bug in the pipeline as these should have been removed. These
# records should also be reported to EHR Ops so that sites may cease to send
# the information.

# +
EHR_CONSENT_PERMISSION_CONCEPT_ID = 1586099
YES_CONCEPT_ID = 1586100

tpl = JINJA_ENV.from_string('''
-- For participants who do NOT satisfy EHR consent requirements as determined 
-- by the temp table below, this dynamic query will provide the      --
-- table_name, person_id, and hpo_id where any EHR records are found --
DECLARE query STRING; 

-- PIDs whose last EHR consent response was affirmative --
CREATE OR REPLACE TEMP TABLE consented AS
( 
  WITH 

  -- For each pid, the latest EHR consent record --
  pid_last_consent_response AS
  (SELECT DISTINCT 
     p.person_id
    ,LAST_VALUE(o.value_source_concept_id)
      OVER (
       PARTITION BY o.person_id
       ORDER BY o.observation_datetime
       ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS last_value_source_concept_id
   FROM `{{DATASET_ID}}.person` p
    JOIN `{{DATASET_ID}}.observation` o
     ON p.person_id = o.person_id
   WHERE observation_source_concept_id = {{EHR_CONSENT_PERMISSION_CONCEPT_ID}})

  -- Only store pids where the latest response is YES --
  SELECT 
   person_id
  FROM pid_last_consent_response
  WHERE 
      last_value_source_concept_id = {{YES_CONCEPT_ID}}
);


SET query = (
 SELECT

 -- Generate a subquery for any table containing HPO-submitted data                    --
 -- which finds any HPO-submitted rows whose pids are not in the consent lookup table. --

 STRING_AGG(
     '(SELECT DISTINCT '
      || '"' || table_name || '" AS table_name '
      || ',t.person_id           AS person_id '
      || ',m.src_hpo_id          AS hpo_id '
      || 'FROM `' || table_schema || '.' || table_name || '` t '
      || 'JOIN `' || table_schema || '.' || table_id || '` m ' 
      || 'USING (' || table_name ||'_id) '
      || 'LEFT JOIN consented c '
      || ' USING (person_id)'
      || 'WHERE m.src_hpo_id <> "rdr" AND c.person_id IS NULL)'
   , ' UNION ALL ')
 FROM `{{DATASET_ID}}.INFORMATION_SCHEMA.COLUMNS` c
 JOIN `{{DATASET_ID}}.__TABLES__` t
  ON t.table_id = '_mapping_' || c.table_name
 WHERE column_name = 'person_id'
   AND t.row_count > 0
);

EXECUTE IMMEDIATE query;
''')
query = tpl.render(
    DATASET_ID=DATASET_ID,
    EHR_CONSENT_PERMISSION_CONCEPT_ID=EHR_CONSENT_PERMISSION_CONCEPT_ID,
    YES_CONCEPT_ID=YES_CONCEPT_ID)
execute(client, query)
# -

# ## Date and datetime fields should have the same date
# The date represented by associated `_date` and `_datetime` fields of the same
# row should be the same. If there any discrepancies, there may be a bug in the
# pipeline (i.e. `ensure_date_datetime_consistency`). It may also be useful to
# report discrepancies to EHR Ops.
#
# ### Implementation notes
# For each associated date/timestamp pair in the dataset the script that follows
# assembles a universal subquery which can be applied to all pertinent tables and
# whose results can be compiled together into a single result set that provides
# relevant troubleshooting information. For legibility, a maximum of 10 problem
# rows is returned for each table.
#
# As an example, the subquery for the `observation` table should look something
# like this:
#
# ```sql
# SELECT
#  "observation"          AS table_name
# ,observation_id         AS row_id
# ,"observation_date"     AS date_field
# ,observation_date       AS date_value
# ,"observation_datetime" AS timestamp_field
# ,observation_datetime   AS timestamp_value
# FROM `DATASET_ID.observation`
# WHERE observation_date<>DATE(observation_datetime)
# LIMIT 10
# ```
#

query = f'''
DECLARE query DEFAULT (
    WITH 

     field_comparison AS
     (SELECT 
       d.table_schema AS table_schema
      ,d.table_name   AS table_name
      ,pk.column_name AS key_field
      ,d.column_name  AS date_field
      ,ts.column_name AS timestamp_field
      FROM `{DATASET_ID}.INFORMATION_SCHEMA.COLUMNS` d
       JOIN `{DATASET_ID}.INFORMATION_SCHEMA.COLUMNS` ts
        ON d.table_name = ts.table_name
           AND STARTS_WITH(ts.column_name, d.column_name)
           AND d.data_type = 'DATE'
           AND ts.data_type = 'TIMESTAMP'
       JOIN `{DATASET_ID}.INFORMATION_SCHEMA.COLUMNS` pk
        ON ts.table_name = pk.table_name
           AND pk.ordinal_position=1)

    SELECT
     STRING_AGG(
      '(SELECT ' 
         || '"' || table_name        || '" AS table_name '
         || ',' || key_field         || '  AS row_id '
         || ',"' || date_field       || '" AS date_field '
         || ',' || date_field        || '  AS date_value '
         || ',"' || timestamp_field  || '" AS timestamp_field '
         || ',' || timestamp_field   || '  AS timestamp_value '
         || 'FROM `' || table_schema || '.' || table_name || '` '
         || 'WHERE ' || date_field || '<>' || 'DATE(' || timestamp_field || ') '
         || ' LIMIT 10 )'
     ,' UNION ALL ')
    FROM field_comparison
);
EXECUTE IMMEDIATE query;
'''
execute(client, query)

# ---
# # Manual Review
# The following evaluation metrics require manual/visual review.

# ## Verify dataset properties
# The dataset metadata must have
# 1. a description indicating the version of the pipeline run and the input datasets
# 2. labels `phase`, `release_tag`, `de_identified` with appropriate values
#
# In the future this should try to find the dataset using labels supplied via notebook
# parameters (e.g. `combined`, `release_tag=2021q3r1`, `phase=clean`). Currently
# there is no way to specify `combined`.


# +
def verify_dataset_labels(dataset):
    """
    Print a warning if labels are missing or do not have expected values
    """
    expected_keys = ['phase', 'de_identified', 'release_tag']
    missing_keys = list(expected_keys - dataset.labels.keys())
    if missing_keys:
        print(
            f"Dataset label validation failed because keys were missing entirely: {missing_keys}"
        )

    expected = {'phase': 'clean', 'de_identified': 'false'}
    for key, value in expected.items():
        if key not in missing_keys:
            if dataset.labels[key] != expected[key]:
                print(f"Label '{key}' was not set to expected value '{value}'")

    # Check that the release tag is somewhere in the dataset name
    # TODO create a check on release_tag that is independent of dataset_id
    release_tag = dataset.labels['release_tag']
    if release_tag not in dataset.dataset_id:
        print(
            f"Release tag '{release_tag}' not in dataset_id '{dataset.dataset_id}'"
        )


verify_dataset_labels(client.get_dataset(DATASET_ID))
# -

# ## Invalid concept prevalence
# EHR submissions may provide source `concept_id`s which we assume would be
# successfully mapped to standard `concept_id`s. This compares the frequency
# of invalid values in required concept_id fields per hpo between the current
# combined dataset and a baseline combined dataset. The frequency of invalid
# values in this dataset should generally be non-increasing compared to baseline.
# If the prevalence of invalid concepts is increasing, EHR Ops should be alerted.

tpl = JINJA_ENV.from_string('''
-- Construct a table that summarizes all rows in all tables --
-- where concept_id fields have an invalid value and reports --
-- the row count per site --
DECLARE ddl DEFAULT("""
CREATE TABLE IF NOT EXISTS `{{DATASET_ID}}_sandbox.invalid_concept` 
(table_name  STRING NOT NULL OPTIONS(description="Identifies the table")
,column_name STRING NOT NULL OPTIONS(description="Identifies the concept_id field")
,src_hpo_id  STRING NOT NULL OPTIONS(description="Identifies the HPO that supplied the records")
,concept_id  INT64           OPTIONS(description="A value that is not in the associated concept table (can be NULL)")
,row_count   INT64  NOT NULL)
OPTIONS
(description='Count of invalid values provided in required concept_id fields for multiple datasets.'
,expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 7 DAY) )
""");

DECLARE query DEFAULT (
    SELECT 
 STRING_AGG('SELECT '
     || '"' || table_name  || '" AS table_name '
     || ',"' || column_name || '" AS column_name '
     || ', m.src_hpo_id AS src_hpo_id '
     || ',' || column_name  || ' AS concept_id '
     || ', COUNT(1) AS row_count '
     -- domain table joined with its mapping table --
     || 'FROM `' || table_schema || '.' || table_name || '` t'
     || ' JOIN `' || table_schema || '.' || '_mapping_' || table_name || '` m'
     || '  USING (' || c.table_name || '_id) '
     || ' LEFT JOIN `{{DATASET_ID}}.concept` c '
     || '  ON ' || column_name || ' = c.concept_id '
     
     -- invalid concept_id --
     || 'WHERE c.concept_id IS NULL '
     || 'GROUP BY 1, 2, 3, 4',
           ' UNION ALL ')
FROM `{{DATASET_ID}}.INFORMATION_SCHEMA.COLUMNS` c
JOIN `{{DATASET_ID}}.__TABLES__` t
 ON c.table_name = t.table_id
WHERE 1=1
  -- the table has data --
  AND t.row_count > 0
  -- there is an associated mapping table --
  AND EXISTS (
       SELECT 1 
       FROM `{{DATASET_ID}}.INFORMATION_SCHEMA.COLUMNS` 
      WHERE table_name = '_mapping_' || c.table_name
        AND column_name = c.table_name || '_id'
  )
  -- the column is a required concept_id --
  AND NOT ENDS_WITH(LOWER(column_name), 'source_concept_id')
  AND ENDS_WITH(LOWER(column_name), 'concept_id')
  AND is_nullable = 'NO'
);

EXECUTE IMMEDIATE (ddl || ' AS ' || query);
''')
query = tpl.render(DATASET_ID=DATASET_ID)
execute(client, query)
query = tpl.render(DATASET_ID=BASELINE_DATASET_ID)
execute(client, query)

query = f'''
WITH 
 baseline AS
 (SELECT * FROM `{BASELINE_DATASET_ID}_sandbox.invalid_concept`)
,latest AS
 (SELECT * FROM `{DATASET_ID}_sandbox.invalid_concept`)
SELECT
 table_name
,column_name
,src_hpo_id
,concept_id
,baseline.row_count        AS _{BASELINE_DATASET_ID}
,latest.row_count          AS _{DATASET_ID}
FROM baseline
FULL OUTER JOIN latest
 USING (table_name, column_name, src_hpo_id, concept_id)
ORDER BY ABS(latest.row_count - baseline.row_count) DESC
'''
execute(client, query)
