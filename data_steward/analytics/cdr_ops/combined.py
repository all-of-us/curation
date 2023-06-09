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
RUN_AS = ''  # Service account email for impersonation

# +
import pandas as pd

from common import JINJA_ENV, MAPPED_CLINICAL_DATA_TABLES
from cdr_cleaner.cleaning_rules.negative_ages import date_fields
from utils import auth
from gcloud.bq import BigQueryClient
from analytics.cdr_ops.notebook_utils import execute, IMPERSONATION_SCOPES, render_message

import matplotlib.pyplot as plt
from matplotlib_venn import venn3_unweighted
from tqdm import tqdm
import math
from IPython.display import display, HTML
# -

impersonation_creds = auth.get_impersonation_credentials(
    RUN_AS, target_scopes=IMPERSONATION_SCOPES)

client = BigQueryClient(PROJECT_ID, credentials=impersonation_creds)

pd.options.display.max_rows = 1000
pd.options.display.max_colwidth = 0
pd.options.display.max_columns = None
pd.options.display.width = None

# -

# ## Check for duplicates across all unique identifier fields.
# This query gathers any duplicates of the {table}_id from each OMOP table listed.
# The OMOP tables `death` and `fact_relationship` are excluded from the check because they do not have primary key fields.
# The output of this query should be empty. If any duplicates are found there may be a bug in the pipeline.
#
# Specific to duplicates in observation:<br>
# In the past duplicate `observation_id`s were introduced in observation due
# to multiple sites submitting data for the same participant (see
# [DC-1512](https://precisionmedicineinitiative.atlassian.net/browse/DC-1512)).
# If any duplicates are found there may be a bug in the pipeline-
# particularly in `ehr_union.move_ehr_person_to_observation`.

query = f"""
DECLARE i INT64 DEFAULT 0;
DECLARE tables ARRAY<STRING>;

SET tables = ["observation", "observation_period", "condition_occurrence",
"care_site", "condition_era", "device_cost", "device_exposure", "dose_era",
"drug_exposure", "location", "measurement", "note", "note_nlp", "person",
"procedure_cost", "procedure_occurrence", "provider", "specimen",
"survey_conduct", "visit_cost", "visit_detail", "visit_occurrence", "aou_death"];

CREATE TEMPORARY TABLE non_unique_primary_keys(table_name STRING, key_column int64);

LOOP
  SET i = i + 1;

  IF i > ARRAY_LENGTH(tables) THEN LEAVE; END IF;

  EXECUTE IMMEDIATE '''
    INSERT
        non_unique_primary_keys
    SELECT
        "''' || tables[ORDINAL(i)] || '''" AS table_name,
        ''' || tables[ORDINAL(i)] || '''_id AS key_column
    FROM
        `{DATASET_ID}.''' || tables[ORDINAL(i)] || '''` o
    WHERE
        ''' || tables[ORDINAL(i)] || '''_id IN (
              SELECT ''' || tables[ORDINAL(i)] || '''_id
              FROM `{DATASET_ID}.''' || tables[ORDINAL(i)] || '''`
              GROUP BY 1
              HAVING COUNT(''' || tables[ORDINAL(i)] || '''_id) > 1
                )
    ORDER BY
        ''' || tables[ORDINAL(i)] || '''_id''';

END LOOP;

SELECT *
FROM non_unique_primary_keys
ORDER BY 2 DESC;

SELECT table_name, COUNT(DISTINCT table_name) AS full_count, COUNT(DISTINCT table_name)/2 AS half_count
FROM non_unique_primary_keys
GROUP BY 1
ORDER BY 3 DESC, 2 DESC;
"""
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
 OR {{PIPELINE_TABLES}}.calculate_age(t.{{date_field}}, EXTRACT(DATE FROM p.birth_datetime)) >= 150
)
{% if not loop.last -%}
   UNION ALL
{% endif %}
{% endfor %}
''')
query = tpl.render(dataset_id=DATASET_ID, date_fields=date_fields)
execute(client, query)

# ## PPI records should never follow death date
# Make sure no one could die before the program began or have PPI records after their death.

# +
query = JINJA_ENV.from_string("""
query = f'''
WITH

 ppi_concept AS
 (SELECT concept_id
  FROM `{{dataset}}.concept` c
  WHERE vocabulary_id = 'PPI')

,pid_ppi AS
 (SELECT
   person_id

  -- use latest ppi record for most rigid check --
  ,MAX(o.observation_date) AS max_ppi_date

  -- in case of multiple death rows, use earliest for most rigid check --
  ,MIN(d.death_date)       AS min_death_date

  FROM `{{dataset}}.aou_death` d
   JOIN `{{dataset}}.observation` o
    USING (person_id)
   JOIN ppi_concept c
    ON o.observation_source_concept_id = c.concept_id
  GROUP BY person_id)

SELECT *
FROM pid_ppi
WHERE min_death_date < max_ppi_date
""").render(dataset=DATASET_ID)
df = execute(client, query)

success_msg = 'No PPI records follow death date.'
failure_msg = '''
    Some PPI are recorded after death date. Potential causes:
        1. The participant's non-primary record (primary_death_record=FALSE) occurs before the PPI.
           It can be because the source death record has incorrect death date. Discuss within Curation
           about next steps (e.g., move forward CDR generation or hot-fix the non-primary death record).
        2. The participant's primary record (primary_death_record=TRUE) occurs before the PPI.
           This should not happen, since the CR NoDataAfterDeath cleans such records before this QC run.
           Investigate and discuss within Curation about next steps.
        3. None of the above. Investigate and discuss within Curation about next steps.
'''
render_message(df, success_msg, failure_msg)
# -

# ## Consent required for EHR Data
# If EHR records are found for participants who have not consented this may
# indicate a bug in the pipeline as these should have been removed. These
# records should also be reported to EHR Ops so that sites may cease to send
# the information.

# +
EHR_CONSENT_PERMISSION_CONCEPT_ID = 1586099
YES_CONCEPT_ID = 1586100

tpl = JINJA_ENV.from_string('''
-- For participants who do NOT satisfy EHR consent requirements as determined --
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

# ## OMOP, Mapping, and Extension Tables Contain Same Record Count

# This check ensures that each OMOP table's corresponding mapping and extension tables:
# 1. Have the same number of rows.
# 2. Contain the same set of primary key ids.
#
# On <span style="color: green">Success</span>, the above conditions will be satisfied for all OMOP tables.
#
# On <span style="color: red">Failure</span>, at least one of the above conditions will have been broken for at least one of the OMOP tables.
#
# The success status of the check is output in the Results section below.
# * The first dataframe shows the row counts for each OMOP table and shows success only when the clinical, mapping, and extension tables share the same row counts.
# * The second dataframe shows the number of primary keys shared between the clinical, mapping, and extension tables and shows success only when all primary keys are in the clinical/mapping/extension group.
# * This dataframe is accompanied by a series of venn diagrams showing the overlap, with the title of failed tables highlighted in red.

# #### Condition 1

# +
# Checks if Clinical/Mapping/Extension row counts are all equal

pbar = tqdm(MAPPED_CLINICAL_DATA_TABLES)

row_counts = []
for mapped_table in pbar:
    pbar.set_description(mapped_table)

    query = f'''
        WITH rows_counts AS (
          SELECT
            'Clinical' cat, COUNT(*) row_count
          FROM `{PROJECT_ID}.{DATASET_ID}.{mapped_table}`
          UNION ALL
          SELECT
            'Mapping' cat, COUNT(*) row_count
          FROM `{PROJECT_ID}.{DATASET_ID}._mapping_{mapped_table}`
          UNION ALL
          SELECT
            'Extension' cat, COUNT(*) row_count
          FROM `{PROJECT_ID}.{DATASET_ID}.{mapped_table}_ext`
        )
        SELECT
          '{mapped_table}' tablename, *
        FROM
        (SELECT cat, row_count FROM rows_counts o)
        PIVOT(SUM(row_count) FOR cat IN ('Clinical', 'Mapping', 'Extension'))
    '''

    row_count = execute(client, query)
    row_counts.append(row_count)

row_counts = pd.concat(row_counts)
row_counts = row_counts.set_index('tablename')
row_counts['cond1'] = row_counts.eq(row_counts.iloc[:, 0],
                                    axis=0).all(1).astype(bool)
# -

# #### Condition 2

# +
# Checks if Clinical/Mapping/Extension tables have some primary key ids

overlaps = []
pbar = tqdm(MAPPED_CLINICAL_DATA_TABLES)
for mapped_table in pbar:

    pbar.set_description(mapped_table)
    query = f'''
    WITH overlap AS (
      SELECT COALESCE(m.{mapped_table}_id, mm.{mapped_table}_id, ext.{mapped_table}_id) {mapped_table}_id,
        CASE
          WHEN m.{mapped_table}_id IS NOT NULL THEN
            CASE
              WHEN mm.{mapped_table}_id IS NOT NULL THEN
                CASE
                  WHEN ext.{mapped_table}_id IS NOT NULL THEN 'ABC'
                  ELSE 'AB'
                END
            ELSE
              CASE
                WHEN ext.{mapped_table}_id IS NOT NULL THEN 'AC'
                ELSE 'A'
              END
            END
          WHEN mm.{mapped_table}_id IS NOT NULL THEN
            CASE
              WHEN ext.{mapped_table}_id IS NOT NULL THEN 'BC'
              ELSE 'B'
            END
          ELSE 'C'
        END cat
      FROM `{PROJECT_ID}.{DATASET_ID}.{mapped_table}` m
      FULL JOIN `{PROJECT_ID}.{DATASET_ID}._mapping_{mapped_table}` mm
        ON mm.{mapped_table}_id = m.{mapped_table}_id
      FULL JOIN `{PROJECT_ID}.{DATASET_ID}.{mapped_table}_ext` ext
        ON ext.{mapped_table}_id = COALESCE(m.{mapped_table}_id, mm.{mapped_table}_id)
    )
    SELECT
      '{mapped_table}' tablename, *
    FROM
    (SELECT cat FROM overlap o)
    PIVOT(COUNT(1) FOR cat IN ('A', 'B', 'AB', 'C', 'AC', 'BC', 'ABC'));
    '''

    overlap = execute(client, query)
    overlaps.append(overlap)

overlaps = pd.concat(overlaps)
overlaps = overlaps.set_index('tablename')
overlaps['cond2'] = overlaps.sum(axis=1) == overlaps['ABC']

# +
# plot venn diagrams for overlap

total_plots = len(overlaps)
cols = 3
rows = math.ceil(total_plots / cols)

fig, axes = plt.subplots(rows, cols, figsize=(5, 5), squeeze=False)

k = 0
while k < total_plots:
    i, j = k // cols, k % cols

    row = overlaps.iloc[k]
    axes[i][j].set_title(row.name, fontdict={'fontweight': 'bold'})

    Abc, aBc, ABc, abC, AbC, aBC, ABC = row['A'], row['B'], row['AB'], row[
        'C'], row['AC'], row['BC'], row['ABC']
    v = venn3_unweighted(subsets=(Abc, aBc, ABc, abC, AbC, aBC, ABC),
                         set_labels=('Clinical', 'Mapping', 'Extension'),
                         ax=axes[i][j],
                         subset_areas=[5] * 7)

    if Abc + aBc + ABc + abC + AbC + aBC > 0:
        axes[i][j].title.set_color('red')

    k += 1

while k < rows * cols:
    i, j = k // cols, k % cols
    fig.delaxes(axes[i][j])

    k += 1

overlaps = overlaps.rename(
    columns={
        'A': 'Clinical',
        'B': 'Mapping',
        'C': 'Extension',
        'AB': 'Clinical/Mapping',
        'AC': 'Clinical/Extension',
        'BC': 'Mapping/Extension',
        'ABC': 'Clinical/Mapping/Extension'
    })

# -

# #### Results

# +
# check both conditions

is_cond1_success = row_counts['cond1'].all()
is_cond2_success = overlaps['cond2'].all()
is_success = is_cond1_success & is_cond2_success

display(
    HTML(f'''
        <h3>
            Check Status:&nbsp <span style="color: {'red' if not is_success else 'green'}">{'Failed' if not is_success else 'Success'}</span>
        </h3>
        {'<p>Check/run the <a href="https://github.com/all-of-us/curation/blob/develop/data_steward/cdr_cleaner/cleaning_rules/clean_mapping.py">CleanMappingExtTables</a> cleaning rule as a potential remedy.</p>' if not is_success else ''}
        <div>
            <h5>
                Condition 1 (All row counts are equal):&nbsp
                <span style="color: {'red' if not is_cond1_success else 'green'}">
                    {'Failed' if not is_cond1_success else 'Success'}
                </span>
                {row_counts.to_html()}
            </h5>
        </div>
        <br/>
        <div>
            <h5>
                Condition 2 (All sets of primary keys are the same):&nbsp
                <span style="color: {'red' if not is_cond2_success else 'green'}">
                    {'Failed' if not is_cond2_success else 'Success'}
                </span>
                {overlaps.to_html()}
            </h5>
        </div>
    '''))
fig

# -

# ---
# # Manual Review
# The following evaluation metrics require manual/visual review.

# ## Verify dataset properties
# The dataset metadata must have
# 1. a description indicating the version of the pipeline run and the input datasets
# 2. labels `owner`, `phase`, `release_tag`, `de_identified` with appropriate values
#
# In the future this should try to find the dataset using labels supplied via notebook
# parameters (e.g. `combined`, `release_tag=2021q3r1`, `phase=clean`). Currently
# there is no way to specify `combined`.


# +
def verify_dataset_labels(dataset):
    """
    Print a warning if labels are missing or do not have expected values
    """
    expected_keys = ['owner', 'phase', 'de_identified', 'release_tag']
    missing_keys = list(expected_keys - dataset.labels.keys())
    if missing_keys:
        print(
            f"[FAILED] Check 1/3: Dataset label validation failed because keys were missing entirely: {missing_keys}"
        )
    else:
        print(
            f"[SUCCEEDED] Check 1/3: All mandatory keys ({', '.join(expected_keys)}) exist in '{dataset.dataset_id}'"
        )

    expected = {'owner': 'curation', 'phase': 'clean', 'de_identified': 'false'}
    for key, value in expected.items():
        if key not in missing_keys:
            if dataset.labels[key] != expected[key]:
                print(
                    f"[FAILED] Check 2/3: Label '{key}' was not set to expected value '{value}'"
                )
            else:
                print(
                    f"[SUCCEEDED] Check 2/3: Label '{key}' is set to '{value}' as expected"
                )

    # Check that the release tag is somewhere in the dataset name
    # TODO create a check on release_tag that is independent of dataset_id
    release_tag = dataset.labels['release_tag']
    if release_tag not in dataset.dataset_id:
        print(
            f"[FAILED] Check 3/3: Release tag '{release_tag}' not in dataset_id '{dataset.dataset_id}'"
        )
    else:
        print(
            f"[SUCCEEDED] Check 3/3: Release tag '{release_tag}' is in dataset_id '{dataset.dataset_id}' as expected"
        )

    print(
        f"verify_dataset_labels() completed. Investigate if any result is [FAILED]."
    )


verify_dataset_labels(client.get_dataset(DATASET_ID))
# -

# ## Invalid concept prevalence
# EHR submissions may provide source `concept_id`s which we assume would be
# successfully mapped to standard `concept_id`s. This compares the frequency
# of invalid values in required concept_id fields per hpo between the current
# raw combined dataset and a baseline raw combined dataset. The frequency of invalid
# values in this dataset should generally be non-increasing compared to baseline.
# If the prevalence of invalid concepts is increasing, EHR Ops should be alerted.
#
# NOTE: This check uses the raw combined datasets(combined_backup) as the invalid rows
# are removed during the cleaning process. See MissingConceptRecordSuppression

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
     || ' LEFT JOIN `{{DATASET_ID}}_backup.concept` c '
     || '  ON ' || column_name || ' = c.concept_id '

     -- invalid concept_id --
     || 'WHERE c.concept_id IS NULL '
     || 'GROUP BY 1, 2, 3, 4',
           ' UNION ALL ')
FROM `{{DATASET_ID}}_backup.INFORMATION_SCHEMA.COLUMNS` c
JOIN `{{DATASET_ID}}_backup.__TABLES__` t
 ON c.table_name = t.table_id
WHERE 1=1
  -- the table has data --
  AND t.row_count > 0
  -- there is an associated mapping table --
  AND EXISTS (
       SELECT 1
       FROM `{{DATASET_ID}}_backup.INFORMATION_SCHEMA.COLUMNS`
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
-- The results are invalid concepts that had increased usage since the baseline dataset. --
-- Inform EHR OPS of these results. --
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
,baseline.row_count                       AS _{BASELINE_DATASET_ID}
,latest.row_count                         AS _{DATASET_ID}
,latest.row_count - baseline.row_count    AS diff
FROM baseline
FULL OUTER JOIN latest
 USING (table_name, column_name, src_hpo_id, concept_id)
GROUP BY 1,2,3,4,5,6
HAVING latest.row_count - baseline.row_count > 0
ORDER BY diff DESC
'''
execute(client, query)

# # Invalid survey_conduct_id check

# The survey_conduct table has a primary key, survey_conduct_id, that is a foreign key value to
# observation.questionnaire_response_id.  For data consistency, curation must ensure that
# survey_conduct.survey_conduct_id does not contain null values or orphaned values once
# the dataset has been cleaned.  If it does, curation must be alerted and a manual fix will
# likely be needed for the short term resolution.
# This is a validation of [DC-2735](https://precisionmedicineinitiative.atlassian.net/browse/DC-2735)
#
# See [DC-2754](https://precisionmedicineinitiative.atlassian.net/browse/DC-2754) for details.

# +
query_null_check = JINJA_ENV.from_string("""
SELECT survey_conduct_id, person_id
FROM `{{project_id}}.{{dataset_id}}.survey_conduct`
WHERE survey_conduct_id IS NULL
""").render(project_id=PROJECT_ID, dataset_id=DATASET_ID)

query_orphaned_check = JINJA_ENV.from_string("""
SELECT survey_conduct_id
FROM `{{project_id}}.{{dataset_id}}.survey_conduct`
WHERE survey_conduct_id NOT IN (
    SELECT DISTINCT questionnaire_response_id
    FROM `{{project_id}}.{{dataset_id}}.observation`
    WHERE questionnaire_response_id IS NOT NULL
)
""").render(project_id=PROJECT_ID, dataset_id=DATASET_ID)

success_null_check = '''No NULL values found for survey_conduct_id.'''
failure_null_check = '''
There are <b>{code_count}</b> NULL survey_conduct_ids in survey_conduct.
Manually remove those NULL records as the short term resolution.
Create an investigation ticket for root cause and the long term resolution.
'''

success_orphaned_check = '''No orphaned survey_conduct_id found.'''
failure_orphaned_check = '''
There are <b>{code_count}</b> survey_conduct_ids in survey_conduct that cannot
be joined to an existing observation.questionnaire_response_id value.
Manually remove those orphaned survey_conduct records as the short term resolution.
Create an investigation ticket for root cause and the long term resolution.
'''

df_null_check = execute(client, query_null_check)
df_orphaned_check = execute(client, query_orphaned_check)

render_message(df_null_check,
               success_null_check,
               failure_null_check,
               failure_msg_args={'code_count': len(df_null_check)})

render_message(df_orphaned_check,
               success_orphaned_check,
               failure_orphaned_check,
               failure_msg_args={'code_count': len(df_orphaned_check)})
# -

# # QC for AOU_DEATH table

# From CDR V8, Curation generates the AOU_DEATH table in Combined. AOU_DEATH has the death records from
# both EHR and RDR, and it can have more than one death record per participant. It has the column `primary_death_record` and
# it flags the primary records for each participant. The logic for deciding which is primary comes from the following
# business requirements:
# - If multiple death records exist from across sources, provide the first date EHR death record in the death table
# - If death_datetime is not available and multiple death records exist for the same death_date, provide the fullest record in the death table
# - Example: Order by HPO site name and insert the first into the death table
#
# This QC confirms that the logic for the primary records are applied as expected in the `AOU_DEATH` table.

# +
query = JINJA_ENV.from_string("""
WITH qc_aou_death AS (
    SELECT 
        aou_death_id, 
        CASE WHEN aou_death_id IN (
            SELECT aou_death_id FROM `{{project_id}}.{{dataset_id}}.aou_death`
            QUALIFY RANK() OVER (
                PARTITION BY person_id 
                ORDER BY
                    LOWER(src_id) NOT LIKE '%healthpro%' DESC, -- EHR records are chosen over HealthPro ones --
                    death_date ASC, -- Earliest death_date records are chosen over later ones --
                    death_datetime ASC NULLS LAST, -- Earliest non-NULL death_datetime records are chosen over later or NULL ones --
                    src_id ASC -- EHR site that alphabetically comes first is chosen --
            ) = 1   
        ) THEN TRUE ELSE FALSE END AS primary_death_record
    FROM `{{project}}.{{dataset}}.aou_death`    
)
SELECT ad.aou_death_id
FROM `{{project_id}}.{{dataset}}.aou_death` ad
LEFT JOIN qc_aou_death qad
ON ad.aou_death_id = qad.aou_death_id
WHERE ad.primary_death_record != qad.primary_death_record
""").render(project_id=PROJECT_ID, dataset=DATASET_ID)
df = execute(client, query)

success_msg = 'All death records have the correct `primary_death_record` values.'
failure_msg = '''
    <b>{code_count}</b> records do not have the correct `primary_death_record` values. 
    Investigate and confirm if (a) any logic is incorrect, (b) the requirement has changed, or (c) something else.
'''
render_message(df,
               success_msg,
               failure_msg,
               failure_msg_args={'code_count': len(df)})
# -

# # QC for DEATH table

# From CDR V8, Combined data stage will have all the death records in the AOU_DEATH table. The DEATH table must exist but must have no records.
# This QC confirms that the DEATH table is there and is empty.

# +
query_if_empty = JINJA_ENV.from_string("""
SELECT COUNT(*)
FROM `{{project_id}}.{{dataset}}.death`
HAVING COUNT(*) > 0
""").render(project_id=PROJECT_ID, dataset=DATASET_ID)
df_if_empty = execute(client, query_if_empty)

success_msg_if_empty = 'Death table is empty.'
failure_msg_if_empty = '''
    Death table is NOT empty. We expect DEATH table to be empty in Combined. Investigate why DEATH is not empty and fix it.
'''
render_message(df_if_empty, success_msg_if_empty, failure_msg_if_empty)
