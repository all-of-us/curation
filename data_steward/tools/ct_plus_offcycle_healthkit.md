# DL-2458: CT+ copy of the off-cycle Apple HealthKit dataset

Copies `<source_dataset>` (controlled tier) into `<output_dataset>` (controlled tier plus), replacing `person_id` with `controlled_tier_plus_id`. Everything else passes through unchanged, including `id`, `uuid`, `summary_id` and `device_source_id`, so a holder of both tiers can still align rows. That is the agreed scope: `person_id` only.

Built 2026-08-19. Results are in the run log at the bottom. This document is kept because BigQuery retains query text for only 180 days and this dataset is issued once rather than per release.

Run the statements one at a time, never as a single script, with `--location=us-central1` on every job. Dry run anything in section 4 before running it.

```bash
bq query --location=us-central1 --use_legacy_sql=false --nouse_cache '<statement>'
```

## Objects

Project and dataset names are masked below. The actual values are recorded on the JIRA ticket. Table and view names are real.

| Placeholder | Role | Location |
|---|---|---|
| `<source_project>.<source_dataset>` | Controlled tier source, not built by Curation | us-central1 |
| `<curation_project>.<output_dataset>` | Controlled tier plus output, created by this document | us-central1 |
| `<curation_project>.<view_dataset>.rdr_participant_research_ids_view` | Controlled tier to controlled tier plus ID mapping | US |
| `<warehouse_project>.<scratch_dataset>` | Scratch, not researcher facing | us-central1 |
| `<job_project>` | Where `bq` created the jobs, and therefore what was billed | n/a |

## Traps

The source and the view are in different locations. A `us-central1` job reads both; a `US` job cannot read the source at all.

The view is hand maintained, is not version controlled, and has been repointed with sharply reduced coverage before. Section 1 pins it. Do not skip it.

Do not substitute `<warehouse_project>.<ops_dataset>.rdr_participant_research_ids_view`. It is a different object with 1,361,904 rows and 424 duplicate participants, and those duplicates would fan out every join below.

## 0. Create the datasets

BigQuery creates tables implicitly but never datasets, so step 4a fails without this. These are `bq` commands, not SQL.

**0a.** The output dataset must not already exist. Section 4 uses `CREATE OR REPLACE TABLE` in a production project and would overwrite an existing build.

```bash
bq show --format=prettyjson <curation_project>:<output_dataset>
```

**0b.** Name settled 2026-08-19: r6 serves 6b and 6c, with no r7 reissue. Datasets cannot be renamed, so changing this later means recopying about 4 billion rows.

**0c.** Create the output dataset. `--location` goes before `mk`. Omitting it lands the dataset in the `US` multi-region, which is immutable and breaks every write in section 4.

```bash
bq --location=us-central1 mk --dataset <curation_project>:<output_dataset>
```

**0d.** Create the scratch dataset for step 3b. It must be `us-central1`, and it must not be the output dataset, because the list holds controlled tier research IDs and the output is researcher facing.

```bash
bq --location=us-central1 mk --dataset <warehouse_project>:<scratch_dataset>
```

**0e.** Confirm both report `"location": "us-central1"` before going further.

## 1. Pin the mapping view

**1a.** Capture the view definition for the ticket. It is not in version control.

```bash
bq --location=US show --format=prettyjson \
  <curation_project>:<view_dataset>.rdr_participant_research_ids_view
```

**1b.** The view must be unique on both join keys or section 4 fans out. Expect about 859,402 rows; materially fewer means it was repointed again, so stop.

```sql
SELECT
  COUNT(*)                                 AS view_rows,
  COUNT(DISTINCT controlled_tier_id)       AS distinct_ct_ids,    -- must equal view_rows
  COUNT(DISTINCT controlled_tier_plus_id)  AS distinct_ctp_ids,   -- must equal view_rows
  COUNTIF(controlled_tier_id IS NULL)      AS null_ct_ids,        -- expect 0
  COUNTIF(controlled_tier_plus_id IS NULL) AS null_ctp_ids        -- expect 0
FROM `<curation_project>.<view_dataset>.rdr_participant_research_ids_view`;
```

## 2. Pin the source

**2a.** Source DDL. Confirms `person_id` is INT64, and surfaces any `PARTITION BY` or `CLUSTER BY` that section 4 would otherwise silently drop.

```sql
SELECT table_name, ddl
FROM `<source_project>.<source_dataset>.INFORMATION_SCHEMA.TABLES`
ORDER BY table_name;  -- expect 5 rows
```

**2b.** Where `person_id` sits, and any ARRAY or STRUCT column, which would break step 5e. Answered by 2a; run only to re-confirm after a source rebuild.

```sql
SELECT table_name, column_name, data_type
FROM `<source_project>.<source_dataset>.INFORMATION_SCHEMA.COLUMNS`
WHERE column_name = 'person_id'
   OR data_type LIKE 'ARRAY%'
   OR data_type LIKE 'STRUCT%'
ORDER BY table_name, column_name;
```

**2c.** Confirm `device_and_source` carries no participant identifier under another name before copying it through untouched.

```sql
SELECT column_name, data_type
FROM `<source_project>.<source_dataset>.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'device_and_source'
ORDER BY ordinal_position;
```

**2d.** The source must be CT keyed. If `ct_matches` does not dominate, the join key behind every statement in section 4 is wrong, so stop.

```sql
WITH src AS (
  SELECT DISTINCT person_id
  FROM `<source_project>.<source_dataset>.physical_measurements`
)
SELECT
  COUNT(*) AS src_participants,
  COUNTIF(v_ct.controlled_tier_id IS NOT NULL)       AS ct_matches,   -- expect nearly all
  COUNTIF(v_rt.registered_tier_id IS NOT NULL)       AS rt_matches,   -- expect ~0
  COUNTIF(v_ctp.controlled_tier_plus_id IS NOT NULL) AS ctp_matches   -- expect ~0
FROM src
LEFT JOIN `<curation_project>.<view_dataset>.rdr_participant_research_ids_view` v_ct
  ON src.person_id = v_ct.controlled_tier_id
LEFT JOIN `<curation_project>.<view_dataset>.rdr_participant_research_ids_view` v_rt
  ON src.person_id = v_rt.registered_tier_id
LEFT JOIN `<curation_project>.<view_dataset>.rdr_participant_research_ids_view` v_ctp
  ON src.person_id = v_ctp.controlled_tier_plus_id;
```

## 3. Coverage

Run before anything is written. Abort above 1 percent unresolved. The view sat at 44.2 percent coverage before it was repointed on 2026-08-10, and a run against that would have completed and looked clean.

**3a.** The gate.

```sql
WITH src_participants AS (
  SELECT DISTINCT person_id FROM `<source_project>.<source_dataset>.activity_summary`
  UNION DISTINCT
  SELECT DISTINCT person_id FROM `<source_project>.<source_dataset>.vital_measurements`
  UNION DISTINCT
  SELECT DISTINCT person_id FROM `<source_project>.<source_dataset>.physical_measurements`
  UNION DISTINCT
  SELECT DISTINCT person_id FROM `<source_project>.<source_dataset>.activity_intraday`
)
SELECT
  COUNT(*)                                       AS input_participants,
  COUNTIF(v.controlled_tier_plus_id IS NOT NULL) AS resolved,
  COUNTIF(v.controlled_tier_plus_id IS NULL)     AS unresolved,
  ROUND(100 * COUNTIF(v.controlled_tier_plus_id IS NULL) / COUNT(*), 3) AS unresolved_pct  -- abort above 1.0
FROM src_participants s
LEFT JOIN `<curation_project>.<view_dataset>.rdr_participant_research_ids_view` v
  ON s.person_id = v.controlled_tier_id;
```

**3b.** Record the unresolved IDs (AC 7). These are controlled tier research IDs, so the table must not live in the researcher facing output dataset.

```sql
CREATE OR REPLACE TABLE `<warehouse_project>.<scratch_dataset>.dl2458_unresolved_ct_ids` AS
WITH src_participants AS (
  SELECT DISTINCT person_id FROM `<source_project>.<source_dataset>.activity_summary`
  UNION DISTINCT
  SELECT DISTINCT person_id FROM `<source_project>.<source_dataset>.vital_measurements`
  UNION DISTINCT
  SELECT DISTINCT person_id FROM `<source_project>.<source_dataset>.physical_measurements`
  UNION DISTINCT
  SELECT DISTINCT person_id FROM `<source_project>.<source_dataset>.activity_intraday`
)
SELECT s.person_id AS controlled_tier_id
FROM src_participants s
LEFT JOIN `<curation_project>.<view_dataset>.rdr_participant_research_ids_view` v
  ON s.person_id = v.controlled_tier_id
WHERE v.controlled_tier_plus_id IS NULL;
```

**3c.** Rows the inner join will drop, per table. This is the arithmetic AC 2 is checked against, so it must be computed before the build rather than after.

```sql
SELECT 'activity_summary' AS table_name, COUNT(*) AS rows_dropped
FROM `<source_project>.<source_dataset>.activity_summary` t
LEFT JOIN `<curation_project>.<view_dataset>.rdr_participant_research_ids_view` v
  ON t.person_id = v.controlled_tier_id
WHERE v.controlled_tier_plus_id IS NULL
UNION ALL
SELECT 'vital_measurements', COUNT(*)
FROM `<source_project>.<source_dataset>.vital_measurements` t
LEFT JOIN `<curation_project>.<view_dataset>.rdr_participant_research_ids_view` v
  ON t.person_id = v.controlled_tier_id
WHERE v.controlled_tier_plus_id IS NULL
UNION ALL
SELECT 'physical_measurements', COUNT(*)
FROM `<source_project>.<source_dataset>.physical_measurements` t
LEFT JOIN `<curation_project>.<view_dataset>.rdr_participant_research_ids_view` v
  ON t.person_id = v.controlled_tier_id
WHERE v.controlled_tier_plus_id IS NULL
UNION ALL
SELECT 'activity_intraday', COUNT(*)
FROM `<source_project>.<source_dataset>.activity_intraday` t
LEFT JOIN `<curation_project>.<view_dataset>.rdr_participant_research_ids_view` v
  ON t.person_id = v.controlled_tier_id
WHERE v.controlled_tier_plus_id IS NULL;
```

## 4. Build

Smallest table first, each verified before the next.

`SELECT * REPLACE` preserves the schema and column order and cannot miss a column. The source has no `PARTITION BY` or `CLUSTER BY` (see 2a), so a plain `CREATE TABLE AS SELECT` loses no physical layout. It does lose column descriptions; that was accepted on 2026-08-19 and is recoverable later with `ALTER TABLE ... ALTER COLUMN ... SET OPTIONS(description=...)`, which is metadata only.

The `JOIN` is an inner join, so it drops unresolved participants by design, in the counts step 3c measured. Do not widen the re-key past `person_id` without a decision to match.

**4a.** Expect 1,718,619 in, 490 dropped, 1,718,129 out.

```sql
CREATE OR REPLACE TABLE `<curation_project>.<output_dataset>.physical_measurements` AS
SELECT t.* REPLACE (v.controlled_tier_plus_id AS person_id)
FROM `<source_project>.<source_dataset>.physical_measurements` t
JOIN `<curation_project>.<view_dataset>.rdr_participant_research_ids_view` v
  ON t.person_id = v.controlled_tier_id;
```

**4b.** Expect 769,254 in, 988 dropped, 768,266 out.

```sql
CREATE OR REPLACE TABLE `<curation_project>.<output_dataset>.activity_intraday` AS
SELECT t.* REPLACE (v.controlled_tier_plus_id AS person_id)
FROM `<source_project>.<source_dataset>.activity_intraday` t
JOIN `<curation_project>.<view_dataset>.rdr_participant_research_ids_view` v
  ON t.person_id = v.controlled_tier_id;
```

Stop here and run section 5 against 4a and 4b before continuing.

**4c.** Expect 1,102,956,914 in, 1,048,935 dropped, 1,101,907,979 out.

```sql
CREATE OR REPLACE TABLE `<curation_project>.<output_dataset>.vital_measurements` AS
SELECT t.* REPLACE (v.controlled_tier_plus_id AS person_id)
FROM `<source_project>.<source_dataset>.vital_measurements` t
JOIN `<curation_project>.<view_dataset>.rdr_participant_research_ids_view` v
  ON t.person_id = v.controlled_tier_id;
```

**4d.** Expect 2,982,662,667 in, 4,069,124 dropped, 2,978,593,543 out. About 5 minutes.

```sql
CREATE OR REPLACE TABLE `<curation_project>.<output_dataset>.activity_summary` AS
SELECT t.* REPLACE (v.controlled_tier_plus_id AS person_id)
FROM `<source_project>.<source_dataset>.activity_summary` t
JOIN `<curation_project>.<view_dataset>.rdr_participant_research_ids_view` v
  ON t.person_id = v.controlled_tier_id;
```

**4e.** `device_and_source` has no `person_id`. Copy it with `bq cp` rather than rebuilding it: the copy is free, byte exact, and keeps the column descriptions, which is what AC 6 asks for.

```bash
bq --location=us-central1 cp \
  <source_project>:<source_dataset>.device_and_source \
  <curation_project>:<output_dataset>.device_and_source
```

## 5. Verification

Substitute the table name for `<table>` and run after each build.

**5a.** AC 1, plus the final row counts, in one read.

```sql
SELECT table_id, row_count, ROUND(size_bytes/POW(1024,3),2) AS size_gb
FROM `<curation_project>.<output_dataset>.__TABLES__`
ORDER BY table_id;  -- expect 5 rows
```

**5b.** AC 2. The difference must equal step 3c exactly. A larger output means the join fanned out despite 1b; a smaller one means it dropped more than coverage predicted.

```sql
SELECT
  (SELECT COUNT(*) FROM `<source_project>.<source_dataset>.<table>`) AS source_rows,
  (SELECT COUNT(*) FROM `<curation_project>.<output_dataset>.<table>`)              AS output_rows;
```

**5c.** AC 3. Every output `person_id` is a valid CT+ ID.

```sql
SELECT COUNT(*) AS unresolved_output_ids  -- expect 0
FROM `<curation_project>.<output_dataset>.<table>` t
LEFT JOIN `<curation_project>.<view_dataset>.rdr_participant_research_ids_view` v
  ON t.person_id = v.controlled_tier_plus_id
WHERE v.controlled_tier_plus_id IS NULL;
```

**5d.** AC 4. No controlled tier research ID survives.

```sql
SELECT COUNT(*) AS surviving_ct_ids  -- expect 0
FROM `<curation_project>.<output_dataset>.<table>` t
JOIN `<curation_project>.<view_dataset>.rdr_participant_research_ids_view` v
  ON t.person_id = v.controlled_tier_id;
```

**5e.** AC 5. Set based, so it cannot see duplicated rows; 5b covers that. It rejects ARRAY columns, which 2b rules out. 668 GB on `activity_summary`, about 7 minutes.

```sql
SELECT COUNT(*) AS changed_rows  -- expect 0
FROM (
  SELECT * EXCEPT(person_id) FROM `<curation_project>.<output_dataset>.<table>`
  EXCEPT DISTINCT
  SELECT * EXCEPT(person_id) FROM `<source_project>.<source_dataset>.<table>`
);
```

**5f.** Fallback for 5e if it ever exhausts resources on a large table. Same guarantee, one pass instead of a full row shuffle, and it also catches duplication. It is not cheaper: priced at 691.6 GB against 5e's 667.7 GB on `activity_summary`. Failed queries are unbilled, so try 5e first regardless.

```sql
WITH out_side AS (
  SELECT
    COUNT(*) AS row_count,
    SUM(CAST(MOD(FARM_FINGERPRINT(TO_JSON_STRING(
      (SELECT AS STRUCT t.* EXCEPT(person_id)))), 1000000007) AS NUMERIC)) AS payload_checksum
  FROM `<curation_project>.<output_dataset>.<table>` t
),
src_side AS (
  SELECT
    COUNT(*) AS row_count,
    SUM(CAST(MOD(FARM_FINGERPRINT(TO_JSON_STRING(
      (SELECT AS STRUCT t.* EXCEPT(person_id)))), 1000000007) AS NUMERIC)) AS payload_checksum
  FROM `<source_project>.<source_dataset>.<table>` t
  JOIN `<curation_project>.<view_dataset>.rdr_participant_research_ids_view` v
    ON t.person_id = v.controlled_tier_id
)
SELECT
  src_side.row_count        AS src_rows,
  out_side.row_count        AS out_rows,        -- must equal src_rows
  src_side.payload_checksum AS src_checksum,
  out_side.payload_checksum AS out_checksum     -- must equal src_checksum
FROM src_side, out_side;
```

**5g.** AC 6. `device_and_source` is byte identical to its source, descriptions included, so compare the DDL rather than only the row count.

```sql
SELECT
  (SELECT ddl FROM `<curation_project>.<output_dataset>.INFORMATION_SCHEMA.TABLES`
   WHERE table_name = 'device_and_source')
  = REPLACE(
      (SELECT ddl FROM `<source_project>.<source_dataset>.INFORMATION_SCHEMA.TABLES`
       WHERE table_name = 'device_and_source'),
      '<source_project>.<source_dataset>',
      '<curation_project>.<output_dataset>')
  AS ddl_identical;  -- expect true
```

## Run log, 2026-08-19

All 8 acceptance criteria met.

Preflight reproduced the 2026-08-18 measurements exactly, so the inputs had not moved. The view held 859,402 rows with both ID columns unique and no nulls. The source had five tables, no partitioning or clustering, no ARRAY or STRUCT columns, and `person_id` INT64 throughout. On keying, 8,947 of 8,984 participants matched `controlled_tier_id` and none matched the other two tiers. Coverage came to 20,634 participants in, 20,486 resolved and 148 unresolved, or 0.717 percent, under the 1 percent gate; the 148 are recorded in `dl2458_unresolved_ct_ids`.

Every table landed on the drop count that step 3c predicted before the build ran.

| Table | Source | Output | Dropped |
|---|---:|---:|---:|
| `physical_measurements` | 1,718,619 | 1,718,129 | 490 |
| `activity_intraday` | 769,254 | 768,266 | 988 |
| `vital_measurements` | 1,102,956,914 | 1,101,907,979 | 1,048,935 |
| `activity_summary` | 2,982,662,667 | 2,978,593,543 | 4,069,124 |
| `device_and_source` | 1,091,081 | 1,091,081 | 0, copied |

AC 3, AC 4 and AC 5 all returned 0 on all four converted tables, and AC 6 held by DDL comparison. Step 4d took 274.7 seconds and 357.9 GB. Step 5e on `activity_summary` took 415.8 seconds and 667.7 GB without exhausting resources. The run scanned about 2.1 TB in total, billed to `<job_project>` because that is where `bq` created the jobs.

Two things were accepted rather than fixed: the four converted tables carry no column descriptions, and CT and CT+ rows remain alignable on the untouched row identifiers.
