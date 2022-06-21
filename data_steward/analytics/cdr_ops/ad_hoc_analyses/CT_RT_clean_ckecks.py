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

# + tags=["parameters"]
project_id = ""
clean_dataset = ""
sandbox_dataset = ""
run_as = ""
# -

# # QC for RT Clean Dataset
#
# Quality checks performed on a new RT clean dataset.

# +
import pandas as pd

from analytics.cdr_ops.notebook_utils import execute
from utils import auth
from gcloud.bq import BigQueryClient
from common import JINJA_ENV

pd.options.display.max_rows = 120

# -

SCOPES = [
    'https://www.googleapis.com/auth/bigquery',
]
impersonation_creds = auth.get_impersonation_credentials(run_as, SCOPES)

client = BigQueryClient(project_id, credentials=impersonation_creds)

# # Verify measurement records are dropped
# Set all value_as_number = 9999999 to NULL.  If not null, an error happened.

query_tmpl = JINJA_ENV.from_string("""
SELECT 
 COUNTIF(value_as_number = 9999999) as n_not_changed,
 CASE WHEN COUNTIF(value_as_number = 9999999) > 0 THEN 'FAILURE'
  ELSE 'SUCCESSFUL'
 END as check_passed
FROM `{{project_id}}.{{clean_dataset}}.measurement`
""")

execute(client,
        query_tmpl.render(project_id=project_id, clean_dataset=clean_dataset))

# All measurements that don't provide meaningful data should be dropped.

query_tmpl = JINJA_ENV.from_string("""
SELECT
 COUNTIF(value_as_number is null AND value_as_concept_id is null) as n_not_dropped,
 CASE WHEN COUNTIF(value_as_number is null AND value_as_concept_id is null) > 0 THEN 'FAILURE'
  ELSE 'SUCCESSFUL'
 END as check_passed
FROM `{{project_id}}.{{clean_dataset}}.measurement`
""")

execute(client,
        query_tmpl.render(project_id=project_id, clean_dataset=clean_dataset))

# All measurement records for the following sites should be dropped.  This happens if the site only
# submits 0 in value_as_number for all their records.  IF a site is singled out, use the CTE query
# to determine the offending values.

query_tmpl = JINJA_ENV.from_string("""
with should_be_dropped as (
    SELECT 
    COUNTIF(value_as_number <> 0) as n_not_equal_zero,
    COUNTIF(value_as_number > 0) as n_greater_than_zero,
    COUNTIF(value_as_number = 0) as n_equal_zero,
    COUNTIF(value_as_number is null) as n_null,
    CASE WHEN
        SUM(CASE WHEN value_as_number = 0  THEN 1 ELSE 0 END) > 0 AND 
        SUM(CASE WHEN value_as_number <> 0 OR value_as_number is null THEN 1 ELSE 0 END) = 0  THEN 'DROP'
        ELSE 'include'
    END AS drop_or_include_site,
    me.src_id
    FROM `{{project_id}}.{{clean_dataset}}.measurement` as m
    LEFT JOIN `{{project_id}}.{{clean_dataset}}.measurement_ext` as me
    USING (measurement_id)
    GROUP BY me.src_id
)
SELECT src_id
FROM should_be_dropped
WHERE drop_or_include_site = 'DROP'
""")

execute(client,
        query_tmpl.render(project_id=project_id, clean_dataset=clean_dataset))

# Make sure data for a site is dropped if the site's data is flagged as
# needing to be dropped.  SUCCESS/FAILURE is indicated.

query_tmpl = JINJA_ENV.from_string("""
with should_be_dropped as (
    SELECT 
    COUNTIF(value_as_number <> 0) as n_not_equal_zero,
    COUNTIF(value_as_number > 0) as n_greater_than_zero,
    COUNTIF(value_as_number = 0) as n_equal_zero,
    COUNTIF(value_as_number is null) as n_null,
    CASE WHEN
        SUM(CASE WHEN value_as_number = 0  THEN 1 ELSE 0 END) > 0 AND 
        SUM(CASE WHEN value_as_number <> 0 OR value_as_number is null THEN 1 ELSE 0 END) = 0  THEN 'DROP'
        ELSE 'include'
    END AS drop_or_include_site,
    me.src_id
    FROM `{{project_id}}.{{clean_dataset}}.measurement` as m
    LEFT JOIN `{{project_id}}.{{clean_dataset}}.measurement_ext` as me
    USING (measurement_id)
    GROUP BY me.src_id
)
, dropped_srcs as (
SELECT src_id
FROM should_be_dropped
WHERE drop_or_include_site = 'DROP'
)

SELECT count(*) as n_that_should_have_been_dropped,
CASE
  WHEN count(*) > 0 THEN 'FAILURE'
  ELSE 'SUCCESS'
END as status
FROM `{{project_id}}.{{clean_dataset}}.measurement` as m
LEFT JOIN `{{project_id}}.{{clean_dataset}}.measurement_ext` as me
USING (measurement_id)
WHERE src_id in (
  SELECT src_id
  FROM dropped_srcs
)
""")

execute(client,
        query_tmpl.render(project_id=project_id, clean_dataset=clean_dataset))

# There should not be any duplicate rows based on the person_id, measurement_source_concept_id,
# unit_concept_id, measurement_concept_id, measurement_datetime, value_as_number, and
# value_as_concept_id fields.  All such duplicates should be dropped.  If this query
# returns any values, there is an error with MeasurementRecordsSuppression.

query_tmpl = JINJA_ENV.from_string("""
Select A.* from (
SELECT 
  ROW_NUMBER() OVER (PARTITION BY person_id, measurement_source_concept_id, unit_concept_id,  measurement_concept_id, measurement_datetime, CAST(value_as_number as string), value_as_concept_id) AS rownum
FROM `{{project_id}}.{{clean_dataset}}.measurement`
group by person_id, measurement_source_concept_id, unit_concept_id,  measurement_concept_id, measurement_datetime, value_as_number,value_as_concept_id)A
where A.rownum>1
order by A.rownum desc
""")

execute(client,
        query_tmpl.render(project_id=project_id, clean_dataset=clean_dataset))

# # Records with 0 or null in specific *_concept_id fields should be dropped
# Identifies if any records are not dropped based on the criteria in the doc string
# of the drop_zero_concept_ids.py module.

query_tmpl = JINJA_ENV.from_string("""
with should_be_dropped as (
    SELECT
     COUNTIF((condition_source_concept_id is null or condition_source_concept_id = 0) AND
             (condition_concept_id is null or condition_concept_id = 0)) as n_records_to_drop,
     'condition_occurrence' as table_to_drop_from
    FROM `{{project_id}}.{{clean_dataset}}.condition_occurrence`

    UNION ALL

    SELECT
     COUNTIF((procedure_source_concept_id is null or procedure_source_concept_id = 0) AND
             (procedure_concept_id is null or procedure_concept_id = 0)) as n_records_to_drop,
     'procedure_occurrence' as table_to_drop_from
    FROM `{{project_id}}.{{clean_dataset}}.procedure_occurrence`

    UNION ALL

    SELECT
     COUNTIF((visit_source_concept_id is null or visit_source_concept_id = 0) AND
             (visit_concept_id is null or visit_concept_id = 0)) as n_records_to_drop,
     'visit_occurrence' as table_to_drop_from
    FROM `{{project_id}}.{{clean_dataset}}.visit_occurrence`

    UNION ALL

    SELECT
     COUNTIF((drug_source_concept_id is null or drug_source_concept_id = 0) AND
             (drug_concept_id is null or drug_concept_id = 0)) as n_records_to_drop,
     'drug_exposure' as table_to_drop_from
    FROM `{{project_id}}.{{clean_dataset}}.drug_exposure`

    UNION ALL

    SELECT
     COUNTIF((device_source_concept_id is null or device_source_concept_id = 0) AND
             (device_concept_id is null or device_concept_id = 0)) as n_records_to_drop,
     'device_exposure' as table_to_drop_from
    FROM `{{project_id}}.{{clean_dataset}}.device_exposure`

    UNION ALL

    SELECT
     COUNTIF((observation_source_concept_id is null or observation_source_concept_id = 0) AND
             (observation_concept_id is null or observation_concept_id = 0)) as n_records_to_drop,
     'observation' as table_to_drop_from
    FROM `{{project_id}}.{{clean_dataset}}.observation`

    UNION ALL

    SELECT
     COUNTIF((measurement_source_concept_id is null or measurement_source_concept_id = 0) AND
             (measurement_concept_id is null or measurement_concept_id = 0)) as n_records_to_drop,
     'measurement' as table_to_drop_from
    FROM `{{project_id}}.{{clean_dataset}}.measurement`
)
SELECT coalesce(n_records_to_drop, 0) as n_violations,
 coalesce(table_to_drop_from, 'NO VIOLATORS') as violating_table
FROM should_be_dropped
WHERE n_records_to_drop > 0
""")

execute(client,
        query_tmpl.render(project_id=project_id, clean_dataset=clean_dataset))

# # Make sure specific units have been converted
# This attempts to make sure unit_concept_id's have been converted as specified in the _unit_mapping
# table by ensuring the measurement_concept_id/unit_concept_id pairs do not exist in the cleaned
# dataset.  It does not attempt to check the performed calculation.  It just makes sure the
# values do not exist any longer as evidence that the rule ran.  If values are returned, curation
# should investigate.

query_tmpl = JINJA_ENV.from_string("""
with remove_unit_concept_ids as (
  select measurement_concept_id, unit_concept_id
  from `{{project_id}.{{sandbox_dataset}}._unit_mapping`
  where unit_concept_id not in (select distinct set_unit_concept_id from `{project_id}.{sandbox_dataset}._unit_mapping`)
)
select m.measurement_concept_id, m.unit_concept_id, count(*) as n_should_be_removed
from `{{project_id}}.{{clean_dataset}}.measurement` as m
left join remove_unit_concept_ids as ruci
using (measurement_concept_id)
where m.unit_concept_id = ruci.unit_concept_id
group by 1, 2
""")

execute(
    client,
    query_tmpl.render(project_id=project_id,
                      sandbox_dataset=sandbox_dataset,
                      clean_dataset=clean_dataset))
#

# # Analyze Changes in Record Counts
# If the record counts change by more than 15%, flag the table for a QC investigation.
# The rules may be behaving correctly and the threshold may need to be adjusted.  This is
# just a baseline query to identify potential quality concerns.

query_tmpl = JINJA_ENV.from_string("""
SELECT 
  table_id, 
  base.row_count AS base_row_count, 
  clean.row_count AS clean_row_count,
  CASE
    WHEN base.row_count = clean.row_count THEN 'unchanged'
    WHEN clean.row_count = 0 and base.row_count > 0 THEN "PROBABLE FAILURE"
    WHEN base.row_count = 0 THEN "CANNOT DIVIDE BY ZERO"
    WHEN ((base.row_count - clean.row_count) / base.row_count) * 100 > 40 THEN "PROBABLE FAILURE"
    WHEN ((base.row_count - clean.row_count) / base.row_count) * 100 > 15 THEN "POSSIBLE FAILURE"
  ELSE "PROBABLE SUCCESS"
  END
  AS analysis_results
FROM `{{project_id}}.{{clean_dataset}}.__TABLES__` AS clean
LEFT JOIN `{{project_id}}.{{clean_dataset[:-5]}}base.__TABLES__` AS base
USING (table_id)
""")

execute(client,
        query_tmpl.render(project_id=project_id, clean_dataset=clean_dataset))
