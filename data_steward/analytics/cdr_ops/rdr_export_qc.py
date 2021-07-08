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
old_rdr = ""
new_rdr = ""
# -

# # QC for RDR Export
#
# Quality checks performed on a new RDR dataset and comparison with previous RDR dataset.

# +
import pandas as pd

pd.options.display.max_rows = 120

# -

# # Table comparison
# The export should generally contain the same tables from month to month. Tables found only in the old or the new export are listed below.

query = f'''
SELECT
  COALESCE(curr.table_id, prev.table_id) AS table_id
 ,curr.row_count AS _{new_rdr}
 ,prev.row_count AS _{old_rdr}
 ,(curr.row_count - prev.row_count) AS row_diff
FROM `{project_id}.{new_rdr}.__TABLES__` curr
FULL OUTER JOIN `{project_id}.{old_rdr}.__TABLES__` prev
  USING (table_id)
WHERE curr.table_id IS NULL OR prev.table_id IS NULL
'''
pd.read_gbq(query, dialect='standard')

# ## Row count comparison
# Generally the row count of clinical tables should increase from one export to the next.

query = f'''
SELECT
  curr.table_id AS table_id
 ,prev.row_count AS _{old_rdr}
 ,curr.row_count AS _{new_rdr}
 ,(curr.row_count - prev.row_count) row_diff
FROM `{project_id}.{new_rdr}.__TABLES__` curr
JOIN `{project_id}.{old_rdr}.__TABLES__` prev
  USING (table_id)
ORDER BY ABS(curr.row_count - prev.row_count) DESC;
'''
pd.read_gbq(query, dialect='standard')

# ## ID range check
# Combine step may break if any row IDs in the RDR are larger than the added constant(1,000,000,000,000,000).
# Rows that are greater than 999,999,999,999,999 the will be listed out here.

domain_table_list = [
    'condition_occurrence', 'device_exposure', 'drug_exposure', 'location',
    'measurement', 'note', 'observation', 'procedure_occurrence', 'provider',
    'specimen', 'visit_occurrence'
]
queries = []
for table in domain_table_list:
    query = f'''
    SELECT
        '{table}' AS domain_table_name,
        {table}_id AS domain_table_id
    FROM
     `{project_id}.{new_rdr}.{table}`
    WHERE
      {table}_id > 999999999999999
    '''
    queries.append(query)
pd.read_gbq('\nUNION ALL\n'.join(queries), dialect='standard')

# ## Concept codes used
# Identify question and answer concept codes which were either added or removed (appear in only the new or only the old RDR datasets, respectively).

query = f'''
WITH curr_code AS (
SELECT
  observation_source_value value
 ,'observation_source_value' field
 ,COUNT(1) row_count
FROM `{project_id}.{new_rdr}.observation` GROUP BY 1

UNION ALL

SELECT
  value_source_value value
 ,'value_source_value' field
 ,COUNT(1) row_count
FROM `{project_id}.{new_rdr}.observation` GROUP BY 1),

prev_code AS (
SELECT
  observation_source_value value
 ,'observation_source_value' field
 ,COUNT(1) row_count
FROM `{project_id}.{old_rdr}.observation` GROUP BY 1

UNION ALL

SELECT
  value_source_value value
 ,'value_source_value' field
 ,COUNT(1) row_count
FROM `{project_id}.{old_rdr}.observation` GROUP BY 1)

SELECT
  prev_code.value prev_code_value
 ,prev_code.field prev_code_field
 ,prev_code.row_count prev_code_row_count
 ,curr_code.value curr_code_value
 ,curr_code.field curr_code_field
 ,curr_code.row_count curr_code_row_count
FROM curr_code
 FULL OUTER JOIN prev_code
  USING (field, value)
WHERE prev_code.value IS NULL OR curr_code.value IS NULL
'''
pd.read_gbq(query, dialect='standard')

# # Question codes should have mapped `concept_id`s
# Question codes in `observation_source_value` should be associated with the concept identified by `observation_source_concept_id` and mapped to a standard concept identified by `observation_concept_id`. The table below lists codes having rows where either field is null or zero and the number of rows where this occurs. This may be associated with an issue in the PPI vocabulary or in the RDR ETL process.
#
# Note: Snap codes are not modeled in the vocabulary but may be used in the RDR export. 
# They are excluded here by filtering out snap codes in the Public PPI Codebook 
# which were loaded into `curation_sandbox.snap_codes`.

query = f"""
SELECT
  observation_source_value
 ,COUNTIF(observation_source_concept_id IS NULL) AS source_concept_id_null
 ,COUNTIF(observation_source_concept_id=0)       AS source_concept_id_zero
 ,COUNTIF(observation_concept_id IS NULL)        AS concept_id_null
 ,COUNTIF(observation_concept_id=0)              AS concept_id_zero
FROM `{project_id}.{new_rdr}.observation`
WHERE observation_source_value IS NOT NULL
AND observation_source_value != ''
AND observation_source_value NOT IN (SELECT concept_code FROM `{project_id}.curation_sandbox.snap_codes`)
GROUP BY 1
HAVING source_concept_id_null + source_concept_id_zero + concept_id_null + concept_id_zero > 0
ORDER BY 2 DESC, 3 DESC, 4 DESC, 5 DESC
"""
pd.read_gbq(query, dialect='standard')

# # Answer codes should have mapped `concept_id`s
# Answer codes in value_source_value should be associated with the concept identified by value_source_concept_id and mapped to a standard concept identified by value_as_concept_id. The table below lists codes having rows where either field is null or zero and the number of rows where this occurs. This may be associated with an issue in the PPI vocabulary or in the RDR ETL process.
#
# Note: Snap codes are not modeled in the vocabulary but may be used in the RDR export. 
# They are excluded here by filtering out snap codes in the Public PPI Codebook 
# which were loaded into `curation_sandbox.snap_codes`.
#

query = f"""
SELECT
  value_source_value
 ,COUNTIF(value_source_concept_id IS NULL) AS source_concept_id_null
 ,COUNTIF(value_source_concept_id=0)       AS source_concept_id_zero
 ,COUNTIF(value_as_concept_id IS NULL)     AS concept_id_null
 ,COUNTIF(value_as_concept_id=0)           AS concept_id_zero
FROM `{project_id}.{new_rdr}.observation`
WHERE value_source_value IS NOT NULL
AND value_source_value != ''
AND value_source_value NOT IN (SELECT concept_code FROM `{project_id}.curation_sandbox.snap_codes`)
GROUP BY 1
HAVING source_concept_id_null + source_concept_id_zero + concept_id_null + concept_id_zero > 0
ORDER BY 2 DESC, 3 DESC, 4 DESC, 5 DESC
"""
pd.read_gbq(query, dialect='standard')

# # Dates are equal in observation_date and observation_datetime
# Any mismatches are listed below.

query = f"""
SELECT
  observation_id
 ,person_id
 ,observation_date
 ,observation_datetime
FROM `{project_id}.{new_rdr}.observation`
WHERE observation_date != EXTRACT(DATE FROM observation_datetime)
"""
pd.read_gbq(query, dialect='standard')

# # Check for duplicates

query = f"""
with duplicates AS (
    SELECT
      person_id
     ,observation_datetime
     ,observation_source_value
     ,value_source_value
     ,value_as_number
     ,value_as_string
   --,questionnaire_response_id
     ,COUNT(1) AS n_data
    FROM `{project_id}.{new_rdr}.observation`
    INNER JOIN `{project_id}.{new_rdr}.cope_survey_semantic_version_map` USING (questionnaire_response_id) -- For COPE only
    GROUP BY 1,2,3,4,5,6
)
SELECT
  n_data   AS duplicates
 ,COUNT(1) AS n_duplicates
FROM duplicates
WHERE n_data > 1
GROUP BY 1
ORDER BY 2 DESC
"""
pd.read_gbq(query, dialect='standard')

# # Check if numeric data in value_as_string
# Some numeric data is expected in value_as_string.  For example, zip codes or other contact specific information.

query = f"""
SELECT
  observation_source_value
 ,COUNT(1) AS n
FROM `{project_id}.{new_rdr}.observation`
WHERE SAFE_CAST(value_as_string AS INT64) IS NOT NULL
GROUP BY 1
ORDER BY 2 DESC
"""
pd.read_gbq(query, dialect='standard')

# # All COPE `questionnaire_response_id`s are in COPE version map
# Any `questionnaire_response_id`s missing from the map will be listed below.

query = f"""
SELECT
  observation_id
 ,person_id
 ,questionnaire_response_id
FROM `{project_id}.{new_rdr}.observation`
 INNER JOIN `{project_id}.pipeline_tables.cope_concepts`
  ON observation_source_value = concept_code
WHERE questionnaire_response_id NOT IN
(SELECT questionnaire_response_id FROM `{project_id}.{new_rdr}.cope_survey_semantic_version_map`)
"""
pd.read_gbq(query, dialect='standard')

# # No duplicate `questionnaire_response_id`s in COPE version map
# Any duplicated `questionnaire_response_id`s will be listed below.

query = f"""
SELECT
  questionnaire_response_id
 ,COUNT(*) n
FROM `{project_id}.{new_rdr}.cope_survey_semantic_version_map`
GROUP BY questionnaire_response_id
HAVING n > 1
"""
pd.read_gbq(query, dialect='standard')

# # Survey version and dates
# Cope survey versions and the minimum and maximum dates associated with those surveys are listed.
# Dates should roughly line up with survey opening and closing dates.

query = f"""
SELECT
  cope_month
 ,MIN(observation_date) AS min_date
 ,MAX(observation_date) AS max_date
FROM `{project_id}.{new_rdr}.observation`
JOIN `{project_id}.{new_rdr}.cope_survey_semantic_version_map` USING (questionnaire_response_id)
GROUP BY 1
"""
pd.read_gbq(query, dialect='standard')

# # Class of PPI Concepts using vocabulary.py
# Concept codes which appear in `observation.observation_source_value` should belong to concept class Question.
# Concept codes which appear in `observation.value_source_value` should belong to concept class Answer.
# Concepts of class Qualifier Value are permitted as a value & Concepts of class Topic and PPI Modifier are permitted as a question
# Discreprancies (listed below) can be caused by misclassified entries in Athena or invalid payloads in the RDR and in further upstream data sources.

query = f'''
WITH ppi_concept_code AS (
 SELECT
   observation_source_value AS code
  ,'Question'               AS expected_concept_class_id
  ,COUNT(1) n
 FROM `{project_id}.{new_rdr}.observation`
 GROUP BY 1, 2

 UNION ALL

 SELECT DISTINCT
   value_source_value AS code
  ,'Answer'           AS expected_concept_class_id
  ,COUNT(1) n
 FROM `{project_id}.{new_rdr}.observation`
 GROUP BY 1, 2
)
SELECT
  code
 ,expected_concept_class_id
 ,concept_class_id
 ,n
FROM ppi_concept_code
JOIN `{project_id}.{new_rdr}.concept`
 ON LOWER(concept_code)=LOWER(code)
WHERE LOWER(concept_class_id)<>LOWER(expected_concept_class_id)
AND CASE WHEN expected_concept_class_id = 'Question' THEN concept_class_id NOT IN('Topic','PPI Modifier') END
AND concept_class_id != 'Qualifier Value'
ORDER BY 1, 2, 3
'''
pd.read_gbq(query, dialect='standard')

# # Make sure smoking data still exists
# Make sure that the cleaning rule clash that previously wiped out all numeric smoking data is corrected.
# If any of the counts come back as zero, there is a problem and it should be fixed.  Zero counts in the
# obs_src_count column when running on an rdr import indicate problems with the RDR ETL and will require
# cross team coordination.  Zero counts in the obs_src_count column when running on a cleaned rdr import
# indicate problems with cleaning rules that should be remediated by curation.

query = f'''
SELECT
    observation_source_value
    ,observation_source_concept_id
    ,count(*) AS obs_src_count
FROM `{project_id}.{new_rdr}.observation`
WHERE observation_source_concept_id IN (1585864, 1585870,1585873, 1586159,1586162)
    AND value_as_number IS NOT NULL
GROUP BY observation_source_concept_id, observation_source_value
ORDER BY observation_source_concept_id
'''
pd.read_gbq(query, dialect='standard')

# # Date conformance check
# COPE surveys contain some concepts that must enforce dates in the observation.value_as_string field.
# For the observation_source_concept_id = 715711, if the value in value_as_string does not meet a standard date format
# of YYYY-mm-dd, return a dataframe with the observation_id and person_id
# Curation needs to contact the RDR team about data discrepancies

query = f'''
SELECT
    observation_id
    ,person_id
    ,value_as_string
FROM `{project_id}.{new_rdr}.observation` 
WHERE observation_source_concept_id = 715711
AND SAFE_CAST(value_as_string AS DATE) IS NULL 
AND value_as_string != 'PMI Skip'
'''
pd.read_gbq(query, dialect='standard')

# # Check pid_rid_mapping table for duplicates
# Duplicates are not allowed in the person_id or research_id columns of the pid_rid_mapping table.
# If found, there is a problem with the RDR import. An RDR On-Call ticket should be opened
# to report the problem. In ideal circumstances, this query will not return any results.
# If a result set is returned, an error has been found for the identified field.
# If the table was not imported, the filename changed, or field names changed,
# this query will fail by design to indicate an unexpected change has occurred.

query = f'''
SELECT
    'person_id' as id_type
    ,person_id as id
    ,COUNT(person_id) as count
FROM `{project_id}.{new_rdr}.pid_rid_mapping`
GROUP BY person_id
HAVING count > 1

UNION ALL

SELECT
    'research_id' as id_type
    ,research_id as id
    ,COUNT(research_id) as count
FROM `{project_id}.{new_rdr}.pid_rid_mapping`
GROUP BY research_id
HAVING count > 1
'''
pd.read_gbq(query, dialect='standard')

# # Ensure all person_ids exist in the person table and have mappings
# All person_ids in the pid_rid_mapping table should exist in the person table.
# If the person record does not exist for a mapping record, there is a problem with the RDR import.
# An RDR On-Call ticket should be opened to report the problem.
# All person_ids in the person table should have a mapping in the pid_rid_mapping table.
# If any person_ids do not have a mapping record, there is a problem with the RDR import.
# An RDR On-Call ticket should be opened to report the problem.
# In ideal circumstances, this query will not return any results.

query = f'''
SELECT
    'missing_person' as issue_type
    ,person_id
FROM `{project_id}.{new_rdr}.pid_rid_mapping`
WHERE person_id NOT IN 
(SELECT person_id
FROM `{project_id}.{new_rdr}.person`)

UNION ALL

SELECT 
    'unmapped_person' as issue_type
    ,person_id
FROM `{project_id}.{new_rdr}.person`
WHERE person_id NOT IN 
(SELECT person_id
FROM `{project_id}.{new_rdr}.pid_rid_mapping`)
'''
pd.read_gbq(query, dialect='standard')

# # Check for inconsistencies between primary and RDR pid_rid_mappings
# Mappings which were in previous exports may be removed from a new export for two reasons:
#   1. Participants have withdrawn or
#   2. They were identified as test or dummy data
# Missing mappings from the previous RDR export are therefore not a significant cause for concern.
# However, mappings in the RDR pid_rid_mapping should always be consistent with the
# primary_pid_rid_mapping in pipeline_tables for existing mappings.
# If the same pid has different rids in the pid_rid_mapping and the primary_pid_rid_mapping,
# there is a problem with the RDR import. An RDR On-Call ticket should be opened to report the problem.
# In ideal circumstances, this query will not return any results.

query = f'''
SELECT
    person_id
FROM `{project_id}.{new_rdr}.pid_rid_mapping` rdr
JOIN `{project_id}.pipeline_tables.primary_pid_rid_mapping` primary
USING (person_id)
WHERE primary.research_id <> rdr.research_id
'''
pd.read_gbq(query, dialect='standard')

# Checks for basics survey module
# Participants with data in other survey modules must also have data from the basics survey module.
# This check identifies survey responses associated with participants that do not have any responses
# associated with the basics survey module.
# In ideal circumstances, this query will not return any results.

query = f'''SELECT DISTINCT person_id FROM `{project_id}.{new_rdr}.observation` 
JOIN `{project_id}.{new_rdr}.concept` on (observation_source_concept_id=concept_id)
WHERE vocabulary_id = 'PPI' AND person_id NOT IN (
SELECT DISTINCT person_id FROM `{project_id}.{new_rdr}.rdr20191203.concept`  
JOIN `{project_id}.{new_rdr}.concept_ancestor` on (concept_id=ancestor_concept_id)
JOIN `{project_id}.{new_rdr}.observation` on (descendant_concept_id=observation_concept_id)
WHERE concept_class_id='Module'
AND concept_name IN ('The Basics')
AND questionnaire_response_id IS NOT NULL)
'''
pd.read_gbq(query, dialect='standard')

# ## Participants must be 18 years of age or older to consent 
#
# AOU participants are required to be 18+ years of age at the time of consent ([DC-1724](https://precisionmedicineinitiative.atlassian.net/browse/DC-1724)), based on the date associated with the [ExtraConsent_TodaysDate](https://athena.ohdsi.org/search-terms/terms/1585482) row. Any violations should be reported to the RDR team as these should have been filtered out by the RDR ETL process ([DA-2073](https://precisionmedicineinitiative.atlassian.net/browse/DA-2073)).

query = f'''
SELECT *
FROM `{project_id}.{new_rdr}.observation`
JOIN `{project_id}.{new_rdr}.person` USING (person_id)
WHERE  (observation_source_concept_id=1585482 OR observation_concept_id=1585482)
AND DATE_DIFF(DATE(observation_date), DATE(birth_datetime), YEAR) < 18
'''
pd.read_gbq(query, dialect='standard')
