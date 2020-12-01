# # QC for RDR Export
#
# Quality checks performed on a new RDR dataset and comparison with previous RDR dataset.
# ## Parameters
# This notebook contains parameters which must be set via the URL. 
#
# For example:
#
# `https://my_notebooks/../cdr_ops/rdr_export_qc.py?project_id=my_project&old_rdr=rdr1&new_rdr=rdr2`
#
# will set variables like these which are used to construct SQL queries:
#
#     project_id = "my_project"
#     old_rdr = "rdr1"
#     new_rdr = "rdr2"

# +
import urllib
import pandas as pd

NOTEBOOK_URL = None

# Parameters required by this notebook
REQUIRED_PARAMS = [
    # identifies the google cloud project
    'project_id',
    # dataset_id of a prior RDR export for comparison
    'old_rdr',
    # dataset_id of the RDR export being evaluated
    'new_rdr'
]
pd.options.display.max_rows = 120


# -

# ## Parse URL parameters
# Retrieve parameters from the query string of the notebook URL. This allows sensitive
# analytical parameters (e.g. project_id, dataset_id) to be specified ad hoc without
# storing them in the notebook, which must be version controlled.
#
# **This approach to parameterizing notebooks has some major limitations and may be replaced in
# the future.**
# 1. The user must remember to run the following cells every time the query string is changed
# 2. It may not be possible to specify the parameters this way when executing the notebook
# programmatically (i.e. for automation)

# + language="javascript"
# // conditional needed to prevent warning
# if (IPython.notebook.kernel) {
#     // set the Python variable NOTEBOOK_URL
#     IPython.notebook.kernel.execute("NOTEBOOK_URL = '" + window.location + "'");
# }

# +
def get_url_params(url):
    """
    Extract params from the query string of a URL
    
    :returns: dict where the keys are parameter names
    """
    split_url = urllib.parse.urlsplit(url)
    params = urllib.parse.parse_qs(split_url.query)
    for key, value in params.items():
        if len(value) == 1:
            params[key] = value[0]
    return params


def validate_params(params, required_params):
    """
    Raise an error if a required parameter is missing from the params dict
    """
    for required_param in required_params:
        if required_param not in params:
            raise ValueError(f'Missing query parameter `{required_param}`')
    global project_id, old_rdr, new_rdr, cope_version_map
    project_id = params.get('project_id')
    old_rdr = params.get('old_rdr')
    new_rdr = params.get('new_rdr')
    print(f'All required parameters are specified.\n{params}')


# -

params = get_url_params(NOTEBOOK_URL)
validate_params(params, REQUIRED_PARAMS)

# # Missing tables

query = f'''
SELECT 
 COALESCE(curr.table_id, prev.table_id) AS table_id, 
 curr.row_count AS {new_rdr}, 
 prev.row_count AS {old_rdr}, 
 (curr.row_count - prev.row_count) row_diff 
FROM {project_id}.{new_rdr}.__TABLES__ curr 
FULL OUTER JOIN {old_rdr}.__TABLES__ prev
  USING (table_id)
WHERE curr.table_id IS NULL OR prev.table_id IS NULL
'''
pd.read_gbq(query, dialect='standard')

# ## Row count comparison

query = f'''
SELECT 
 curr.table_id AS table_id, 
 curr.row_count AS {new_rdr}, 
 prev.row_count AS {old_rdr}, 
 (curr.row_count - prev.row_count) row_diff 
FROM {project_id}.{new_rdr}.__TABLES__ curr 
JOIN {project_id}.{old_rdr}.__TABLES__ prev
  USING (table_id)
ORDER BY ABS(curr.row_count - prev.row_count) DESC;
'''
pd.read_gbq(query, dialect='standard')

# ## Concept codes used
# Identify question and answer concept codes which were either added or removed (appear in only the new or only the old RDR datasets, respectively).

query = f'''
WITH curr_code AS (
SELECT observation_source_value value, 
 'observation_source_value' field, 
 COUNT(1) row_count 
FROM {project_id}.{new_rdr}.observation GROUP BY 1

UNION ALL

SELECT value_source_value value, 
 'value_source_value' field, 
 COUNT(1) row_count 
FROM {project_id}.{new_rdr}.observation GROUP BY 1),

prev_code AS (
SELECT observation_source_value value,
 'observation_source_value' field, 
 COUNT(1) row_count 
FROM {project_id}.{old_rdr}.observation GROUP BY 1

UNION ALL

SELECT value_source_value value, 
 'value_source_value' field, 
 COUNT(1) row_count 
FROM {project_id}.{old_rdr}.observation GROUP BY 1)

SELECT 
  prev_code.value prev_code_value, 
  prev_code.field prev_code_field, 
  prev_code.row_count prev_code_row_count, 
  curr_code.value curr_code_value, 
  curr_code.field curr_code_field, 
  curr_code.row_count curr_code_row_count, 
FROM curr_code 
 FULL OUTER JOIN prev_code
  USING (field, value)
WHERE prev_code.value IS NULL OR curr_code.value IS NULL
'''
pd.read_gbq(query, dialect='standard')

# # Check if observation_source_value vs concept ids

query = f"""
SELECT 
    SUM(CASE WHEN observation_source_concept_id IS NULL THEN 1 ELSE 0 END) as n_null_source_concept_id
    , SUM(CASE WHEN observation_source_concept_id=0 THEN 1 ELSE 0 END) as n_zero_source_concept_id
FROM `{project_id}.{new_rdr}.observation`
WHERE observation_source_value IS NOT NULL
and observation_source_value != ''
"""
pd.read_gbq(query,
            dialect='standard')

query = f"""
SELECT 
    SUM(CASE WHEN observation_concept_id IS NULL THEN 1 ELSE 0 END) as n_null_concept_id
    , SUM(CASE WHEN observation_concept_id=0 THEN 1 ELSE 0 END) as n_zero_concept_id
FROM `{project_id}.{new_rdr}.observation`
WHERE observation_source_value IS NOT NULL
and observation_source_value != ''
"""
pd.read_gbq(query,
            dialect='standard')

# # Check value_source_value unmapped to source

query = f"""
SELECT 
    SUM(CASE WHEN value_source_concept_id IS NULL THEN 1 ELSE 0 END) as n_null_source_concept_id
    , SUM(CASE WHEN value_source_concept_id=0 THEN 1 ELSE 0 END) as n_zero_source_concept_id
FROM `{project_id}.{new_rdr}.observation`
WHERE value_source_value IS NOT NULL
and value_source_value != ''
"""
pd.read_gbq(query,
            dialect='standard')

# # Check value_source_value unmapped to standard

query = f"""
SELECT 
    SUM(CASE WHEN value_as_concept_id IS NULL THEN 1 ELSE 0 END) as n_null_concept_id
    , SUM(CASE WHEN value_as_concept_id=0 THEN 1 ELSE 0 END) as n_zero_concept_id
FROM `{project_id}.{new_rdr}.observation`
WHERE value_source_value IS NOT NULL
and value_source_value != ''
"""
pd.read_gbq(query,
            dialect='standard')

# # Dates are equal in observation_date and observation_datetime
# Any mismatches are listed below.

query = f"""
SELECT 
    observation_id,
    person_id,
    observation_date,
    observation_datetime
FROM `{project_id}.{new_rdr}.observation`
WHERE observation_date != EXTRACT(DATE FROM observation_datetime)
"""
pd.read_gbq(query,
            dialect='standard')

# # Check for duplicates

query = f"""
with duplicates AS (
    SELECT
        person_id
        , observation_datetime
        , observation_source_value
        , value_source_value
        , value_as_number
        , value_as_string
        --, questionnaire_response_id
        , COUNT(1) as n_data
    FROM `{project_id}.{new_rdr}.observation`
    INNER JOIN `{project_id}.{new_rdr}.cope_survey_semantic_version_map` USING (questionnaire_response_id) -- For COPE only
    GROUP BY 1,2,3,4,5,6
)
SELECT 
    n_data as duplicates
    , COUNT(1) as n_duplicates
FROM duplicates
WHERE n_data > 1
GROUP BY 1
ORDER BY 2 DESC
"""
pd.read_gbq(query, dialect='standard')

# # Check if numeric data in value_as_string

query = f"""
SELECT
    observation_source_value
    , COUNT(1) AS n
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
    observation_id,
    person_id,
    questionnaire_response_id
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
    questionnaire_response_id,
    COUNT(*) n
FROM `{project_id}.{new_rdr}.cope_survey_semantic_version_map` 
GROUP BY questionnaire_response_id
HAVING n > 1
"""
pd.read_gbq(query, dialect='standard')

# # Survey version and dates

query = f"""
SELECT
    cope_month
    , MIN(observation_date) AS min_date
    , MAX(observation_date) AS max_date
FROM `{project_id}.{new_rdr}.observation`
JOIN `{project_id}.{new_rdr}.cope_survey_semantic_version_map` USING (questionnaire_response_id)
GROUP BY 1
"""
pd.read_gbq(query, dialect='standard')
