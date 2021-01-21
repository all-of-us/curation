# +
import pandas as pd

pd.options.display.max_rows = 1000
pd.options.display.max_colwidth = 100
# -

# identifies the google cloud project
project_id = ''
# identifies the BigQuery dataset where vocabulary is loaded
dataset_id = ''
# identifies the BigQuery dataset where RDR export is loaded
rdr_dataset = ''

# # Class of PPI Concepts
# Concept codes which appear in `observation.observation_source_value` should belong to concept class Question.
# Concept codes which appear in `observation.value_source_value` should belong to concept class Answer. Discreprancies (listed below) can be caused by misclassified entries in Athena or invalid payloads in the RDR and in further upstream data sources.

query = f'''
WITH ppi_concept_code AS (
 SELECT 
   observation_source_value AS code
  ,'Question'               AS expected_concept_class_id
  ,COUNT(1) n
 FROM {project_id}.{rdr_dataset}.observation
 GROUP BY 1, 2
 
 UNION ALL
 
 SELECT DISTINCT 
   value_source_value AS code
  ,'Answer'           AS expected_concept_class_id 
  ,COUNT(1) n
 FROM {project_id}.{rdr_dataset}.observation
 GROUP BY 1, 2
)

SELECT 
  code
 ,expected_concept_class_id
 ,concept_class_id
 ,n
FROM ppi_concept_code
JOIN {project_id}.{dataset_id}.concept
 ON LOWER(concept_code)=LOWER(code)
WHERE LOWER(concept_class_id)<>LOWER(expected_concept_class_id)
ORDER BY 1, 2, 3
'''
pd.read_gbq(query, dialect='standard')
