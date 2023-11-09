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

# # Notebook for YOB retraction on V6 and V7 datasets.

# Parameters
run_as = ""
project_id = ""
old_dataset_id = ""
release_tag = ""
data_stage = ""
data_tier = ""

import logging
from analytics.cdr_ops.notebook_utils import execute, IMPERSONATION_SCOPES, render_message
from common import JINJA_ENV
from utils import auth, pipeline_logging
from gcloud.bq import BigQueryClient
from cdr_cleaner import clean_cdr
from cdr_cleaner.args_parser import add_kwargs_to_args

# +
impersonation_creds = auth.get_impersonation_credentials(
    run_as, target_scopes=IMPERSONATION_SCOPES)

client = BigQueryClient(project_id, credentials=impersonation_creds)
# -

LOGGER = logging.getLogger(__name__)
pipeline_logging.configure(level=logging.INFO, add_console_handler=True)

# ## Identify and remove non-confirming records
# ### All observation concept ids other than 4013886, 4135376, 4271761 with dates similar to birth dates should be removed

query = JINJA_ENV.from_string("""

WITH rows_having_brith_date as (

SELECT observation_id
  FROM `{{project_id}}.{{old_dataset_id}}.observation` ob
JOIN  `{{project_id}}.{{old_dataset_id}}.person` p USING (person_id)
 WHERE (observation_concept_id NOT IN (4013886, 4135376, 4271761) OR observation_concept_id IS NULL)
  AND ABS(EXTRACT(YEAR FROM observation_date)- p.year_of_birth) < 2
  )

SELECT
'observation' AS table_name,
 'observation_date' AS column_name,
 COUNT(*) AS row_counts_failure,
CASE WHEN
  COUNT(*) > 0
  THEN 1 ELSE 0
END
 AS Failure_no_birth_date
FROM `{{project_id}}.{{old_dataset_id}}.observation` ob
WHERE  observation_id IN (SELECT observation_id FROM rows_having_brith_date)
""")
q = query.render(project_id=project_id, old_dataset_id=old_dataset_id)
execute(client, q)

# ## Creating new datasets to apply retraction on

dataset_definition = {
    'clean': {
        'name': f'C{release_tag}_{data_stage}',
        'desc':
            f"Hot fix applied to {old_dataset_id}."
            f"--"
            f"{old_dataset_id}'s description for reference -> Certain records removed from {old_dataset_id} based on the retraction rule",
        'labels': {
            "owner": "curation",
            "phase": "clean",
            "data_tier": "controlled",
            "release_tag": release_tag,
            "de_identified": "true",
            "issue_number": "dc3563"
        }
    },
    'sandbox': {
        'name': f'{release_tag}_{data_stage}_sandbox',
        'desc': (
            f'Sandbox created for storing records affected by the retraction applied to C{release_tag}_{data_stage}.'
        ),
        'labels': {
            "owner": "curation",
            "phase": "sandbox",
            "data_tier": "controlled",
            "release_tag": release_tag,
            "de_identified": "true",
            "issue_number": "dc3563"
        }
    }
}

for dataset_type in ['clean', 'sandbox']:
    dataset_object = client.define_dataset(
        dataset_definition[dataset_type]['name'],
        dataset_definition[dataset_type]['desc'],
        dataset_definition[dataset_type]['labels'])

    client.create_dataset(dataset_object, exists_ok=False)

# ## Copy tables from existing dataset to hotfix dataset

client.copy_dataset(f'{client.project}.{old_dataset_id}',
                    f"{client.project}.{dataset_definition['clean']['name']}")

# ## Applying cleaning rule to retract records from the dataset

cleaning_args = [
    '-p', client.project, '-d', f"{dataset_definition['clean']['name']}",
    '-b', f"{dataset_definition['sandbox']['name']}", '--data_stage',
    {data_stage}, '--run_as', run_as, '-s'
]

all_cleaning_args = add_kwargs_to_args(cleaning_args)
clean_cdr.main(args=all_cleaning_args)

# ## Running validation check on new dataset

q = query.render(project_id=project_id,
                 old_dataset_id=dataset_definition['clean']['name'])
execute(client, q)
