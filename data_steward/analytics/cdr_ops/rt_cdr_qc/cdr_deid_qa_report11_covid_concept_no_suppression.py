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

# # Verify the covid drug concepts are not suppressed in the May 2022 CDR

# This notebook is to verify COVID drug concepts are NOT suppressed in the May 2022 CDR.
# For more details, please see [DC-2119](https://precisionmedicineinitiative.atlassian.net/browse/DC-2119)

# #### 1. Preparation

import pandas as pd
from utils import auth
from common import JINJA_ENV
from gcloud.bq import BigQueryClient
from analytics.cdr_ops.notebook_utils import execute, IMPERSONATION_SCOPES
pd.set_option("max_colwidth", None)

# + tags=["parameters"]
project_id: str = ""  # identifies the project where datasets are located
post_deid_dataset: str = ""  # the deid dataset
run_as = ""


# df will have a summary in the end
df = pd.DataFrame(columns=['QC category', 'Result'])

# +
impersonation_creds = auth.get_impersonation_credentials(
    run_as, target_scopes=IMPERSONATION_SCOPES)

client = BigQueryClient(project_id, credentials=impersonation_creds)
# -

# #### 2. Executing the quality check SQL query
# The concept IDs in the following query used to be suppressed until the Fall 2021 CDR. This query checks how many records exist for each of the concept IDs in the specified dataset.

query = JINJA_ENV.from_string("""
select drug_concept_id, count(*) as count
from `{{project_id}}.{{post_deid_dataset}}.drug_exposure`
where drug_concept_id in (
19052425, 19052557, 19082103,19052425,19052557,19082103,19082104,19098973,19115035,36217210,
46274363,46274409,724904,724906,724907,766231,766232,766233,766234,766235,766236,766237,
766238,766239,766240,766241,821336,1201837,1214698,1217525,1227568,1230962,1230963,35894915,
36388974,36394196,42639775,42639776,42639777,42639778,42639779,42639780,42795630,42796343)
group by 1
""")

q = query.render(project_id=project_id,post_deid_dataset=post_deid_dataset) 
df_query=execute(client, q) 
df_query.shape

# #### 3. Result
# If the following dataframe shows no data, these concept IDs are highly likely to remain suppressed. Reach out to Curation developers for troubleshooting.
# If it shows some data, we can verify that the concept IDs are NOT suppressed as expected.

df_query

# #### 4. Summary
# This is the summary of this notebook execution. Check the "Result" column and make sure everything is "PASS".

# +
qc_category = 'COVID concept no suppression (DC-2119)'

if df_query.empty:
    result = 'NOT PASS - COVID concepts look to be still suppressed.'
else:
    result = 'PASS'

df = df.append({
    'QC category': qc_category,
    'Result': result
},
               ignore_index=True)
# -

df_display = df.style.set_properties(**{'text-align': 'left'})
df_display = df_display.set_table_styles(
    [dict(selector='th', props=[('text-align', 'left')])])
display(df_display)

# If the "Result" column says "NOT PASS", check with Curation developers for troubleshooting.
