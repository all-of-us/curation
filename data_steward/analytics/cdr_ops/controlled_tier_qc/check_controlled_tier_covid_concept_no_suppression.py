# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.7.1
#   kernelspec:
#     display_name: 'Python 3.7.12 64-bit (''.venv'': venv)'
#     language: python
#     name: python3712jvsc74a57bd0bd48e0bf57cdd6803c27e3ca6c55ba8b4bab4d98ca7d312da29d41465508be2c
# ---

# # Verify the covid drug concepts are not suppressed in the May 2022 CDR

# ## [DC-2119]

import pandas as pd

# + tags=["parameters"]
project_id: str = ""  # identifies the project where datasets are located
post_deid_dataset: str = ""  # the deid dataset
# -

# df will have a summary in the end
df = pd.DataFrame(columns = ['query', 'result']) 

query = f'''
select drug_concept_id, count(*)
from `{project_id}.{post_deid_dataset}.drug_exposure`
where drug_concept_id in (
19052425, 19052557, 19082103,19052425,19052557,19082103,19082104,19098973,19115035,36217210,
46274363,46274409,724904,724906,724907,766231,766232,766233,766234,766235,766236,766237,
766238,766239,766240,766241,821336,1201837,1214698,1217525,1227568,1230962,1230963,35894915,
36388974,36394196,42639775,42639776,42639777,42639778,42639779,42639780,42795630,42796343)
group by 1
'''

df_query = pd.read_gbq(query, dialect='standard')

df_query

if df_query.empty:
    df = df.append({'query' : 'COVID empty', 'result' : ''},
                ignore_index = True) 
else:
    df = df.append({'query' : 'COVID not empty', 'result' : 'PASS'},
                ignore_index = True) 

# if not pass, will be highlighted in red
df = df.mask(df.isin(['Null','']))
df.style.highlight_null(null_color='red').set_properties(**{'text-align': 'left'})
