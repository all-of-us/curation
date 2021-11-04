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

# +
import pandas as pd

pd.set_option('display.max_colwidth',500)
pd.set_option('display.max_rows',500)

# +
qc_retired_codes_raw_path = r"{path to file}"
man_retired_codes_raw_path =r"{path to file}"
sheet_name_retired= "retired_codes"

qc_new_codes_raw_path =r"{path to file}"
man_new_codes_raw_path =r"{path to file}"
sheet_name_new= "new_codes"

# +
qc_retired_codes_raw  = pd.read_excel(qc_retired_codes_raw_path, sheet_name= sheet_name_retired) 
man_retired_codes_raw  = pd.read_excel(man_retired_codes_raw_path, sheet_name= sheet_name_retired) 

qc_new_codes_raw  = pd.read_excel(qc_retired_codes_raw_path, sheet_name= sheet_name_new) 
man_new_codes_raw  = pd.read_excel(man_new_codes_raw_path, sheet_name= sheet_name_new) 
qc_retired_codes_raw


# +
qc_retired_codes_raw = qc_retired_codes_raw.code.str.lower()
qc_retired_codes_clean = pd.DataFrame(qc_retired_codes_raw, columns=['code'])
qc_retired_codes_clean.code =qc_retired_codes_clean.code.str.strip()

man_retired_codes_raw = man_retired_codes_raw.code.str.lower()
man_retired_codes_clean = pd.DataFrame(man_retired_codes_raw, columns=['code'])
man_retired_codes_clean.code =man_retired_codes_clean.code.str.strip()
man_retired_codes_clean.code=man_retired_codes_clean.code.str.slice(0,50)

qc_new_codes_raw = qc_new_codes_raw.code.str.lower()
qc_new_codes_clean = pd.DataFrame(qc_new_codes_raw, columns=['code'])
qc_new_codes_clean.code =qc_new_codes_clean.code.str.strip()

man_new_codes_raw = man_new_codes_raw.code.str.lower()
man_new_codes_clean = pd.DataFrame(man_new_codes_raw, columns=['code'])
man_new_codes_clean.code =man_new_codes_clean.code.str.strip()
man_new_codes_clean.code=man_new_codes_clean.code.str.slice(0,50)
# -

# # Find the long codes

long=man_new_codes_clean[man_new_codes_clean.code.str.len() >=50]


shortened = long['code'].str.slice(0,49)
shortened

# # Compare the retired codes

# 
merge_retired=pd.merge(man_retired_codes_clean,qc_retired_codes_clean,how='outer',on='code',indicator=True)

explore_retired = merge_retired[merge_retired._merge != 'both']
explore_retired = explore_retired.reset_index(drop=True)
#explore_retired = pd.DataFrame(explore_retired, columns=['code','exists'])
explore_retired

search= explore_retired[explore_retired.code.str.contains('cancerconditions')]
search

# # Compare the new codes

merge_new=pd.merge(man_new_codes_clean,qc_new_codes_clean,how='outer',on='code',indicator=True)

explore_new=merge_new[merge_new._merge != 'both']
#explore_new = pd.DataFrame(explore_new, columns=['code','exists'])
explore_new=explore_new.reset_index(drop=True)
explore_new
#This df is where our qc does not match up for new codes. 

search2= explore_new[explore_new.code.str.contains('otherhealthcondition_obesity')]
search2

len()

# +
print(f'qc retired ' + str(len(qc_retired_codes_clean)))
print(f'man retired ' +str(len(man_retired_codes_clean)))
print(f'both retired added ' +str(len(qc_retired_codes_clean) +len(man_retired_codes_clean)))
print(f'where we disagree -  retired ' +str(len(explore_retired)))
print('_______________________')

print(f'qc new ' + str(len(qc_new_codes_clean)))
print(f'man new '+str(len(man_new_codes_clean)))
print(f'both new added ' +str(len(qc_new_codes_clean) +len(man_new_codes_clean)))
print(f'where we disagree - new ' +str(len(explore_new)))
# -


