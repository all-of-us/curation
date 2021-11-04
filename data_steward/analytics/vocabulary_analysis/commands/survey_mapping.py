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

import pandas as pd
import Levenshtein
import re
import numpy as np
from datetime import date
import xlsxwriter
pd.set_option('display.max_colwidth',1000)
pd.set_option('display.max_rows',500)
pd.options.mode.chained_assignment=None #default='warn'
# ## 1. Insert the paths to all historic surveys, the new survey,and the data storage. 

dd_raw_path=r"{path to file}"
concept_raw_path=r"{path to file}"
relationship_raw_path=r"{path to file}"
#athena_raw_path=r"{path to file}"


dd_all = pd.read_csv(dd_raw_path)
dd_raw = pd.read_csv(dd_raw_path,usecols=['Variable / Field Name','Choices, Calculations, OR Slider Labels',"Branching Logic (Show field only if...)"])
concept_raw = pd.read_csv(concept_raw_path,usecols=['concept_code','concept_name','concept_id','concept_class_id'])
relationship_raw = pd.read_csv(relationship_raw_path,usecols=['concept_code','concept_id_1','concept_id_2','relationship_id','concept_id','concept_class_id'])
#athena_raw = pd.read_csv(athena_raw_path)

# +
#athena_raw
# -


# rename for understanding
# class can be a question, answer, topic, module, etc
dd_clean=dd_raw.rename(columns={'Variable / Field Name':'concept_code','Choices, Calculations, OR Slider Labels':'answer',"Branching Logic (Show field only if...)":"branch"})
concept_clean_for_mapping=concept_raw.drop(columns=['concept_class_id','concept_name'])
relationship_clean=relationship_raw.copy()
dd_clean

no_answers=dd_clean[dd_clean.answer.isnull()]
no_answers=no_answers.rename(columns={'answer':'concept_class_id'})
oddities = pd.merge(no_answers, dd_all,how='inner', left_on=['concept_code'] , right_on=['Variable / Field Name'])
no_answers


# # Clean the DD

# +
# Start with all answers with their display associated with a question on a row.
# End with all answers without their display and exploded to new rows.

# to explode answers, must drop n/a
dd_answers_drop=dd_clean[dd_clean['answer'].notna()]

# to explode branching logic must have all branches even where answer is n/a
dd_branching=dd_clean.drop(columns=['answer'])
dd_branching_drop=dd_branching[dd_branching['branch'].notna()]

dd_branching_drop
# -

# Split answers, stored in a list.
dd_branching_drop.branch=dd_branching_drop.branch.str.split(r"or")
dd_branching_drop

explode_branch = dd_branching_drop.set_index(['concept_code'])['branch'].apply(pd.Series).stack()
explode_branch = explode_branch.reset_index()
explode_branch.columns=['concept_code','ans_num','ans_code_display']
explode_branch.head(2)

# Split answers, stored in a list.
explode_branch.ans_code_display=explode_branch.ans_code_display.str.split(r"=")
explode_branch=explode_branch.drop(columns=['ans_num'])

explode_branch = explode_branch.set_index(['concept_code'])['ans_code_display'].apply(pd.Series).stack()
explode_branch = explode_branch.reset_index()
explode_branch.columns=['concept_code','qcode_or_acode','bconcept_code_answer']
explode_branch.head(2)

# +
clean_branch=explode_branch.copy()
clean_branch2=clean_branch[clean_branch.qcode_or_acode==0]
clean_branch2=clean_branch2.rename(columns={'concept_code':'q2','bconcept_code_answer':'q'})
clean_branch3=clean_branch[clean_branch.qcode_or_acode==1]
clean_branch3=clean_branch3.rename(columns={'concept_code':'q2','bconcept_code_answer':'a1'})
clean_branch4=pd.merge(clean_branch2, clean_branch3,how='left', on=['q2'],indicator=True)
clean_branch4['q']=clean_branch4['q'].str.strip(' ,[,]')
clean_branch4['a1']=clean_branch4['a1'].str.strip(" ,','")
clean_branch4=clean_branch4.drop_duplicates()
clean_branch4=clean_branch4.drop(columns=['qcode_or_acode_x','qcode_or_acode_y','_merge'])

# holds tertiary question branching logic
tertiary=clean_branch4[clean_branch4['a1'].str.contains('1')]

# holds all secondary branching logic - third level removed
clean_branch5=pd.merge(clean_branch4,tertiary, how='left', on= ['q2','q','a1'],indicator=True).query('_merge=="left_only"')
clean_branch5=clean_branch5.drop(columns=['_merge'])

clean_branch5.head(2)
# -

tertiary['q']=tertiary.q.str.split(r"\(")
tertiary = tertiary.set_index(['q2','a1'])['q'].apply(pd.Series).stack()
tertiary = tertiary.reset_index()

tertiary_clean2=tertiary[tertiary.level_2==0]
tertiary_clean2=tertiary_clean2.rename(columns={'q2':'q3','a1':'a3',0:'q2'})
tertiary_clean2=tertiary_clean2.drop(columns=['level_2'])
tertiary_clean3=tertiary[tertiary.level_2==1]
tertiary_clean3=tertiary_clean3.rename(columns={'q2':'q3','a1':'a3',0:'a2'})
tertiary_clean3=tertiary_clean3.drop(columns=['level_2'])
tertiary_clean3['a2']=tertiary_clean3.a2.str.strip(')')
tertiary_clean4=pd.merge(tertiary_clean2,tertiary_clean3,how='outer',on=['q3','a3'])
tertiary_clean4

# +
# Split answers, stored in a list.
dd_answers_drop.answer=dd_answers_drop.answer.str.split(r"|")

dd_answers_drop
# -


explode = dd_answers_drop.set_index(['concept_code'])['answer'].apply(pd.Series).stack()
explode = explode.reset_index()
explode.columns=['concept_code','ans_num','ans_code_display']

explode.ans_code_display=explode.ans_code_display.str.split(r",")
explode_display = explode.set_index(['concept_code','ans_num'])['ans_code_display'].apply(pd.Series).stack()
explode_display = explode_display.reset_index()
explode_display=explode_display.rename(columns={0:'concept_code_2'})
explode_display


code_association=explode_display[explode_display.level_2==0]
code_association['concept_code_2']=code_association.concept_code_2.str.strip()
code_association

# +
#add relationship to the module
# -

# # Do the classes match?

q_only=code_association[["concept_code"]].copy()
unique_only=q_only.concept_code.unique()
questions_only=pd.DataFrame(unique_only,columns=['concept_code'])
questions_only['concept_class_id']='Question'
questions_only=questions_only.drop_duplicates().reset_index(drop=True)

a_only=code_association[['concept_code_2']].copy()
unique_onlya=a_only.concept_code_2.unique()
answers_only=pd.DataFrame(unique_onlya,columns=['concept_code_2'])
answers_only=answers_only.rename(columns={'concept_code_2':'concept_code'})
answers_only['concept_class_id']='Answer'


df_list=[questions_only,answers_only]
tidy_codes=no_answers.append(df_list, ignore_index=True,sort=True)
tidy_codes


# +
# Where code class in the dd does not match the concept_survey table.
class_check_merge = pd.merge(tidy_codes, concept_raw,how='outer',on=['concept_code','concept_class_id'],indicator=True)
class_check_against_concept=class_check_merge[class_check_merge._merge!='both']

class_check_against_concept

# +

class_check_merge = pd.merge(tidy_codes, relationship_clean,how='outer',on=['concept_code','concept_class_id'],indicator=True)
class_check_against_relationship=class_check_merge[class_check_merge._merge!='both']

class_check_against_relationship=class_check_against_relationship
# -

# # Are all codes in the dd accounted for in the concept code doc?

# # Relationship mapping

concept_clean

code_association

# Create a WIP df from code_association to be the base for wrangling.
my_base=code_association[['concept_code','concept_code_2']].copy()
my_base=my_base.rename(columns={'code':'concept_code','concept_id':'concept_code_2'})
my_base

# Attach the relationship 'Maps to' to the proper codes.
concept_clean_to=concept_clean.copy()
concept_clean_to['relationship_id']='Maps to'
concept_clean_to['concept_id_1']=concept_clean_to['concept_id']
concept_clean_to['concept_id_2']=concept_clean_to['concept_id']
concept_clean_to['concept_code_2']=concept_clean_to['concept_code']
concept_clean_to

# Attach the relationship 'Maps from' to the proper codes.
concept_clean_from=concept_clean.copy()
concept_clean_from['relationship_id']='Mapped from'
concept_clean_from['concept_id_1']=concept_clean_from['concept_id']
concept_clean_from['concept_id_2']=concept_clean_from['concept_id']
concept_clean_from['concept_code_2']=concept_clean_from['concept_code']
concept_clean_from

# my_base2=my_base.append(concept_clean, ignore_index=True, sort=True)

# +
# Join concept codes to concept ids. Base to add other relationships.
base_half=my_base.merge(concept_clean, on='concept_code')
base_half['concept_id_1']=base_half['concept_id']

base_full1=base_half.merge(concept_clean, left_on='concept_code_2', right_on='concept_code')
base_full1=base_full1.rename(columns={'concept_code_x':'concept_code','concept_id_x':'concept_id','concept_id_y':'concept_id_2','concept_code_y':'join_code'})
base_full1=base_full1.drop(columns=['join_code'])

base_full1

# +
# Join concept codes to concept ids. Base to add other relationships.
base_half2=my_base.merge(concept_clean, left_on='concept_code_2', right_on='concept_code')
base_half2['concept_id_1']=base_half2['concept_id']

base_full2=base_half2.merge(concept_clean, left_on='concept_code_x', right_on='concept_code')
base_full2=base_full2.rename(columns={'concept_code_x':'concept_code_2','concept_code_2':'concept_code','concept_id_x':'concept_id','concept_id_y':'concept_id_2','concept_code_y':'join_code','concept_code':'join_code2'})
base_full2=base_full2.drop(columns=['join_code','join_code2'])

#base_half2
base_full2

# +
# Join concept codes to concept ids. Base to add other relationships.
base_half3=my_base.merge(concept_clean, left_on='concept_code_2', right_on='concept_code')
base_half3['concept_id_1']=base_half3['concept_id']

base_full3=base_half3.merge(concept_clean, left_on='concept_code_x', right_on='concept_code')
base_full3=base_full3.rename(columns={'concept_code_x':'concept_code_2','concept_code_2':'concept_code','concept_id_x':'concept_id','concept_id_y':'concept_id_1','concept_id_1':'concept_id_2','concept_code_y':'join_code','concept_code':'join_code2'})
base_full3=base_full3.drop(columns=['join_code','join_code2'])

#base_half3
base_full3
# -

base_doubled=[base_full1,base_full2,base_full3]
base_full=pd.concat(base_doubled, sort=False)
print(len(base_full1))
print(len(base_full2))
print(len(base_full3))
print(len(base_full))

# +
base_parent_of=base_full.copy()
    
def applyFunc(s):
    if s == base_parent_of.concept_id:
        return 'none'
    elif s == base_parent_of.concept_id_2:
        return 'none'
    return 'PPI parent code of'

base_parent_of['relationship_id'] = base_parent_of.concept_id_1.apply(applyFunc)
base_parent_of    
# -

base_parent_of=base_full.copy()
base_parent_of["relationship_id"] = ["PPI parent code of" for x in base_parent_of.concept_id_1 if [(x  != base_parent_of.concept_id) or (x  != base_parent_of.concept_id_2)]]
base_parent_of

base_parent_of=base_full.copy()
base_parent_of["relationship_id"] = ["PPI parent code of" if [(x  != base_parent_of.concept_id) & (x  != base_parent_of.concept_id_2)] else "none" for x in base_parent_of.concept_id_1]
base_parent_of

base_parent_of=base_full.copy()
data2 = ["PPI parent code of" if [(x != base_parent_of.concept_id_2) & (x != base_parent_of.concept_id)] else "none" for x in base_parent_of.concept_code_1]
df = pd.DataFrame(list(zip(base_parent_of.concept_code_1, data2)), columns=['A','B'])

# base_parent_of=base_full.copy()
# def applyFunc(s):
#     if s != base_parent_of.concept_code_2:
#         if s != base_parent_of.concept_code:
#             return 'PPI parent code of'
#         else:
#             return 'None'
#     else:
#         return 'None'
#
# base_parent_of['relationship_id'] = base_parent_of['concept_id_1'].apply(applyFunc)
# base_parent_of
#

base_has_parent=base_full.copy()
base_has_parent['relationship_id']='Has PPI parent code'
b2=base_has_parent[base_has_parent.concept_code =='ahc_2']
len(b2)

base_has_answer=base_full.copy()
base_has_answer['relationship_id']='Has answer (PPI)'
b3=base_has_answer[base_has_answer.concept_code =='ahc_2']
len(b3)
b3

base_answer=base_full.copy()
base_answer['relationship_id']='Answer of (PPI)'
b4=base_answer[base_answer.concept_code =='ahc_2']
len(b4)

base_rel_list=[concept_clean_to,concept_clean_from,base_parent_of,base_has_parent,base_has_answer,base_answer]
base_maps=pd.concat(base_rel_list, sort=False)
base_maps=base_maps.reset_index(drop=True)
base_maps=base_maps.sort_values('concept_code')
print(len(base_maps))
base_maps

# # Find discrepancies
#

discrepancies = pd.merge(relationship_raw, base_maps,how='outer',on=['concept_code','concept_id','concept_id_1','concept_id_2','relationship_id'],indicator=True)
discrepancies

look=discrepancies[(discrepancies.concept_code=='SDOH_35')]
look2=discrepancies[(discrepancies._merge!='both') &(discrepancies.concept_code=='SDOH_35')]
print(len(look))
look
look2



print(f'These have no answers associated with them '+str(len(oddities)))
print(f'List of all concept_ids and their class '+str(len(tidy_codes)))
print(f'Where the dd class does not match the relationship class '+str(len(class_check)))
#view branching_drop for possible missed branching logic




