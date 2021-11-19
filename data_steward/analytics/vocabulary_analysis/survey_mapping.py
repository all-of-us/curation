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

#     To update
#         Are all codes in the dd accounted for in the concept code doc?
#         Check for truncation issues

import pandas as pd
import re
import numpy as np
from datetime import date
import xlsxwriter
import os
import glob
pd.set_option('display.max_colwidth',1000)
pd.set_option('display.max_rows',500)
pd.options.mode.chained_assignment=None #default='warn'

# +
## Insert the paths to all historic surveys, the new survey,and the data storage. 

# +
dd_raw_path=r"mapping_docs\ENGLISHSocialDeterminantsOfHea_DataDictionary_2021-10-25.csv"
dd_all = pd.read_csv(dd_raw_path)
dd_raw = pd.read_csv(dd_raw_path,usecols=['Variable / Field Name',
                                          'Choices, Calculations, OR Slider Labels',
                                          'Branching Logic (Show field only if...)'])

concept_raw_path=r"mapping_docs\concept_sdoh.csv"
concept_raw = pd.read_csv(concept_raw_path,usecols=['concept_code',
                                                    'concept_name',
                                                    'concept_id',
                                                    'concept_class_id'])

relationship_raw_path=r"mapping_docs\concept_relationship_sdoh.csv"
relationship_raw = pd.read_csv(relationship_raw_path,usecols=['concept_code',
                                                              'concept_id_1',
                                                              'concept_id_2',
                                                              'relationship_id',
                                                              'concept_id',
                                                              'concept_class_id'])

data_storage_path='data_storage/'+str('sdoh_mapping')




# -


# rename for understanding
# class can be a question, answer, topic, module, etc
dd_raw=dd_raw.rename(columns={'Variable / Field Name':'concept_code','Choices, Calculations, OR Slider Labels':'answer',"Branching Logic (Show field only if...)":"branch"})
dd_clean=dd_raw[(dd_raw['concept_code'] != 'record_id')&(dd_raw.concept_code.str.contains('_intro')==False)&(dd_raw.concept_code.str.contains('_outro')==False)]
concept_clean=concept_raw.copy()
concept_clean_for_mapping=concept_raw.drop(columns=['concept_class_id','concept_name'])
relationship_clean=relationship_raw.copy()

# +
not_questions=dd_clean[dd_clean.answer.isnull()]

not_questions=not_questions.rename(columns={'answer':'concept_class_id'})
not_questions=not_questions.drop(columns=['branch'])
oddities = pd.merge(not_questions, dd_all,how='inner', left_on=['concept_code'] , right_on=['Variable / Field Name'])
# Connect Module 
ccode_of_module=concept_clean.loc[concept_clean.concept_class_id=='Module','concept_code'].values[0]
# Add relationship to dd
not_questions.loc[not_questions['concept_code']==ccode_of_module, 'concept_class_id'] = 'Module'
# Module id for later
cid_of_module=concept_clean.loc[concept_clean.concept_class_id=='Module','concept_id'].values[0]
# -

# # Clean the DD
# Start with all answers with their display associated with a question on a row.
# End with all answers without their display and exploded to new rows.

# +
# The following code block will separate the answers and displays from eachother, and keep their association to their concept code.

# To Separate the answers from themselves
# Drop n/a to explode/stack
dd_answers_drop=dd_clean[dd_clean['answer'].notna()]
# Split answers from the list of answers.
dd_answers_drop.answer=dd_answers_drop.answer.str.split(r"|")
explode = dd_answers_drop.set_index(['concept_code'])['answer'].apply(pd.Series).stack()
explode = explode.reset_index()
explode.columns=['concept_code','ans_num','ans_code_display']
# Split the displays from the list of answers/displays. 
explode.ans_code_display=explode.ans_code_display.str.split(r",")
explode_display = explode.set_index(['concept_code','ans_num'])['ans_code_display'].apply(pd.Series).stack()
explode_display = explode_display.reset_index()
explode_display=explode_display.rename(columns={0:'concept_code_2'})
# Keep the answer concept codes only. 
# Displays are stored in 'explode_display.level_2==1'
# The order of the answers can be found in 'primary_answers_clean.ans_num'
primary_answers_clean=explode_display[explode_display.level_2==0]
primary_answers_clean['concept_code_2']=primary_answers_clean.concept_code_2.str.strip()
primary_clean=primary_answers_clean.drop(columns=['level_2','ans_num']).reset_index(drop=True)
# End with each answer concept code associated with its question.
primary_clean
# -

dd_clean

# +
# The following code block will separate the branching logic from eachother, and keep their association to their concept code.

# To separate the branching from themselves
# To explode branching logic must have all branches even where answer is n/a
dd_branching=dd_clean.drop(columns=['answer'])
dd_branching_drop=dd_branching[dd_branching['branch'].notna()]
# Split branches, stored in a list.
dd_branching_drop.branch=dd_branching_drop.branch.str.split(r" or ")
explode_branch = dd_branching_drop.set_index(['concept_code'])['branch'].apply(pd.Series).stack()
explode_branch = explode_branch.reset_index()
explode_branch.columns=['concept_code','ans_num','ans_code_display']
# Split 'show field only if' statement, stored in a list.
explode_branch.ans_code_display=explode_branch.ans_code_display.str.split(r"=")
explode_branch=explode_branch.drop(columns=['ans_num'])
explode_branch = explode_branch.set_index(['concept_code'])['ans_code_display'].apply(pd.Series).stack()
explode_branch = explode_branch.reset_index()
explode_branch.columns=['concept_code','qcode_or_acode','bconcept_code_answer']
# Bring both columns to the next df to then clean
clean_branch1=explode_branch.copy()
clean_branch2=clean_branch1[clean_branch1.qcode_or_acode==0]
clean_branch2=clean_branch2.rename(columns={'concept_code':'q2','bconcept_code_answer':'q'})
clean_branch3=clean_branch1[clean_branch1.qcode_or_acode==1]
clean_branch3=clean_branch3.rename(columns={'concept_code':'q2','bconcept_code_answer':'a1'})
clean_branch4=pd.merge(clean_branch2, clean_branch3,how='left', on=['q2'],indicator=True)
clean_branch4['q']=clean_branch4['q'].str.strip(' ,[,]')
clean_branch4['a1']=clean_branch4['a1'].str.strip(" ,','")
clean_branch4=clean_branch4.drop_duplicates()
clean_branch4=clean_branch4.drop(columns=['qcode_or_acode_x','qcode_or_acode_y','_merge'])
# Holds tertiary question branching logic to be removed from this step, and used later.
tertiary=clean_branch4[clean_branch4['a1'].str.contains('1')]
# Holds all secondary branching logic - third level removed
#q2 is the question asked after answer a1 was given to the question q.
clean_branch=pd.merge(clean_branch4,tertiary, how='left', on= ['q2','q','a1'],indicator=True).query('_merge=="left_only"')
# Drop unneccessary columns and rename for mapping
secondary_clean=clean_branch.drop(columns=['_merge','q'])
# concept_code is the question for the class check, but for mapping purposes the q needs to map to an answer.
secondary_clean_mapping=secondary_clean.rename(columns={'q2':'concept_code_2','a1':'concept_code'})
secondary_concept=secondary_clean.copy()
secondary_clean_concept=secondary_concept.rename(columns={'q2':'concept_code','a1':'concept_code_2'})
secondary_clean
# -

tertiary

# holds tertiary question branching logic 
# Add a check for the case that a1 does not contain either 1 or 0. Cross your fingers.
tertiary=clean_branch4[clean_branch4['a1'].str.contains('1','0')]
# Split 'show field only if' statement, stored in a list.
tertiary['q']=tertiary.q.str.split(r"\(")
tertiary = tertiary.set_index(['q2','a1'])['q'].apply(pd.Series).stack()
tertiary = tertiary.reset_index()
# Bring both columns to the next df to then clean
# q3 is the question asked after a2 is answered from q2
# Example "Other"(a2) is specified as an answer to the second question(q2). The followup(q3) being 'Please Specify'
tertiary_clean2=tertiary[tertiary.level_2==0]
tertiary_clean2=tertiary_clean2.rename(columns={'q2':'q3','a1':'answer_a2?_bool',0:'q2'})
tertiary_clean2=tertiary_clean2.drop(columns=['level_2'])
tertiary_clean3=tertiary[tertiary.level_2==1]
tertiary_clean3=tertiary_clean3.rename(columns={'q2':'q3','a1':'answer_a2?_bool',0:'a2'})
tertiary_clean3=tertiary_clean3.drop(columns=['level_2'])
tertiary_clean3['a2']=tertiary_clean3.a2.str.strip(')')
tertiary_clean=pd.merge(tertiary_clean2,tertiary_clean3,how='outer',on=['q3','answer_a2?_bool'])
# Remove unneccessary to mapping columns.
tertiary_clean=tertiary_clean.drop(columns=['answer_a2?_bool','q2'])
tertiary_clean_concept=tertiary_clean.rename(columns={'a2':'concept_code_2','q3':'concept_code'})
tertiary_mapping=tertiary_clean.copy()
tertiary_clean_mapping=tertiary_mapping.rename(columns={'a2':'concept_code','q3':'concept_code_2'})
tertiary_clean_mapping


# +
#Get all codes in a column associated with their class_id, join to the given relationship table to look for problems.
# -

# connect all codes to all concept_ids for concept class
association_list=[secondary_clean_concept,tertiary_clean_concept]
code_association_concept=primary_clean.append(association_list, ignore_index=True,sort=True)
code_association_concept

# connect all codes to all concept_ids for mapping
association_list=[secondary_clean_mapping,tertiary_clean_mapping]
code_association_mapping=primary_clean.append(association_list, ignore_index=True,sort=True)
code_association_mapping

# # Do the classes match?

# Collect the questions
q_only=code_association_concept[["concept_code"]].copy()
unique_only=q_only.concept_code.unique()
questions_only=pd.DataFrame(unique_only,columns=['concept_code'])
questions_only['concept_class_id']='Question'
questions_only=questions_only.drop_duplicates().reset_index(drop=True)


# Collect the answers
a_only=code_association_concept[['concept_code_2']].copy()
unique_onlya=a_only.concept_code_2.unique()
answers_only=pd.DataFrame(unique_onlya,columns=['concept_code_2'])
answers_only=answers_only.rename(columns={'concept_code_2':'concept_code'})
answers_only['concept_class_id']='Answer'

# Combine the n/a, questions and answers, into tidy data.
df_list=[questions_only,answers_only]
tidy_codes=not_questions.append(df_list, ignore_index=True,sort=True)


# Where code class in the dd does not match the concept_survey table. Code level.
class_check_merge = pd.merge(tidy_codes, concept_clean,how='outer',on=['concept_code','concept_class_id'],indicator=True)
class_check_against_concept=class_check_merge[class_check_merge._merge!='both']
class_check_against_concept

# Where code class in the dd does not match the relationship table. Every issue.
class_check_merge = pd.merge(tidy_codes, relationship_clean,how='outer',on=['concept_code','concept_class_id'],indicator=True)
class_check_against_relationship=class_check_merge[class_check_merge._merge!='both']

# # Relationship mapping

# +
# Join concept codes to concept ids. With the first relationship possibility. q-q-a

base_half=code_association_mapping.merge(concept_clean_for_mapping, on='concept_code')
base_half['concept_id_1']=base_half['concept_id']

base_full1=base_half.merge(concept_clean_for_mapping, left_on='concept_code_2', right_on='concept_code')
base_full1=base_full1.rename(columns={'concept_code_x':'concept_code','concept_id_x':'concept_id','concept_id_y':'concept_id_2','concept_code_y':'join_code'})
base_qqa=base_full1.drop(columns=['join_code'])
base_qqa['concept_id_bonus']=base_qqa['concept_id_2']
# -

# Join concept codes to concept ids. With the second relationship possibility. q-a-q
base_qaq=base_qqa.rename(columns={'concept_id_1':'concept_id_2','concept_id_2':'concept_id_1'})
# Join concept codes to concept ids. With the third relationship possibility. a-a-q
base_aaq=base_qqa.rename(columns={'concept_id':'concept_id_2','concept_id_1':'concept_id_bonus','concept_id_2':'concept_id','concept_id_bonus':'concept_id_1'})
# Join concept codes to concept ids. With the fourth relationship possibility. a-q-a
base_aqa=base_qqa.rename(columns={'concept_id':'concept_id_1','concept_id_1':'concept_id_bonus','concept_id_2':'concept_id','concept_id_bonus':'concept_id_2'})

# Collect all parent codes
q_list=[base_qqa,base_qaq]
qbase_covered=pd.concat(q_list, sort=False)
# Collect all child codes
a_list=[base_aaq,base_aqa]
abase_covered=pd.concat(a_list, sort=False)

# +
# Covers all relationships made by parents of child codes
base_parent_of=qbase_covered.copy()
base_parent_of=base_parent_of[base_parent_of.concept_id!=base_parent_of.concept_id_2]
base_parent_of['relationship_id']='PPI parent code of'
b1=base_parent_of[base_parent_of.concept_code =='ahc_2']

base_has_answer=qbase_covered.copy()
base_has_answer=base_has_answer[base_has_answer.concept_id!=base_has_answer.concept_id_2]
base_has_answer['relationship_id']='Has answer (PPI)'
b2=base_has_answer[base_has_answer.concept_code =='ahc_2']

basea_parent_of=abase_covered.copy()
basea_parent_of=basea_parent_of.rename(columns={'concept_code':'concept_code_2','concept_code_2':'concept_code'})
basea_parent_of=basea_parent_of[basea_parent_of.concept_id!=basea_parent_of.concept_id_1]
basea_parent_of['relationship_id']='PPI parent code of'
b3=basea_parent_of[basea_parent_of.concept_code =='SDOH_41']

basea_has_answer=abase_covered.copy()
basea_has_answer=basea_has_answer.rename(columns={'concept_code':'concept_code_2','concept_code_2':'concept_code'})
basea_has_answer=basea_has_answer[basea_has_answer.concept_id!=basea_has_answer.concept_id_1]
basea_has_answer['relationship_id']='Has answer (PPI)'
b4=basea_has_answer[basea_has_answer.concept_code =='SDOH_41']


# +
# Covers all relationships made by children of parent codes
baseq_has_parent=qbase_covered.copy()
baseq_has_parent=baseq_has_parent[baseq_has_parent.concept_id!=baseq_has_parent.concept_id_1]
baseq_has_parent['relationship_id']='Has PPI parent code'
b1=baseq_has_parent[baseq_has_parent.concept_code_2 =='SDOH_50']

baseq_answer=qbase_covered.copy()
baseq_answer=baseq_answer[baseq_answer.concept_id!=baseq_answer.concept_id_1]
baseq_answer['relationship_id']='Answer of (PPI)'
b2=baseq_answer[baseq_answer.concept_code_2 =='SDOH_50']

base_has_parent=abase_covered.copy()
base_has_parent=base_has_parent[base_has_parent.concept_id!=base_has_parent.concept_id_2]
base_has_parent['relationship_id']='Has PPI parent code'
b3=base_has_parent[base_has_parent.concept_code_2 =='SDOH_30']

base_answer=abase_covered.copy()
base_answer=base_answer[base_answer.concept_id!=base_answer.concept_id_2]
base_answer['relationship_id']='Answer of (PPI)'
b4=base_answer[base_answer.concept_code_2 =='SDOH_41']

basea_has_parent=abase_covered.copy()
basea_has_parent=basea_has_parent.rename(columns={'concept_code':'concept_code_2','concept_code_2':'concept_code'})
basea_has_parent=basea_has_parent[basea_has_parent.concept_id!=basea_has_parent.concept_id_2]
basea_has_parent['relationship_id']='Has PPI parent code'
b5=basea_has_parent[basea_has_parent.concept_code_2 =='SDOH_41']

basea_answer=abase_covered.copy()
basea_answer=basea_answer.rename(columns={'concept_code':'concept_code_2','concept_code_2':'concept_code'})
basea_answer=basea_answer[basea_answer.concept_id!=basea_answer.concept_id_2]
basea_answer['relationship_id']='Answer of (PPI)'
b6=basea_answer[basea_answer.concept_code_2 =='SDOH_41']

# -

# Combine all relationships
base_rel_list=[
               base_parent_of,base_has_answer,
               basea_parent_of,basea_has_answer,
               baseq_has_parent,baseq_answer,
               basea_has_parent,basea_answer
              ]
base_maps=pd.concat(base_rel_list, sort=False)
base_maps=base_maps.reset_index(drop=True)
base_maps=base_maps.sort_values('concept_code')

# # Find discrepancies
#

# Join to given relationship table to find discrepancies. 
# Mapped from/Maps to are no longer relevant. 
discrepancies = pd.merge(relationship_raw, base_maps,how='outer',on=['concept_code','concept_id','concept_id_1','concept_id_2','relationship_id'],indicator=True)
discrepancies=discrepancies[(discrepancies.relationship_id != 'Mapped from')&
                            (discrepancies.relationship_id != 'Maps to')&
                            (discrepancies.concept_id != cid_of_module)&
                            (discrepancies.concept_id_1 != cid_of_module)&
                            (discrepancies.concept_id_2 != cid_of_module)]
# All instances where a mapping is missing, or not expected. Not without error. QC visually.
discrepancies_actual=(discrepancies[discrepancies._merge!='both'])

# Individual code pairs where a mapping is missing, or not expected. Not without error. QC visually.
individual_code_issues=pd.DataFrame(discrepancies_actual,columns=['concept_code','concept_code_2'])
individual_code_issues=individual_code_issues.drop_duplicates()

#Print explainations for the excel workbook.
prints={
    1:f'These have no answers associated with them '+str(len(oddities)),
    3:f'Where the dd class does not match the concept class '+str(len(class_check_against_concept)),
    4:f'Where the dd class does not match the relationship class '+str(len(class_check_against_relationship)),
    5:f'Visualize branching logic '+str(len(dd_branching_drop)),
    7:f'Individual codes that are missing mapping '+str(len(individual_code_issues)),
}
printss=pd.DataFrame.from_dict(prints, orient='index')


# Dictionary holds sheet names and their data.
dfs ={
    'overview':printss,
    'oddities':oddities,
    'class check concept doc':class_check_against_concept,
    'class check relationship doc':class_check_against_relationship,
    'branching visual':dd_branching_drop,
    'individual_code_issues':individual_code_issues,
}


# # Prints to path variable set above.
# path=data_storage_path+ str(date.today()) + '.xlsx'
#
# writer = pd.ExcelWriter(path, engine='xlsxwriter')
# for i in dfs:
#     dfs[i].to_excel(writer, sheet_name=str(i))
#     
# writer.save()
# writer.close()

# Search for an individual mapping in the relationship table
search=relationship_raw[(relationship_raw.concept_id_1=="40192394")&(relationship_raw.concept_id_2=="40192381")]
search


