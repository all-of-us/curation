# -*- coding: utf-8 -*-
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

# ## Functions: 
#     Print all codes(q&a) and their associated survey to csv.
#         Add additional information from the data dicts? Do it here: historic_clean_for_code_survey. Perhaps on import.
#     Answers in the additional tools:
#         How many times has a code been used accross all included surveys?
#         What survey is a specific concept_code in?

# +
import pandas as pd
from datetime import date
import numpy as np
import os
import glob

# Set options
pd.set_option('display.max_colwidth',500)
pd.set_option('display.max_rows',500)
pd.options.mode.chained_assignment=None #default='warn'
# -

# Make the left table. Surveys to check against.
path = os.getcwd() + str('\\left')
left_glob = glob.glob(os.path.join(path,"*.csv"))
left_files = []
questions_in_each_historic_survey=[]

# +
for file in left_glob:
      
    df = pd.read_csv(file, index_col=None, header=0, usecols=['field_name',
                                                              'form_name',
                                                              'field_type',
                                                              'field_label',
                                                              'branching_logic',
                                                              'select_choices_or_calculations'])
    left_files.append(df)  
    questions_in_each_historic_survey.append(len(file))
    
historic_surveys = pd.concat(left_files, axis=0, ignore_index=True)

historic=historic_surveys.copy()

historic=historic.rename(columns={'field_name':'concept_code',
                                  'field_type':'ans_type',
                                  'form_name':'survey',
                                  'field_label':'question',
                                  'branching_logic':'branch',
                                  'select_choices_or_calculations':'answer'})
# -
historic_clean_for_separation=historic[['concept_code','answer','branch']]
historic_clean_for_code_survey=historic[['concept_code','survey']]


# +
# Historic survey cleaning
# Separate the answers and displays from each other, and keep their association to their concept code.

# To Separate the answers from themselves
# Drop n/a to explode/stack
dd_answers_drop=historic_clean_for_separation[historic_clean_for_separation['answer'].notna()]
# Split answers from the list of answers.
dd_answers_drop.answer=dd_answers_drop.answer.str.split(r"|")
explode = dd_answers_drop.set_index(['concept_code'])['answer'].apply(pd.Series).stack()
explode = explode.reset_index()
explode.columns=['concept_code','ans_num','ans_code_display']
# Split the displays from the list of answers/displays. 
explode.ans_code_display=explode.ans_code_display.str.split(r",")
explode_display = explode.set_index(['concept_code','ans_num'])['ans_code_display'].apply(pd.Series).stack()
explode_display = explode_display.reset_index()
explode_display=explode_display.rename(columns={0:'answer'})
# Keep the answer concept codes only. 
# Displays are stored in 'explode_display.level_2==1'
# The order of the answers can be found in 'primary_answers_clean.ans_num'
primary_answers_clean=explode_display[explode_display.level_2==0]
primary_answers_clean['answer']=primary_answers_clean.answer.str.strip()
primary_clean=primary_answers_clean.drop(columns=['level_2','ans_num']).reset_index(drop=True)
# End with each answer concept code associated with its question.


# Separate the branching logic from eachother, and keep their association to their concept code.

# To separate the branching from themselves
# To explode branching logic must have all branches even where answer is n/a
dd_branching=historic_clean_for_separation.drop(columns=['answer'])
dd_branching_drop=dd_branching[dd_branching['branch'].notna()]
# Split branches, stored in a list.
dd_branching_drop.branch=dd_branching_drop.branch.str.split(r" or ")
explode_branch = dd_branching_drop.set_index(['concept_code'])['branch'].apply(pd.Series).stack()
explode_branch = explode_branch.reset_index()
explode_branch.columns=['concept_code','ans_num','ans_code_display']
# Split 'show field only if' statement, stored in a list.
explode_branch.ans_code_display=explode_branch.ans_code_display.str.split("=|>")
explode_branch=explode_branch.drop(columns=['ans_num'])
explode_branch = explode_branch.set_index(['concept_code'])['ans_code_display'].apply(pd.Series).stack()
explode_branch = explode_branch.reset_index()
explode_branch.columns=['concept_code','qcode_or_acode','bconcept_code_answer']
# Bring both columns to the next df to then clean
clean_branch1=explode_branch.copy()
clean_branch2=clean_branch1[clean_branch1.qcode_or_acode==0]
clean_branch2=clean_branch2.rename(columns={'bconcept_code_answer':'branching_question'})
clean_branch3=clean_branch1[clean_branch1.qcode_or_acode==1]
clean_branch3=clean_branch3.rename(columns={'bconcept_code_answer':'answer_to_branch'})
clean_branch4=pd.merge(clean_branch2,
                       clean_branch3,
                       how='left', 
                       on=['concept_code'],
                       indicator=True)
clean_branch4['branching_question']=clean_branch4['branching_question'].str.strip(' ,[,]')
clean_branch4['answer_to_branch']=clean_branch4['answer_to_branch'].str.strip(" ,','")
clean_branch4=clean_branch4.drop_duplicates()
clean_branch4=clean_branch4.drop(columns=['qcode_or_acode_x','qcode_or_acode_y','_merge'])
# Holds tertiary question branching logic to be removed from this step, and used later.
tertiary=clean_branch4[clean_branch4['answer_to_branch'].str.len() <= 1]
# Holds all secondary branching logic - third level removed
#q2 is the question asked after answer a1 was given to the question q.
clean_branch=pd.merge(clean_branch4,
                      tertiary, 
                      how='left', 
                      on= ['concept_code','branching_question','answer_to_branch'],
                      indicator=True).query('_merge=="left_only"')
# Drop unneccessary columns and rename for mapping
secondary_clean=clean_branch.drop(columns=['_merge','concept_code'])
secondary_clean=secondary_clean.rename(columns={'branching_question':'concept_code','answer_to_branch':'answer'})


# holds tertiary question branching logic 
# Split 'show field only if' statement, stored in a list.
tertiary['branching_question']=tertiary.branching_question.str.split(r"\(")
tertiary = tertiary.set_index(['concept_code','answer_to_branch'])['branching_question'].apply(pd.Series).stack()
tertiary = tertiary.reset_index()
# Bring both columns to the next df to then clean
tertiary_clean2=tertiary[tertiary.level_2==0]
tertiary_clean2=tertiary_clean2.rename(columns={0:'branching_question'})
tertiary_clean2=tertiary_clean2.drop(columns=['answer_to_branch','level_2'])
tertiary_clean3=tertiary[tertiary.level_2==1]
tertiary_clean3=tertiary_clean3.rename(columns={'answer_to_branch':'a1',0:'answer_to_branch'})
tertiary_clean3=tertiary_clean3.drop(columns=['a1','level_2'])
tertiary_clean3['answer_to_branch']=tertiary_clean3.answer_to_branch.str.strip(')')
tertiary_clean=pd.merge(tertiary_clean2,
                        tertiary_clean3,
                        how='outer',
                        on=['concept_code'])
# Remove unneccessary to mapping columns.
tertiary_clean=tertiary_clean.drop(columns=['concept_code'])
tertiary_clean=tertiary_clean.rename(columns={'branching_question':'concept_code','answer_to_branch':'answer'})

# connect all codes to all concept_ids for concept class
association_list=[secondary_clean,tertiary_clean]
all_historic_questions_and_answers=primary_clean.append(association_list, ignore_index=True,sort=True)



# -


codes_w_surveys= all_historic_questions_and_answers.merge(historic_clean_for_code_survey, how='left', on=['concept_code'])
codes_w_surveys=codes_w_surveys.drop_duplicates()

# # Collect dfs and print to csv

# +
copy=codes_w_surveys.copy()
historic_questions=copy[['concept_code', 'survey']]

list_of_historic_answers=codes_w_surveys[['answer', 'survey']]
historic_answers=list_of_historic_answers.rename(columns={'answer':'concept_code'})

complete_codes=historic_answers.append(historic_questions, ignore_index=True,sort=True)
complete_codes=complete_codes.drop_duplicates()


path='data_storage/dd_codes_in_athena'+ str(date.today()) + '.csv'

complete_codes.to_csv(path,index=False)
len(complete_codes)


# -
# ## Additional QA tools

# How many times has a code been used accross all included surveys?
dup_code_series = all_historic_questions_and_answers.pivot_table(index=['concept_code'],aggfunc='size')
dup_code_answer=dup_code_series[dup_code_series >1]
#dup_code_answer

# What survey was a specific concept_code in?
codes_w_surveys.dropna(inplace=True)
search=codes_w_surveys[codes_w_surveys['concept_code'].str.contains('ipaq_7')]
#search



