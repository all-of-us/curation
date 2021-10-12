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

# Question 1: Do all reused codes map to the same questions? <br>
# Question 2: Are there new codes that didn’t exist in any previous survey?  

import pandas as pd
import Levenshtein
pd.set_option('display.max_colwidth',500)
pd.set_option('display.max_rows',500)
# +
# Read-in the raw survey data.

personal_raw_path=r""
family_raw_path=r""
personal_family_raw_path=r""
personal_raw = pd.read_csv(personal_raw_path)
family_raw = pd.read_csv(family_raw_path)
personal_family_raw = pd.read_csv(personal_family_raw_path)
# -

# Make useful for all new surveys
#     - insert dictionary to hold read-in `previous survey` data.
#     - get values of dictionary into `frames_to_concat`.
# Will have to designate a variable for the `new` survey from the `previous survey` dictionary. <br>
# Print to csv

# +
# Reduce the data to important fields. 

personal_clean=personal_raw[['Variable / Field Name','Form Name','Field Label']]
personal_clean=personal_clean.rename(columns={'Variable / Field Name':'code','Form Name':'survey','Field Label':'question'})

family_clean=family_raw[['Variable / Field Name','Form Name','Field Label']]
family_clean=family_clean.rename(columns={'Variable / Field Name':'code','Form Name':'survey','Field Label':'question'})

personal_family_clean=personal_family_raw[['Variable / Field Name','Form Name','Field Label']]
personal_family_clean=personal_family_clean.rename(columns={'Variable / Field Name':'code','Form Name':'survey','Field Label':'question'})                                

# +
# Combine the personal and family surveys to run against the combined `PFHH` survey.

frames_to_concat = [personal_clean, family_clean]
previous = pd.concat(frames_to_concat)

# +
# Print data to increase understanding and qc process.

print(f"personal: "+str(len(personal_raw)))
print(f"family: "+str(len(family_raw)))
print(f"personal+family: "+str(len(personal_raw)+len(family_raw)))
print(f"previous-concat: "+str(len(previous)))
print(f"personal_family: "+str(len(personal_family_raw))) 

# +
# How many codes are duplicated in previous?

dup_code_series = previous.pivot_table(index=['code'],aggfunc='size')
dup_code_answer=dup_code_series[dup_code_series >1]
dup_code_answer

# +
#how many questions are duplicated in previous?

dup_question_series = previous.pivot_table(index=['question'],aggfunc='size')
dup_question_answer=dup_question_series[dup_question_series >1]
dup_question_answer
# -

# ## Question 1: Do all reused codes map to the same questions?  

# +
# Merge the DFs and designate the table where the codes are located(_merge).

compare_codes_df=pd.merge(previous,personal_family_clean,how='outer',on='code',indicator=True)

# +
# Answer question 1. Do all codes have the same question?

reused_codes=compare_codes_df[(compare_codes_df._merge=='both')]
codes_same_question=reused_codes[(reused_codes.question_x==reused_codes.question_y)]
codes_dif_question=reused_codes[(reused_codes.question_x!=reused_codes.question_y)]

print(f"codes have same question: "+str(len(codes_same_question)))
print(f"codes have different question: "+str(len(codes_dif_question)))
# -

# Calculate the difference between the questions and filter.
# Calculate the distances for all possible combinations
codes_dif_question["distance"] = codes_dif_question.apply(lambda row: Levenshtein.ratio(str(row["question_x"]), str(row["question_y"])), axis=1)
codes_dif_question=codes_dif_question.sort_values(by=['distance'], ascending=True)
codes_dif_question=codes_dif_question.reset_index(drop=True)
codes_dif_question.head(5)

# ## Question 2:  Are there new codes that didn’t exist in any previous survey?

# +
# Answer Question 2.

print(f"codes in right_only (added codes): "+str(len(compare_codes_df[(compare_codes_df._merge=='right_only')])))

# +
# visualize location of codes ('left_only'/'right_only'/'both')

table="right_only"
loc_of_codes=compare_codes_df[(compare_codes_df._merge==table)]
loc_of_codes=loc_of_codes.sort_values(by=['_merge','code'])
loc_of_codes=loc_of_codes.reset_index(drop=True)
print(f"codes exist in the "+ table +" table: "+str(len(loc_of_codes[(loc_of_codes._merge== table)])))

loc_of_codes.head(10)
# -


