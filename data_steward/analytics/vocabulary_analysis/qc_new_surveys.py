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
# Point to the paths of all survey data.
personal_raw_path=r"{path to file}"
family_raw_path=r"{path to file}"
personal_family_raw_path=r"{path to file}"
COPE202012_raw_path=r"{path to file}"
COPE202007_raw_path=r"{path to file}"
COPE202006_raw_path=r"{path to file}"
COPE202005_raw_path=r"{path to file}"
COPE202011_raw_path=r"{path to file}"
COPE202102_raw_path=r"{path to file}"
basics_raw_path=r"{path to file}"
fall_min_covid_raw_path=r"{path to file}"
healthcare_aau_raw_path=r"{path to file}"
lifestyle_raw_path=r"{path to file}"
overall_health_raw_path=r"{path to file}"
social_determinants_raw_path=r"{path to file}"

store_csv_path="{path to file}"
# -


# Read-in the raw survey data.
COPE202012_raw = pd.read_csv(COPE202012_raw_path, usecols=['Variable / Field Name','Form Name','Field Label'])
COPE202007_raw = pd.read_csv(COPE202007_raw_path, usecols=['Variable / Field Name','Form Name','Field Label'])
COPE202006_raw = pd.read_csv(COPE202006_raw_path, usecols=['Variable / Field Name','Form Name','Field Label'])
COPE202005_raw = pd.read_csv(COPE202005_raw_path, usecols=['Variable / Field Name','Form Name','Field Label'])
COPE202011_raw = pd.read_csv(COPE202011_raw_path, usecols=['Variable / Field Name','Form Name','Field Label'])
COPE202102_raw = pd.read_csv(COPE202102_raw_path, usecols=['Variable / Field Name','Form Name','Field Label'])
personal_raw = pd.read_csv(personal_raw_path, usecols=['Variable / Field Name','Form Name','Field Label'])
family_raw = pd.read_csv(family_raw_path, usecols=['Variable / Field Name','Form Name','Field Label'])
personal_family_raw = pd.read_csv(personal_family_raw_path, usecols=['Variable / Field Name','Form Name','Field Label'])
basics_raw = pd.read_csv(basics_raw_path, usecols=['Variable / Field Name','Form Name','Field Label'])
fall_min_covid_raw = pd.read_csv(fall_min_covid_raw_path, usecols=['Variable / Field Name','Form Name','Field Label'])
healthcare_aau_raw = pd.read_csv(healthcare_aau_raw_path, usecols=['Variable / Field Name','Form Name','Field Label'])
lifestyle_raw = pd.read_csv(lifestyle_raw_path, usecols=['Variable / Field Name','Form Name','Field Label'])
overall_health_raw = pd.read_csv(overall_health_raw_path, usecols=['Variable / Field Name','Form Name','Field Label'])
social_determinants_raw = pd.read_csv(social_determinants_raw_path, usecols=['Variable / Field Name','Form Name','Field Label'])

previous_surveys=[
    personal_raw,
    family_raw,
    COPE202012_raw,
    COPE202007_raw,
    COPE202006_raw,
    COPE202005_raw,
    COPE202011_raw,
    COPE202102_raw,
    basics_raw,
    fall_min_covid_raw,
    healthcare_aau_raw,
    lifestyle_raw,
    overall_health_raw,
    social_determinants_raw
]

# Set and clean the variables `previous` and `new_survey`
previous = pd.concat(previous_surveys)
previous=previous.rename(columns={'Variable / Field Name':'code','Form Name':'survey','Field Label':'question'})
new_survey=personal_family_raw
new_survey=new_survey.rename(columns={'Variable / Field Name':'code','Form Name':'survey','Field Label':'question'})

# Print data to increase understanding and qc process.
total_in_each=[]
for i in previous_surveys:
    total_in_each.append(len(i))
def run():
    sum=0
    for i in total_in_each:
        sum+=i
        print(sum)
run()    
# print(total_in_each)
print(f"previous_concat: "+str(len(previous)))
print(f"new_survey: "+str(len(new_survey))) 

# How many `codes` are duplicated in previous?
dup_code_series = previous.pivot_table(index=['code'],aggfunc='size')
dup_code_answer=dup_code_series[dup_code_series >1]
dup_code_answer

#how many `questions` are duplicated in previous?
dup_question_series = previous.pivot_table(index=['question'],aggfunc='size')
dup_question_answer=dup_question_series[dup_question_series >1]
dup_question_answer

# ## Question 1: Do all reused codes map to the same questions?  

# Merge the DFs and designate the table where the codes are located(_merge).
compare_codes_df=pd.merge(previous,new_survey,how='outer',on='code',indicator=True)

# +
# Answer question 1. Do all codes have the same question?
reused_codes=compare_codes_df[(compare_codes_df._merge=='both')]
codes_same_question=reused_codes[(reused_codes.question_x==reused_codes.question_y)]
codes_dif_question=reused_codes[(reused_codes.question_x!=reused_codes.question_y)]

print(f"codes have same question: "+str(len(codes_same_question)))
print(f"codes have different question: "+str(len(codes_dif_question)))
# -

# Calculate the difference between the questions and filter. # Print to csv at notebook end.
codes_dif_question["distance"] = codes_dif_question.apply(lambda row: Levenshtein.ratio(str(row["question_x"]), str(row["question_y"])), axis=1)
codes_dif_question=codes_dif_question.sort_values(by=['distance'], ascending=True)
codes_dif_question=codes_dif_question.reset_index(drop=True)

# ## Question 2:  Are there new codes that didn’t exist in any previous survey?

# Answer Question 2.
print(f"codes in right_only (added codes): "+str(len(compare_codes_df[(compare_codes_df._merge=='right_only')])))

# visualize location of codes ('left_only'/'right_only'/'both')
table="right_only"
loc_of_codes=compare_codes_df[(compare_codes_df._merge==table)]
loc_of_codes=loc_of_codes.sort_values(by=['_merge','code'])
loc_of_codes=loc_of_codes.reset_index(drop=True)
print(f"codes exist in the "+ table +" table: "+str(len(loc_of_codes[(loc_of_codes._merge== table)])))
# +
#print to csv
# codes_dif_question.to_csv(store_csv_path1)

# if table == "right_only":
#     loc_of_codes.to_csv(store_csv_path2)
# else:
#     print('no')
