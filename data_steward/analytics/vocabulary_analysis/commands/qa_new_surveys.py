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

# # Check codes, questions, and answers of a new survey against the historic surveys.

import pandas as pd
import Levenshtein
import re
import numpy as np
from datetime import date
import xlsxwriter
pd.set_option('display.max_colwidth',500)
pd.set_option('display.max_rows',500)
pd.options.mode.chained_assignment=None #default='warn'
# ## 1. Insert the paths to all historic surveys, the new survey,and the data storage. 

# +
# Point to the paths of all survey data.
COPE202012_raw_path=r"{path to file}"
COPE202007_raw_path=r"{path to file}"
COPE202006_raw_path=r"{path to file}"
COPE202005_raw_path=r"{path to file}"
COPE202011_raw_path=r"{path to file}"
COPE202102_raw_path=r"{path to file}"
personal_raw_path=r"{path to file}"
family_raw_path=r"{path to file}"
basics_raw_path=r"{path to file}"
fall_min_covid_raw_path=r"{path to file}"
healthcare_aau_raw_path=r"{path to file}"
lifestyle_raw_path=r"{path to file}"
overall_health_raw_path=r"{path to file}"
social_determinants_raw_path=r"{path to file}"
personal_family_raw_path=r"{path to file}"

data_storage_path='./path/to/file/'+str('lifestyle_qc')

# -


# ## 2. If additional surveys were added above, add them here as well.

# Read-in the raw survey data.
COPE202012_raw = pd.read_csv(COPE202012_raw_path, usecols=['Variable / Field Name','Form Name','Field Type','Field Label','Choices, Calculations, OR Slider Labels'])
COPE202007_raw = pd.read_csv(COPE202007_raw_path, usecols=['Variable / Field Name','Form Name','Field Type','Field Label','Choices, Calculations, OR Slider Labels'])
COPE202006_raw = pd.read_csv(COPE202006_raw_path, usecols=['Variable / Field Name','Form Name','Field Type','Field Label','Choices, Calculations, OR Slider Labels'])
COPE202005_raw = pd.read_csv(COPE202005_raw_path, usecols=['Variable / Field Name','Form Name','Field Type','Field Label','Choices, Calculations, OR Slider Labels'])
COPE202011_raw = pd.read_csv(COPE202011_raw_path, usecols=['Variable / Field Name','Form Name','Field Type','Field Label','Choices, Calculations, OR Slider Labels'])
COPE202102_raw = pd.read_csv(COPE202102_raw_path, usecols=['Variable / Field Name','Form Name','Field Type','Field Label','Choices, Calculations, OR Slider Labels'])
personal_raw = pd.read_csv(personal_raw_path, usecols=['Variable / Field Name','Form Name','Field Type','Field Label','Choices, Calculations, OR Slider Labels'])
family_raw = pd.read_csv(family_raw_path, usecols=['Variable / Field Name','Form Name','Field Type','Field Label','Choices, Calculations, OR Slider Labels'])
basics_raw = pd.read_csv(basics_raw_path, usecols=['Variable / Field Name','Form Name','Field Type','Field Label','Choices, Calculations, OR Slider Labels'])
fall_min_covid_raw = pd.read_csv(fall_min_covid_raw_path, usecols=['Variable / Field Name','Form Name','Field Type','Field Label','Choices, Calculations, OR Slider Labels'])
healthcare_aau_raw = pd.read_csv(healthcare_aau_raw_path, usecols=['Variable / Field Name','Form Name','Field Type','Field Label','Choices, Calculations, OR Slider Labels'])
lifestyle_raw = pd.read_csv(lifestyle_raw_path, usecols=['Variable / Field Name','Form Name','Field Type','Field Label','Choices, Calculations, OR Slider Labels'])
overall_health_raw = pd.read_csv(overall_health_raw_path, usecols=['Variable / Field Name','Form Name','Field Type','Field Label','Choices, Calculations, OR Slider Labels'])
social_determinants_raw = pd.read_csv(social_determinants_raw_path, usecols=['Variable / Field Name','Form Name','Field Type','Field Label','Choices, Calculations, OR Slider Labels'])
personal_family_raw = pd.read_csv(personal_family_raw_path, usecols=['Variable / Field Name','Form Name','Field Type','Field Label','Choices, Calculations, OR Slider Labels'])

# ## 3. Add surveys to the bank. Set the new survey

# List to designate the 'historic'
historic_surveys=[
    COPE202012_raw,
    COPE202007_raw,
    COPE202006_raw,
    COPE202005_raw,
    COPE202011_raw,
    COPE202102_raw,
    personal_raw,
    family_raw,
    basics_raw,
    fall_min_covid_raw,
    healthcare_aau_raw,
    social_determinants_raw,
    overall_health_raw,
    personal_family_raw
]
new_survey=lifestyle_raw

# Set and clean the variables `historic` and `new_survey`
historic = pd.concat(historic_surveys)
historic=historic.rename(columns={'Variable / Field Name':'code','Field Type':'ans_type','Form Name':'survey','Field Label':'question','Choices, Calculations, OR Slider Labels':'answer'})
new_survey=new_survey.rename(columns={'Variable / Field Name':'code','Field Type':'ans_type','Form Name':'survey','Field Label':'question','Choices, Calculations, OR Slider Labels':'answer'})
#new_survey

# Print data to increase understanding and qc process.
total_in_each=[]
for i in historic_surveys:
    total_in_each.append(len(i))
def run():
    sum=0
    for i in total_in_each:
        sum+=i
        print(sum)


# Merge the DFs and designate the table where the codes are located(_merge).
compare_codes_df=pd.merge(historic,new_survey,how='outer',on='code',indicator=True)
search=compare_codes_df[compare_codes_df.code.str.contains('sdoh')]
#search

# ## Where Codes from the Historic Surveys are Reused in the New Survey

# DF only contains the reused 'new_survey' codes 
reused_codes=compare_codes_df[(compare_codes_df._merge=='both')]

# Get DF for every combination of questions or answers matching with the historic questions or answers.See diagram.
# EX: pqmad = Historic/Questions Match, Answers Don't. (because i said so...)
hqm=reused_codes[(reused_codes.question_x==reused_codes.question_y) & (reused_codes.ans_type_x!='text')]
hqmam=hqm[(hqm.answer_x==hqm.answer_y)]
hqmad=hqm[(hqm.answer_x!=hqm.answer_y) & (reused_codes.ans_type_y!='text')]
hqd=reused_codes[(reused_codes.question_x!=reused_codes.question_y)]
ham=reused_codes[(reused_codes.answer_x==reused_codes.answer_y)]
hamqm=ham[(ham.question_x==ham.question_y)]
hamqd=ham[(ham.question_x!=ham.question_y)]
had=reused_codes[(reused_codes.answer_x!=reused_codes.answer_y) & (reused_codes.ans_type_y!='text')]

# +
# Calculate the difference between questions and/or answers and sort. Print to csv at notebook end.

# hqm distance not needed
# hqmam distance not needed

if len(hqmad)>0:
    hqmad["distance"] = hqmad.apply(lambda row: Levenshtein.ratio(str(row["answer_x"]), str(row["answer_y"])), axis=1)
    hqmad=hqmad.sort_values(by=['distance'], ascending=True)
    
if len(hqd)>0:
    hqd["distance"] = hqd.apply(lambda row: Levenshtein.ratio(str(row["question_x"]), str(row["question_y"])), axis=1)
    hqd=hqd.sort_values(by=['distance'], ascending=True)
    

# ham distance not needed
# hamqm distance not needed
if len(hamqd)>0:
    hamqd["distance"] = hamqd.apply(lambda row: Levenshtein.ratio(str(row["question_x"]), str(row["question_y"])), axis=1)
    hamqd=hamqd.sort_values(by=['distance'], ascending=True)

if len(had)>0:
    had["distance"] = had.apply(lambda row: Levenshtein.ratio(str(row["answer_x"]), str(row["answer_y"])), axis=1)
    had=had.sort_values(by=['distance'], ascending=True)
# -

# # Are the new codes ...old codes?
# Where the codes are new, do their questions &| answers match questions &| answers from other surveys?
# This does not answer where the questions &| answers are worded differently. Use the searching tool at the end of the notebook for all new codes.

# DF only contains the new 'new_survey' codes 
new_quesiton_codes=compare_codes_df[(compare_codes_df._merge=='right_only')]


# Merge the DFs and designate the table where the codes are located(_merge).
code_qa_check_df=pd.merge(historic,new_survey,how='outer',on=['question','answer'],indicator=True)
code_q_check_df=pd.merge(historic,new_survey,how='outer',on='question',indicator=True)
code_a_check_df=pd.merge(historic,new_survey,how='outer',on='answer',indicator=True)

same_qa=code_qa_check_df[(code_qa_check_df._merge=='both') & (code_qa_check_df.code_x!=code_qa_check_df.code_y)& (code_qa_check_df.question!='Please specify.')]
same_q=code_q_check_df[(code_q_check_df._merge=='both') & (code_q_check_df.code_x!=code_q_check_df.code_y)& (code_q_check_df.question!='Please specify.')]
same_a=code_a_check_df[(code_a_check_df._merge=='both') & (code_a_check_df.code_x!=code_a_check_df.code_y) & (code_a_check_df.ans_type_x!='text') & (code_a_check_df.ans_type_y!='text') & (code_a_check_df.answer.str.len() > 0)]

# # Separate answers for analysis. 

# +
# Start with all answers with their display associated with a question on a row.
# End with all answers without their display exploded to new rows.
historic_answers_pre=historic
# Split answers, stored in a list.
historic_answers_pre.answer=historic_answers_pre.answer.str.split(r"|")
historic_answers_pre = historic_answers_pre.dropna()

 # Get the max number of answers possible for a question.
num_historic_answers=[len(part) for part in historic_answers_pre.answer]
ans_max=max(num_historic_answers)

# Name the columns to store each answer.
num=1
answer_columns = []
for i in range(1,ans_max+1):
    var='answer_'+str(num)
    answer_columns.append(var)
    num += 1
    
# Make a df to hold answers split into columns. 
historic_split_df = pd.DataFrame(historic_answers_pre['answer'].tolist(),columns=answer_columns)

# Gather all answers to one column
historic_answers = historic_split_df.melt()
historic_answers=historic_answers.dropna()  

# Split the answers from their displays
historic_answers.value=historic_answers.value.str.split(r",")
historic_answers = historic_answers.dropna()

# Get the max number of displays possible for an answer.
num_historic_displays=[len(i) for i in historic_answers['value']]
dis_max=max(num_historic_displays)
# Name the columns to store each answer.
num=1
display_columns = []
for i in range(1,dis_max+1):
    var='display_'+str(num)
    display_columns.append(var)
    num += 1
displays_df = pd.DataFrame(historic_answers['value'].tolist(),columns=display_columns)
displays_only = displays_df[['display_1']]
displays_only=displays_only.display_1.unique()
historic_answers_codes_only = pd.DataFrame(displays_only,columns=['code'])

# +
# Start with all answers with their display associated with a question on a row.
# End with all answers without their display exploded to new rows, still associated with their question_code and survey.
new_answers_pre=new_survey
# Split answers, stored in a list.
new_answers_pre.answer=new_answers_pre.answer.str.split(r"|")
new_answers_pre = new_answers_pre.dropna()

 # Get the max number of answers possible for a question.
num_new_answers=[len(part) for part in new_answers_pre.answer]
ans_max=max(num_new_answers)

# Name the columns to store each answer.
num=1
answer_columns = []
for i in range(1,ans_max+1):
    var='answer_'+str(num)
    answer_columns.append(var)
    num += 1
    
# Make a df to hold answers split into columns. 
new_split_df = pd.DataFrame(new_answers_pre['answer'].tolist(),columns=answer_columns)

# Gather all answers to one column
new_answers = new_split_df.melt()
new_answers=new_answers.dropna()  

# Split the answers from their displays
new_answers.value=new_answers.value.str.split(r",")
new_answers = new_answers.dropna()

# Get the max number of displays possible for an answer.
num_new_displays=[len(i) for i in new_answers['value']]
dis_max=max(num_new_displays)
# Name the columns to store each answer.
num=1
display_columns = []
for i in range(1,dis_max+1):
    var='display_'+str(num)
    display_columns.append(var)
    num += 1
displays_df = pd.DataFrame(new_answers['value'].tolist(),columns=display_columns)
displays_only = displays_df[['display_1']]
displays_only=displays_only.display_1.unique()
new_answers_codes_only = pd.DataFrame(displays_only,columns=['code'])

# -

# Merge the DFs and designate the table where the codes are located(_merge).
compare_answers_df=pd.merge(historic_answers_codes_only,new_answers_codes_only,how='outer',on='code',indicator=True)

# # How many question codes are retired/new?

# +
# df shows retired question codes from pmh/fhh
retired_questions=compare_codes_df[(compare_codes_df._merge=='left_only')]

#retired_questions

# +
# df shows retired answer codes from pmh/fhh
retired_answers=compare_answers_df[(compare_answers_df._merge=='left_only')]

#retired_answers
# -


# df shows new answer codes
new_answers=compare_answers_df[(compare_answers_df._merge=='right_only')]
new_answers.survey='social_determinants_of_health_english'


# Combine all retired codes(question/answer) from pmh/fhh
all_retired_codes = pd.concat([retired_questions,retired_answers],sort=False)
# Combine all new codes(question/answer)
all_new_codes = pd.concat([new_quesiton_codes,new_answers],sort=False)


# # Collect dfs/analysis and print to Excel

prints={
    1:f"number of codes in each included survey "+ str(total_in_each),
    2:f"The number of questions included in the survey bank: "+str(len(historic)),
    3:f"The number of answers included in the survey bank: "+str(len(historic_answers_codes_only)),
    4:f"The number of questions included in the new survey: "+str(len(new_survey)),
    5:f"The number of answers included in the new survey: "+str(len(new_answers_codes_only)),
    6:f"The number of questions found in the new survey that were in the survey bank(reused): "+str(len(reused_codes)),
    7:f"hqm   -reused codes, where their questions also match: "+str(len(hqm)),
    8:f"hqmam -reused codes, where their questions match and answers match: "+str(len(hqmam)),
    9:f"hqmad -reused codes, where their questions match and answers don't: "+str(len(hqmad)),
    10:f"hqd   -reused codes, where their questions don't match: "+str(len(hqd)),
    11:f"ham   -reused codes, where their answers match: "+str(len(ham)),
    12:f"hamqm -reused codes, where their answers match and questions match: "+str(len(hamqm)),
    13:f"hamqd -reused codes, where their answers match and questions don't: "+str(len(hamqd)),
    14:f"had   -reused codes, where their answers don't match: "+str(len(had)),
    15:f"same_qa    -questions and answers that match exactly, but don't have matching codes: "+str(len(same_qa)),
    16:f"same_q    -questions that match exactly,but don't have matching codes: "+str(len(same_q)),
    17:f"same_a    -answers that match exactly, but don't have matching codes: "+str(len(same_a)),
    18:f"Number of codes(q&a) not present in new doc: "+str(len(all_retired_codes)),
    19:f"Number of all codes(q&a) not present in the survey bank -new codes: "+str(len(all_new_codes)),
    20:"To QC for codes without knowing the exact wording of a question- use the notebook's search function"
    
    }
printss=pd.DataFrame.from_dict(prints, orient='index')
printss

dfs ={
    'overview':printss,
    'reused_codes':reused_codes,
    'hqm':hqm,
    'hqmam':hqmam,
    'hqmad':hqmad,
    'hqd':hqd,
    'ham':ham,
    'hamqm':hamqm,
    'hamqd':hamqd,
    'had':had,
    'same_qa':same_qa,
    'same_q':same_q,
    'same_a':same_a,
    'retired_codes':all_retired_codes,
    'new_codes':all_new_codes,
}

# path=data_storage_path+ str(date.today()) + '.xlsx'
#
# writer = pd.ExcelWriter(path, engine='xlsxwriter')
# for i in dfs:
#     dfs[i].to_excel(writer, sheet_name=str(i))
#     
# writer.save()
# writer.close()

# ## Additional QA tools

# How many `codes` are duplicated in historic?
dup_code_series = historic.pivot_table(index=['code'],aggfunc='size')
dup_code_answer=dup_code_series[dup_code_series >1]

#how many `questions` are duplicated in historic?
dup_question_series = historic.pivot_table(index=['question'],aggfunc='size')
dup_question_answer=dup_question_series[dup_question_series >1]

# To search in dfs for a value.
historic.dropna(inplace=True)
search=historic[historic['code'].str.contains('ipaq_7')]
#search


new_survey.dropna(inplace=True)
search2=new_survey[new_survey['code'].str.contains('ipaq_7')]
search2

# Visualize the location of all codes ('left_only'/'right_only'/'both')
table="left_only"
loc_of_codes=compare_codes_df[(compare_codes_df._merge==table) & compare_codes_df['question_x'].str.contains('{search text here}')]
loc_of_codes=loc_of_codes.reset_index(drop=True)
print(f"codes exist in the \""+ table +"\" table: "+str(len(loc_of_codes[(loc_of_codes._merge== table)])))
loc_of_codes

