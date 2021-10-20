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
from datetime import date
import xlsxwriter
pd.set_option('display.max_colwidth',500)
pd.set_option('display.max_rows',500)
pd.options.mode.chained_assignment=None #default='warn'
# ## 1. Insert the paths to all historic surveys, the new survey, and storage location for new csvs.
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

data_storage_path='path/to/file/survey_qc '


# -


# ## 2. If additional surveys were added above, add them here as well.

# +
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
# -

# ## 3. Add additional surveys. (Do not include the new survey)

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
    lifestyle_raw,
    overall_health_raw,
    social_determinants_raw
]

# ## 4. Set the new_survey equal to the name of the new survey. 

# Set and clean the variables `historic` and `new_survey`
historic = pd.concat(historic_surveys)
historic=historic.rename(columns={'Variable / Field Name':'code','Field Type':'ans_type','Form Name':'survey','Field Label':'question','Choices, Calculations, OR Slider Labels':'answer'})
new_survey=personal_family_raw
new_survey=new_survey.rename(columns={'Variable / Field Name':'code','Field Type':'ans_type','Form Name':'survey','Field Label':'question','Choices, Calculations, OR Slider Labels':'answer'})

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

# ## Where Codes from the Historic Surveys are Reused in the New Survey

# DF only contains the reused 'new_survey' codes 
reused_codes=compare_codes_df[(compare_codes_df._merge=='both')]

# Get DF for every combination of questions or answers matching with the historic questions or answers.See diagram.
# EX: pqmad = Historic/Questions Match, Answers Don't. (because i said so...)
hqm=reused_codes[(reused_codes.question_x==reused_codes.question_y)]
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
hqmad["distance"] = hqmad.apply(lambda row: Levenshtein.ratio(str(row["answer_x"]), str(row["answer_y"])), axis=1)
hqmad=hqmad.sort_values(by=['distance'], ascending=True)

hqd["distance"] = hqd.apply(lambda row: Levenshtein.ratio(str(row["question_x"]), str(row["question_y"])), axis=1)
hqd=hqd.sort_values(by=['distance'], ascending=True)


# ham distance not needed
# hamqm distance not needed
hamqd["distance"] = hamqd.apply(lambda row: Levenshtein.ratio(str(row["question_x"]), str(row["question_y"])), axis=1)
hamqd=hamqd.sort_values(by=['distance'], ascending=True)

had["distance"] = had.apply(lambda row: Levenshtein.ratio(str(row["answer_x"]), str(row["answer_y"])), axis=1)
had=had.sort_values(by=['distance'], ascending=True)
# -

# ## Where 'new_survey' codes are new.

# DF only contains the new 'new_survey' codes 
new_codes=compare_codes_df[(compare_codes_df._merge=='right_only')]

# Get DF for every combination of questions or answers for the new codes.See diagram.
# EX: nqmad = New/Questions Match, Answers Don't. (again...)
nqd=new_codes[(new_codes.question_x!=new_codes.question_y)]
nqdad=nqd[(nqd.answer_x!=nqd.answer_y)]
nqdam=nqd[(nqd.answer_x==nqd.answer_y)]
nqm=new_codes[(new_codes.question_x==new_codes.question_y)]
nad=new_codes[(new_codes.answer_x!=new_codes.answer_y)]
nadqd=nad[(nad.question_x!=nad.question_y)]
nadqm=nad[(nad.question_x==nad.question_y)]
nam=new_codes[(new_codes.answer_x==new_codes.answer_y)]


# # Are the new codes ...old codes?
# Where the codes are new, do their questions &| answers match questions &| answers from other surveys?
# This does not answer where the questions &| answers are worded differently. Use the searching tool at the end of the notebook for all new codes.

# Merge the DFs and designate the table where the codes are located(_merge).
code_qa_check_df=pd.merge(historic,new_survey,how='outer',on=['question','answer'],indicator=True)
code_q_check_df=pd.merge(historic,new_survey,how='outer',on='question',indicator=True)
code_a_check_df=pd.merge(historic,new_survey,how='outer',on='answer',indicator=True)

same_qa=code_qa_check_df[(code_qa_check_df._merge=='both') & (code_qa_check_df.code_x!=code_qa_check_df.code_y)]
same_q=code_q_check_df[(code_q_check_df._merge=='both') & (code_q_check_df.code_x!=code_q_check_df.code_y)]
same_a=code_a_check_df[(code_a_check_df._merge=='both') & (code_a_check_df.code_x!=code_a_check_df.code_y) & (code_a_check_df.ans_type_x!='text') & (code_a_check_df.ans_type_y!='text') & (code_a_check_df.answer.str.len() > 0)]

# # Separate answers for analysis. 

historic_answers_pre=historic
# Split answers, stored in a list.
historic_answers_pre.answer=historic_answers_pre.answer.str.split(r"|")
historic_answers_pre = historic_answers_pre.dropna()
# Make a base df to add to later.
split_df = pd.DataFrame(historic_answers_pre, columns=['code','survey'])
# Make a df to hold answers split into columns. 
split_df2 = pd.DataFrame(historic_answers_pre['answer'].tolist(),columns=["answer_1","answer_2","answer_3","answer_4","answer_5",
                                                                        "answer_6","answer_7","answer_8","answer_9","answer_10",
                                                                        "answer_11","answer_12","answer_13","answer_14","answer_15",
                                                                        "answer_16","answer_17","answer_18","answer_19","answer_20",
                                                                        "answer_21","answer_22","answer_23","answer_24","answer_25",
                                                                        "answer_26","answer_27","answer_28","answer_29","answer_30",
                                                                        "answer_31","answer_32","answer_33","answer_34","answer_35",
                                                                        "answer_36","answer_37","answer_38","answer_39","answer_40",
                                                                        "answer_41","answer_42","answer_43","answer_44","answer_45",
                                                                        "answer_46","answer_47","answer_48","answer_49","answer_50",
                                                                        "answer_51"])
split_df=split_df.reset_index(drop=True)
# Combine the 'split' dfs on their index
split_df3 = pd.concat([split_df,split_df2],axis=1)
# Melt the answers, reshape the df
historic_answers = split_df3.melt(id_vars=['code','survey'],value_vars=["answer_1","answer_2","answer_3","answer_4","answer_5",
                                                                        "answer_6","answer_7","answer_8","answer_9","answer_10",
                                                                        "answer_11","answer_12","answer_13","answer_14","answer_15",
                                                                        "answer_16","answer_17","answer_18","answer_19","answer_20",
                                                                        "answer_21","answer_22","answer_23","answer_24","answer_25",
                                                                        "answer_26","answer_27","answer_28","answer_29","answer_30",
                                                                        "answer_31","answer_32","answer_33","answer_34","answer_35",
                                                                        "answer_36","answer_37","answer_38","answer_39","answer_40",
                                                                        "answer_41","answer_42","answer_43","answer_44","answer_45",
                                                                        "answer_46","answer_47","answer_48","answer_49","answer_50",
                                                                        "answer_51"])
historic_answers=historic_answers.dropna()   

# # How many codes are retired?

retired_pmh=compare_codes_df[(compare_codes_df._merge=='left_only') & (compare_codes_df.survey_x=='personal_medical_history')]
retired_fhh=compare_codes_df[(compare_codes_df._merge=='left_only') & (compare_codes_df.survey_x=='family_health_history')]
retired_other=compare_codes_df[(compare_codes_df._merge=='left_only') & (compare_codes_df.survey_x!='personal_medical_history') & (compare_codes_df.survey_x!='family_health_history')]

# # Collect dfs/analysis and print to Excel

prints={
    1:f"number of codes in each included survey "+ str(total_in_each),
    2:f"historic_concat: "+str(len(historic)),
    3:f"new_survey: "+str(len(new_survey)),
    4:f"Total number of reused codes: "+str(len(reused_codes)),
    5:f"hqm   -reused codes have same question: "+str(len(hqm)),
    6:f"hqmam -reused codes have same question and same answer: "+str(len(hqmam)),
    7:f"hqmad -reused codes have same question and different answers: "+str(len(hqmad)),
    8:f"hqd   -reused codes have different questions: "+str(len(hqd)),
    9:f"ham   -reused codes have same answer: "+str(len(ham)),
    10:f"hamqm -reused codes have same answer and same question: "+str(len(hamqm)),
    11:f"hamqd -reused codes have same answer and different questions: "+str(len(hamqd)),
    12:f"had   -reused codes have different answers: "+str(len(had)),
    13:f"Total number of new codes: "+str(len(new_codes)),
    14:f"nqd   -new codes questions match: "+str(len(nqd)),
    15:f"nqdad -new codes questions match answers match: "+str(len(nqdad)),
    16:f"nqdam -new codes questions match answers don't: "+str(len(nqdam)),
    17:f"nqm   -new codes questions don't match: "+str(len(nqm)),
    18:f"nad   -new codes answers match: "+str(len(nad)),
    19:f"nadqd -new codes answers mach questions match: "+str(len(nadqd)),
    20:f"nadqm -new codes answers match questions don't: "+str(len(nadqm)),
    21:f"nam   -new codes answers don't match: "+str(len(nam)),
    22:f"exact matching questions and answer, not matching codes: "+str(len(same_qa)),
    23:f"exact matching questions, not matching codes: "+str(len(same_q)),
    24:f"exact matching answer, not matching codes: "+str(len(same_a)),
    25:f"retired codes from pmh: "+str(len(retired_pmh)),
    26:f"retired codes from fhh: "+str(len(retired_fhh)),
    27:f"retired codes from elsewhere: "+str(len(retired_other))
    }
printss=pd.DataFrame.from_dict(prints, orient='index')
printss

dfs ={
    0:printss,
    1:reused_codes,
    2:hqm,
    3:hqmam,
    4:hqmad,
    5:hqd,
    6:ham,
    7:hamqm,
    8:hamqd,
    9:ham,
    10:new_codes,
    11:nqd,
    12:nqdad,
    13:nqdam,
    14:nqm,
    15:nad,
    16:nadqd,
    17:nadqm,
    18:nam,
    19:same_qa,
    20:same_q,
    21:same_a,
    22:retired_pmh,
    23:retired_fhh,
    24:retired_other
}

# +
path=data_storage_path+ str(date.today()) + '.xlsx'

writer = pd.ExcelWriter(path, engine='xlsxwriter')
for i in dfs:
    dfs[i].to_excel(writer, sheet_name=str(i))
    

writer.save()
writer.close()
# -

# ## Additional QA tools

# How many `codes` are duplicated in historic?
dup_code_series = historic.pivot_table(index=['code'],aggfunc='size')
dup_code_answer=dup_code_series[dup_code_series >1]

#how many `questions` are duplicated in historic?
dup_question_series = historic.pivot_table(index=['question'],aggfunc='size')
dup_question_answer=dup_question_series[dup_question_series >1]

# To search in dfs for a value.
historic.dropna(inplace=True)
search=historic[historic['question'].str.contains('Parkinson')]
#search


new_survey.dropna(inplace=True)
search2=new_survey[new_survey['code'].str.contains('parkinson')]
#search2

# Visualize the location of all codes ('left_only'/'right_only'/'both')
table="both"
loc_of_codes=compare_codes_df[(compare_codes_df._merge==table) & compare_codes_df['question_x'].str.contains('Parkinson')]
loc_of_codes=loc_of_codes.sort_values(by=['_merge','code'])
loc_of_codes=loc_of_codes.reset_index(drop=True)
print(f"codes exist in the "+ table +" table: "+str(len(loc_of_codes[(loc_of_codes._merge== table)])))
#loc_of_codes