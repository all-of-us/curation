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

store_csv_path1='./newsurveyqa_reusedcode_differentquestion.csv'
store_csv_path2='./newsurveyqa_reusedcode_samequestion_differentanswer.csv'
store_csv_path3='./newsurveyqa_reusedcode_differenctanswer.csv'
store_csv_path4='./newsurveyqa_reusedcode_sameanswer_differentquestion.csv'
store_csv_path5='./newsurveyqa_newcode_differentquestion_sameanswer.csv'
store_csv_path6='./newsurveyqa_newcode_samequestion.csv'
store_csv_path7='./newsurveyqa_newcode_differenctanswer_samequestion.csv'
store_csv_path8='./newsurveyqa_newcode_sameanswer.csv'
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
run()    
# print(total_in_each)
print(f"historic_concat: "+str(len(historic)))
print(f"new_survey: "+str(len(new_survey))) 

# Merge the DFs and designate the table where the codes are located(_merge).
compare_codes_df=pd.merge(historic,new_survey,how='outer',on='code',indicator=True)

# ## Where Codes from the Historic Surveys are Reused in the New Survey

# DF only contains the reused 'new_survey' codes 
reused_codes=compare_codes_df[(compare_codes_df._merge=='both')]

# +
# Get DF for every combination of questions or answers matching with the historic questions or answers.See diagram.
# EX: pqmad = Historic/Questions Match, Answers Don't. (because i said so...)
print(f"Total number of reused codes: "+str(len(reused_codes)))

hqm=reused_codes[(reused_codes.question_x==reused_codes.question_y)]
print(f"hqm   -reused codes have same question: "+str(len(hqm)))

hqmam=hqm[(hqm.answer_x==hqm.answer_y)]
print(f"hqmam -reused codes have same question and same answer: "+str(len(hqmam)))

hqmad=hqm[(hqm.answer_x!=hqm.answer_y)]
print(f"hqmad -reused codes have same question and different answers: "+str(len(hqmad)))

hqd=reused_codes[(reused_codes.question_x!=reused_codes.question_y)]
print(f"hqd   -reused codes have different questions: "+str(len(hqd)))



ham=reused_codes[(reused_codes.answer_x==reused_codes.answer_y)]
print(f"ham   -reused codes have same answer: "+str(len(ham)))

hamqm=ham[(ham.question_x==ham.question_y)]
print(f"hamqm -reused codes have same answer and same question: "+str(len(hamqm)))

hamqd=ham[(ham.question_x!=ham.question_y)]
print(f"hamqd -reused codes have same answer and different questions: "+str(len(hamqd)))

had=reused_codes[(reused_codes.answer_x!=reused_codes.answer_y)]
print(f"had   -reused codes have different answers: "+str(len(had)))

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

# +
# Get DF for every combination of questions or answers for the new codes.See diagram.
# EX: nqmad = New/Questions Match, Answers Don't. (again...)
print(f"Total number of new codes: "+str(len(new_codes)))

nqm=new_codes[(new_codes.question_x==new_codes.question_y)]
print(f"nqm   -new codes have same question: "+str(len(nqm)))

nqmam=nqm[(nqm.answer_x==nqm.answer_y)]
print(f"nqmam -new codes have same question and same answer: "+str(len(nqmam)))

nqmad=nqm[(nqm.answer_x!=nqm.answer_y)]
print(f"nqmad -new codes have same question and different answers: "+str(len(nqmad)))

nqd=new_codes[(new_codes.question_x!=new_codes.question_y)]
print(f"nqd   -new codes have different questions: "+str(len(nqd)))



nam=new_codes[(new_codes.answer_x==new_codes.answer_y)]
print(f"nam   -new codes have same answer: "+str(len(nam)))

namqm=nam[(nam.question_x==nam.question_y)]
print(f"namqm -new codes have same answer and same question: "+str(len(namqm)))

namqd=nam[(nam.question_x!=nam.question_y)]
print(f"namqd -new codes have same answer and different questions: "+str(len(namqd)))

nad=new_codes[(new_codes.answer_x!=new_codes.answer_y)]
print(f"nad   -new codes have different answers: "+str(len(nad)))
# -

# # 5. When ready change back to 'Code' format and run. 
# # Print to csv.
# hqm.to_csv(store_csv_path1)
# hqmam.to_csv(store_csv_path2)
# ham.to_csv(store_csv_path3)
# hamqm.to_csv(store_csv_path4)
#
# #nqdam.to_csv(store_csv_path5)
# nqm.to_csv(store_csv_path6)
# #nad.to_csv(store_csv_path7)
# #nadqd.to_csv(store_csv_path8)

# ## Additional QA tools

# How many `codes` are duplicated in historic?
dup_code_series = historic.pivot_table(index=['code'],aggfunc='size')
dup_code_answer=dup_code_series[dup_code_series >1]

#how many `questions` are duplicated in historic?
dup_question_series = historic.pivot_table(index=['question'],aggfunc='size')
dup_question_answer=dup_question_series[dup_question_series >1]

# To search in dfs for a value.
historic.dropna(inplace=True)
search=historic[historic['question'].str.contains('pair')]
#search

# Visualize the location of all codes ('left_only'/'right_only'/'both')
table="right_only"
loc_of_codes=compare_codes_df[(compare_codes_df._merge==table)]
loc_of_codes=loc_of_codes.sort_values(by=['_merge','code'])
loc_of_codes=loc_of_codes.reset_index(drop=True)
print(f"codes exist in the "+ table +" table: "+str(len(loc_of_codes[(loc_of_codes._merge== table)])))