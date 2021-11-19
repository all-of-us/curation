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

# Things to update:
#
# where reused codes have the same '...'
# Run a ~spell check on new codes.


# +
import pandas as pd
import Levenshtein
from datetime import date
import os
import glob

# Set options
pd.set_option('display.max_colwidth',500)
pd.set_option('display.max_rows',500)
pd.options.mode.chained_assignment=None #default='warn'

# +
# Make the left table. Surveys to check against.
path = os.getcwd() + str('\\left')
left_glob = glob.glob(os.path.join(path,"*.csv"))
left_files = []
questions_in_each_historic_survey=[]


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

# +
# Make the left table. Surveys to check against.
path = os.getcwd() + str('\\right')
right_glob = glob.glob(os.path.join(path,"*.csv"))
right_files = []
questions_in_the_new_survey=[]

for file in right_glob:
      
    df = pd.read_csv(file, index_col=None, header=0, usecols=['field_name',
                                                              'form_name',
                                                              'field_type',
                                                              'field_label',
                                                              'branching_logic',
                                                              'select_choices_or_calculations'])
    right_files.append(df)  
    questions_in_the_new_survey.append(len(file))
    
new_survey_to_verify = pd.concat(right_files, axis=0, ignore_index=True)

new_survey=new_survey_to_verify.copy()

new_survey=new_survey.rename(columns={'field_name':'concept_code',
                                  'field_type':'ans_type',
                                  'form_name':'survey',
                                  'field_label':'question',
                                  'branching_logic':'branch',
                                  'select_choices_or_calculations':'answer'})
# -

title=new_survey.survey[1]
data_storage_path='data_storage/'+str(title)



# +
# DataFrames for analysis

# Merge the DFs and designate the table where the codes are located(_merge).
compare_codes_df=pd.merge(historic,
                          new_survey,
                          how='outer',
                          on='concept_code',
                          indicator=True)

# Find all who's question,answer,and question/answer combination match/don't with another.
reused_codes=compare_codes_df[(compare_codes_df._merge=='both')]

# locate new all new questions
new_quesiton_codes=compare_codes_df[(compare_codes_df._merge=='right_only')]

historic_clean_for_separation=historic[['concept_code','answer','branch']]
new_survey_clean_for_separation=new_survey[['concept_code','answer','branch']]


# +
# Where codes from the historic surveys are reused in the new survey.

hqm=reused_codes[(reused_codes.question_x==reused_codes.question_y) & (reused_codes.ans_type_x!='text')]

hqmam=hqm[(hqm.answer_x==hqm.answer_y)]
          
hqmad=hqm[(hqm.answer_x!=hqm.answer_y) & (reused_codes.ans_type_y!='text')]
if len(hqmad)>0:
    hqmad["distance"] = hqmad.apply(lambda row: Levenshtein.ratio(str(row["answer_x"]), str(row["answer_y"])), axis=1)
    hqmad=hqmad.sort_values(by=['distance'], ascending=True)

hqd=reused_codes[(reused_codes.question_x!=reused_codes.question_y)]
if len(hqd)>0:
    hqd["distance"] = hqd.apply(lambda row: Levenshtein.ratio(str(row["question_x"]), str(row["question_y"])), axis=1)
    hqd=hqd.sort_values(by=['distance'], ascending=True)          
          
ham=reused_codes[(reused_codes.answer_x==reused_codes.answer_y)]
          
hamqm=ham[(ham.question_x==ham.question_y)]
          
hamqd=ham[(ham.question_x!=ham.question_y)]
if len(hamqd)>0:
    hamqd["distance"] = hamqd.apply(lambda row: Levenshtein.ratio(str(row["question_x"]), str(row["question_y"])), axis=1)
    hamqd=hamqd.sort_values(by=['distance'], ascending=True)          
          
had=reused_codes[(reused_codes.answer_x!=reused_codes.answer_y) & (reused_codes.ans_type_y!='text')]
if len(had)>0:
    had["distance"] = had.apply(lambda row: Levenshtein.ratio(str(row["answer_x"]), str(row["answer_y"])), axis=1)
    had=had.sort_values(by=['distance'], ascending=True)


# +
# Merge the DFs and designate the table where the codes are located(_merge).
code_qa_check_df=pd.merge(historic,new_survey,how='outer',on=['question','answer'],indicator=True)
same_qa=code_qa_check_df[(code_qa_check_df._merge=='both') 
                         & (code_qa_check_df.concept_code_x!=code_qa_check_df.concept_code_y)
                         & (code_qa_check_df.question!='Please specify.')]

code_q_check_df=pd.merge(historic,new_survey,how='outer',on='question',indicator=True)
same_q=code_q_check_df[(code_q_check_df._merge=='both') 
                       & (code_q_check_df.concept_code_x!=code_q_check_df.concept_code_y)
                       & (code_q_check_df.question!='Please specify.')]

code_a_check_df=pd.merge(historic,new_survey,how='outer',on='answer',indicator=True)
same_a=code_a_check_df[(code_a_check_df._merge=='both') 
                       & (code_a_check_df.concept_code_x!=code_a_check_df.concept_code_y) 
                       & (code_a_check_df.ans_type_x!='text') 
                       & (code_a_check_df.ans_type_y!='text') 
                       & (code_a_check_df.answer.str.len() > 0)]



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

historic_long_codes=all_historic_questions_and_answers[(all_historic_questions_and_answers.concept_code.str.len()>45)
                                                      |(all_historic_questions_and_answers.concept_code.str.len()>45)]


# +
# New survey cleaning
# Separate the answers and displays from eachother, and keep their association to their concept code.

# To Separate the answers from themselves
# Drop n/a to explode/stack
dd_answers_drop=new_survey_clean_for_separation[new_survey_clean_for_separation['answer'].notna()]
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
all_new_questions_and_answers=primary_clean.append(association_list, ignore_index=True,sort=True)


new_long_codes=all_new_questions_and_answers[(all_new_questions_and_answers.concept_code.str.len()>45)
                                            |(all_new_questions_and_answers.concept_code.str.len()>45)]
# -



# Merge the DFs and designate the table where the codes are located(_merge).
compare_answers_df=pd.merge(all_historic_questions_and_answers,
                            all_new_questions_and_answers,
                            how='outer',
                            on='concept_code',
                            indicator=True)

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

prints={'Priority':["Low",
                    "Low",
                    "Low",
                    "Low",
                    "Low",
                    "High",
                    "High",
                    "Low",
                    "Low",
                    "High",
                    'High',
                    'High',
                    'High',
                    'High',
                    'Low',
                    'Low',
                    'Low'],
    'Sheet Name': ["none",
                    "none",
                    "reused_codes",
                    "hqm",
                    "hqmam",
                    "hqmad",
                    "hqd",
                    "ham",
                    "hamqm",
                    "hamqd",
                    'had',
                    'same_qa',
                    'same_q',
                    'same_a',
                    'new_codes',
                    'historic_long_codes',
                    'new_long_codes'],
     'Description':["codes in each included survey",
                    "questions included in the new survey",
                    "questions found in the new survey that were in the survey bank",
                    "reused codes, where their questions also match",
                    "reused codes, where their questions match and answers match",
                    "reused codes, where their questions match and answers don't",
                    "reused codes, where their questions don't match",
                    "reused codes, where their answers match",
                    "reused codes, where their answers match and questions match",
                    "reused codes, where their answers match and questions don't",
                    "reused codes, where their answers don't match",
                    "questions and answers that match exactly, but don't have matching codes",
                    "questions that match exactly,but don't have matching codes",
                    "answers that match exactly, but don't have matching codes",
                    "codes(q&a) not present in the survey bank -new codes",
                    "historic codes that are over 45 characters long",
                    "new codes that are over 45 characters long"],
       'Occurances':[str(questions_in_each_historic_survey),
                    str(len(new_survey)),
                    str(len(reused_codes)),
                    str(len(hqm)),
                    str(len(hqmam)),
                    str(len(hqmad)),
                    str(len(hqd)),
                    str(len(ham)),
                    str(len(hamqm)),
                    str(len(hamqd)),
                    str(len(had)),
                    str(len(same_qa)),
                    str(len(same_q)),
                    str(len(same_a)),
                    str(len(all_new_codes)),
                    str(len(historic_long_codes)),
                    str(len(new_long_codes))]}
printss = pd.DataFrame(data=prints)

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
    'new_codes':all_new_codes,
    'historic_long_codes':historic_long_codes,
    'new_long_codes':new_long_codes
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
dup_code_series = historic.pivot_table(index=['concept_code'],aggfunc='size')
dup_code_answer=dup_code_series[dup_code_series >1]

#how many `questions` are duplicated in historic?
dup_question_series = historic.pivot_table(index=['question'],aggfunc='size')
dup_question_answer=dup_question_series[dup_question_series >1]

# To search in dfs for a value.
historic.dropna(inplace=True)
search=historic[historic['concept_code'].str.contains('secondcontact')]
search


new_survey.dropna(inplace=True)
search2=new_survey[new_survey['concept_code'].str.contains('ipaq_7')]
search2

# Visualize the location of all codes ('left_only'/'right_only'/'both')
table="left_only"
loc_of_codes=compare_codes_df[(compare_codes_df._merge==table)
                              & compare_codes_df['question_x'].str.contains('{search text here}')]
loc_of_codes=loc_of_codes.reset_index(drop=True)
print(f"codes exist in the \""+ table +"\" table: "+str(len(loc_of_codes[(loc_of_codes._merge== table)])))
loc_of_codes

