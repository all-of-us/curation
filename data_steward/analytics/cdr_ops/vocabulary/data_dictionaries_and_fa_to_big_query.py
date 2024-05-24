# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.15.2
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# # Purpose
#  - Pulls all data dictionaries from REDCap
#  - Combines and cleans them.
#  - Exports data locally as well as into bigquery project
#  
#  # Updates
#  Add table for privacy rule check - field annotations
#  Use UPDATE instead of WRITE_TRUNCATE 
#  issues created by dd formatting ex extraconsent_agreetoconsent
#  add check that all FAs join to the updated data dictionaries. 

# # Request and combine the data
#  - requirement: config file with the data dictionaries to request selected
#  - requirement: local storage
#  - Improvment: Directly to df, instead of through local storage.
#  - Improvment: automate joining module concept_ids

# +
# parameters 
# insert 'yes' if you want to perform the action
update_individual_dd = 'no'
update_data_dictionaries = 'no'
update_field_annotations = 'no'
update_branching_logic = 'no'
print_dd_and_fa_to_csv = 'yes'

project_id =''
vocabulary_dataset =''

# +
import pandas as pd
from datetime import date
import numpy as np
import os
import glob
import logging
import requests
from google.cloud import bigquery
import pytz

# Set options
pd.set_option('display.max_colwidth',500)
pd.set_option('display.max_rows',500)
pd.options.mode.chained_assignment=None #default='warn'

# %run data_storage/resources/config.ipynb
# %run data_storage/resources/nb_utils.py

# +
# import path
dd_storage_path = 'data_storage/data_dictionary_storage'

# export path
dd_path=f'data_storage/exports/redcap_datadictionaries'+ str(date.today()) + '.csv'
fa_path=f'data_storage/exports/redcap_field_annotations'+ str(date.today()) + '.csv'

# +
CLIENT = bigquery.Client(project=project_id)

def execute(query, **kwargs):
    df = CLIENT.query(query, **kwargs).to_dataframe()
    return df


# -

def transfer_job(dataframe,table_id,job_config):
    df = CLIENT.load_table_from_dataframe(
    dataframe, table_id,job_config=job_config
)  # Make an API request.
# job.result()  # Wait for the job to complete.


def read_file(file_name, custom_column_names=None):
    encodings = ['utf-8', 'latin1', 'ISO-8859-1', 'cp1252'] 

    for encoding in encodings:
        try:
            # Try reading with tab separator and automatically detect header
            df = pd.read_csv(file_name, 
                             sep='\t', 
                             header='infer' if custom_column_names is None else None, 
                             names=custom_column_names, 
                             encoding=encoding)
            print(f'{file_name} File read successfully with tab separator using {encoding} encoding.')
            return df
        except (pd.errors.ParserError, UnicodeDecodeError):
            pass

        try:
            # If reading with tab separator fails, try reading with comma separator and automatically detect header
            df = pd.read_csv(file_name, 
                             sep=',', 
                             header='infer' if custom_column_names is None else None, 
                             names=custom_column_names, 
                             encoding=encoding)
            print(f'{file_name} File read successfully with comma separator using {encoding} encoding.')
            return df
        except (pd.errors.ParserError, UnicodeDecodeError):
            pass

    # If all attempts fail, print an error message
    print("Error: Unable to read the file. Please check the file format and encoding.")
    return None


# +
# Use the RedcapApi function to get all survey data dictionaries. Store them locally.
redcap = RedcapApi(config['api_url'])
projects = config.get('projects', {})

list_of_names=[]
for name, token in projects.items():
    csv_path=f'{dd_storage_path}/{name}.csv'
    print(name) # Test a bad response status code.
    metadata = redcap.download_metadata(token)
    
    with open(csv_path, 'w') as fp:
        fp.write(metadata)
        
    list_of_names.append(name)


# +
# read in the selected surveys and store them together
# dataframes_list = []

# for i in range(len(list_of_names)):
#     temp_df = pd.read_csv(dd_storage_path + '/' + list_of_names[i]+".csv")
#     dataframes_list.append(temp_df)

    
left_glob = glob.glob(os.path.join(dd_storage_path,"*.csv"))
left_files = []
rows_in_each_survey=[]

for file in left_glob:
    df = read_file(file)
    left_files.append(df)  
    rows_in_each_survey.append(len(file))
    
historic_surveys = pd.concat(left_files, axis=0, ignore_index=True)     
# -

# # Create 'data dictionaries' table (without concept_ids)

historic=historic_surveys.copy()

no_answers= historic[historic['select_choices_or_calculations'].isnull()]
no_answers['value_source_value']=np.nan
no_answers['display']=np.nan
normalized_no_answers =no_answers[['field_name',
                              'form_name',
#                               'section_header',
                              'field_type',
                              'field_label',
                              'field_note',
                              'text_validation_type_or_show_slider_number',
                              'text_validation_min',
                              'text_validation_max',
#                               'identifier',
                              'branching_logic',
                              'required_field',
                              'custom_alignment',
#                               'question_number',
#                               'matrix_group_name',
#                               'matrix_ranking',
                              'field_annotation',
#                               'select_choices_or_calculations',
                              'value_source_value',
                              'display']]

# Split answers from the list of answers.
historic['select_choices_or_calculations']=historic['select_choices_or_calculations'].str.split(r"|")
explode = historic.set_index(['field_name',
                              'form_name',
                              'section_header',
                              'field_type',
                              'field_label',
                              'field_note',
                              'text_validation_type_or_show_slider_number',
                              'text_validation_min',
                              'text_validation_max',
#                               'identifier',
                              'branching_logic',
                              'required_field',
                              'custom_alignment',
#                               'question_number',
#                               'matrix_group_name',
#                               'matrix_ranking',
                              'field_annotation'])['select_choices_or_calculations'].apply(pd.Series).stack()
explode=explode.reset_index()
explode2=explode.rename(columns={0:'select_choices_or_calculations'}).reset_index(drop=True)

step3 = explode2.copy()

# +
# Split the displays from the list of answers/displays. 
step3.select_choices_or_calculations=step3.select_choices_or_calculations.str.split(r",",1)
# step3[['select_choices_or_calculations','display']] = step3['select_choices_or_calculations'].str.split(r",",index= step3.index,expand=True)
step4=pd.DataFrame(step3["select_choices_or_calculations"].to_list(), columns=['value_source_value','display'])
all_together_now=pd.concat([step3, step4],1)
all_together_now=all_together_now.drop(columns={'level_13','select_choices_or_calculations'})

normalized_all_together =all_together_now[['field_name',
                              'form_name',
#                               'section_header',
                              'field_type',
                              'field_label',
                              'field_note',
                              'text_validation_type_or_show_slider_number',
                              'text_validation_min',
                              'text_validation_max',
#                               'identifier',
                              'branching_logic',
                              'required_field',
                              'custom_alignment',
#                               'question_number',
#                               'matrix_group_name',
#                               'matrix_ranking',
                              'field_annotation',
#                               'select_choices_or_calculations',
                              'value_source_value',
                              'display']]

# -

all_merged=normalized_all_together.append(normalized_no_answers, ignore_index=True)

# +
all_merged=all_merged.rename(columns={'field_name':'observation_source_value'})

all_merged['observation_source_value']=all_merged['observation_source_value'].str.lower()
all_merged['value_source_value']=all_merged['value_source_value'].str.lower()

all_merged['value_source_value']=all_merged['value_source_value'].str.strip()
all_merged['observation_source_value']=all_merged['observation_source_value'].str.strip().replace(",",'', regex=True)

all_merged['field_label']=all_merged['field_label'].str.strip().replace(",",'', regex=True).replace("'",'', regex=True).replace('"','', regex=True).replace(".\r\n\r\n",'', regex=True).replace("'",'', regex=True).replace("</i>?",'', regex=True).replace("\r\n\t",'', regex=True).replace("\r\n",'', regex=True)
all_merged['display']=all_merged['display'].str.strip().replace(",",'', regex=True).replace('"','', regex=True).replace("'",'', regex=True).replace(".\r\n\r\n",'', regex=True)
# all_merged['field_annotation']=all_merged['field_annotation'].str.strip().replace(",",'', regex=True).replace('"','', regex=True).replace("'",'', regex=True).replace(".\r\n\r\n",'', regex=True).replace("\r\n",'', regex=True)
all_merged['field_note']=all_merged['field_note'].str.strip().replace(",",'', regex=True).replace('"','', regex=True).replace("'",'', regex=True).replace(".\r\n\r\n",'', regex=True).replace("\r\n",'', regex=True)
all_merged=all_merged.applymap(str)

# +
# holds the concept_ids for the surveys. Create this by joining to the vocab eventually.
data = [['ehr_consent', 1586098],
        ['family_health_history', 43528698],
        ['the_basics', 1586134],
        ['healthcare_access_and_utilization', 43528895],
        ['life_functioning_survey', 705190],
        ['lifestyle', 1585855],
        ['physical_measurements', 1000001],
        ['social_determinants_of_health_english', 40192389],
        ['personal_and_family_health_history', 1740639],
        ['new_year_survey_on_covid19_vaccines', 1741006],
        ['winter_minute_survey_on_covid19_vaccines', 765936],
        ['overall_health', 1585710],
        ['fall_minute_survey_on_covid19_vaccines', 905055],
        ['personal_medical_history', 43529712],
        ['summer_minute_survey_on_covid19_vaccines', 905047],
        ['november_covid19_participant_experience_cope_surve', 1333342],
        ['december_covid19_participant_experience_cope_surve', 1333342],
        ['covid19_participant_experience_cope_survey', 1333342],
        ['june_covid19_participant_experience_cope_survey', 1333342],
        ['july_covid19_participant_experience_cope_survey', 1333342],
        ['consent_for_dna_results', 903505],
        ['wear_consent', 2100000011],
        ['wear_consent_ptsc', 2100000012],
        ['behavioral_health_and_personality', 0],
        ['emotional_health_history_and_well_being', 0],
        ['english_exploring_the_mind_consent_form', 0],
        ['pediatric_environmental_health', 0],
        ['pediatric_basics', 0],
        ['pediatric_overall_health', 0]
        
       ]

module_ids = pd.DataFrame(data, columns=['form_name', 'module_concept_id'])
# -

# join the module_ids with the data
all_merged_1 = all_merged.merge(module_ids,
                               how = 'left',
                               on = 'form_name'
                               ).drop_duplicates(keep='first')


"""
# This is a hint when eventually trying to troubleshoot the primary_consent issue.
e = all_merged[all_merged.form_name == 'primary_consent']
# e = e.loc[1:,'form_name']
e = e['form_name'].head(1)
"""

# # Clean field annotations 

# narrow the field to focus on field annotations
fa1= all_merged_1[(all_merged_1['field_annotation']!='nan') & 
                 (all_merged_1['field_annotation'].str.contains('='))]
fa1=fa1[['field_annotation']]
fa2=fa1.copy().reset_index(drop=True)

fa2['code_changes'] = fa2['field_annotation'].str.split(r"\r\n")
fa2 = fa2.set_index(['field_annotation'])['code_changes'].apply(pd.Series).stack()
fa2 = fa2.reset_index()
fa2=fa2.rename(columns={0:'code_changes'})
fa3=fa2.copy()

# +
# Initial weeding out and cleaning
remove_strings_list = ['@NONEOFTHEABOVE=','CONTROLLED_QUESTION_SUPPRESSED','@NONEOFTHEABOVE=','@noneoftheabove =','REGISTERED_ANSWERS_BUCKETED','CONTROLLED_ANSWER_SUPPRESSED','CONTROLLED_ANSWER_SUPPRESSED=','REGISTERED_QUESTION_SUPPRESSED','PMI_PreferNotToAnswer =']

for string in remove_strings_list:
    fa3['code_changes'] = fa3['code_changes'].str.replace(string, '')

fa4= fa3[(fa3['code_changes'].str.contains('='))].reset_index(drop=True)


# +
# separate pmi_codes from short_codes
# One field annotation has a bad format. hence 'broken' fab5 - fab7 for the fix.
fa4['here'] = fa4['code_changes'].str.split(r"=")
fa4_split = pd.concat([fa4,fa4['here'].apply(pd.Series)],axis=1)
fa5 =fa4_split.drop(columns = {'level_1','code_changes','here'})
fa5 = fa5.rename(columns={0:'pmi_code',1:'short_code',2:'broken'})

# base copy to append to later
base=fa5.copy() 

if 'broken' in base.columns:
    base=base.drop(columns = {'broken'})
    
    # clean 'broken' field_annotations
    fa6 = fa5.replace(to_replace='None', value=np.nan).dropna().drop_duplicates(keep='first')
    fa6['here'] = fa6['short_code'].str.split(r",")
    fa6_split = pd.concat([fa6,fa6['here'].apply(pd.Series)],axis=1)
    fa6_split = fa6_split.rename(columns={0:'one',1:'two'})
    fa6_1 =fa6_split[['field_annotation','pmi_code','one']]
    fa6_1 = fa6_1.rename(columns={'one':'short_code'})
    fa6_2 =fa6_split[['field_annotation','two',"broken"]]
    fa6_2 = fa6_2.rename(columns={'two':'pmi_code','broken':'short_code'})
    
    # append the corrected rows to the clean base 
    list = [fa6_1, fa6_2]
    fa7=base.append(list) 
else:
    fa7=base

# +
# strip punctuation
strip_list = [',',', ',]

for string in strip_list:
    fa7['pmi_code'] = fa7['pmi_code'].str.strip(string)
    fa7['short_code'] = fa7['short_code'].str.strip(string)
    
# remove problem strings    
replace_list = ['PMI_None,PMI_DontKnow,PMI_PreferNotToAnswer,','MentalHealthCondition_NoMentalHealthSubstanceUse,','InfectiousDiseaseCondition_NoInfectiousDisease,']    

for string in replace_list:
    fa7['pmi_code'] = fa7['pmi_code'].str.replace(string, '')

# drop duplicates
fa8 = fa7[fa7['pmi_code']!= ''].drop_duplicates(keep='first').reset_index(drop=True)


# filter out problem rows
filter_list = ['DaughterOtherHealthCondition_ReactToAnesthesia,GrandparentOtherHealthCondition_ReactionsToAnesthesia','REGISTERED_ANSWER_SUPPRESSED','@NONEOFTHEABOVE','If you get sick or have an accident, how worried are you that you will be able to pay your medical bills? Are you very worried, somewhat worried, or not at all worried?']

for string in filter_list:
    fa8 = fa8[fa8['pmi_code'] != string]

# This was separate for some reason. might check on if it can be added to the for loop.
fa8 = fa8[fa8['short_code'] != 'DaughterOtherHealthCondition_ReactToAnesthesia, GrandparentOtherHealthCondition_ReactionsToAnesthesia']

# make everything lower           
fa8 = fa8.apply(lambda x: x.astype(str).str.lower())    
fa8 = fa8.apply(lambda x: x.astype(str).str.strip())    
# field annotations are clean! fa8
# -

# # Clean branching logic

q1 = all_merged_1[all_merged_1['branching_logic'] == 'nan']
q2 = all_merged_1[all_merged_1['branching_logic'] != 'nan']
q2 = q2[['form_name','observation_source_value','branching_logic']]


q2['here'] = q2['branching_logic'].str.split(r" or ")
q2_split = pd.concat([q2,q2['here'].apply(pd.Series)],axis=1)
q2_2 =q2_split.drop(columns = {'branching_logic','here'})

# +
bucket = pd.DataFrame()
for i in range(len(q2_2.columns) - 3):
    base = q2_2[['form_name','observation_source_value', i]]
    base = base.rename(columns={i:'branching_logic'})
    base = base[base['branching_logic'].notna()]
    bucket = bucket.append(base)
                
                
bucket = bucket.drop_duplicates(keep='first').reset_index(drop=True)

# +
bucket['here'] = bucket['branching_logic'].str.split(r" = ")
branching_split = pd.concat([bucket,bucket['here'].apply(pd.Series)],axis=1)
branching_2 =branching_split.drop(columns = {'branching_logic','here'})

if 'broken' in branching_2.columns:
    branching_2 =branching_split.drop(columns = {2}) #eventually add a step for 2
else:
    pass
# fa5 = fa5.rename(columns={0:'pmi_code',1:'short_code',2:'broken'})

# +
half1=branching_2[branching_2[1] != '1']
half1[0]= half1[0].str.strip('[,]')
half1= half1.rename(columns={'observation_source_value':'child_question',0:'parent_question',1:'parent_value'}) # named in the same way as ppi_branching files

# make everything lower           
half1 = half1.apply(lambda x: x.astype(str).str.lower()) 

# start here. rename columns and this is the file
# -

# at some point, include these
half2=branching_2[branching_2[1] == '1']
if len(half2 > 0):
    half2=half2.drop(columns={1})
    half2[0.5] = half2[0].str.split(r"(")
    half2=half2.rename(columns={0:'dele'})
    half2_split = pd.concat([half2,half2[0.5].apply(pd.Series)],axis=1)
    branching_3 =half2_split.drop(columns = {'dele',0.5}) 
    branching_3 = branching_3.rename(columns={'observation_source_value':'child_question',0:'parent_question',1:'parent_value'})
    
    branching_3['parent_question']= branching_3['parent_question'].str.replace('[','')
    branching_3['parent_value']= branching_3['parent_value'].str.replace(']','')
    branching_3['parent_value']= branching_3['parent_value'].str.replace(')','')
    
    # make everything lower           
    branching_3 = branching_3.apply(lambda x: x.astype(str).str.lower()) 

    branching_merge_list=[half1,branching_3]
    better_branching=  pd.concat(branching_merge_list, axis=0, ignore_index=True)
    better_branching = better_branching.drop_duplicates(keep='first')
else:
    better_branching=half1.copy()
    better_branching = better_branching.drop_duplicates(keep='first')

# # show the problem! look into these. These are just really long conditions. figure out how to separate them properly.
# # b = branching_split[branching_split[2].notna()]
#

# # Add FAs and concept_ids to data dictionaries. 
# Add concept_ids
# Add field annotations. Joining pmi_code to short code. Using short code to add concept_ids
#

# Get ppi concepts
vocab=execute(f'''
SELECT LOWER(concept_code) as concept_code,
concept_id
FROM `{project_id}.{vocabulary_dataset}.concept`
WHERE vocabulary_id = 'PPI'
''')


# +
# Get a column containing the short code that joins to either the question or answer. 
# A q/a in a pair could both have a short code. Need 2 columns.

# make everything lower           
fa8 = fa8.apply(lambda x: x.astype(str).str.lower()) 

# add field annotation questions
fa_questions = fa8[['pmi_code','short_code']]
fa_questions = fa_questions.rename(columns={'short_code':'question_short_code'})


fa_answers = fa8[['pmi_code','short_code']]
fa_answers = fa_answers.rename(columns={'short_code':'answer_short_code'})

agg_with_updates_add_q_fas = all_merged_1.merge(fa_questions,
                                      how = 'left',
                                      left_on = all_merged_1['observation_source_value'],
                                      right_on = fa_questions['pmi_code']
                                     ).drop(['key_0','pmi_code'], axis=1)
agg_with_updates_add_q_fas



# add field annotation answers
fa_answers = fa8[['pmi_code','short_code']]
fa_answers = fa_answers.rename(columns={'short_code':'answer_short_code'})

agg_with_updates_add_a_fas = agg_with_updates_add_q_fas.merge(fa_answers,
                                      how = 'left',
                                      left_on = agg_with_updates_add_q_fas['value_source_value'],
                                      right_on = fa_answers['pmi_code']
                                     ).drop(['key_0','pmi_code'], axis=1)

# -

updates1 = agg_with_updates_add_a_fas.copy()
# ends with all_merged_1 with two more columns. 'question_short_code' and 'answer_short_code'

# +
# Add question concept_ids. Join once to the observation_source_values then to the fa. Then combine into one column each for q and a.
updates_q_ids = updates1.merge(vocab,
                            how = 'left',
                            left_on = updates1['observation_source_value'],
                            right_on = vocab['concept_code']
                            ).drop(['key_0','concept_code'], axis=1).rename(columns={'concept_id':'q_concept_id'})

updates_faq_ids = updates_q_ids.merge(vocab,
                            how = 'left',
                            left_on = updates_q_ids['question_short_code'],
                            right_on = vocab['concept_code']
                            ).drop(['key_0','concept_code'], axis=1).rename(columns={'concept_id':'faq_concept_id'})



# Add question concept_ids. Join once to the observation_source_values then to the fa. Then combine into one column each for q and a.
updates_a_ids = updates_faq_ids.merge(vocab,
                            how = 'left',
                            left_on = updates_faq_ids['value_source_value'],
                            right_on = vocab['concept_code']
                            ).drop(['key_0','concept_code'], axis=1).rename(columns={'concept_id':'a_concept_id'})

updates_all_ids = updates_a_ids.merge(vocab,
                            how = 'left',
                            left_on = updates_a_ids['answer_short_code'],
                            right_on = vocab['concept_code']
                            ).drop(['key_0','concept_code'], axis=1).rename(columns={'concept_id':'faa_concept_id'})



# +
#combine the question concept_ids
updates_all_ids['question_concept_id'] = updates_all_ids.apply(
    lambda row: row['q_concept_id'] if pd.isna(row['faq_concept_id']) else row['faq_concept_id'],
    axis=1
)

# combine the answer concept_ids
updates_all_ids['answer_concept_id'] = updates_all_ids.apply(
    lambda row: row['a_concept_id'] if pd.isna(row['faa_concept_id']) else row['faa_concept_id'],
    axis=1
)
# drop duplicates created when adding the field annotations.
updates_all_ids =updates_all_ids.drop_duplicates(keep = 'first')

data_dictionaries_with_ids = updates_all_ids.copy()
# -

e = data_dictionaries_with_ids[data_dictionaries_with_ids.duplicated(keep = 'first')]
e

# # Last minute cleaning
# Rename columns, etc.
#
# put validation here. 
#
# looking for:
# expected surveys
# Correct number of fields.
# Duplicates
# Other problems that may arise when adding a new survey to the list.

data_dictionaries_clean = data_dictionaries_with_ids.rename(columns={'observation_source_value':'question_code','value_source_value':'answer_code'})


# view dds
data_dictionaries_clean[500:]

# view fas
fa8

# view branching
better_branching

# # Data Transfers

# GCP set-up for data dictionary transfer
if update_data_dictionaries == 'yes':   
    dd_dataframe = pd.DataFrame(
        data_dictionaries_clean,
        # In the loaded table, the column order reflects the order of the
        # columns in the DataFrame.
        columns=[
            'question_code',
            'question_concept_id',
            'form_name',
            'module_concept_id',
            'field_type',
            'field_label',
            'field_note',
            'text_validation_type_or_show_slider_number',
            'text_validation_min',
            'text_validation_max',
            'branching_logic',
            'required_field',
            'custom_alignment',
            'field_annotation',
            'answer_code',
            'answer_concept_id',
            'display',
            'question_short_code',
            'q_concept_id',
            'faq_concept_id',
            'answer_short_code',
            'a_concept_id',
            'faa_concept_id'
            
        ])

    dd_job_config = bigquery.LoadJobConfig(
        # Specify a (partial) schema. All columns are always written to the
        # table. The schema is used to assist in data type definitions.
        schema=[
            bigquery.SchemaField("question_code", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("question_concept_id", bigquery.enums.SqlTypeNames.INTEGER),
            # question_code joined to the current vocabulary. if question has a short code the concept_id will be that of the short code.
            # where faq_concept_id is null, use q_concept_id
            bigquery.SchemaField("form_name", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("module_concept_id", bigquery.enums.SqlTypeNames.INTEGER),
            bigquery.SchemaField("field_type", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("field_label", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("field_note", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("text_validation_type_or_show_slider_number", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("text_validation_min", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("text_validation_max", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("branching_logic", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("required_field", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("custom_alignment", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("field_annotation", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("answer_code", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("answer_concept_id", bigquery.enums.SqlTypeNames.INTEGER),
            # answer_code joined to the current vocabulary. if answer has a short code the concept_id will be that of the short code.
            # where faa_concept_id is null, use a_concept_id
            bigquery.SchemaField("display", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("question_short_code", bigquery.enums.SqlTypeNames.STRING),
            # using the field annotaions short codes are joined to questions using pmi_code. If joins, the short code is here.
            bigquery.SchemaField("q_concept_id", bigquery.enums.SqlTypeNames.INTEGER),
            # join vocabulary to every original question_code. If null, it is possible these would have a short_code.
            bigquery.SchemaField("faq_concept_id", bigquery.enums.SqlTypeNames.INTEGER),
            # join vocabulary to every question short code.
            bigquery.SchemaField("answer_short_code", bigquery.enums.SqlTypeNames.STRING),
            # using the field annotaions short codes are joined to answers using pmi_code. If joins, the short code is here.
            bigquery.SchemaField("a_concept_id", bigquery.enums.SqlTypeNames.INTEGER),
            # join vocabulary to every original answer_code. If null, it is possible these would have a short_code.
            bigquery.SchemaField("faa_concept_id", bigquery.enums.SqlTypeNames.INTEGER)
            # join vocabulary to every answer short code.
                    ],
        write_disposition="WRITE_TRUNCATE",
    )

    transfer_job(dd_dataframe,data_dictionary_table,dd_job_config)
    print('updated_data_dictionaries')
else:
    pass

# +
# GCP set-up for data dictionary transfer

# This broke when I added primary_consent. initial issue with line 58 possibly still the problem. 
# could be the updated transfer function but the other transfers are working so idk..

if update_individual_dd == 'yes':
    for dd in dataframes_list:
#         dd['text_validation_min']=dd['text_validation_min'].replace('-',' ').astype(str)
#         dd['text_validation_max']=dd['text_validation_max'].replace('-',' ').astype(str)
        ind_dd_dataframe = pd.DataFrame(
                                        dd,
                                        # In the loaded table, the column order reflects the order of the
                                        # columns in the DataFrame.
                                        columns=[
                                            'field_name',
                                            'form_name',
                                            'section_header',
                                            'field_type',
                                            'field_label',
                                            'select_choices_or_calculations',
                                            'field_note',
                                            'text_validation_type_or_show_slider_number',
#                                             'text_validation_min',
#                                             'text_validation_max',
                                            'identifier',
                                            'branching_logic',
                                            'required_field',
                                            'custom_alignment',
                                            'question_number',
                                            'matrix_group_name',
                                            'matrix_ranking',
                                            'field_annotation'
                                        ])

        ind_dd_job_config = bigquery.LoadJobConfig(
                                            # Specify a (partial) schema. All columns are always written to the
                                            # table. The schema is used to assist in data type definitions.
                                            schema=[
                                                bigquery.SchemaField("field_name", bigquery.enums.SqlTypeNames.STRING),
                                                bigquery.SchemaField("form_name", bigquery.enums.SqlTypeNames.STRING),
                                                bigquery.SchemaField("section_header", bigquery.enums.SqlTypeNames.STRING),
                                                bigquery.SchemaField("field_type", bigquery.enums.SqlTypeNames.STRING),
                                                bigquery.SchemaField("field_label", bigquery.enums.SqlTypeNames.STRING),
                                                bigquery.SchemaField("select_choices_or_calculations", bigquery.enums.SqlTypeNames.STRING),
                                                bigquery.SchemaField("field_note", bigquery.enums.SqlTypeNames.STRING),
                                                bigquery.SchemaField("text_validation_type_or_show_slider_number", bigquery.enums.SqlTypeNames.STRING),
#                                                 bigquery.SchemaField("text_validation_min", bigquery.enums.SqlTypeNames.STRING),
#                                                 bigquery.SchemaField("text_validation_max", bigquery.enums.SqlTypeNames.STRING),
                                                bigquery.SchemaField("identifier", bigquery.enums.SqlTypeNames.STRING),
                                                bigquery.SchemaField("branching_logic", bigquery.enums.SqlTypeNames.STRING),
                                                bigquery.SchemaField("required_field", bigquery.enums.SqlTypeNames.STRING),
                                                bigquery.SchemaField("custom_alignment", bigquery.enums.SqlTypeNames.STRING),
                                                bigquery.SchemaField("question_number", bigquery.enums.SqlTypeNames.STRING),
                                                bigquery.SchemaField("matrix_group_name", bigquery.enums.SqlTypeNames.STRING),
                                                bigquery.SchemaField("matrix_ranking", bigquery.enums.SqlTypeNames.STRING), 
                                                bigquery.SchemaField("field_annotation", bigquery.enums.SqlTypeNames.STRING),
                                            ],
                                            write_disposition="WRITE_TRUNCATE",
                                    )
       
        table = test_dataset + dd.loc[1,'form_name']
#         table = str(dd['form_name'].head(1))
        print(table)
        transfer_job(ind_dd_dataframe,table,ind_dd_job_config)
    else:
        pass
# -

# GCP set-up for field_annotation transfer
if update_field_annotations == 'yes':    
    fa_dataframe = pd.DataFrame(
                            fa8,
                            # In the loaded table, the column order reflects the order of the
                            # columns in the DataFrame.
                            columns=[
                                'field_annotation',
                                'pmi_code',
                                'short_code'
                            ])

    fa_job_config = bigquery.LoadJobConfig(
                            # Specify a (partial) schema. All columns are always written to the
                            # table. The schema is used to assist in data type definitions.
                            schema=[
                                bigquery.SchemaField("field_annotation", bigquery.enums.SqlTypeNames.STRING),
                                bigquery.SchemaField("pmi_code", bigquery.enums.SqlTypeNames.STRING),
                                bigquery.SchemaField("short_code", bigquery.enums.SqlTypeNames.STRING),       
                            ],
                            write_disposition="WRITE_TRUNCATE",
                        )
    transfer_job(fa_dataframe,field_annotation_table,fa_job_config)
    print('updated_field_annotations')
else:
    pass

# GCP set-up for branching_logic transfer
if update_branching_logic == 'yes':
    bl_dataframe = pd.DataFrame(
                                better_branching,
                                # In the loaded table, the column order reflects the order of the
                                # columns in the DataFrame.
                                columns=[
                                    'form_name',
                                    'child_question',
                                    'parent_question',
                                    'parent_value'
                                ])

    bl_job_config = bigquery.LoadJobConfig(
                                # Specify a (partial) schema. All columns are always written to the
                                # table. The schema is used to assist in data type definitions.
                                schema=[
                                    bigquery.SchemaField("form_name", bigquery.enums.SqlTypeNames.STRING),
                                    bigquery.SchemaField("child_question", bigquery.enums.SqlTypeNames.STRING),
                                    bigquery.SchemaField("parent_question", bigquery.enums.SqlTypeNames.STRING),
                                    bigquery.SchemaField("parent_value", bigquery.enums.SqlTypeNames.STRING),       
                                ],
                                write_disposition="WRITE_TRUNCATE",
                            )

    transfer_job(bl_dataframe,branching_logic_table,bl_job_config)
    print('updated_branching_logic')
else:
    pass

# print to csv
if print_dd_and_fa_to_csv == 'yes':
    data_dictionaries_clean.to_csv(dd_path,index=False)
    fa8.to_csv(fa_path,index=False)
else:
    pass


