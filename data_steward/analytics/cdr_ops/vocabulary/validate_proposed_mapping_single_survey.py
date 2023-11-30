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

# # Purpose
# Validate proposed additions to the vocabulary. Odysseus will send CONCEPT and CONCEPT_RELATIONSHIP files that need to be reviewed. <br> 
# This notebook validates the files by: <br>
# * Creating mock CONCEPT and CONCEPT_RELATIONSHIP files using the current REDCap data dictionaries
# * Compairing the mock files against the proposed files.
#
# Concept creation and mapping practices are known to differ between modules. Varify anything out of the ordinary and create new checks when required.
#

# # Requirements
# Before running the notebook ensure all data has been updated recently.
#
# 1. In the test project the dataset redcap_surveys should have been updated recently. Tables 'data_dictionaries', 'field_annotations' and 'branching_logic' are required.
# 2. Odysseus provided files are placed in the `vocabulary/data_storage/odysseus_proposals` folder. Recommendation: Include a date in the file names. There will most likely be versions of the proposals. EX: `20230831_new_concepts.csv`, `20230831_new_relationships.csv`

# +
from google.cloud import bigquery
import pandas as pd
import datetime as dt
import numpy as np

pd.set_option('display.max_rows', None)

file_path = 'data_storage/odysseus_proposals/'

current_date = dt.datetime.now().strftime("%Y%m%d")

# file names of generated feedback tables
feedback_c = current_date + f'_feedback_concept.csv'
feedback_cr = current_date + f'_feedback_concept_relationship.csv'

# +
test_project = ''
redcap_dataset = '' # dataset containing the aggregate redcap survey tables
vocab_dataset = '' # latest vocabulary dataset

# insert module data manually. ie module concept_name : module concept_code
form_names = [
    {'behavioral_health_and_personality': 'bhp'},
    {'emotional_health_history_and_wellbeing': 'ehhwb'}
]

# odysseus proposal file names
concept_file_name = '20231128_new_concepts.csv'
concept_relationship_file_name = '20231128_new_relationships.csv'

# the concept_codes in this list will be removed from filters
ignore_list=['nan', 'pmi_dontknow', 'pmi_prefernottoanswer', 'record_id']

# +
CLIENT = bigquery.Client(project=test_project)

def execute(query, **kwargs):
    df = CLIENT.query(query, **kwargs).to_dataframe()
    return df


# -

def read_csv_file(file_path, table_file_name, custom_column_names=None):
    try:
        # Try reading with tab separator and automatically detect header
        df = pd.read_csv(file_path + table_file_name
                         , sep='\t'
                         , header='infer' if custom_column_names is None else None
                         , names=custom_column_names)
        print("File read successfully with tab separator.")
        return df
    except pd.errors.ParserError:
        # If reading with tab separator fails, try reading with comma separator and automatically detect header
        try:
            df = pd.read_csv(file_path + table_file_name
                             , sep=','
                             , header='infer' if custom_column_names is None else None
                             , names=custom_column_names)
            print("File read successfully with comma separator.")
            return df
        except pd.errors.ParserError:
            # If both attempts fail, print an error message
            print("Error: Unable to read the file. Please check the file format.")
            return None


# # Extract data

# ## odysseus files

# +
# import odysseus concept csv using the expected column header
concept_column_names = ['concept_id',
                        'concept_name',
                        'domain_id',
                        'vocabulary_id',
                        'concept_class_id',
                        'standard_concept',
                        'concept_code',
                        'valid_start_date',
                        'valid_end_date',
                        'invalid_reason']

ody_concept = read_csv_file(file_path, concept_file_name, concept_column_names)

# Sanity check
ody_concept.head()

# +
# import odysseus concept_relationship csv using the expected column header
cr_column_names = ['concept_id_1',
                   'concept_id_2',
                   'relationship_id',
                   'valid_start_date',
                   'valid_end_date',
                   'invalid_reason']

ody_cr = read_csv_file(file_path, concept_relationship_file_name, cr_column_names)

# Sanity check
ody_cr.head()
# -

# initial cleaning of odysseus concept file.
ody_concept['concept_code'] = ody_concept['concept_code'].str.lower()

# initial cleaning of odysseus concept_relationship file.
# The first three fields are needed at this time.
ody_cr = ody_cr.loc[:,['concept_id_1','concept_id_2','relationship_id']]

# ## current vocabulary

current_vocabulary = execute(f'''
SELECT *
FROM `{test_project}.{vocab_dataset}.concept`
WHERE vocabulary_id = 'PPI'
''')

# +
# initial cleaning of the current ppi vocabulary for analysis use.
current_vocabulary['concept_code']=current_vocabulary['concept_code'].str.lower()

# create filter_vocabulary. Use when joining via concept_code or concept_name
filter_vocabulary= current_vocabulary[['concept_code','concept_name']]

# create filter_vocabulary_ids. Use when joining via concept_code or concept_id
filter_vocabulary_ids= current_vocabulary[['concept_code','concept_id']]
# -

# # Create the mock tables

# ## Mock - concept

# +
build_concept_query = ('''
SELECT 
  0 as concept_id,
  concept_name,
  'Observation' as domain_id,
  'PPI' as vocabulary_id,
  concept_class_id,
  standard_concept,
  concept_code,
  DATE('1970-01-01') as valid_start_date,
  DATE('2099-12-31') as valid_end_date,
  NULL as invalid_reason,
FROM (SELECT 
  field_label as concept_name,
  "Question" as concept_class_id,
  "S" as standard_concept,
  LOWER(question_code) as concept_code

FROM (
  SELECT 
    DISTINCT question_code, field_label
  FROM `{project_id}.{redcap_dataset}.data_dictionaries`
  WHERE form_name = '{table}'
  AND field_type NOT IN ( 'descriptive')
  )

UNION ALL

SELECT 
  display as concept_name,
  "Answer" as concept_class_id,
  "S" as standard_concept, -- Assume standard. Will be looked at in a qc check. --
  LOWER(answer_code) as concept_code,
FROM (
  SELECT 
    DISTINCT answer_code, display
  FROM `{project_id}.{redcap_dataset}.data_dictionaries`
  WHERE form_name = '{table}'
  AND field_type NOT IN ( 'descriptive')  ))
''')

# For each module, create the mock concept data and append together.
sql_statements = []
for form_dict in form_names:
    for concept_name in form_dict:
        sql_statements.append(
        build_concept_query.format(project_id=test_project,
                                   redcap_dataset=redcap_dataset,
                                   table=concept_name)
        )
final_build_query = ' UNION ALL '.join(sql_statements)
build_concept = execute(final_build_query)
# -

# cleaning the generated mock concept data
mock_concept = build_concept.drop_duplicates(keep='first')
mock_concept

# +
# Create and insert the Module Concept
module_concepts = []

for item in form_names:
    for concept_name, concept_code in item.items():
        module_concept = {
            'concept_id': 0,
            'concept_name': concept_name,
            'domain_id': 'Observation',
            'vocabulary_id': 'PPI',
            'concept_class_id': 'Module',
            'standard_concept': 'S',
            'concept_code': concept_code,
            'valid_start_date': '1970-01-01',
            'valid_end_date': '2099-12-31',
            'invalid_reason': None
        }
    module_concepts.append(module_concept)
    
mock_concept = mock_concept.append(module_concepts, ignore_index=True)
mock_concept.tail()
# -

#final cleaning
mock_concept = mock_concept[~mock_concept['concept_code'].isin(ignore_list)]

mock_concept_w_ids = mock_concept.copy()

# ## Mock - concept_relationship
# questions to Topics <br>
# Topics to Module  <br>
# questions to Module  <br>
# parent to child questions - branching  <br>
# questions to Answers  <br>
# questions to standard  <br>
# answers to standard <br>
#
# The reverse mappings <br>

# +
# Create half of the CONCEPT_RELATIONSHIP table
# These are all relationships from parent to child and concept to standard.
build_concept_relationship_query = (f'''

SELECT -- questions to answers 1 --
  question_code as concept_code_1,
  answer_code as concept_code_2,
  'PPI parent code of' as relationship_id
FROM  (SELECT 
  DISTINCT question_code, answer_code, b.*
FROM `{test_project}.{redcap_dataset}.data_dictionaries` dd
LEFT JOIN `{test_project}.{redcap_dataset}.branching_logic` b
  ON dd.question_code = b.parent_question
WHERE dd.form_name = '{{full_name}}'
  AND field_type NOT IN ( 'descriptive')  )

UNION ALL

SELECT -- questions to answers 2 --
  question_code as concept_code_1,
  answer_code as concept_code_2,
  'Has answer (PPI)' as relationship_id
FROM (SELECT 
  DISTINCT question_code, answer_code, b.*
FROM `{test_project}.{redcap_dataset}.data_dictionaries` dd
LEFT JOIN `{test_project}.{redcap_dataset}.branching_logic` b
  ON dd.question_code = b.parent_question
WHERE dd.form_name = '{{full_name}}'
  AND field_type NOT IN ('descriptive')  )

UNION ALL

SELECT -- parent to child questions, branching logic --
  parent_question as concept_code_1,
  child_question as concept_code_2,
  'PPI parent code of' as relationship_id
FROM (SELECT 
  DISTINCT question_code, answer_code, b.*
FROM `{test_project}.{redcap_dataset}.data_dictionaries` dd
LEFT JOIN `{test_project}.{redcap_dataset}.branching_logic` b
  ON dd.question_code = b.parent_question
WHERE dd.form_name = '{{full_name}}'
  AND field_type NOT IN ('descriptive')  )

UNION ALL

SELECT -- Questions to Module --
  '{{short_name}}' as concept_code_1,
  question_code as concept_code_2,
  'PPI parent code of' as relationship_id
FROM (SELECT 
  DISTINCT question_code, answer_code, b.*
FROM `{test_project}.{redcap_dataset}.data_dictionaries` dd
LEFT JOIN `{test_project}.{redcap_dataset}.branching_logic` b
  ON dd.question_code = b.parent_question
WHERE dd.form_name = '{{full_name}}'
  AND field_type NOT IN ('descriptive')  )

UNION ALL

SELECT -- Questions to Standard --
  question_code as concept_code_1,
  question_code as concept_code_2,
  'Maps to' as relationship_id
FROM (SELECT 
  DISTINCT question_code, answer_code, b.*
FROM `{test_project}.{redcap_dataset}.data_dictionaries` dd
LEFT JOIN `{test_project}.{redcap_dataset}.branching_logic` b
  ON dd.question_code = b.parent_question
WHERE dd.form_name = '{{full_name}}'
  AND field_type NOT IN ('descriptive')  )

UNION ALL

SELECT -- Answers to Standard. Still assuming standard at this point. test_project later. --
  answer_code as concept_code_1,
  answer_code as concept_code_2,
  'Maps to' as relationship_id
FROM (SELECT 
  DISTINCT question_code, answer_code, b.*
FROM `{test_project}.{redcap_dataset}.data_dictionaries` dd
LEFT JOIN `{test_project}.{redcap_dataset}.branching_logic` b
  ON dd.question_code = b.parent_question
WHERE dd.form_name = '{{full_name}}'
  AND field_type NOT IN ('descriptive')  )

''')
sql_statements = []
for form_dict in form_names:
    for full_name, short_name in form_dict.items():
        sql_statements.append(
        build_concept_relationship_query.format(test_project=test_project,
                               redcap_dataset=redcap_dataset,
                               full_name=full_name,
                               short_name = short_name))
final_build_query = ' UNION ALL '.join(sql_statements)
build_cr = execute(final_build_query)
half_concept_relationship = build_cr.drop_duplicates(keep='first')

half_concept_relationship
# -

# # copy to make a base df for the reverse mappings
reverse_half_cr = half_concept_relationship.copy()
reverse_half_cr

# +
# create the reverse mappings
# These will be child to parent and standard to concept

# reverse concept_codes 1 and 2
reverse_half_cr= reverse_half_cr.rename(columns={'concept_code_1':'concept_code_2','concept_code_2':'concept_code_1' })

# reverse relationships
reverse_half_cr['relationship_id'] = np.where(reverse_half_cr['relationship_id'].str.contains('Maps to'),
                                                     'Mapped from', 
                                                     reverse_half_cr['relationship_id'])
reverse_half_cr['relationship_id'] = np.where(reverse_half_cr['relationship_id'].str.contains('PPI parent code of'),
                                                     'Has PPI parent code', 
                                                     reverse_half_cr['relationship_id'])
reverse_half_cr['relationship_id'] = np.where(reverse_half_cr['relationship_id'].str.contains('Has answer \(PPI\)'),
                                                     'Answer of (PPI)', 
                                                     reverse_half_cr['relationship_id'])

# +
# Append the relationships into one df
# view changes as sanity check

cr_dfs_list = [half_concept_relationship,reverse_half_cr]

concat_cr = pd.concat(cr_dfs_list, axis=0, ignore_index=True) 
print(len(concat_cr))

# remove any duplicates
mock_concept_relationship = concat_cr.drop_duplicates(keep='first', ignore_index=True).reset_index(drop=True)
print(len(mock_concept_relationship))


# final cleaning
mock_concept_relationship = mock_concept_relationship[~mock_concept_relationship['concept_code_1'].isin(ignore_list) &
                                                      ~mock_concept_relationship['concept_code_2'].isin(ignore_list)]

print(len(mock_concept_relationship))
# -

mock_cr_w_ids = mock_concept_relationship.copy()

# +
# replace the mock concept_ids with the ody provided ids
# mock concept relationship table id replacement

# merge with only few concept fields
slice_concept = ody_concept[['concept_code', 'concept_id']]

add_ids_cr_merge_half = mock_cr_w_ids.merge(slice_concept,
                        how = 'outer',
                        left_on = 'concept_code_1',
                        right_on = 'concept_code',
                        indicator='merge_concept_1')

add_ids_cr_merge = add_ids_cr_merge_half.merge(slice_concept,
                        how = 'outer',
                        left_on = 'concept_code_2',
                        right_on = 'concept_code',
                        indicator='merge_concept_2')

# -

full_relationships=add_ids_cr_merge.copy()

mock_fin = full_relationships[['concept_code_1','concept_code_2','concept_id_x','concept_id_y','relationship_id']]
mock_fin = mock_fin.rename(columns={'concept_id_x':'concept_id_1', 'concept_id_y':'concept_id_2'})
mock_fin

# # Concept table checks

# +
# replace the mock concept_ids with the ody provided ids
# mock concept table id replacement

add_ids_merge = mock_concept_w_ids.merge(ody_concept,
                    how = 'outer',
                    on = 'concept_code',
                    indicator=True)

# -

# mock concept complete 
concept_qc_base=add_ids_merge.copy()

# # manual edits to send back to odysseus
# Create a table that shows the status of each concept_code (accepted, concept_code_does not match dictionary, no matching concept_id)

# step 1 make a copy to work off of 
frank = ody_concept.copy()

# +
# Find missing Concepts.
# All concepts seen in the data dictionary should have a new record in the concept csv

# concept_qc_base is the aou generated concept file(left) with odysseus concept file(right)

missing_ids =concept_qc_base[concept_qc_base['_merge']=='left_only'].reset_index(drop=True)
missing_cut = missing_ids[['concept_code', 'concept_name_x']]
missing_cut = missing_cut.rename(columns={'concept_name_x':'concept_name'})
missing_cut['status']='no matching concept_id'

incorrect_ids =concept_qc_base[concept_qc_base['_merge']=='right_only']
incorrect_cut = incorrect_ids[[ 'concept_name_y']]
incorrect_cut = incorrect_cut.rename(columns={'concept_name_y':'concept_name'})
incorrect_cut['status']='concept_code does not match dictionary'

agreement_ids =concept_qc_base[concept_qc_base['_merge']=='both'].reset_index(drop=True)
agreement_cut = agreement_ids.loc[:,['concept_code']]
agreement_cut['status']='Accepted'



# +
# step 5 merge tables 
merge_agree = frank.merge(agreement_cut,
                         how = 'outer',
                         on = 'concept_code',
                         indicator=False)

merge_incorrect = merge_agree.merge(incorrect_cut,
                         how = 'outer',
                         on = 'concept_name',
                         indicator=False)

merge_missing = merge_incorrect.merge(missing_cut,
                         how = 'outer',
                         on = 'concept_name',
                         indicator=False)
# -

concept_w_status = merge_missing.copy()

# +
# create a single status after the merge. coalesce status
concept_w_status['status_comb'] = (concept_w_status['status'].fillna('') +
                 concept_w_status['status_x'].fillna('') +
                 concept_w_status['status_y'].fillna(''))


concept_w_status

# +
# look into these. They might join oddly. For example those with the same concept_name 
# sanity check

looksee = concept_w_status[(concept_w_status['status_comb'] != 'Accepted') &
                         (concept_w_status['status_comb'] != 'concept_code does not match dictionary') &
                         (concept_w_status['status_comb'] != 'no matching concept_id')]
looksee
# -

# final clean up for the status
concept_w_status = concept_w_status.drop(columns=['status_x','status_y','status'])
concept_w_status = concept_w_status.rename(columns={'concept_code_x':'concept_code','status_comb':'status','concept_code_y':'concept_code_lacking_id'})
concept_w_status = concept_w_status.sort_values(by='status', ascending=True)
concept_w_status =concept_w_status.reset_index(drop=True)

# +
# apply the ignore list 

concept_w_status = concept_w_status[~concept_w_status['concept_code_lacking_id'].isin(ignore_list)] 
concept_w_status=concept_w_status.drop_duplicates(ignore_index=True,keep='first')
concept_w_status
# -

# concept_w_status is all concept records(aou and ody) with their current status
concept_w_status

# it is possible that the concept code was not given an id from odysseus because it already exists in athena.
# look for concept codes in the file that are already in athena in two ways.
# concepts that are expected to be mapped by the dd (concept_code_lacking_id)
# concepts that were mapped by ody but already exist in athena - these should not be mapped again.
athena_join=concept_w_status.merge(filter_vocabulary,
                         how ='left',
                         left_on='concept_code_lacking_id',
                         right_on='concept_code'
                         )
athena_join2=athena_join.merge(filter_vocabulary,
                         how ='left',
                         left_on='concept_code_x',
                         right_on='concept_code'
                         )
# create a status column for ease of use
athena_join2['concept_code_exists_in_athena']=np.where((~athena_join2['concept_code_y'].isnull()) |
                                                      (~athena_join2['concept_code'].isnull()),
                                                     'True', 
                                                     'False')

# If concept_code_exists_in_athena = True and status = Accepted these need to go back to ody. They already have an ID in athena and should not be remapped.
# If concept_code_exists_in_athena = True and status = no_matching_concept_id these are known to already be mapped in Athena. Have broader conversation to make sure these are as they should be.
# this is just a view. Summary of what you can find in feedback_with_athena
already_exist_in_athena = athena_join2[athena_join2['concept_code_exists_in_athena'] == 'True']
already_exist_in_athena

# Concept check: Not mapped concepts, already mapped concepts, ids not unique

# +
feedback_with_athena= athena_join2[['concept_id',
                                   'concept_name_x',
                                   'domain_id',
                                   'vocabulary_id',
                                   'concept_class_id',
                                   'standard_concept',
                                   'concept_code_x',
                                   'valid_start_date',
                                   'valid_end_date',
                                   'invalid_reason',
                                   'concept_code_lacking_id',
                                   'status',
                                   'concept_code_exists_in_athena']]
feedback_with_athena=feedback_with_athena.rename(columns={'concept_name_x':'concept_name','concept_code_x':'concept_code' })
feedback_with_athena=feedback_with_athena.drop_duplicates(ignore_index=True,keep='first')

feedback_with_athena
feedback_with_athena.to_csv(f'data_storage/mapping_files/{feedback_c}')
# -

# ## concept check: concept_class_id

# Ignore module codes
# TODO: stop module codes from having Question concept_class_id in the mock concept df
# concept_qc_base is the aou generated concept file(left) with odysseus concept file(right)
class_ids_check = concept_qc_base[(concept_qc_base['concept_class_id_x']!=concept_qc_base['concept_class_id_y']) &
                                 (concept_qc_base['_merge']=='both')]
print(len(class_ids_check))

# ## concept check: vocabulary_id

# concept_qc_base is the aou generated concept file(left) with odysseus concept file(right)
# vocabulary_ids should match between files
vocabulary_ids_check = concept_qc_base[(concept_qc_base['vocabulary_id_x']!=concept_qc_base['vocabulary_id_y']) &
                                 (concept_qc_base['_merge']=='both')]
print(len(vocabulary_ids_check))


# ## concept check: standard status
#
# Odysseus will sometimes map concepts to standards in one of the last stages of concept creation. Most likely after AoU has approved the concepts(concept_codes/concept_names/concept_ids) in the concept file. This check is more of a reminder that the mappings to standard concepts should be checked. If the concept maps to itself ('Mapped from','Maps to') then it should be standard in the concept table. If it does not have those mappings it should be non-standard. 
#

# +
# concept_qc_base is the aou generated concept file(left) with odysseus concept file(right)
# whether a concept is standard varies. The aou generated file will always populate them as 'S' standards.
# visually check these. Questions have been expected as 'S' in the past. Answers may be non-standard.

# TODO these should be checked for mapping to other vocabularies in the CR table.

standards_check = concept_qc_base[(concept_qc_base['standard_concept_x']!=concept_qc_base['standard_concept_y']) &
                                 (concept_qc_base['_merge']=='both')]
print(len(standards_check))
# -

# ## TODO Add second standard check. Are the concepts marked standard mapped to themselves and are the non-standard ones mapped to another vocabulary.

# ## TODO Take a look at the non-standard to standard mappings. Visual check. Do they look correct.

# # Concept_relationship table checks

mock_vs_ody = mock_fin.merge(ody_cr,
                        how = 'outer',
                        left_on = ['concept_id_1','concept_id_2'],
                        right_on = ['concept_id_1','concept_id_2'],
                        indicator= True)
mock_vs_ody

cr_qc_base=mock_vs_ody.copy()

# these relationships were generated by aou and no relationship is found in the new ody file.
# if these relationships are not found in athena already. report to ody
missing_rel = cr_qc_base[cr_qc_base['_merge'] == 'left_only']
missing_rel=missing_rel.sort_values(by='concept_id_1', ascending=True).reset_index(drop=True)
missing_rel

# +
# relationships that ody has but were not generated by aou These would most likely be joining to standard concepts
added_rel = cr_qc_base[cr_qc_base['_merge'] == 'right_only']

added_rel=added_rel.sort_values(by='concept_id_1', ascending=True).reset_index(drop=True)

# -

#these relationships are in both files. Aou and Ody agree to add these relationships
approved_rel = cr_qc_base[cr_qc_base['_merge'] == 'both']
approved_rel=approved_rel.sort_values(by='concept_id_1', ascending=True).reset_index(drop=True)
approved_rel

# +
# sanity check pop up analysis. Many of the results above were linked to these two concept_ids which are a known issue for odysseus to fix.
# look at the ones that are not linked to the ids
id_list = [903079.0,903087.0]
issues_added = added_rel[
   ( ~added_rel['concept_id_1'].isin(id_list) )&
    (~added_rel['concept_id_2'].isin(id_list))
].reset_index(drop=True)
                         
issues_added

# +
# manual edits to send back to odysseus
# Create a table that shows the status of each concept_code (accepted, concept_code_does not match dictionary, no matching concept_id)
# -

# step 1 make a copy to work off of 
ody_cr_no_ids = ody_cr.copy()


# +
# merge with only few concept fields
# slice_concept = ody_concept[['concept_code', 'concept_id']] Used above

frank_merge_id_1 = ody_cr_no_ids.merge(slice_concept,
                        how = 'outer',
                        left_on = 'concept_id_1',
                        right_on = 'concept_id',
                        indicator=False)

frank_cr = frank_merge_id_1.merge(slice_concept,
                        how = 'outer',
                        left_on = 'concept_id_2',
                        right_on = 'concept_id',
                        indicator=False)
frank_cr = frank_cr.rename(columns={'concept_code_x':'concept_code_1','concept_code_y':'concept_code_2'})
frank_cr =frank_cr.drop(columns={'concept_id_x','concept_id_y'})
                                    
frank_cr       

# +
# it is possible that the concept code was not given an id from odysseus because it already exists in athena.
# look for concept ids in the file that are already in athena in two ways.
athena_join_cr=frank_cr.merge(filter_vocabulary_ids,
                         how ='left',
                         left_on='concept_id_1',
                         right_on='concept_id'
                         )
athena_join_cr2=athena_join_cr.merge(filter_vocabulary_ids,
                         how ='left',
                         left_on='concept_id_2',
                         right_on='concept_id'
                                
                         )
# create a status column to flag where codes already exist in athena.
athena_join_cr2['athena_status']=np.where((~athena_join_cr2['concept_code_x'].isnull()),
                                                     'concept_code_1 exists in athena',
                                                     '')
athena_join_cr2['athena_status']=np.where((~athena_join_cr2['concept_code_y'].isnull()),
                                                     'concept_code_2 exists in athena',
                                                     athena_join_cr2['athena_status'])
athena_join_cr2['athena_status']=np.where((~athena_join_cr2['concept_code_x'].isnull()) &
                                   (~athena_join_cr2['concept_code_y'].isnull()),
                                                     'both concept codes exist in athena',
                                                     athena_join_cr2['athena_status'])



# insert athena codes into concept_code fields
athena_join_cr2['concept_code_1']=np.where((~athena_join_cr2['concept_code_x'].isnull()),
                                                     athena_join_cr2['concept_code_x'], 
                                                     athena_join_cr2['concept_code_1'])
athena_join_cr2['concept_code_2']=np.where((~athena_join_cr2['concept_code_y'].isnull()),
                                                     athena_join_cr2['concept_code_y'], 
                                                     athena_join_cr2['concept_code_2'])

# drop unnecessary fields
athena_join_cr2 =athena_join_cr2.drop(columns={'concept_code_x','concept_code_y','concept_id_x','concept_id_y'})
athena_join_cr2
# -

# sanity check. At this point concept_codes should not be null
test =athena_join_cr2[(athena_join_cr2['concept_code_1'].isnull()) |
                  (athena_join_cr2['concept_code_2'].isnull())]
test

# step 2 create the table for accepted relationships
approved_rel_cut = approved_rel.loc[:, :'relationship_id_x']
approved_rel_cut['status']='Accepted'
approved_rel_cut=approved_rel_cut.rename(columns={'relationship_id_x':'relationship_id' })
approved_rel_cut

# step  3 create the table for missing relationships
missing_rel_cut = missing_rel.loc[:, : 'relationship_id_x']
missing_rel_cut['status']='aou generated might need to be added'
missing_rel_cut=missing_rel_cut.rename(columns={'relationship_id_x':'relationship_id' })
missing_rel_cut

# step 4 create the table for new ody relationships
added_rel_cut = added_rel.loc[:, ['concept_code_1','concept_code_2','concept_id_1','concept_id_2', 'relationship_id_y']]
added_rel_cut['status']='ody generated. manually review'
added_rel_cut=added_rel_cut.rename(columns={'relationship_id_y':'relationship_id' })
added_rel_cut

# +
#merge concept_relationships  statuses

# step 5 merge tables 
cr_merge_approved = athena_join_cr2.merge(approved_rel_cut,
                         how = 'outer',
                         left_on = ['concept_id_1','concept_id_2', 'relationship_id'],
                         right_on = ['concept_id_1','concept_id_2', 'relationship_id'],
                         indicator=False)

cr_merge_approved['concept_code_1'] = cr_merge_approved['concept_code_1_x'].combine_first(cr_merge_approved['concept_code_1_y'])
cr_merge_approved['concept_code_2'] = cr_merge_approved['concept_code_2_x'].combine_first(cr_merge_approved['concept_code_2_y'])
cr_merge_approved = cr_merge_approved.drop(columns={'concept_code_1_x','concept_code_1_y','concept_code_2_x','concept_code_2_y'})

cr_merge_added = cr_merge_approved.merge(added_rel_cut,
                         how = 'outer',
                         left_on = ['concept_id_1','concept_id_2', 'relationship_id'],
                         right_on = ['concept_id_1','concept_id_2', 'relationship_id'],
                         indicator=False)

cr_merge_added['concept_code_1'] = cr_merge_added['concept_code_1_x'].combine_first(cr_merge_added['concept_code_1_y'])
cr_merge_added['concept_code_2'] = cr_merge_added['concept_code_2_x'].combine_first(cr_merge_added['concept_code_2_y'])
cr_merge_added['status'] = cr_merge_added['status_x'].combine_first(cr_merge_added['status_y'])
cr_merge_added = cr_merge_added.drop(columns={'concept_code_1_x','concept_code_1_y','concept_code_2_x','concept_code_2_y','status_x','status_y'})

cr_merge_missing = cr_merge_added.merge(missing_rel_cut,
                         how = 'outer',
                         left_on = ['concept_id_1','concept_id_2', 'relationship_id'],
                         right_on = ['concept_id_1','concept_id_2', 'relationship_id'],
                         indicator=False)

cr_merge_missing['concept_code_1'] = cr_merge_missing['concept_code_1_x'].combine_first(cr_merge_missing['concept_code_1_y'])
cr_merge_missing['concept_code_2'] = cr_merge_missing['concept_code_2_x'].combine_first(cr_merge_missing['concept_code_2_y'])
cr_merge_missing['status'] = cr_merge_missing['status_x'].combine_first(cr_merge_missing['status_y'])
cr_merge_missing = cr_merge_missing.drop(columns={'concept_code_1_x','concept_code_1_y','concept_code_2_x','concept_code_2_y','status_x','status_y'})

cr_merge_missing
# -

base_cr_with_status = cr_merge_missing.copy()

# +
# When all status types are joined back to the odysseus file. Each row should have a status. This df should be empty

test = base_cr_with_status[(base_cr_with_status['status'] != 'Accepted') &
                         (base_cr_with_status['status'] != 'aou generated might need to be added') &
                         (base_cr_with_status['status'] != 'ody generated. manually review')]
test

# +
feedback_with_athena_cr= base_cr_with_status[['concept_id_1',
                                    'concept_id_2',
                                    'concept_code_1',
                                    'concept_code_2',
                                    'relationship_id',
                                    'athena_status',
                                    'status']]
feedback_with_athena_cr=feedback_with_athena_cr.drop_duplicates(ignore_index=True,keep='first')


feedback_with_athena_cr.to_csv(f'data_storage/mapping_files/{feedback_cr}')
# -


