#!/usr/bin/env python
# coding: utf-8

# # REQUIREMENTS
# - Replace the ```observation_source_value``` and ```observation_source_concept_id``` for all records with
# ```observation_source_value = HealthInsurance_InsuranceTypeUpdate (ID 43528428, from The Basics)``` with the
# ```observation_source_value``` and ```observation_source_concept_ids``` for records with
# ```observation_source_value = Insurance_InsuranceType (ID 1384450, from HCAU)```.
# 
# 
# - Map the [HCAU] field values to the corresponding [The Basics] fields when replacing.
# If there are no values in the [HCAU] fields, set [The Basics] fields to NULL.

# ## Set up 


# Load Libraries
from __future__ import print_function
from termcolor import colored
import pandas as pd


PROJECT_ID = ''
DATASET_ID = ''

# read as csv the list of PIDS where data needs to be overwritten
AC70_pids = pd.read_csv("AC70_PIDs.csv")

int(AC70_pids.iloc[1, 0][1:])

# removing the 'p' before the ids

AC70_pids["pid"] = None
for p in range(len(AC70_pids)):
    AC70_pids.iloc[p, 1] = int(AC70_pids.iloc[p, 0][1:])

# ## Obtaining dataframes to use in the SQL query
# - ```obs_pids_notin_list``` is a dataframe of person_ids in ```AC70_pids``` that ***are not** in the
#  observation table when observation_source_concept_id = 43528428. For these, we will replace the
# corresponding fields in the observation table with NULL--> see below ```update1_observation_table```
#
# - ```obs_pids_in_list``` is a dataframe of person_ids in ```AC70_pids``` that ***are*** in the
# observation table when observation_source_concept_id = 43528428. For these, we will replace the
# corresponding fields in the observation table with hcau fields (observation_source_concept_id = 1384450)
# --> see below ```update2_observation_table```


obs_overwrite = pd.read_gbq('''
SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.observation` o WHERE o.observation_source_concept_id = 43528428 
'''.format(PROJECT_ID=PROJECT_ID, DATASET_ID=DATASET_ID),
           dialect="standard")


obs_pids_notin_list = [int(x) for x in AC70_pids['pid'] if x not in obs_overwrite['person_id']]
obs_pids_notin_list = tuple(obs_pids_notin_list)

obs_pids_in_list = [int(x) for x in AC70_pids['pid'] if x in obs_overwrite['person_id']]


print((colored("This shows that none of person_ids in [AC70_pids] \n are in the observation table "
              "with observation_source_concept_id = 1384450 table).They are not in the hcau table either.", 'green')))


# # THESE ARE THE TWO QUERIES THAT WILL UPDATE THE FIELDS TO HCAU FIELDS-


update1_observation_table = pd.read_gbq('''

UPDATE `{PROJECT_ID}.{DATASET_ID}.observation` 
    SET  observation_id = NULL,
         person_id = person_id,
         observation_concept_id = NULL,
         observation_date = NULL,
         observation_datetime = NULL,
         observation_type_concept_id = NULL,
         value_as_number = NULL,
         value_as_string = NULL,
         value_as_concept_id = NULL,
         qualifier_concept_id = NULL,
         unit_concept_id = NULL,
         provider_id = NULL,
         visit_occurrence_id = NULL,
         observation_source_value = NULL,
         observation_source_concept_id = NULL,
         unit_source_value = NULL,
         qualifier_source_value = NULL,
         value_source_concept_id = NULL,
         value_source_value = NULL,
         questionnaire_response_id = NULL
    WHERE observation_source_concept_id = 43528428
    AND person_id IN {pids}'''.format(PROJECT_ID=PROJECT_ID, DATASET_ID=DATASET_ID, pids=obs_pids_notin_list),
                                      dialect="standard")


# ###
# 
# This next query gives this error because [obs_pids_in_list] is empty as said earlier.
# 
# This should not be a problem when curation loads the correct list of pids in [AC70_pids <- read.csv("AC70_PIDs.csv")]
#


update2_observation_table = pd.read_gbq('''

WITH hcau as (SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.observation` h WHERE h.observation_source_concept_id = 1384450)

    UPDATE `{PROJECT_ID}.{DATASET_ID}.observation` as o
        SET  o.observation_id = hcau.observation_id,
             o.person_id = o.person_id,
             o.observation_concept_id = hcau.observation_concept_id,
             o.observation_date = hcau.observation_date,
             o.observation_datetime = hcau.observation_datetime,
             o.observation_type_concept_id = hcau.observation_type_concept_id,
             o.value_as_number = hcau.value_as_number,
             o.value_as_string = hcau.value_as_string,
             o.value_as_concept_id = hcau.value_as_concept_id,
             o.qualifier_concept_id = hcau.qualifier_concept_id,
             o.unit_concept_id = hcau.unit_concept_id,
             o.provider_id = hcau.provider_id,
             o.visit_occurrence_id = hcau.visit_occurrence_id,
             o.observation_source_value = hcau.observation_source_value,
             o.observation_source_concept_id = hcau.observation_source_concept_id,
             o.unit_source_value = hcau.unit_source_value,
             o.qualifier_source_value = hcau.qualifier_source_value,
             o.value_source_concept_id = hcau.value_source_concept_id,
             o.value_source_value = hcau.value_source_value,
             o.questionnaire_response_id = hcau.questionnaire_response_id

     #   FROM (SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.observation` h WHERE h.observation_source_concept_id = 1384450) as hcau

    WHERE o.observation_source_concept_id = 43528428 
    AND person_id IN {pids})
    '''.format(PROJECT_ID=PROJECT_ID, DATASET_ID=DATASET_ID, pids=obs_pids_notin_list),
               dialect="standard")
