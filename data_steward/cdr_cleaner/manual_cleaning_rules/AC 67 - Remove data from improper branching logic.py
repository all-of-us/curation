#!/usr/bin/env python
# coding: utf-8

# # REQUIREMENTS
# 
# Remove data for questions participants should not have received (based on improper branching logic)
# 
# Data for child questions should only occur for those participants who should have received
# the questions based on the skip and branching logic of the PPI survey.
# For the identified PIDs for each question, drop the row.

# ## Set up


from termcolor import colored
import pandas as pd


PROJECT_ID = ''
DATASET_ID = ''


# ## Load the PIDs with data to be removed 
# 
# This is a list of pids identified by the team who received child questions that they should not have
# received based on their answers to the parent questions.


pids_rem_lifestyle = pd.read_csv("AC67_rem_lifestyle.csv")  # pids for lifestyle questions
pids_rem_overallh = pd.read_csv("AC67 _rem_overallh.csv") # pids for overall health questions
pids_rem_personalmed = pd.read_csv("AC67 _rem_personalmed.csv") # pids for personam medical history questions
pids_rem_hcau = pd.read_csv("AC67 _rem_hcau.csv") # pids for hcau- insurance questions


# # ```HCAU``` QUESTIONS
# The SQL query below will delete from the observation table all the records for the person_ids in
# ```pids_remv_hcau``` related to these child HCAU questions

pids_remv_hcau = pids_rem_hcau.iloc[:, 0].dropna().tolist() \
                 + pids_rem_hcau.iloc[:, 1].dropna().tolist()

pids_remv_hcau = [int(x) for x in pids_remv_hcau]

pids_remv_hcau = tuple(pids_remv_hcau)


# query to find the correct observation_source_concept_ids
pd.read_gbq('''

SELECT DISTINCT observation_source_concept_id, concept_name, concept_code

FROM `{PROJECT_ID}.{DATASET_ID}.observation` o
INNER JOIN `{PROJECT_ID}.{DATASET_ID}.concept` c ON o.observation_source_concept_id = c.concept_id
WHERE concept_code LIKE '%Insurance_InsuranceType%' #hcau
OR concept_code LIKE 'HealthInsurance_InsuranceTypeUpdate'#hcau
'''.format(PROJECT_ID=PROJECT_ID, DATASET_ID=DATASET_ID), dialect="standard")


# # QUERY TO REMOVE RECORDS FOR THESE PIDS RECORDS

pd.read_gbq('''

DELETE 
 FROM `{PROJECT_ID}.{DATASET_ID}.observation` 
 WHERE person_id IN {pids}
    AND (observation_source_concept_id = 43528428 #hcau
    OR observation_source_concept_id = 1384450) #hcau
#ORDER BY concept_code
'''.format(PROJECT_ID=PROJECT_ID, DATASET_ID=DATASET_ID, pids=pids_remv_hcau), dialect="standard")


# # ```PERSONAL MEDICAL HISTORY``` QUESTIONS¶

pids_remv_personalmed = pids_rem_personalmed.iloc[:, 0].dropna().tolist() \
                        + pids_rem_personalmed.iloc[:, 1].dropna().tolist() \
                        + pids_rem_personalmed.iloc[:, 2].dropna().tolist()

pids_remv_personalmed = [int(x) for x in pids_remv_personalmed]

# pids_remv_hcau = na.omit(c(pids_rem_hcau[,1], pids_rem_hcau[,2]))
pids_remv_personalmed= tuple(pids_remv_personalmed)


# query to find the correct observation_source_concept_ids
pd.read_gbq('''

SELECT DISTINCT observation_source_concept_id, concept_name, concept_code

FROM `{PROJECT_ID}.{DATASET_ID}.observation` o
INNER JOIN `{PROJECT_ID}.{DATASET_ID}.concept` c ON o.observation_source_concept_id = c.concept_id
WHERE concept_code LIKE 'NervousSystem_DementiaCurrently'#personal medical hist
OR concept_code LIKE 'NervousSystem_HowOldWereYouDementia'#personal medical hist  ### COULD NOT FIND QUESTION IN CDR
OR concept_code LIKE 'NervousSystem_RxMedsforDementia'#personal medical hist
'''.format(PROJECT_ID=PROJECT_ID, DATASET_ID=DATASET_ID), dialect="standard")


# # QUERY TO REMOVE RECORDS FOR THESE PIDS RECORDS

pd.read_gbq('''

DELETE 
 FROM `{PROJECT_ID}.{DATASET_ID}.observation` 
 WHERE person_id IN {pids}
 AND (observation_source_concept_id = 43530367 #NervousSystem_DementiaCurrently
     # OR observation_source_concept_id = ??? #NervousSystem_HowOldWereYouDementia ### COULD NOT FIND tTHIS QUESTION IN CDR
      OR observation_source_concept_id = 43528852) #NervousSystem_RxMedsforDementia
'''.format(PROJECT_ID=PROJECT_ID, DATASET_ID=DATASET_ID, pids=pids_remv_personalmed), dialect="standard")


# # REMOVE THESE PIDS FOR THESE ```overall health``` QUESTIONS¶

pids_remv_overallh = pids_rem_overallh.iloc[:, 0].dropna().tolist() \
                     + pids_rem_overallh.iloc[:, 1].dropna().tolist() \
                     + pids_rem_overallh.iloc[:, 2].dropna().tolist() \
                     + pids_rem_overallh.iloc[:, 3].dropna().tolist()

pids_remv_overallh = [int(x) for x in pids_remv_personalmed]

pids_remv_overallh = tuple(pids_remv_overallh)


# query to find the correct observation_source_concept_ids
pd.read_gbq('''

SELECT DISTINCT observation_source_concept_id, concept_name, concept_code

FROM `{PROJECT_ID}.{DATASET_ID}.observation` o
INNER JOIN `{PROJECT_ID}.{DATASET_ID}.concept` c ON o.observation_source_concept_id = c.concept_id
WHERE (concept_code LIKE 'Pregnancy_1PregnancyStatus'#overall health
OR c.concept_code LIKE 'YesNone_MenstrualStoppedReason'#overall health
OR c.concept_code LIKE 'OverallHealth_HysterectomyHistory'#overall health
OR c.concept_code LIKE 'OverallHealth_OvaryRemovalHistory')#overall health
'''.format(PROJECT_ID=PROJECT_ID, DATASET_ID=DATASET_ID), dialect="standard")


# # QUERY TO REMOVE RECORDS FOR THESE PIDS RECORDS

pd.read_gbq('''

DELETE 
 FROM `{PROJECT_ID}.{DATASET_ID}.observation` 
 WHERE person_id IN {pids}
 AND (observation_source_concept_id = 1585811 # Pregnancy_1PregnancyStatus
    OR observation_source_concept_id = 1585789 #YesNone_MenstrualStoppedReason
    OR observation_source_concept_id = 1585791 #OverallHealth_HysterectomyHistory
    OR observation_source_concept_id = 1585796) #OverallHealth_OvaryRemovalHistory
'''.format(PROJECT_ID=PROJECT_ID, DATASET_ID=DATASET_ID, pids=pids_remv_overallh), dialect="standard")


# # ```LIFESTYLE``` QUESTIONS¶

pids_remv_lifestyle = pids_rem_lifestyle.iloc[:, 0].dropna().tolist() \
                      + pids_rem_lifestyle.iloc[:, 1].dropna().tolist() \
                      + pids_rem_lifestyle.iloc[:, 2].dropna().tolist() \
                      + pids_rem_lifestyle.iloc[:, 3].dropna().tolist() \
                      + pids_rem_lifestyle.iloc[:, 4].dropna().tolist() \
                      + pids_rem_lifestyle.iloc[:, 5].dropna().tolist() \
                      + pids_rem_lifestyle.iloc[:, 6].dropna().tolist()

pids_remv_lifestyle = [int(x) for x in pids_remv_lifestyle]

pids_remv_lifestyle = tuple(pids_remv_lifestyle)


# query to find the correct observation_source_concept_ids
pd.read_gbq('''

SELECT DISTINCT observation_source_concept_id, concept_name, concept_code

FROM `{PROJECT_ID}.{DATASET_ID}.observation` o
INNER JOIN `{PROJECT_ID}.{DATASET_ID}.concept` c ON o.observation_source_concept_id = c.concept_id
WHERE 
(concept_code LIKE 'Smoking_DailySmokeStartingAge'#Lifestyle
OR concept_code LIKE 'AttemptQuitSmoking_CompletelyQuitAge'#Lifestyle
OR concept_code LIKE 'Smoking_NumberOfYears'#Lifestyle
OR concept_code LIKE 'Smoking_CurrentDailyCigaretteNumber'#Lifestyle
OR concept_code LIKE 'Smoking_AverageDailyCigaretteNumber'#Lifestyle
OR concept_code LIKE 'Alcohol_AverageDailyDrinkCount'#Lifestyle
OR concept_code LIKE 'Alcohol_6orMoreDrinksOccurence')#Lifestyle
'''.format(PROJECT_ID=PROJECT_ID, DATASET_ID=DATASET_ID), dialect="standard")


# # QUERY TO REMOVE RECORDS FOR THESE PIDS RECORDS

pd.read_gbq('''

DELETE 
 FROM `{PROJECT_ID}.{DATASET_ID}.observation` 
 WHERE person_id IN {pids}
 AND (observation_source_concept_id = 1585864 #Smoking_DailySmokeStartingAge
        OR observation_source_concept_id = 1585870 #AttemptQuitSmoking_CompletelyQuitAge
        OR observation_source_concept_id = 1585873 #Smoking_NumberOfYears'#Lifestyle
        OR observation_source_concept_id = 1586159 #Smoking_CurrentDailyCigaretteNumber
        OR observation_source_concept_id = 1586162 #Smoking_AverageDailyCigaretteNumber
        OR observation_source_concept_id = 1586207 #Alcohol_AverageDailyDrinkCount
        OR observation_source_concept_id = 1586213) #Alcohol_6orMoreDrinksOccurence
'''.format(PROJECT_ID=PROJECT_ID, DATASET_ID=DATASET_ID, pids=pids_remv_lifestyle), dialect="standard")


all_pids = pids_remv_lifestyle + pids_remv_hcau + pids_remv_overallh + pids_remv_personalmed
