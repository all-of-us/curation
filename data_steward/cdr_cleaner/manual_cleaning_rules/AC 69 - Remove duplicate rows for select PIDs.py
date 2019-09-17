import pandas as pd
import warnings

warnings.filterwarnings('ignore')

#######################################
print('Setting everything up...')
#######################################

print('done.')

DATASET_ID = ''
dub_pids = ()

######################################
print('Getting the data from the database...')
######################################

dup_df_observation = pd.io.gbq.read_gbq('''
    SELECT
        *
    FROM
       `{dataset_id}.observation` AS t1
    WHERE
        person_id IN  {dup_pids}
    '''.format(dataset_id=DATASET_ID, dup_pids=dub_pids), dialect='standard')

print(dup_df_observation.shape[0], 'records received.')

dup_df_observation = dup_df_observation.sort_values(by=["person_id", "observation_concept_id", "value_as_concept_id",
                                                        "observation_date", "observation_type_concept_id",
                                                        "value_as_number", "observation_source_value",
                                                        "observation_source_concept_id"])

observation_columns = ["person_id", "observation_concept_id", "observation_date", "observation_datetime",
                       "observation_type_concept_id", "value_as_number", "value_as_string", "value_as_concept_id",
                       "qualifier_concept_id",
                       "unit_concept_id", "provider_id", "visit_occurrence_id", "observation_source_value",
                       "observation_source_concept_id", "unit_source_value", "qualifier_source_value",
                       "value_source_concept_id", "value_source_value",
                       "questionnaire_response_id"]

# Find all occurrence of the duplicate records
dup_df_observation_all = dup_df_observation[dup_df_observation.duplicated(subset=observation_columns, keep=False)]

# Find the last occurrence of the duplicate records
dup_df_last = dup_df_observation[dup_df_observation.duplicated(subset=observation_columns, keep='last')]

# get duplicate observation ids.
all_dup_observation_ids = dup_df_observation_all["observation_id"]

# get the last duplicate observation ids.
last_dup_observation_ids = dup_df_last["observation_id"]

# find all observation_id except for the most recent one
observation_ids_to_delete = set(all_dup_observation_ids).difference(last_dup_observation_ids)

######################################
print('Deleting the data from the database...')
######################################

dup_df_observation = pd.io.gbq.read_gbq('''
    DELETE
    FROM
       `{dataset_id}.observation` AS t1
    WHERE
        observation_id IN {observation_ids}
    '''.format(dataset_id=DATASET_ID, observation_ids=observation_ids_to_delete), dialect='standard')
