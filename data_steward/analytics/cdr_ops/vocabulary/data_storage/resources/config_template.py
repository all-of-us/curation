# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.15.2
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# +


# # +
config = {
    # URL for the REDCap API
    'api_url': '{}',


    # this maps (module name -> API token)
    'projects': {
        # a human-friendly name for the survey module (for internal use only)
        # prefer snake_case (i.e avoid whitespace and special characters)
        'english_basics':
            # the_basics
            '{key}',

        'english_cope_202005':
            # covid19_participant_experience_cope_survey
            '{key}',

        'english_cope_202006':
            # june_covid19_participant_experience_cope_survey
            '{key}',

        'english_cope_202007':	
            # july_covid19_participant_experience_cope_survey
            '{key}',

        'english_cope_202011':
            # november_covid19_participant_experience_cope_surve
            '{key}',

        'english_cope_202012':	
            # december_covid19_participant_experience_cope_surve
            '{key}',

        'english_cope_202102':	
            # february_covid19_participant_experience_cope_surve
            '{key}',

        'english_fall_minute':	
            # fall_minute_survey_on_covid19_vaccines
            '{key}',

        'english_family_health_history':
        	# family_health_history
            '{key}',

        'english_healthcare_access_and_utilization':
            # healthcare_access_and_utilization
            '{key}',

        'english_lifestyle':
            # lifestyle
            '{key}',
        
        'english_new_year_minute':
            # new_year_survey_on_covid19_vaccines
            '{key}',
            
        'english_overall_health':	
            # overall_health
            '{key}',

        'english_personal_and_family_health_history':	
            # personal_and_family_health_history
            '{key}',

        'english_personal_medical_history':	
             # personal_medical_history
            '{key}',
        
        'english_physical_measurements':	
            # physical_measurements
            '{key}',

        'english_social_determinants_of_health':	
            # social_determinants_of_health_english
            '{key}',

        'english_summer_minute':	
            # summer_minute_survey_on_covid19_vaccines
            '{key}',

        'english_winter_minute':	
            # winter_minute_survey_on_covid19_vaccines
            '{key}',

#         'spanish_basics':	
#             # lo_bsico
#             '{key}',

#         'spanish_cope_202005':
#             # spanish_covid19_participant_experience_cope_survey
#             '{key}',

#         'spanish_cope_202006':	
#             # spanish_june_covid19_participant_experience_cope_survey
#             '{key}',

#         'spanish_cope_202007':	
#             # spanish_covid19_participant_experience_cope_survey
#             '{key}',

#         'spanish_cope_202011':	
#             # november_covid19_participant_experience_cope_surve
#             '{key}',

#         'spanish_cope_202012':	
#             # december_covid19_participant_experience_cope_surve
#             '{key}',

#         'spanish_cope_202102':	
#             # february_covid19_participant_experience_cope_surve
#             '{key}',

#         'spanish_fall_minute':	
#             # fall_minute_survey_on_covid19_vaccines
#             '{key}',

#         'spanish_family_health_history':
#             # family_health_history_s
#             '{key}',

#         'spanish_healthcare_access_and_utilization':	
#             # acceso_a_y_uso_de_atencin_mdica
#             '{key}',

#         'spanish_lifestyle':
#             # estilo_de_vida
#             '{key}',
        
#         'spanish_new_year_minute':
#             # new_year_minute_survey_on_covid19_vaccines
#             '{key}',        

#         'spanish_overall_health'	:
#             # salud_general
#             '{key}',

#         'spanish_personal_and_family_health_history':	
#             # personal_and_family_health_history
#             '{key}',

#         'spanish_personal_medical_history ':	
#             # personal_medical_history
#             '{key}',
        
#         'spanish_physical_measurements':	
#             # physical_measurements
#             '{key}',

#         'spanish_social_determinants_of_health':
#             # social_determinants_of_health_english
#         	'{key}',

#         'spanish_summer_minute':	
#             # covid19_vaccine_survey
#             '{key}',

#         'spanish_winter_minute':
#              # winter_minute_survey_on_covid19_vaccines
#         	'{key}',
     
        'ehr_consent':
            # ehr_consent
        	'{key}',
        
        'primary_consent':
            # primary_consent
        	'{key}',
        
        'consent_for_dna_results':
            # consent_for_dna_results
        	'{key}',
        
        'wear_consent':
            # wear_consent
        	'{key}',

    }
}

