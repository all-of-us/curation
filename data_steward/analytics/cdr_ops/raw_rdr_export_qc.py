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

# + tags=["parameters"]
project_id = ""
old_rdr = ""
new_rdr = ""
run_as = ""
rdr_cutoff_date = ""
# -

# # QC for RDR Export
#
# Quality checks performed on a new RDR dataset and comparison with previous RDR dataset.
import pandas as pd
from common import CATI_TABLES, DEATH, FACT_RELATIONSHIP, JINJA_ENV, PIPELINE_TABLES, SITE_MASKING_TABLE_ID, SRC_ID_TABLES
from utils import auth
from gcloud.bq import BigQueryClient
from analytics.cdr_ops.notebook_utils import execute, IMPERSONATION_SCOPES, render_message
from resources import old_map_short_codes_path
from IPython.display import display, HTML

impersonation_creds = auth.get_impersonation_credentials(
    run_as, target_scopes=IMPERSONATION_SCOPES)

client = BigQueryClient(project_id, credentials=impersonation_creds)

# +
# Get the current old_map_short_codes
old_map_csv = pd.read_csv(old_map_short_codes_path)

# These are the long codes expected in the rdr export.
LONG_CODES = old_map_csv.iloc[:, 1].str.lower().tolist()
# -

# wear_consent and wear_consent_ptsc concepts that are not associated with an OMOP concept_id.
WEAR_SURVEY_CODES = [
    'havesmartphone', 'wearwatch', 'usetracker', 'wear12months', 'receivesms',
    'frequency', 'agreetoshare', 'onlyparticipantinhousehold', 'haveaddress',
    'resultsconsent_wear', 'email_help_consent', 'timeofday',
    'wearconsent_signature', 'wearconsent_todaysdate', 'wear_consent',
    'wear_consent_ptsc'
]

# Static list created by querying the redcap surveys. In case of needed update, query provided in the comments of DC3407
expected_strings = ["sensitivetype2_mentalhealth","sensitivetype2_hivaids","sensitivetype2_substanceuse",
                    "sensitivetype2_genetictesting","sensitivetype2_domesticviolence","ehrconsentpii_todaysdate",
                    "ehrconsentpii_todaysdateilhippawitness","smoking_dailysmokestartingagenumber",
                    "smoking_numberofyearsnumber","self_reported_weight_pounds","self_reported_weight_kg",
                    "otherkidneycondition_motherfreetextbox","otherkidneycondition_fatherfreetextbox",
                    "otherkidneycondition_siblingfreetextbox","otherkidneycondition_daughterfreetextbox",
                    "otherkidneycondition_sonfreetextbox","otherkidneycondition_grandparentfreetextbox",
                    "otherrespiratory_motherfreetextbox","otherrespiratory_fatherfreetextbox",
                    "otherrespiratory_siblingfreetextbox","otherrespiratory_daughterfreetextbox",
                    "otherrespiratory_sonfreetextbox","otherrespiratory_grandparentfreetextbox",
                    "organtransplant_hearttransplantdate","organtransplant_kidneytransplantdate",
                    "organtransplant_livertransplantdate","organtransplant_lungtransplantdate",
                    "organtransplant_pancreastransplantdate","organtransplant_intestinetransplantdate",
                    "organtransplant_otherorgantransplantdate","organtransplant_corneatransplantdate",
                    "organtransplant_bonetransplantdate","organtransplant_valvetransplantdate",
                    "organtransplant_skintransplantdate","organtransplant_bloodvesseltransplantdate",
                    "organtransplant_othertissuetransplantdate","livingsituation_howmanypeople",
                    "livingsituation_peopleunder18","socialsecurity_socialsecuritynumber",
                    "secondarycontactinfo_persononefirstname","secondarycontactinfo_secondcontactsfirstname",
                    "outsidetravel6month_outsidetravel6monthwheretraveled","cdc_covid_19_7_xx23_other_cope_a_204",
                    "dmfs_29_additionaldose_other","cdc_covid_xx_b_firstdose_other",
                    "cdc_covid_xx_symptom_seconddose_cope_350","mhqukb_48_age","mhqukb_50_number","mhqukb_51_number",
                    "mhqukb_52_number","mhqukb_53_number","cidi5_24","cidi5_20","cidi5_28","cidi5_31",
                    "resultsconsent_emailmecopy","resultsconsent_signaturedate","helpmewithconsent_name",
                    "other_reasons","other_concerns","signature_type","ehrconsentpii_helpwithconsentsignature",
                    "cidi5_15","mhqukb_25_number","mhqukb_26_age","mhqukb_28_age","ss_2_age","ss_3_number",
                    "ss_3_age_1","ss_3_age_2","english_exploring_the_mind_consent_form","etm_help_name",
                    "self_reported_height_ft","self_reported_height_in","self_reported_height_cm",
                    "extraconsent_signature_type","extraconsent_todaysdate","consentpii_helpwithconsentsignature",
                    "piiname_first","piiname_middle","piiname_last","piiaddress_streetaddress",
                    "piiaddress_streetaddress2","streetaddress_piicity","streetaddress_piizip",
                    "piicontactinformation_phone","piibirthinformation_birthdate","timeofday","wearconsent_todaysdate",
                    "attemptquitsmoking_completelyquitage","smoking_currentdailycigarettenumber",
                    "smoking_averagedailycigarettenumber","overallhealth_hysterectomyhistoryage",
                    "overallhealthovaryremovalhistoryage","outsidetravel6month_outsidetravel6monthhowlong",
                    "urs_8c","cdc_covid_19_7_xx22_date","cope_a_126","ipaq_1_cope_a_24","ipaq_2_cope_a_160",
                    "ipaq_2_cope_a_85","ipaq_3_cope_a_24","ipaq_4_cope_a_160","ipaq_4_cope_a_85","ipaq_5_cope_a_24",
                    "ipaq_6_cope_a_160","ipaq_6_cope_a_85","cope_a_160","cope_a_85","copect_50_xx19_cope_a_57",
                    "copect_50_xx19_cope_a_198","copect_50_xx19_cope_a_152","lifestyle_2_xx12_cope_a_57",
                    "lifestyle_2_xx12_cope_a_198","lifestyle_2_xx12_cope_a_152","cdc_covid_xx_symptom_cope_350",
                    "basics_xx","basics_xx20","cdc_covid_xx_a_date1","cdc_covid_xx_a_date2",
                    "otherheartorbloodcondition_motherfreetextbox","otherheartorbloodcondition_fatherfreetextbox",
                    "otherheartorbloodcondition_siblingfreetextbox","otherheartorbloodcondition_daughterfreetextbox",
                    "otherheartorbloodcondition_sonfreetextbox","otherheartorbloodcondition_grandparentfreetextbox",
                    "otherdigestivecondition_grandparentfreetextbox","otherbrainnervoussystem_motherfreetextbox",
                    "othercancer_motherfreetextbox","othercancer_fatherfreetextbox","othercancer_siblingfreetextbox",
                    "othercancer_daughterfreetextbox","othercancer_sonfreetextbox","othercancer_grandparentfreetextbox",
                    "othercancer_freetextbox","otherheartorbloodcondition_freetextbox",
                    "otherdigestivecondition_freetextbox","otherdiabetes_freetextbox","otherthyroid_freetextbox",
                    "otherhormoneendocrine_freetextbox","otherkidneycondition_freetextbox",
                    "otherrespiratory_freetextbox", "otherbonejointmuscle_freetextbox",
                    "otherhearingeye_freetextbox","otherdiagnosis_freetextbox",
                    "otherinfectiousdisease_freetextbox","dmfs_29a","cdc_covid_xx_b_other","cu_covid_cope_a_204",
                    "basics_11a_cope_a_33","cdc_covid_19_n_a2","nhs_covid_fhc17b_cope_a_226","msds_17_c",
                    "cdc_covid_19_n_a4","cdc_covid_19_n_a8","cope_aou_xx_2_a","cdc_covid_xx_a_date3",
                    "cdc_covid_xx_a_date4","cdc_covid_xx_a_date5","cdc_covid_xx_a_date6",
                    "cdc_covid_xx_a_date7","cdc_covid_xx_a_date8","cdc_covid_xx_a_date9",
                    "cdc_covid_xx_a_date10","cdc_covid_xx_a_date11","cdc_covid_xx_a_date12",
                    "cdc_covid_xx_a_date13","cdc_covid_xx_a_date14","cdc_covid_xx_a_date15","cdc_covid_xx_a_date16",
                    "cdc_covid_xx_a_date17","otherdigestivecondition_motherfreetextbox",
                    "otherdigestivecondition_fatherfreetextbox","otherdigestivecondition_siblingfreetextbox",
                    "otherdigestivecondition_daughterfreetextbox","otherdigestivecondition_sonfreetextbox",
                    "otherdiabetes_motherfreetextbox","otherdiabetes_fatherfreetextbox",
                    "otherdiabetes_siblingfreetextbox","otherdiabetes_grandparentfreetextbox",
                    "otherthyroid_motherfreetextbox","otherthyroid_fatherfreetextbox",
                    "otherthyroid_siblingfreetextbox","otherthyroid_daughterfreetextbox",
                    "otherthyroid_sonfreetextbox","otherthyroid_grandparentfreetextbox",
                    "otherhormoneendocrine_motherfreetextbox","otherhormoneendocrine_fatherfreetextbox",
                    "otherhormoneendocrine_siblingfreetextbox","otherhormoneendocrine_daughterfreetextbox",
                    "otherhormoneendocrine_sonfreetextbox","otherhormoneendocrine_grandparentfreetextbox",
                    "otherbrainnervoussystem_fatherfreetextbox","otherbrainnervoussystem_siblingfreetextbox",
                    "otherbrainnervoussystem_daughterfreetextbox","otherbrainnervoussystem_sonfreetextbox",
                    "otherbrainnervoussystem_grandparentfreetextbox","othermentalhealthsubstanceuse_motherfreetextbox",
                    "othermentalhealthsubstanceuse_fatherfreetextbox",
                    "othermentalhealthsubstanceuse_siblingfreetextbox",
                    "othermentalhealthsubstanceuse_daughterfreetextbox","othermentalhealthsubstanceuse_sonfreetextbox",
                    "othermentalhealthsubstanceuse_grandparentfreetextb","otherarthritis_motherfreetextbox",
                    "otherarthritis_fatherfreetextbox","otherarthritis_siblingfreetextbox",
                    "otherarthritis_daughterfreetextbox","otherarthritis_sonfreetextbox",
                    "otherarthritis_freetextbox",
                    "otherarthritis_grandparentfreetextbox","otherbonejointmuscle_motherfreetextbox",
                    "otherbonejointmuscle_fatherfreetextbox","otherbonejointmuscle_siblingfreetextbox",
                    "otherbonejointmuscle_daughterfreetextbox","otherbonejointmuscle_sonfreetextbox",
                    "otherbonejointmuscle_grandparentfreetextbox","otherhearingeye_motherfreetextbox",
                    "otherhearingeye_fatherfreetextbox","otherhearingeye_siblingfreetextbox",
                    "otherhearingeye_daughterfreetextbox","otherhearingeye_sonfreetextbox"
                    ,"otherhearingeye_grandparentfreetextbox","otherdiagnosis_motherfreetextbox",
                    "otherdiagnosis_fatherfreetextbox","otherdiagnosis_siblingfreetextbox",
                    "otherdiagnosis_daughterfreetextbox","otherdiagnosis_sonfreetextbox",
                    "otherdiagnosis_grandparentfreetextbox","otherbrainnervoussystem_freetextbox",
                    "cdc_covid_xx_b_seconddose_other","dmfs_29_seconddose_other","thebasics_countryborntextbox",
                    "whatraceethnicity_raceethnicitynoneofthese","aian_tribe","aiannoneofthesedescribeme_aianfreetext",
                    "noneofthesedescribeme_asianfreetext","blacknoneofthesedescribeme_blackfreetext",
                    "hispanicnoneofthesedescribeme_hispanicfreetext","menanoneofthesedescribeme_menafreetext",
                    "nhpinoneofthesedescribeme_nhpifreetext","whitenoneofthesedescribeme_whitefreetext",
                    "specifiedgender_specifiedgendertextbox","sexatbirthnoneofthese_sexatbirthtextbox",
                    "somethingelse_sexualitysomethingelsetextbox","otherhealthplan_freetext",
                    "employmentworkaddress_addresslineone","employmentworkaddress_addresslinetwo",
                    "employmentworkaddress_city","employmentworkaddress_zipcode","employmentworkaddress_country",
                    "livingsituation_livingsituationfreetext","secondarycontactinfo_persononemiddleinitial",
                    "secondarycontactinfo_persononelastname","secondarycontactinfo_persononeaddressone",
                    "secondarycontactinfo_persononeaddresstwo","persononeaddress_persononeaddresscity",
                    "persononeaddress_persononeaddresszipcode","secondarycontactinfo_persononeemail",
                    "secondarycontactinfo_persononetelephone","secondarycontactinfo_secondcontactsmiddleinitial",
                    "secondarycontactinfo_secondcontactslastname","secondarycontactinfo_secondcontactsaddressone",
                    "secondarycontactinfo_secondcontactsaddresstwo","secondcontactsaddress_secondcontactcity",
                    "secondcontactsaddress_secondcontactzipcode","secondarycontactinfo_secondcontactsemail",
                    "secondarycontactinfo_secondcontactsnumber","otherspecify_otherdrugstextbox",
                    "otherorgan_freetextbox","othertissue_freetextbox","otherdelayedmedicalcare_freetext",
                    "sdoh_eds_follow_up_1_xx","tsu_ds5_13_xx42_cope_a_226","eds_follow_up_1_xx",
                    "othercondition_motherfreetextbox","othercondition_fatherfreetextbox",
                    "othercondition_siblingfreetextbox","othercondition_daughterfreetextbox",
                    "othercondition_sonfreetextbox","othercondition_grandparentfreetextbox",
                    "cdc_covid_xx_b_dose3_other","cdc_covid_xx_symptom_cope_350_dose3","cdc_covid_xx_type_dose3_other",
                    "cdc_covid_xx_b_dose4_other",
                    "cdc_covid_xx_symptom_cope_350_dose4","cdc_covid_xx_type_dose4_other","cdc_covid_xx_b_dose5_other",
                    "cdc_covid_xx_symptom_cope_350_dose5","cdc_covid_xx_type_dose5_other","cdc_covid_xx_b_dose6_other",
                    "cdc_covid_xx_symptom_cope_350_dose6","cdc_covid_xx_type_dose6_other","cdc_covid_xx_b_dose7_other",
                    "cdc_covid_xx_symptom_cope_350_dose7","cdc_covid_xx_type_dose7_other","cdc_covid_xx_b_dose8_other",
                    "cdc_covid_xx_symptom_cope_350_dose8","cdc_covid_xx_type_dose8_other","cdc_covid_xx_b_dose9_other",
                    "cdc_covid_xx_symptom_cope_350_dose9","cdc_covid_xx_type_dose9_other","cdc_covid_xx_b_dose10_other",
                    "cdc_covid_xx_symptom_cope_350_dose10","cdc_covid_xx_type_dose10_other",
                    "cdc_covid_xx_b_dose11_other","cdc_covid_xx_symptom_cope_350_dose11",
                    "cdc_covid_xx_type_dose11_other","cdc_covid_xx_b_dose12_other",
                    "cdc_covid_xx_symptom_cope_350_dose12","cdc_covid_xx_type_dose12_other",
                    "cdc_covid_xx_b_dose13_other","cdc_covid_xx_symptom_cope_350_dose13",
                    "cdc_covid_xx_type_dose13_other","cdc_covid_xx_b_dose14_other",
                    "cdc_covid_xx_symptom_cope_350_dose14","cdc_covid_xx_type_dose14_other",
                    "cdc_covid_xx_b_dose15_other","cdc_covid_xx_symptom_cope_350_dose15",
                    "cdc_covid_xx_type_dose15_other","cdc_covid_xx_b_dose16_other",
                    "cdc_covid_xx_symptom_cope_350_dose16","cdc_covid_xx_type_dose16_other",
                    "cdc_covid_xx_b_dose17_other","cdc_covid_xx_symptom_cope_350_dose17",
                    "cdc_covid_xx_type_dose17_other"]

# # Table comparison
# The export should generally contain the same tables from month to month.
# Tables found only in the old or the new export are listed below.

tpl = JINJA_ENV.from_string('''
SELECT
  COALESCE(curr.table_id, prev.table_id) AS table_id
 ,curr.row_count AS _{{new_rdr}}
 ,prev.row_count AS _{{old_rdr}}
 ,(curr.row_count - prev.row_count) AS row_diff
FROM `{{project_id}}.{{new_rdr}}.__TABLES__` curr
FULL OUTER JOIN `{{project_id}}.{{old_rdr}}.__TABLES__` prev
  USING (table_id)
WHERE curr.table_id IS NULL OR prev.table_id IS NULL
''')
query = tpl.render(new_rdr=new_rdr, old_rdr=old_rdr, project_id=project_id)
execute(client, query)

# ## Row count comparison
# Generally the row count of clinical tables should increase from one export to the next.

tpl = JINJA_ENV.from_string('''
SELECT
  curr.table_id AS table_id
 ,prev.row_count AS _{{old_rdr}}
 ,curr.row_count AS _{{new_rdr}}
 ,(curr.row_count - prev.row_count) row_diff
FROM `{{project_id}}.{{new_rdr}}.__TABLES__` curr
JOIN `{{project_id}}.{{old_rdr}}.__TABLES__` prev
  USING (table_id)
ORDER BY ABS(curr.row_count - prev.row_count) DESC;
''')
query = tpl.render(new_rdr=new_rdr, old_rdr=old_rdr, project_id=project_id)
execute(client, query)

# ## ID range check
# Combine step may break if any row IDs in the RDR are larger than the added constant(1,000,000,000,000,000).
# Rows that are greater than 999,999,999,999,999 the will be listed out here.

domain_table_list = [
    table for table in CATI_TABLES if table not in [DEATH, FACT_RELATIONSHIP]
]
queries = []
for table in domain_table_list:
    tpl = JINJA_ENV.from_string('''
    SELECT
        '{{table}}' AS domain_table_name,
        {{table}}_id AS domain_table_id
    FROM
     `{{project_id}}.{{new_rdr}}.{{table}}`
    WHERE
      {{table}}_id > 999999999999999
    ''')
    query = tpl.render(new_rdr=new_rdr, table=table, project_id=project_id)
    queries.append(query)
execute(client, '\nUNION ALL\n'.join(queries))

# ## Concept codes used
# Identify question and answer concept codes which were either added or removed
# (appear in only the new or only the old RDR datasets, respectively).

tpl = JINJA_ENV.from_string('''
WITH curr_code AS (
SELECT
  observation_source_value value
 ,'observation_source_value' field
 ,COUNT(1) row_count
FROM `{{project_id}}.{{new_rdr}}.observation` GROUP BY 1

UNION ALL

SELECT
  value_source_value value
 ,'value_source_value' field
 ,COUNT(1) row_count
FROM `{{project_id}}.{{new_rdr}}.observation` GROUP BY 1),

prev_code AS (
SELECT
  observation_source_value value
 ,'observation_source_value' field
 ,COUNT(1) row_count
FROM `{{project_id}}.{{old_rdr}}.observation` GROUP BY 1

UNION ALL

SELECT
  value_source_value value
 ,'value_source_value' field
 ,COUNT(1) row_count
FROM `{{project_id}}.{{old_rdr}}.observation` GROUP BY 1)

SELECT
  prev_code.value prev_code_value
 ,prev_code.field prev_code_field
 ,prev_code.row_count prev_code_row_count
 ,curr_code.value curr_code_value
 ,curr_code.field curr_code_field
 ,curr_code.row_count curr_code_row_count
FROM curr_code
 FULL OUTER JOIN prev_code
  USING (field, value)
WHERE prev_code.value IS NULL OR curr_code.value IS NULL
''')
query = tpl.render(new_rdr=new_rdr, old_rdr=old_rdr, project_id=project_id)
execute(client, query)

# # Questions lacking concept_ids to be dropped
# Question codes in `observation_source_value` should be associated with the concept identified by
# `observation_source_concept_id` and mapped to a standard concept identified by `observation_concept_id`.
# The table below lists codes having rows where both fields are null or zero and the number of rows where this occurs.
# This may be associated with an issue in the PPI vocabulary or in the RDR ETL process.
#
# **If the check is failing.** The concepts in the results dataframe will need to be reviewed manually. These concepts will have thier rows deleted in later pipeline stages. Confirm the expectation for these concepts. The results of this check should not be refered to the rdr team.
#
# More information on the codes that are expected not to map:
# * Snap codes are not modeled in the vocabulary but may be used in the RDR export.
# They are excluded here by filtering out snap codes in the Public PPI Codebook
# which were loaded into `curation_sandbox.snap_codes`.
#
# * Long codes are not OMOP concepts and are not expected to have concept_ids. This issue is accounted for in the RDR cleaning class `SetConceptIdsForSurveyQuestionsAnswers`.
#
# * Wear codes. Most concept codes in the wear survey modules do not have an OMOP concept_id. This is expected as these records will not be included in the CDR.

# +
tpl = JINJA_ENV.from_string("""
SELECT
  observation_source_value
 ,COUNTIF(observation_source_concept_id IS NULL) AS source_concept_id_null
 ,COUNTIF(observation_source_concept_id=0)       AS source_concept_id_zero
 ,COUNTIF(observation_concept_id IS NULL)        AS concept_id_null
 ,COUNTIF(observation_concept_id=0)              AS concept_id_zero
FROM `{{project_id}}.{{new_rdr}}.observation`
WHERE observation_source_value IS NOT NULL
  AND observation_source_value != ''
  AND observation_source_value NOT IN (SELECT concept_code FROM `{{project_id}}.curation_sandbox.snap_codes`)
  AND LOWER(observation_source_value) NOT IN UNNEST ({{wear_codes}})
  AND LOWER(observation_source_value) NOT IN UNNEST ({{long_codes}})
  AND NOT REGEXP_CONTAINS(observation_source_value,'(?i)consent|sitepairing|pii|michigan|feedback|nonotsure|schedul|address|screen2|get_help|stopoptions|sensitivetype2|withdrawal|etm|other_|stateofcare|organization|reviewagain|results_decision|saliva_whole_blood_transfusion')
GROUP BY 1
HAVING source_concept_id_null + source_concept_id_zero  > 0
  AND  concept_id_null + concept_id_zero > 0
  AND MAX(observation_date) > '2019-01-01'
ORDER BY 2 DESC, 3 DESC, 4 DESC, 5 DESC
""")
query = tpl.render(new_rdr=new_rdr, project_id=project_id,long_codes=LONG_CODES, wear_codes=WEAR_SURVEY_CODES)
df = execute(client, query)

success_msg = 'Questions expected to have concept_ids have concept_ids'
failure_msg = 'These question codes did not map to concept_ids. See description.'

render_message(df,
               success_msg,
               failure_msg)
# -

# # Check the ETL mapped `concept_id`s to question codes
# If most concepts are mapped, this check passes. If only some concepts are not mapping properly these are most likely known vocabulary issues.
#
# **If the check fails.** Investigate. If none, or only a few, of the codes are being mapped notify rdr.
#

# +
tpl = JINJA_ENV.from_string("""
WITH cte AS (
SELECT
  'questions' as field
  ,observation_source_value as code
  ,COUNTIF(observation_source_concept_id IS NULL OR observation_source_concept_id=0 OR observation_concept_id IS NULL OR observation_concept_id=0) AS n_not_mapped_by_etl 
  ,COUNTIF(observation_source_concept_id IS NOT NULL AND observation_source_concept_id != 0 AND observation_concept_id IS NOT NULL AND observation_concept_id != 0) AS n_mapped_by_etl 
  FROM `{{project_id}}.{{new_rdr}}.observation`
LEFT JOIN (SELECT concept_id_1 FROM `{{project_id}}.{{new_rdr}}.concept_relationship` WHERE relationship_id = 'Maps to') cr1
ON observation_source_concept_id = cr1.concept_id_1
WHERE cr1.concept_id_1 IS NOT NULL 
GROUP BY 2

)
SELECT 
  field,
  COUNTIF(n_not_mapped_by_etl != 0) AS count_not_mapped,
  COUNTIF(n_mapped_by_etl != 0) AS count_mapped
FROM cte
GROUP BY 1


""")
query = tpl.render(new_rdr=new_rdr, project_id=project_id, long_codes=LONG_CODES)
df = execute(client, query)

if sum(df['count_not_mapped']) > .33 * sum(df['count_mapped']):
    display(df,
        HTML(f'''
                <h3>
                    Check Status: <span style="color:red">FAILURE</span>
                </h3>
                <p>
                    Concept_ids are not being mapped. See description.
                </p>
            '''))
else:
    display(df,
        HTML(f'''
                <h3>
                    Check Status: <span style="color:green">PASS</span>
                </h3>
                <p>
                    Concept_ids have been mapped.
                </p>
                
            '''))
# -

# # Check the ETL mapped `concept_id`s to answer codes
# If most concepts are mapped, this check passes. If only some concepts are not mapping properly these are most likely known vocabulary issues.
#
# **If the check fails.** Investigate. If none, or only a few, of the codes are being mapped notify rdr.

# +
tpl = JINJA_ENV.from_string("""
WITH cte AS (
SELECT
  'answers' as field
  ,value_source_value as code
  ,COUNTIF(value_source_value IS NOT NULL AND value_source_value != '' AND value_source_concept_id IS NULL OR value_source_concept_id=0 OR value_as_concept_id IS NULL OR value_as_concept_id=0) AS n_not_mapped_by_etl 
  ,COUNTIF(value_source_value IS NOT NULL AND value_source_value != '' AND value_source_concept_id IS NOT NULL AND value_source_concept_id != 0 AND value_as_concept_id IS NOT NULL AND value_as_concept_id != 0) AS n_mapped_by_etl 
FROM `{{project_id}}.{{new_rdr}}.observation`
LEFT JOIN (SELECT distinct concept_id_1 FROM `{{project_id}}.{{new_rdr}}.concept_relationship` WHERE relationship_id IN ('Maps to','Mapped from','Maps to value')) cr2
ON value_source_concept_id = cr2.concept_id_1
WHERE cr2.concept_id_1 IS NOT NULL 
GROUP BY 2
)
SELECT distinct field,
countif(n_not_mapped_by_etl != 0) as count_not_mapped,
countif(n_mapped_by_etl != 0) as count_mapped
FROM cte
GROUP BY 1


""")
query = tpl.render(new_rdr=new_rdr, project_id=project_id, long_codes=LONG_CODES)
df = execute(client, query)

if sum(df['count_not_mapped']) > .33 * sum(df['count_mapped']):
    display(df,
        HTML(f'''
                <h3>
                    Check Status: <span style="color:red">FAILURE</span>
                </h3>
                <p>
                    Concept_ids are not being mapped. See description.
                </p>
            '''))
else:
    display(df,
        HTML(f'''
                <h3>
                    Check Status: <span style="color:green">PASS</span>
                </h3>
                <p>
                    Concept_ids have been mapped.
                </p>
                
            '''))


# -

# # Dates are equal in observation_date and observation_datetime
# Any mismatches are listed below.

tpl = JINJA_ENV.from_string("""
SELECT
  observation_id
 ,person_id
 ,observation_date
 ,observation_datetime
FROM `{{project_id}}.{{new_rdr}}.observation`
WHERE observation_date != EXTRACT(DATE FROM observation_datetime)
""")
query = tpl.render(new_rdr=new_rdr, project_id=project_id)
execute(client, query)

# # Check for duplicates

tpl = JINJA_ENV.from_string("""
with duplicates AS (
    SELECT
      person_id
     ,observation_datetime
     ,observation_source_value
     ,value_source_value
     ,value_as_number
     ,value_as_string
   -- ,questionnaire_response_id --
     ,COUNT(1) AS n_data
    FROM `{{project_id}}.{{new_rdr}}.observation`
    INNER JOIN `{{project_id}}.{{new_rdr}}.cope_survey_semantic_version_map`
        USING (questionnaire_response_id) -- For COPE only --
    GROUP BY 1,2,3,4,5,6
)
SELECT
  n_data   AS duplicates
 ,COUNT(1) AS n_duplicates
FROM duplicates
WHERE n_data > 1
GROUP BY 1
ORDER BY 2 DESC
""")
query = tpl.render(new_rdr=new_rdr, project_id=project_id)
execute(client, query)

# # Check numeric data in value_as_string
# Some numeric data is expected in value_as_string.  For example, zip codes or other contact specific information.
#
# **If the check fails, manually review the results. <br>
# As new surveys are created the static list `expected_strings` will need to be updated. This is one possible reason for this check to fail with false positives.

# +
tpl = JINJA_ENV.from_string("""
SELECT
  observation_source_value
 ,COUNT(1) AS n
FROM `{{project_id}}.{{new_rdr}}.observation`
WHERE SAFE_CAST(value_as_string AS INT64) IS NOT NULL
AND value_source_concept_id = 0
AND LOWER(observation_source_value) NOT IN UNNEST ({{expected_strings}}) 
AND NOT REGEXP_CONTAINS(LOWER(observation_source_value), '(?i)snap|signature|address|email|number|cohortgroup')
GROUP BY 1
ORDER BY 2 DESC
""")
query = tpl.render(new_rdr=new_rdr, project_id=project_id,expected_strings=expected_strings)
df = execute(client, query)

success_msg = 'All records with a number in value_as_string are expected to be text.'
failure_msg = 'Some records that have a number value_as_string might not be expected. See description.'

render_message(df,
                success_msg,
                failure_msg)
# -

# # All COPE `questionnaire_response_id`s are in COPE version map
# Any `questionnaire_response_id`s missing from the map will be listed below.

tpl = JINJA_ENV.from_string("""
SELECT
  observation_id
 ,person_id
 ,questionnaire_response_id
FROM `{{project_id}}.{{new_rdr}}.observation`
 INNER JOIN `{{project_id}}.pipeline_tables.cope_concepts`
  ON observation_source_value = concept_code
WHERE questionnaire_response_id NOT IN
(SELECT questionnaire_response_id FROM `{{project_id}}.{{new_rdr}}.cope_survey_semantic_version_map`)
""")
query = tpl.render(new_rdr=new_rdr, project_id=project_id)
execute(client, query)

# # No duplicate `questionnaire_response_id`s in COPE version map
# Any duplicated `questionnaire_response_id`s will be listed below.

tpl = JINJA_ENV.from_string("""
SELECT
  questionnaire_response_id
 ,COUNT(*) n
FROM `{{project_id}}.{{new_rdr}}.cope_survey_semantic_version_map`
GROUP BY questionnaire_response_id
HAVING n > 1
""")
query = tpl.render(new_rdr=new_rdr, project_id=project_id)
execute(client, query)

# # Survey version and dates
# This query checks the validity of cope_survey_semantic_version_map table which contains the version of each
# COPE and/or Minute module that each participant took.
# This table is created by RDR and is included in the rdr export. <br>
# For each COPE and Minute module the min and max survey observation dates in the RDR are listed,
# as well as a count of surveys taken outside of each module's expected implementation range. <br>
# Expected implementation ranges are found in the query or in
# [this documentation](https://docs.google.com/document/d/1IhRnvAymSZeko8AbS4TCaqnw_78Qa_NkTROfHOFqGGQ/edit?usp=sharing)
#  - If all surveys have data(10 modules), and the *_failure columns have a result = 0 this check PASSES.
#  - If all 10 surveys are not represented in the query results, this check FAILS.
#  Notify RDR of the missing survey data.
#  - If any of the *_failure columns have a result > 0 this check FAILS.
#  Notify RDR that there are surveys with observation_dates outside of the survey's expected implementation range.
#

tpl = JINJA_ENV.from_string("""
SELECT
 cope_month AS survey_version
,MIN(observation_date) AS min_obs_date
,MAX(observation_date) AS max_obs_date
,COUNTIF(cope_month ='may' AND observation_date NOT BETWEEN '2020-05-07' AND '2020-05-30' ) AS may_failure
,COUNTIF(cope_month ='june' AND observation_date NOT BETWEEN '2020-06-02' AND '2020-06-26' ) AS june_failure
,COUNTIF(cope_month ='july' AND observation_date NOT BETWEEN '2020-07-07' AND '2020-09-25' ) AS july_failure
,COUNTIF(cope_month ='nov' AND observation_date NOT BETWEEN '2020-10-27' AND '2020-12-03' ) AS nov_failure
,COUNTIF(cope_month ='dec' AND observation_date NOT BETWEEN '2020-12-08' AND '2021-01-04' ) AS dec_failure
,COUNTIF(cope_month ='feb' AND observation_date NOT BETWEEN '2021-02-08' AND '2021-03-05' ) AS feb_failure
,COUNTIF(cope_month ='vaccine1' AND observation_date NOT BETWEEN '2021-06-10' AND '2021-08-19' ) AS summer_failure
,COUNTIF(cope_month ='vaccine2' AND observation_date NOT BETWEEN '2021-08-19' AND '2021-10-28' ) AS fall_failure
,COUNTIF(cope_month ='vaccine3' AND observation_date NOT BETWEEN '2021-10-28' AND '2022-01-20' ) AS winter_failure
,COUNTIF(cope_month ='vaccine4' AND observation_date NOT BETWEEN '2022-01-20' AND '2022-03-08' ) AS new_year_failure
FROM `{{project_id}}.{{new_rdr}}.observation`
JOIN `{{project_id}}.{{new_rdr}}.cope_survey_semantic_version_map` USING (questionnaire_response_id)
GROUP BY 1
ORDER BY MIN(observation_date)
""")
query = tpl.render(new_rdr=new_rdr, project_id=project_id)
execute(client, query)

# # Check the expectations of survey_conduct cleaning rules
#
# This query checks the validity of cope_survey_semantic_version_map table which contains the version of each COPE and/or Minute module that each participant took. This table is created by RDR and is included in the rdr export.
#
# The series of CRs applied to the survey_conduct table rely on the assumption that the values in `cope_month` do not change and it is expected that none should be added.
#
# **An empty df is a passing check.** <br>
# Contact the rdr team if there are any unexpected cope_month values. CRs that use this field may need to be updated. Ex:RDR_CLEANING_CLASS:CleanSurveyConductRecurringSurveys

tpl = JINJA_ENV.from_string("""
SELECT
cope_month AS unexpected_cope_month
,COUNT(*) AS n_records
FROM `{{project_id}}.{{new_rdr}}.cope_survey_semantic_version_map`
WHERE cope_month NOT IN ('dec' ,'feb', 'may', 'nov', 'july', 'june', 'vaccine1','vaccine2','vaccine3','vaccine4')
GROUP BY cope_month
""")
query = tpl.render(new_rdr=new_rdr, project_id=project_id)
execute(client, query)

# # Class of PPI Concepts using vocabulary.py
# Concept codes which appear in `observation.observation_source_value` should belong to concept class Question.
# Concept codes which appear in `observation.value_source_value` should belong to concept class Answer.
# Concepts of class Qualifier Value are permitted as a value and
# Concepts of class Topic and PPI Modifier are permitted as a question
# Discreprancies (listed below) can be caused by misclassified entries in Athena or
# invalid payloads in the RDR and in further upstream data sources.

tpl = JINJA_ENV.from_string('''
WITH ppi_concept_code AS (
 SELECT
   observation_source_value AS code
  ,'Question'               AS expected_concept_class_id
  ,COUNT(1) n
 FROM `{{project_id}}.{{new_rdr}}.observation`
 GROUP BY 1, 2

 UNION ALL

 SELECT DISTINCT
   value_source_value AS code
  ,'Answer'           AS expected_concept_class_id
  ,COUNT(1) n
 FROM `{{project_id}}.{{new_rdr}}.observation`
 GROUP BY 1, 2
)
SELECT
  code
 ,expected_concept_class_id
 ,concept_class_id
 ,n
FROM ppi_concept_code
JOIN `{{project_id}}.{{new_rdr}}.concept`
 ON LOWER(concept_code)=LOWER(code)
WHERE LOWER(concept_class_id)<>LOWER(expected_concept_class_id)
AND CASE WHEN expected_concept_class_id = 'Question' THEN concept_class_id NOT IN('Topic','PPI Modifier') END
AND concept_class_id != 'Qualifier Value'
ORDER BY 1, 2, 3
''')
query = tpl.render(new_rdr=new_rdr, project_id=project_id)
execute(client, query)

# # Identify Questions That Dont Exist in the RDR Export
# This identifies questions as indicated by a PPI vocabulary and Question concept_class_id that
# do not exist in the dataset.

tpl = JINJA_ENV.from_string("""
with question_codes as (select c.concept_id, c.concept_name, c.concept_class_id
from `{{project_id}}.{{new_rdr}}.concept` as c
where REGEXP_CONTAINS(c.vocabulary_id, r'(?i)(ppi)') and REGEXP_CONTAINS(c.concept_class_id, r'(?i)(question)'))
, used_q_codes as (
    select distinct o.observation_source_concept_id, o.observation_source_value
    from `{{project_id}}.{{new_rdr}}.observation` as o
    join `{{project_id}}.{{new_rdr}}.concept` as c
    on o.observation_source_concept_id = c.concept_id
    where REGEXP_CONTAINS(c.vocabulary_id, r'(?i)(ppi)') and REGEXP_CONTAINS(c.concept_class_id, r'(?i)(question)')
)
    SELECT *
    from question_codes
    where concept_id not in (select observation_source_concept_id from used_q_codes)
    """)
query = tpl.render(new_rdr=new_rdr, project_id=project_id)
execute(client, query)

# # Make sure previously corrected missing data still exists
# Make sure that the cleaning rule clash that previously wiped out all numeric smoking data is corrected.
# Any returned rows indicate a problem that needs to be fixed.  Identified rows when running on a raw rdr
# import indicates a problem with the RDR ETL and will require cross team coordination.  Identified rows
# when running on a cleaned rdr import indicate problems with cleaning rules that should be remediated by curation.
#
# Make sure the Sexuality Closer Description (observation_source_concept_id = 1585357) rows still exist
# Curation has lost this data due to bad ppi branching logic.  This check is to ensure we do
# not lose this particular data again.  If rows are identified, then there is an issue with the cleaning
# rules (possibly PPI branching) that must be resolved.  This has resulted in a previous hotfix.
# We do not want to repeat the hotfix process.

tpl = JINJA_ENV.from_string('''
SELECT
    observation_source_concept_id
    ,observation_source_value
    ,value_source_concept_id
    ,value_source_value
FROM `{{project_id}}.{{new_rdr}}.observation`
WHERE
  -- check for smoking answers --
  ((observation_source_concept_id IN (1585864, 1585870,1585873, 1586159, 1586162)
    AND value_as_number IS NOT NULL)
    -- check for sexuality answers --
  OR (observation_source_concept_id in (1585357)))
GROUP BY 1, 2, 3, 4
HAVING count(*) = 0
ORDER BY 1, 3
''')
query = tpl.render(new_rdr=new_rdr, project_id=project_id)
execute(client, query)

# ## Participants must have basics data
# Identify any participants who have don't have any responses
# to questions in the basics survey module (see [DC-706](https://precisionmedicineinitiative.atlassian.net/browse/DC-706)). These should be
# reported to the RDR as they are supposed to be filtered out
# from the RDR export.

# +
BASICS_MODULE_CONCEPT_ID = 1586134

# Note: This assumes that concept_ancestor sufficiently
# represents the hierarchy
tpl = JINJA_ENV.from_string("""
WITH

 -- all PPI question concepts in the basics survey module --
 basics_concept AS
 (SELECT
   c.concept_id
  ,c.concept_name
  ,c.concept_code
  FROM `{{DATASET_ID}}.concept_ancestor` ca
  JOIN `{{DATASET_ID}}.concept` c
   ON ca.descendant_concept_id = c.concept_id
  WHERE 1=1
    AND ancestor_concept_id={{BASICS_MODULE_CONCEPT_ID}}
    AND c.vocabulary_id='PPI'
    AND c.concept_class_id='Question')

 -- maps pids to all their associated basics questions in the rdr --
,pid_basics AS
 (SELECT
   person_id
  ,ARRAY_AGG(DISTINCT c.concept_code IGNORE NULLS) basics_codes
  FROM `{{DATASET_ID}}.observation` o
  JOIN basics_concept c
   ON o.observation_concept_id = c.concept_id
  WHERE 1=1
  GROUP BY 1)

 -- list all pids for whom no basics questions are found --
SELECT *
FROM `{{DATASET_ID}}.person`
WHERE person_id not in (select person_id from pid_basics)
""")
query = tpl.render(DATASET_ID=new_rdr,
                   BASICS_MODULE_CONCEPT_ID=BASICS_MODULE_CONCEPT_ID)
execute(client, query)
# -

# # Date conformance check
# COPE surveys contain some concepts that must enforce dates in the observation.value_as_string field.
# For the observation_source_concept_id = 715711, if the value in value_as_string does not meet a standard date format
# of YYYY-mm-dd, return a dataframe with the observation_id and person_id
# Curation needs to contact the RDR team about data discrepancies

tpl = JINJA_ENV.from_string('''
SELECT
    observation_id
    ,person_id
    ,value_as_string
FROM `{{project_id}}.{{new_rdr}}.observation`
WHERE observation_source_concept_id = 715711
AND SAFE_CAST(value_as_string AS DATE) IS NULL
AND value_as_string != 'PMI Skip'
''')
query = tpl.render(new_rdr=new_rdr, project_id=project_id)
execute(client, query)

# # Check pid_rid_mapping table for duplicates
# Duplicates are not allowed in the person_id or research_id columns of the pid_rid_mapping table.
# If found, there is a problem with the RDR import. An RDR On-Call ticket should be opened
# to report the problem. In ideal circumstances, this query will not return any results.
# If a result set is returned, an error has been found for the identified field.
# If the table was not imported, the filename changed, or field names changed,
# this query will fail by design to indicate an unexpected change has occurred.

tpl = JINJA_ENV.from_string('''
SELECT
    'person_id' as id_type
    ,person_id as id
    ,COUNT(person_id) as count
FROM `{{project_id}}.{{new_rdr}}.pid_rid_mapping`
GROUP BY person_id
HAVING count > 1

UNION ALL

SELECT
    'research_id' as id_type
    ,research_id as id
    ,COUNT(research_id) as count
FROM `{{project_id}}.{{new_rdr}}.pid_rid_mapping`
GROUP BY research_id
HAVING count > 1
''')
query = tpl.render(new_rdr=new_rdr, project_id=project_id)
execute(client, query)

# # Ensure all person_ids exist in the person table and have mappings
# All person_ids in the pid_rid_mapping table should exist in the person table.
# If the person record does not exist for a mapping record, there is a problem with the RDR import.
# An RDR On-Call ticket should be opened to report the problem.
# All person_ids in the person table should have a mapping in the pid_rid_mapping table.
# If any person_ids do not have a mapping record, there is a problem with the RDR import.
# An RDR On-Call ticket should be opened to report the problem.
# In ideal circumstances, this query will not return any results.

tpl = JINJA_ENV.from_string('''
SELECT
    'missing_person' as issue_type
    ,person_id
FROM `{{project_id}}.{{new_rdr}}.pid_rid_mapping`
WHERE person_id NOT IN
(SELECT person_id
FROM `{{project_id}}.{{new_rdr}}.person`)

UNION ALL

SELECT
    'unmapped_person' as issue_type
    ,person_id
FROM `{{project_id}}.{{new_rdr}}.person`
WHERE person_id NOT IN
(SELECT person_id
FROM `{{project_id}}.{{new_rdr}}.pid_rid_mapping`)
''')
query = tpl.render(new_rdr=new_rdr, project_id=project_id)
execute(client, query)

# # Check for inconsistencies between primary and RDR pid_rid_mappings
# Mappings which were in previous exports may be removed from a new export for two reasons:
#   1. Participants have withdrawn or
#   2. They were identified as test or dummy data
# Missing mappings from the previous RDR export are therefore not a significant cause for concern.
# However, mappings in the RDR pid_rid_mapping should always be consistent with the
# primary_pid_rid_mapping in pipeline_tables for existing mappings.
# If the same pid has different rids in the pid_rid_mapping and the primary_pid_rid_mapping,
# there is a problem with the RDR import. An RDR On-Call ticket should be opened to report the problem.
# In ideal circumstances, this query will not return any results.

tpl = JINJA_ENV.from_string('''
SELECT
    person_id
FROM `{{project_id}}.{{new_rdr}}.pid_rid_mapping` rdr
JOIN `{{project_id}}.pipeline_tables.primary_pid_rid_mapping` primary
USING (person_id)
WHERE primary.research_id <> rdr.research_id
''')
query = tpl.render(new_rdr=new_rdr, project_id=project_id)
execute(client, query)

# # Checks for basics survey module
# Participants with data in other survey modules must also have data from the basics survey module.
# This check identifies survey responses associated with participants that do not have any responses
# associated with the basics survey module.
# In ideal circumstances, this query will not return any results.

tpl = JINJA_ENV.from_string('''
SELECT DISTINCT person_id FROM `{{project_id}}.{{new_rdr}}.observation`
JOIN `{{project_id}}.{{new_rdr}}.concept` on (observation_source_concept_id=concept_id)
WHERE vocabulary_id = 'PPI' AND person_id NOT IN (
SELECT DISTINCT person_id FROM `{{project_id}}.{{new_rdr}}.concept`
JOIN `{{project_id}}.{{new_rdr}}.concept_ancestor` on (concept_id=ancestor_concept_id)
JOIN `{{project_id}}.{{new_rdr}}.observation` on (descendant_concept_id=observation_concept_id)
WHERE concept_class_id='Module'
AND concept_name IN ('The Basics')
AND questionnaire_response_id IS NOT NULL)
''')
query = tpl.render(new_rdr=new_rdr, project_id=project_id)
execute(client, query)

# ## Participants must be 18 years of age or older to consent
#
# AOU participants are required to be 18+ years of age at the time of consent
# ([DC-1724](https://precisionmedicineinitiative.atlassian.net/browse/DC-1724)),
# based on the date associated with the [ExtraConsent_TodaysDate](https://athena.ohdsi.org/search-terms/terms/1585482)
# row. Any violations should be reported to the RDR team as these should have been filtered out by the RDR ETL process
# ([DA-2073](https://precisionmedicineinitiative.atlassian.net/browse/DA-2073)).

tpl = JINJA_ENV.from_string('''
SELECT *
FROM `{{project_id}}.{{new_rdr}}.observation`
JOIN `{{project_id}}.{{new_rdr}}.person` USING (person_id)
WHERE  (observation_source_concept_id=1585482 OR observation_concept_id=1585482)
AND {{PIPELINE_TABLES}}.calculate_age(observation_date, EXTRACT(DATE FROM birth_datetime)) < 18
''')
query = tpl.render(new_rdr=new_rdr,
                   project_id=project_id,
                   PIPELINE_TABLES=PIPELINE_TABLES)
execute(client, query)

# # Check if concepts for operational use still exist in the data

# According to [this ticket](https://precisionmedicineinitiative.atlassian.net/browse/DC-1792),
# the RDR export should not contain some operational concepts that are irrelevant to researchers.
# Any violations should be reported to the RDR team.

tpl = JINJA_ENV.from_string("""
SELECT
    observation_source_value,
    COUNT(1) AS n_row_violation
FROM `{{project_id}}.{{new_rdr}}.observation`
WHERE observation_source_value IN (
  SELECT observation_source_value FROM `{{project_id}}.operational_data.operational_ehr_consent`
)
GROUP BY 1
HAVING count(1) > 0
""")
query = tpl.render(new_rdr=new_rdr, project_id=project_id)
execute(client, query)

# # Check if Responses for question [46234786](https://athena.ohdsi.org/search-terms/terms/46234786)
# # are updated to 2000000010 - AoUDRC_ResponseRemoval from dates ranging 11/1/2021 – 11/9/2021

# According to [this ticket](https://precisionmedicineinitiative.atlassian.net/browse/DC-2118),
# the RDR export should not contain any responses other than 2000000010 - AoUDRC_ResponseRemoval for
# question - [46234786](https://athena.ohdsi.org/search-terms/terms/46234786) ranging from dates 11/1/2021 – 11/9/2021
# this check will give count of responses that does not meet this condition. Having ) count means this check is passed.

tpl = JINJA_ENV.from_string("""
SELECT
    value_source_concept_id, value_as_concept_id, count(*) as n_row_violation
FROM
 `{{project_id}}.{{new_rdr}}.observation`
WHERE
  observation_source_concept_id = 46234786
  AND (observation_date >= DATE('2021-11-01')
    AND observation_date <= DATE('2021-11-09'))
  AND (value_as_concept_id <> 2000000010
    OR value_source_concept_id <> 2000000010)
group by value_source_concept_id, value_as_concept_id
""")
query = tpl.render(new_rdr=new_rdr, project_id=project_id)
execute(client, query)

# # COPE survey mapping

# There is a known issue that COPE survey questions all map to the module
# 1333342 (COPE survey with no version specified). This check is to confirm
# if this issue still exists in the vocabulary or not.
# If this issue is fixed, each COPE survey questions will have mapping to
# individual COPE survey modules and will no longer have mapping to 1333342.
# cope_question_concept_ids are collected using the SQL listed in DC-2641:
# [DC-2641](https://precisionmedicineinitiative.atlassian.net/browse/DC-2641).

cope_question_concept_ids = [
    596884, 596885, 596886, 596887, 596888, 702686, 713888, 715711, 715713,
    715714, 715719, 715720, 715721, 715722, 715723, 715724, 715725, 715726,
    903629, 903630, 903631, 903632, 903633, 903634, 903635, 903641, 903642,
    1310051, 1310052, 1310053, 1310054, 1310056, 1310058, 1310060, 1310062,
    1310065, 1310066, 1310067, 1310132, 1310133, 1310134, 1310135, 1310136,
    1310137, 1310138, 1310139, 1310140, 1310141, 1310142, 1310144, 1310145,
    1310146, 1310147, 1310148, 1332734, 1332735, 1332737, 1332738, 1332739,
    1332741, 1332742, 1332744, 1332745, 1332746, 1332747, 1332748, 1332749,
    1332750, 1332751, 1332752, 1332753, 1332754, 1332755, 1332756, 1332762,
    1332763, 1332767, 1332769, 1332792, 1332793, 1332794, 1332795, 1332796,
    1332797, 1332800, 1332801, 1332802, 1332803, 1332804, 1332805, 1332806,
    1332807, 1332808, 1332819, 1332820, 1332822, 1332824, 1332826, 1332828,
    1332829, 1332830, 1332831, 1332832, 1332833, 1332835, 1332843, 1332847,
    1332848, 1332849, 1332853, 1332854, 1332861, 1332862, 1332863, 1332866,
    1332867, 1332868, 1332869, 1332870, 1332871, 1332872, 1332874, 1332876,
    1332878, 1332880, 1332935, 1332937, 1332944, 1332998, 1333004, 1333011,
    1333012, 1333013, 1333014, 1333015, 1333016, 1333017, 1333018, 1333019,
    1333020, 1333021, 1333022, 1333023, 1333024, 1333102, 1333104, 1333105,
    1333118, 1333119, 1333120, 1333121, 1333156, 1333163, 1333164, 1333165,
    1333166, 1333167, 1333168, 1333182, 1333183, 1333184, 1333185, 1333186,
    1333187, 1333188, 1333189, 1333190, 1333191, 1333192, 1333193, 1333194,
    1333195, 1333200, 1333216, 1333221, 1333234, 1333235, 1333274, 1333275,
    1333276, 1333277, 1333278, 1333279, 1333280, 1333281, 1333285, 1333286,
    1333287, 1333288, 1333289, 1333291, 1333292, 1333293, 1333294, 1333295,
    1333296, 1333297, 1333298, 1333299, 1333300, 1333301, 1333303, 1333311,
    1333312, 1333313, 1333314, 1333324, 1333325, 1333326, 1333327, 1333328
]

tpl = JINJA_ENV.from_string("""
WITH question_topic_module AS (
  SELECT
      cr1.concept_id_1 AS question,
      cr1.concept_id_2 AS topic,
      cr2.concept_id_2 AS module
  FROM `{{projcet_id}}.{{dataset}}.concept_relationship` cr1
  JOIN `{{projcet_id}}.{{dataset}}.concept` c1 ON cr1.concept_id_2 = c1.concept_id
  JOIN `{{projcet_id}}.{{dataset}}.concept_relationship` cr2 ON c1.concept_id = cr2.concept_id_1
  JOIN `{{projcet_id}}.{{dataset}}.concept` c2 ON cr2.concept_id_2 = c2.concept_id
  WHERE cr1.concept_id_1 IN ({{cope_question_concept_ids}})
  AND c1.concept_class_id = 'Topic'
  AND c2.concept_class_id = 'Module'
)
SELECT DISTINCT question FROM question_topic_module
WHERE module = 1333342
""")
query = tpl.render(
    new_rdr=new_rdr,
    project_id=project_id,
    dataset=new_rdr,
    cope_question_concept_ids=", ".join(
        str(concept_id) for concept_id in cope_question_concept_ids))
df = execute(client, query)

# +
success_msg = '''
    The mapping issue is resolved. Double-check each concept is mapped to individual COPE module.
    Once we double-checked it, we can remove this QC from this notebook.
'''
failure_msg = '''
    The mapping issue still exists. There are <b>{code_count}</b> concepts for COPE questions
    that map to 1333342. Notify Odysseus that the issue still persists.
    For pipeline, we can use cope_survey_semantic_version_map to diffrentiate COPE module versions,
    so we can still move on. See DC-2641 for detail.
'''

render_message(df,
               success_msg,
               failure_msg,
               failure_msg_args={'code_count': len(df)})
# -

# ### RDR date cutoff check

# Check that survey dates are not beyond the RDR cutoff date, also check observation.
query = JINJA_ENV.from_string("""
SELECT
  'observation' AS TABLE,
  COUNT(*) AS rows_beyond_cutoff
FROM
  `{{project_id}}.{{new_rdr}}.observation`
WHERE
  observation_date > DATE('{{rdr_cutoff_date}}')
UNION ALL
SELECT
  'survey_conduct_start' AS TABLE,
  COUNT(*) AS rows_beyond_cutoff
FROM
  `{{project_id}}.{{new_rdr}}.survey_conduct`
WHERE
  survey_start_date > DATE('{{rdr_cutoff_date}}')
UNION ALL
SELECT
  'survey_conduct_end' AS TABLE,
  COUNT(*) AS rows_beyond_cutoff
FROM
  `{{project_id}}.{{new_rdr}}.survey_conduct`
WHERE
  survey_end_date > DATE('{{rdr_cutoff_date}}')
""").render(project_id=project_id,
            new_rdr=new_rdr,
            rdr_cutoff_date=rdr_cutoff_date)

execute(client, query)

# # DEATH table - HealthPro deceased records validation

# From CDR V8, Curation receives HealthPro deceased records from RDR. We must ensure the incoming records follow the requirement.
# Here is the highlight of the technical requirement of the incoming `death` records from RDR.
# - Person_id and death_type_concept_id are populated
# - Death_date and death_datetime are populated but can be NULL
# - Map all deceased records from HealthPro as “Case Report Form” (concept ID: 32809)
# - Cause_concept_id, cause_source_value, and cause_source_concept_id columns, set value to NULL
# - Src_id filled in with “healthpro”

# +
query_if_empty = JINJA_ENV.from_string("""
SELECT COUNT(*)
FROM `{{project_id}}.{{dataset}}.aou_death`
HAVING COUNT(*) = 0
""").render(project_id=project_id, dataset=new_rdr)
df_if_empty = execute(client, query_if_empty)

query_if_duplicate = JINJA_ENV.from_string("""
SELECT person_id, COUNT(*) 
FROM `{{project_id}}.{{dataset}}.aou_death`
GROUP BY person_id
HAVING COUNT(*) > 1
""").render(project_id=project_id, dataset=new_rdr)
df_if_duplicate = execute(client, query_if_duplicate)

query = JINJA_ENV.from_string("""
SELECT
    person_id
FROM `{{project_id}}.{{dataset}}.aou_death`
WHERE death_type_concept_id != 32809
OR cause_concept_id IS NOT NULL
OR cause_source_value IS NOT NULL
OR cause_source_concept_id IS NOT NULL
OR src_id != 'healthpro'
""").render(project_id=project_id, dataset=new_rdr)
df = execute(client, query)

success_msg_if_empty = 'AOU_DEATH table has some records.'
failure_msg_if_empty = '''AOU_DEATH table is empty. Investigate if the data is empty from the beginning or our import is not working.
If it's empty from the beginning, contact RDR and have them send HealthPro deceased records. 
If it's import issue, investigate what's causing the issue and solve the issue ASAP.
'''
success_msg_if_duplicate = 'Death records are up to one record per person_id.'
failure_msg_if_duplicate = '''
    <b>{code_count}</b> participants have more than one death records. We expect only up to one death record per person_id from RDR.
    Investigate and confirm if (a) bad data is coming from RDR, (b) the requirement has changed, or (c) something else.
'''
success_msg = 'All death records follow the technical requirement for the CDR V8 release.'
failure_msg = '''
    <b>{code_count}</b> records do not follow the technical requirement for the CDR V8 release. 
    Investigate and confirm if (a) bad data is coming from RDR, (b) the requirement has changed, or (c) something else.
'''
render_message(df_if_empty, success_msg_if_empty, failure_msg_if_empty)

render_message(df_if_duplicate,
               success_msg_if_duplicate,
               failure_msg_if_duplicate,
               failure_msg_args={'code_count': len(df_if_duplicate)})

render_message(df,
               success_msg,
               failure_msg,
               failure_msg_args={'code_count': len(df)})
# -
# # Check src_ids
# Check that every record contains a valid src_id. The check passes if no records are returned.

queries = []
ids_template = JINJA_ENV.from_string("""
with ids as (
  SELECT 
    hpo_id 
  FROM
    `{{project_id}}.{{pipeline}}.{{site_maskings}}`
  WHERE NOT
    REGEXP_CONTAINS(src_id, r'(?i)(PPI/PM)|(EHR site)')
)
""")
src_ids_table = ids_template.render(project_id=project_id,
                                    pipeline=PIPELINE_TABLES,
                                    site_maskings=SITE_MASKING_TABLE_ID)
for table in SRC_ID_TABLES:
    tpl = JINJA_ENV.from_string("""
    SELECT
      \'{{table_name}}\' AS table_name,
      src_id,
      count(*) as n_violations
    FROM
      `{{project_id}}.{{new_rdr}}.{{table_name}}`
    WHERE
    (
      LOWER(src_id) NOT IN
        (SELECT * FROM ids) OR src_id IS NULL
    )
    GROUP BY 1,2
  """)
    query = tpl.render(project_id=project_id, new_rdr=new_rdr, table_name=table)
    queries.append(query)
all_queries = '\nUNION ALL\n'.join(queries)
execute(client, f'{src_ids_table}\n{all_queries}')

# # Check Wear Consent Counts
#
# `Wear_consent` and `wear_consent_ptsc` records should be seen in the export.
#
# Results expectations: The result should be roughly 16 rows. Differences here most likely aren't an issue.
#
# **Visual check:** <br>
# * **PASS**   The result **includes** observation_source_value: `resultsconsent_wear` <br>
# * **FAIL**   The result **does not include** observation_source_value: `resultsconsent_wear`.  If this row does not exist, confirm the finding, and report to RDR. These records are required for proper suppression of wear fitbit records.

# Get counts of wear_consent records
query = JINJA_ENV.from_string("""
SELECT
  observation_source_value,
  COUNT(*) AS n
FROM
  `{{project_id}}.{{new_rdr}}.observation` o
  LEFT JOIN   `{{project_id}}.{{new_rdr}}.survey_conduct` sc
  ON sc.survey_conduct_id = o.questionnaire_response_id
WHERE sc.survey_concept_id IN (2100000011,2100000012) -- captures questions asked in multiple surveys --
OR LOWER(observation_source_value) IN UNNEST ({{wear_codes}}) -- captures those that might be missing from survey_conduct --
GROUP BY 1

""").render(project_id=project_id,
            new_rdr=new_rdr,
            wear_codes=WEAR_SURVEY_CODES)
execute(client, query)
