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

# This list is created by querying the redcap surveys. In case of needed update, query provided in the comments of DC3407
expected_strings = ['cidi5_20', 'cidi5_24', 'cidi5_28', 'cidi5_31', 'mhqukb_48_age',
 'mhqukb_50_number', 'mhqukb_51_number', 'mhqukb_52_number',
 'mhqukb_53_number', 'record_id', 'helpmewithconsent_name',
 'other_concerns', 'other_reasons', 'resultsconsent_emailmecopy',
 'resultsconsent_signaturedate', 'consentpii_helpwithconsentsignature',
 'extraconsent_signature_type', 'extraconsent_todaysdate',
 'piiaddress_streetaddress', 'piiaddress_streetaddress2',
 'piibirthinformation_birthdate', 'piicontactinformation_phone',
 'piiname_first', 'piiname_last', 'piiname_middle',
 'streetaddress_piicity', 'streetaddress_piizip', 'basics_11a_cope_a_33',
 'basics_xx', 'basics_xx20', 'cdc_covid_19_7_xx22_date', 'cope_a_126',
 'cope_a_160', 'cope_a_85', 'copect_50_xx19_cope_a_152',
 'copect_50_xx19_cope_a_198', 'copect_50_xx19_cope_a_57',
 'cu_covid_cope_a_204', 'eds_follow_up_1_xx', 'ipaq_1_cope_a_24',
 'ipaq_2_cope_a_160', 'ipaq_2_cope_a_85', 'ipaq_3_cope_a_24',
 'ipaq_4_cope_a_160', 'ipaq_4_cope_a_85', 'ipaq_5_cope_a_24',
 'ipaq_6_cope_a_160', 'ipaq_6_cope_a_85', 'lifestyle_2_xx12_cope_a_152',
 'lifestyle_2_xx12_cope_a_198', 'lifestyle_2_xx12_cope_a_57',
 'tsu_ds5_13_xx42_cope_a_226', 'cdc_covid_19_7_xx23_other_cope_a_204',
 'cdc_covid_19_n_a2', 'cdc_covid_19_n_a4', 'cdc_covid_19_n_a8',
 'cope_aou_xx_2_a', 'dmfs_29a', 'msds_17_c',
 'nhs_covid_fhc17b_cope_a_226', 'ehrconsentpii_helpwithconsentsignature',
 'ehrconsentpii_todaysdate', 'ehrconsentpii_todaysdateilhippawitness',
 'sensitivetype2_domesticviolence', 'sensitivetype2_genetictesting',
 'sensitivetype2_hivaids', 'sensitivetype2_mentalhealth',
 'sensitivetype2_substanceuse', 'signature_type', 'cidi5_15',
 'mhqukb_25_number', 'mhqukb_26_age', 'mhqukb_28_age', 'ss_2_age',
 'ss_3_age_1', 'ss_3_age_2', 'ss_3_number',
 'english_exploring_the_mind_consent_form', 'etm_help_name',
 'cdc_covid_xx_a_date1', 'cdc_covid_xx_a_date2',
 'cdc_covid_xx_b_firstdose_other', 'cdc_covid_xx_b_seconddose_other',
 'cdc_covid_xx_symptom_cope_350',
 'cdc_covid_xx_symptom_seconddose_cope_350', 'dmfs_29_seconddose_other',
 'othercancer_daughterfreetextbox', 'othercancer_fatherfreetextbox',
 'othercancer_grandparentfreetextbox', 'othercancer_motherfreetextbox',
 'othercancer_siblingfreetextbox', 'othercancer_sonfreetextbox',
 'othercondition_daughterfreetextbox', 'othercondition_fatherfreetextbox',
 'othercondition_grandparentfreetextbox',
 'othercondition_motherfreetextbox', 'othercondition_siblingfreetextbox',
 'othercondition_sonfreetextbox', 'cdc_covid_xx_b_other',
 'otherdelayedmedicalcare_freetext',
 'attemptquitsmoking_completelyquitage', 'otherspecify_otherdrugstextbox',
 'smoking_averagedailycigarettenumber',
 'smoking_currentdailycigarettenumber',
 'smoking_dailysmokestartingagenumber', 'smoking_numberofyearsnumber',
 'cdc_covid_xx_a_date10', 'cdc_covid_xx_a_date11',
 'cdc_covid_xx_a_date12', 'cdc_covid_xx_a_date13',
 'cdc_covid_xx_a_date14', 'cdc_covid_xx_a_date15',
 'cdc_covid_xx_a_date16', 'cdc_covid_xx_a_date17', 'cdc_covid_xx_a_date3',
 'cdc_covid_xx_a_date4', 'cdc_covid_xx_a_date5', 'cdc_covid_xx_a_date6',
 'cdc_covid_xx_a_date7', 'cdc_covid_xx_a_date8', 'cdc_covid_xx_a_date9',
 'cdc_covid_xx_b_dose10_other', 'cdc_covid_xx_b_dose11_other',
 'cdc_covid_xx_b_dose12_other', 'cdc_covid_xx_b_dose13_other',
 'cdc_covid_xx_b_dose14_other', 'cdc_covid_xx_b_dose15_other',
 'cdc_covid_xx_b_dose16_other', 'cdc_covid_xx_b_dose17_other',
 'cdc_covid_xx_b_dose3_other', 'cdc_covid_xx_b_dose4_other',
 'cdc_covid_xx_b_dose5_other', 'cdc_covid_xx_b_dose6_other',
 'cdc_covid_xx_b_dose7_other', 'cdc_covid_xx_b_dose8_other',
 'cdc_covid_xx_b_dose9_other', 'cdc_covid_xx_symptom_cope_350_dose10',
 'cdc_covid_xx_symptom_cope_350_dose11',
 'cdc_covid_xx_symptom_cope_350_dose12',
 'cdc_covid_xx_symptom_cope_350_dose13',
 'cdc_covid_xx_symptom_cope_350_dose14',
 'cdc_covid_xx_symptom_cope_350_dose15',
 'cdc_covid_xx_symptom_cope_350_dose16',
 'cdc_covid_xx_symptom_cope_350_dose17',
 'cdc_covid_xx_symptom_cope_350_dose3',
 'cdc_covid_xx_symptom_cope_350_dose4',
 'cdc_covid_xx_symptom_cope_350_dose5',
 'cdc_covid_xx_symptom_cope_350_dose6',
 'cdc_covid_xx_symptom_cope_350_dose7',
 'cdc_covid_xx_symptom_cope_350_dose8',
 'cdc_covid_xx_symptom_cope_350_dose9', 'cdc_covid_xx_type_dose10_other',
 'cdc_covid_xx_type_dose11_other', 'cdc_covid_xx_type_dose12_other',
 'cdc_covid_xx_type_dose13_other', 'cdc_covid_xx_type_dose14_other',
 'cdc_covid_xx_type_dose15_other', 'cdc_covid_xx_type_dose16_other',
 'cdc_covid_xx_type_dose17_other', 'cdc_covid_xx_type_dose3_other',
 'cdc_covid_xx_type_dose4_other', 'cdc_covid_xx_type_dose5_other',
 'cdc_covid_xx_type_dose6_other', 'cdc_covid_xx_type_dose7_other',
 'cdc_covid_xx_type_dose8_other', 'cdc_covid_xx_type_dose9_other',
 'dmfs_29_additionaldose_other',
 'organtransplant_bloodvesseltransplantdate',
 'organtransplant_bonetransplantdate',
 'organtransplant_corneatransplantdate',
 'organtransplant_hearttransplantdate',
 'organtransplant_intestinetransplantdate',
 'organtransplant_kidneytransplantdate',
 'organtransplant_livertransplantdate',
 'organtransplant_lungtransplantdate',
 'organtransplant_otherorgantransplantdate',
 'organtransplant_othertissuetransplantdate',
 'organtransplant_pancreastransplantdate',
 'organtransplant_skintransplantdate',
 'organtransplant_valvetransplantdate', 'otherorgan_freetextbox',
 'othertissue_freetextbox',
 'outsidetravel6month_outsidetravel6monthhowlong',
 'outsidetravel6month_outsidetravel6monthwheretraveled',
 'overallhealth_hysterectomyhistoryage',
 'overallhealthovaryremovalhistoryage',
 'otherarthritis_daughterfreetextbox', 'otherarthritis_fatherfreetextbox',
 'otherarthritis_freetextbox', 'otherarthritis_grandparentfreetextbox',
 'otherarthritis_motherfreetextbox', 'otherarthritis_siblingfreetextbox',
 'otherarthritis_sonfreetextbox',
 'otherbonejointmuscle_daughterfreetextbox',
 'otherbonejointmuscle_fatherfreetextbox',
 'otherbonejointmuscle_freetextbox',
 'otherbonejointmuscle_grandparentfreetextbox',
 'otherbonejointmuscle_motherfreetextbox',
 'otherbonejointmuscle_siblingfreetextbox',
 'otherbonejointmuscle_sonfreetextbox',
 'otherbrainnervoussystem_daughterfreetextbox',
 'otherbrainnervoussystem_fatherfreetextbox',
 'otherbrainnervoussystem_freetextbox',
 'otherbrainnervoussystem_grandparentfreetextbox',
 'otherbrainnervoussystem_motherfreetextbox',
 'otherbrainnervoussystem_siblingfreetextbox',
 'otherbrainnervoussystem_sonfreetextbox', 'othercancer_freetextbox',
 'otherdiabetes_daughterfreetextbox', 'otherdiabetes_fatherfreetextbox',
 'otherdiabetes_freetextbox', 'otherdiabetes_grandparentfreetextbox',
 'otherdiabetes_motherfreetextbox', 'otherdiabetes_siblingfreetextbox',
 'otherdiabetes_sonfreetextbox', 'otherdiagnosis_daughterfreetextbox',
 'otherdiagnosis_fatherfreetextbox', 'otherdiagnosis_freetextbox',
 'otherdiagnosis_grandparentfreetextbox',
 'otherdiagnosis_motherfreetextbox', 'otherdiagnosis_siblingfreetextbox',
 'otherdiagnosis_sonfreetextbox',
 'otherdigestivecondition_daughterfreetextbox',
 'otherdigestivecondition_fatherfreetextbox',
 'otherdigestivecondition_freetextbox',
 'otherdigestivecondition_grandparentfreetextbox',
 'otherdigestivecondition_motherfreetextbox',
 'otherdigestivecondition_siblingfreetextbox',
 'otherdigestivecondition_sonfreetextbox',
 'otherhearingeye_daughterfreetextbox',
 'otherhearingeye_fatherfreetextbox', 'otherhearingeye_freetextbox',
 'otherhearingeye_grandparentfreetextbox',
 'otherhearingeye_motherfreetextbox',
 'otherhearingeye_siblingfreetextbox', 'otherhearingeye_sonfreetextbox',
 'otherheartorbloodcondition_daughterfreetextbox',
 'otherheartorbloodcondition_fatherfreetextbox',
 'otherheartorbloodcondition_freetextbox',
 'otherheartorbloodcondition_grandparentfreetextbox',
 'otherheartorbloodcondition_motherfreetextbox',
 'otherheartorbloodcondition_siblingfreetextbox',
 'otherheartorbloodcondition_sonfreetextbox',
 'otherhormoneendocrine_daughterfreetextbox',
 'otherhormoneendocrine_fatherfreetextbox',
 'otherhormoneendocrine_freetextbox',
 'otherhormoneendocrine_grandparentfreetextbox',
 'otherhormoneendocrine_motherfreetextbox',
 'otherhormoneendocrine_siblingfreetextbox',
 'otherhormoneendocrine_sonfreetextbox',
 'otherinfectiousdisease_freetextbox',
 'otherkidneycondition_daughterfreetextbox',
 'otherkidneycondition_fatherfreetextbox',
 'otherkidneycondition_freetextbox',
 'otherkidneycondition_grandparentfreetextbox',
 'otherkidneycondition_motherfreetextbox',
 'otherkidneycondition_siblingfreetextbox',
 'otherkidneycondition_sonfreetextbox',
 'othermentalhealthsubstanceuse_daughterfreetextbox',
 'othermentalhealthsubstanceuse_fatherfreetextbox',
 'othermentalhealthsubstanceuse_freetextbox',
 'othermentalhealthsubstanceuse_grandparentfreetextb',
 'othermentalhealthsubstanceuse_motherfreetextbox',
 'othermentalhealthsubstanceuse_siblingfreetextbox',
 'othermentalhealthsubstanceuse_sonfreetextbox',
 'otherrespiratory_daughterfreetextbox',
 'otherrespiratory_fatherfreetextbox', 'otherrespiratory_freetextbox',
 'otherrespiratory_grandparentfreetextbox',
 'otherrespiratory_motherfreetextbox',
 'otherrespiratory_siblingfreetextbox', 'otherrespiratory_sonfreetextbox',
 'otherthyroid_daughterfreetextbox', 'otherthyroid_fatherfreetextbox',
 'otherthyroid_freetextbox', 'otherthyroid_grandparentfreetextbox',
 'otherthyroid_motherfreetextbox', 'otherthyroid_siblingfreetextbox',
 'otherthyroid_sonfreetextbox', 'self_reported_height_cm',
 'self_reported_height_ft', 'self_reported_height_in',
 'self_reported_weight_kg', 'self_reported_weight_pounds',
 'sdoh_eds_follow_up_1_xx', 'urs_8c', 'aian_tribe',
 'aiannoneofthesedescribeme_aianfreetext',
 'blacknoneofthesedescribeme_blackfreetext',
 'employmentworkaddress_addresslineone',
 'employmentworkaddress_addresslinetwo', 'employmentworkaddress_city',
 'employmentworkaddress_country', 'employmentworkaddress_zipcode',
 'hispanicnoneofthesedescribeme_hispanicfreetext',
 'livingsituation_howmanypeople',
 'livingsituation_livingsituationfreetext',
 'livingsituation_peopleunder18',
 'menanoneofthesedescribeme_menafreetext',
 'nhpinoneofthesedescribeme_nhpifreetext',
 'noneofthesedescribeme_asianfreetext', 'otherhealthplan_freetext',
 'persononeaddress_persononeaddresscity',
 'persononeaddress_persononeaddresszipcode',
 'secondarycontactinfo_persononeaddressone',
 'secondarycontactinfo_persononeaddresstwo',
 'secondarycontactinfo_persononeemail',
 'secondarycontactinfo_persononefirstname',
 'secondarycontactinfo_persononelastname',
 'secondarycontactinfo_persononemiddleinitial',
 'secondarycontactinfo_persononetelephone',
 'secondarycontactinfo_secondcontactsaddressone',
 'secondarycontactinfo_secondcontactsaddresstwo',
 'secondarycontactinfo_secondcontactsemail',
 'secondarycontactinfo_secondcontactsfirstname',
 'secondarycontactinfo_secondcontactslastname',
 'secondarycontactinfo_secondcontactsmiddleinitial',
 'secondarycontactinfo_secondcontactsnumber',
 'secondcontactsaddress_secondcontactcity',
 'secondcontactsaddress_secondcontactzipcode',
 'sexatbirthnoneofthese_sexatbirthtextbox',
 'socialsecurity_socialsecuritynumber',
 'somethingelse_sexualitysomethingelsetextbox',
 'specifiedgender_specifiedgendertextbox', 'thebasics_countryborntextbox',
 'whatraceethnicity_raceethnicitynoneofthese',
 'whitenoneofthesedescribeme_whitefreetext', 'timeofday',
 'wearconsent_todaysdate']


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
# If most concepts are mapped, this check passes. If only some concepts are not mapping properly these are most likely known vocabulary issues or linked to new surveys not yet in Athena.
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

# # Check numeric data in value_as_string
# Some numeric data is expected in value_as_string.  For example, zip codes or other contact specific information.
#
# **If the check fails, manually review the results.** <br>
# False positives are possible. The suggested first step of investigation is to run the query in the comments of DC3407. This will provide any new text type questions from the surveys that can be added to the list `expected_strings`.

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

# # Check the expectations of survey_conduct - survey list
#
# Confirm that all expected surveys have records in survey_conduct. Check ignores snap surveys because these surveys are not expected in any release.
#
# Generally the list of surveys should increase from one export to the next.
#
# Investigate any surveys that were available in the previous export but not in the current export. 
# Also make sure that any new expected surveys are listed in the current rdr.

tpl = JINJA_ENV.from_string('''
SELECT
  prev.survey_source_value AS survey_in_previous_rdr,
  prev.n as previous_count,
  curr.survey_source_value AS survey_in_current_rdr,
  curr.n as current_count
FROM (SELECT survey_source_value, COUNT(survey_conduct_id) as n FROM `{{project_id}}.{{new_rdr}}.survey_conduct` GROUP BY survey_source_value) curr
FULL OUTER JOIN (SELECT survey_source_value, COUNT(survey_conduct_id) as n FROM `{{project_id}}.{{old_rdr}}.survey_conduct` GROUP BY survey_source_value) prev
  USING (survey_source_value)
WHERE NOT (REGEXP_CONTAINS(prev.survey_source_value,'(?i)SNAP')
      OR REGEXP_CONTAINS(curr.survey_source_value,'(?i)SNAP'))
AND (prev.survey_source_value IS NULL 
     OR curr.survey_source_value IS NULL
     OR curr.n < prev.n)
ORDER BY prev.survey_source_value
''')
query = tpl.render(new_rdr=new_rdr, old_rdr=old_rdr, project_id=project_id)
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

# # Participants must be 18 years of age or older to consent
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

# # RDR date cutoff check

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
# This query checks the count of wear consent observations. If the number of observations is not decreasing this check will pass.
#
# **If this check fails** Investigate why the number of observations have decreased. These data are important for the creation of the wear_study table and therefore data suppression

# +
# Get counts of wear_consent records
query = JINJA_ENV.from_string("""

SELECT
  curr.observation_source_value AS concept
 ,prev.row_count AS _{{old_rdr}}
 ,curr.row_count AS _{{new_rdr}}
 ,(curr.row_count - prev.row_count) row_diff
FROM (SELECT DISTINCT observation_source_value, COUNT(*) as row_count
    FROM `{{project_id}}.{{new_rdr}}.observation` o
    WHERE observation_source_value = 'resultsconsent_wear'
    GROUP BY 1) curr
JOIN (SELECT DISTINCT observation_source_value, COUNT(*) row_count 
    FROM `{{project_id}}.{{old_rdr}}.observation` o
    WHERE observation_source_value = 'resultsconsent_wear'
    GROUP BY 1) prev
USING (observation_source_value)
GROUP BY 1,2,3

""").render(project_id=project_id,
            new_rdr=new_rdr,
            old_rdr=old_rdr)
df = execute(client, query)

if sum(df['row_diff']) < 0:
    display(df,
        HTML(f'''
                <h3>
                    Check Status: <span style="color:red">FAILURE</span>
                </h3>
                <p>
                    Wear consent records have been lost since the last rdr. Investigate. See description.
                </p>
            '''))
else:
    display(df,
        HTML(f'''
                <h3>
                    Check Status: <span style="color:green">PASS</span>
                </h3>
                <p>
                    An increasing number of wear consents are expected.
                </p>
                
            '''))
# -

# # Check Wear Consent Mapping
# This mapping is required to keep the observations being dropped in the rdr cleaning stage and also required to create the wear_study table.
#
# **If this check fails**, verify the query results before notifying the rdr team.

# +
query = JINJA_ENV.from_string("""
SELECT
  'Mandatory mapping to standard is missing' as issue,
  COUNT(*) AS n 
FROM `{{project_id}}.{{new_rdr}}.observation` o
WHERE observation_source_value = 'resultsconsent_wear'
AND (observation_source_concept_id != 2100000010 OR
    value_source_concept_id NOT IN (2100000008, 2100000009, 903096) -- wear_no, wear_yes, pmi_skip --
    )

""").render(project_id=project_id,
            new_rdr=new_rdr)
df = execute(client, query)

if sum(df['n']) != 0:
    display(df,
        HTML(f'''
                <h3>
                    Check Status: <span style="color:red">FAILURE</span>
                </h3>
                <p>
                    These are mandatory mappings. Investigate. See description.
                </p>
            '''))
else:
    display(df,
        HTML(f'''
                <h3>
                    Check Status: <span style="color:green">PASS</span>
                </h3>
                <p>
                    All mandatory wear concent records are mapped as expected.
                </p>
                
            '''))
# -

# # Check consent_validation for expected number of consent status
#
# The 'consent_validation' table is renamed from 'consent' in the rdr import script. This table is used to suppress data in `remove_ehr_data_without_consent.py`.
#
# **"Have duplicate consent statuses"** These participants have multiple consent_validation records with the same status. 
# **"Descrepancy btn consent_validation and obs"** Where a consent_validation record has no record in observation or vice versa.
# **"Consent status is NULL"** Whereconsent_for_electronic_health_records(consent status) are NULL
# **"Varying consent statuses per consent answer"** Where a single consent record in observation has more than one consent status in consent_validation.
#

# +
# Count of participants with multiple validation status' for their ehr consent records.
query = JINJA_ENV.from_string("""

WITH obs_consents AS (SELECT 
*
FROM `{{project_id}}.{{new_rdr}}.observation`
WHERE observation_source_value = 'EHRConsentPII_ConsentPermission' ),


issue_queries AS (
SELECT 
"Have duplicate consent statuses" AS issue,
COUNT(*) AS n
FROM (SELECT DISTINCT * EXCEPT (consent_for_electronic_health_records_authored) FROM `{{project_id}}.{{new_rdr}}.consent_validation` )
GROUP BY person_id
HAVING n>1

UNION ALL 

SELECT 
"Descrepancy btn consent_validation and obs" AS issue,
cv.person_id
FROM `{{project_id}}.{{new_rdr}}.consent_validation` cv
FULL OUTER JOIN obs_consents o
ON cv.person_id = o.person_id AND cv.consent_for_electronic_health_records_authored = CAST(o.observation_datetime AS DATETIME)
WHERE cv.person_id IS NULL OR o.person_id IS NULL

UNION ALL 

SELECT 
"Consent status is NULL" AS issue,
COUNT(*) AS n
FROM `{{project_id}}.{{new_rdr}}.consent_validation` cv
WHERE consent_for_electronic_health_records IS NULL
GROUP BY cv.person_id

UNION ALL 

SELECT 
"Varying consent statuses per consent answer" as issue
,COUNT(DISTINCT(consent_for_electronic_health_records)) as n
FROM obs_consents o
FULL OUTER JOIN `{{project_id}}.{{new_rdr}}.consent_validation` cv
ON cv.person_id = o.person_id AND cv.consent_for_electronic_health_records_authored = CAST(o.observation_datetime AS DATETIME)
GROUP BY o.person_id, value_source_value
HAVING n >1

)
SELECT DISTINCT issue,
COUNT(*) AS n_person_ids
FROM issue_queries
GROUP BY issue
ORDER BY issue

""").render(project_id=project_id,
            new_rdr=new_rdr)
df = execute(client, query)



success_msg = 'consent_validation passes these checks'
failure_msg = '''
    <b> consent_validation has issues. Investigate.
'''

render_message(df,
               success_msg,
               failure_msg)
# -


