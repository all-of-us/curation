"""
Removing data associated with the combined Personal Family Health Survey.

This survey is expected to be included in the November CDR and later.  We
need to be ready to remove this data from the May CDR.

Original Issue:  DC-2146
"""
# Python Imports
import logging

# Third party imports
from jinja2 import Template

# Project imports
from common import OBSERVATION
from cdr_cleaner.cleaning_rules.base_cleaning_rule import BaseCleaningRule
from constants.cdr_cleaner import clean_cdr as cdr_consts

LOGGER = logging.getLogger(__name__)

ISSUE_NUMBERS = ['DC2146']

# Save rows that will be dropped to a sandboxed dataset.
DROP_SELECTION_QUERY_TMPL = Template("""
CREATE OR REPLACE TABLE `{{project}}.{{sandbox}}.{{drop_table}}` AS
SELECT
  *
FROM
  `{{project}}.{{dataset}}.observation`
WHERE
  -- separated surveys were removed from participant portal on 2021-11-01 --
  -- combined survey was launced on 2021-11-01 --
  observation_date > '2021-10-31'
  AND LOWER(observation_source_value) IN ('record_id',
    'personalfamilyhistory', 'familyhistory_familymedicalhistoryaware',
    'diagnosedhealthcondition_cancercondition', 'cancercondition_bladdercancer_yes',
    'cancer_bladdercancercurrently', 'cancer_howoldwereyoubladdercancer',
    'cancer_rxmedsforbladdercancer', 'cancercondition_bloodcancer_yes',
    'cancer_bloodcancercurrently', 'cancer_howoldwereyoubloodcancer',
    'cancer_rxmedsforbloodcancer', 'cancercondition_bonecancer_yes',
    'cancer_bonecancercurrently', 'cancer_howoldwereyoubonecancer',
    'cancer_rxmedsforbonecancer', 'cancercondition_braincancer_yes',
    'cancer_braincancercurrently', 'cancer_howoldwereyoubraincancer',
    'cancer_rxmedsforbraincancer', 'cancercondition_breastcancer_yes',
    'cancer_breastcancercurrently', 'cancer_howoldwereyoubreastcancer',
    'cancer_rxmedsforbreastcancer', 'cancercondition_cervicalcancer_yes',
    'cancer_cervicalcancercurrently', 'cancer_howoldwereyoucervicalcancer',
    'cancer_rxmedsforcervicalcancer', 'cancercondition_colonrectalcancer_yes',
    'cancer_colonrectalcancercurrently', 'cancer_howoldwereyoucolonrectalcancer',
    'cancer_rxmedsforcolonrectalcancer', 'cancercondition_endocrinecancer_yes',
    'cancer_endocrinecancercurrently', 'cancer_howoldwereyouendocrinecancer',
    'cancer_rxmedsforendocrinecancer', 'cancercondition_endometrialcancer_yes',
    'cancer_endometrialcancercurrently', 'cancer_howoldwereyouendometrialcancer',
    'cancer_rxmedsforendometrialcancer', 'cancercondition_esophagealcancer_yes',
    'cancer_esophagealcancercurrently', 'cancer_howoldwereyouesophagealcancer',
    'cancer_rxmedsforesophagealcancer', 'cancercondition_eyecancer_yes',
    'cancer_eyecancercurrently', 'cancer_howoldwereyoueyecancer',
    'cancer_rxmedsforeyecancer', 'cancercondition_headneckcancer_yes',
    'cancer_headneckcancercurrently', 'cancer_howoldwereyouheadneckcancer',
    'cancer_rxmedsforheadneckcancer', 'cancercondition_kidneycancer_yes',
    'cancer_kidneycancercurrently', 'cancer_howoldwereyoukidneycancer',
    'cancer_rxmedsforkidneycancer', 'cancercondition_lungcancer_yes',
    'cancer_lungcancercurrently', 'cancer_howoldwereyoulungcancer',
    'cancer_rxmedsforlungcancer', 'cancercondition_ovariancancer_yes',
    'cancer_ovariancancercurrently', 'cancer_howoldwereyouovariancancer',
    'cancer_rxmedsforovariancancer', 'cancercondition_pancreaticcancer_yes',
    'cancer_pancreaticcancercurrently', 'cancer_howoldwereyoupancreaticcancer',
    'cancer_rxmedsforpancreaticcancer', 'cancercondition_prostatecancer_yes',
    'cancer_prostatecancercurrently', 'cancer_howoldwereyouprostatecancer',
    'cancer_rxmedsforprostatecancer', 'cancercondition_skincancer_yes',
    'cancer_skincancercurrently', 'cancer_howoldwereyouskincancer',
    'cancer_rxmedsforskincancer', 'cancercondition_stomachcancer_yes',
    'cancer_stomachcancercurrently', 'cancer_howoldwereyoustomachcancer',
    'cancer_rxmedsforstomachcancer', 'cancercondition_thyroidcancer_yes',
    'cancer_thyroidcancercurrently', 'cancer_howoldwereyouthyroidcancer',
    'cancer_rxmedsforthyroidcancer', 'cancercondition_othercancer_yes',
    'othercancer_freetextbox', 'othercancer_motherfreetextbox',
    'othercancer_fatherfreetextbox', 'othercancer_siblingfreetextbox',
    'othercancer_daughterfreetextbox', 'othercancer_sonfreetextbox',
    'othercancer_grandparentfreetextbox', 'cancer_othercancercurrently',
    'cancer_howoldwereyouothercancer', 'cancer_rxmedsforothercancer',
    'diagnosedhealthcondition_circulatorycondition', 'diagnosedhealthcondition_anemia_yes',
    'circulatory_anemiacurrently', 'circulatory_howoldwereyouanemia',
    'circulatory_rxmedsforanemia', 'circulatorycondition_aorticaneurysm_yes',
    'circulatory_aorticaneurysmcurrently', 'circulatory_howoldwereyouaorticaneurysm',
    'circulatory_rxmedsforaorticaneurysm', 'circulatorycondition_atrialfibrilation_yes',
    'circulatory_atrialfibrillationcurrently', 'circulatory_howoldwereyouatrialfibrillation',
    'circulatory_rxmedsforatrialfibrillation', 'circulatorycondition_bleedingdisorder_yes',
    'circulatory_bleedingdisordercurrently', 'circulatory_howoldwereyoubleedingdisorder',
    'circulatory_rxmedsforbleedingdisorder', 'circulatorycondition_congestiveheartfailure_yes',
    'circulatory_congestiveheartfailurecurrently', 'circulatory_howoldwereyoucongestiveheartfailure',
    'circulatory_rxmedsforcongestiveheartfailure', 'circulatorycondition_coronaryarteryheartdisease_yes',
    'circulatory_coronaryarterycurrently', 'circulatory_howoldwereyoucoronaryartery',
    'circulatory_rxmedsforcoronaryartery', 'circulatorycondition_heartattack_yes',
    'circulatory_heartattackcurrently', 'circulatory_howoldwereyouheartattack',
    'circulatory_rxmedsforheartattack', 'circulatorycondition_heartvalvedisease_yes',
    'circulatory_heartvalvediseasecurrently', 'circulatory_howoldwereyouheartvalvedisease', 'circulatory_rxmedsforheartvalvedisease',
    'circulatorycondition_highbloodpressure_yes', 'circulatory_hypertensioncurrently',
    'circulatory_howoldwereyouhypertension', 'circulatory_prescribedmedsforhypertension',
    'circulatorycondition_highcholesterol_yes', 'circulatory_highcholesterolcurrently',
    'circulatory_howoldwereyouhighcholesterol', 'circulatory_rxmedsforhighcholesterol',
    'circulatorycondition_peripheralvasculardisease_yes', 'circulatory_peripheralvasculardiseasecurrently',
    'circulatory_howoldwereyouperipheralvasculardisease', 'circulatory_rxmedsforperipheralvasculardisease',
    'circulatorycondition_pulmonaryembolismthrombosis_yes', 'circulatory_pulmonaryembolismcurrently',
    'circulatory_howoldwereyoupulmonaryembolism', 'circulatory_rxmedsforpulmonaryembolism',
    'circulatorycondition_sicklecelldisease_yes', 'circulatory_sicklecelldiseasecurrently',
    'circulatory_howoldwereyousicklecelldisease', 'circulatory_rxmedsforsicklecelldisease',
    'circulatorycondition_stroke_yes', 'circulatory_strokecurrently',
    'circulatory_howoldwereyoustroke', 'circulatory_rxmedsforstroke',
    'circulatorycondition_suddendeath_yes', 'circulatorycondition_transientischemicattack_yes',
    'circulatory_transientischemicattackcurrently', 'circulatory_howoldwereyoutransientischemicattack',
    'circulatory_rxmedsfortransientischemicattack', 'circulatorycondition_otherheartorbloodcondition_ye',
    'otherheartorbloodcondition_freetextbox', 'otherheartorbloodcondition_motherfreetextbox',
    'otherheartorbloodcondition_fatherfreetextbox', 'otherheartorbloodcondition_siblingfreetextbox',
    'otherheartorbloodcondition_daughterfreetextbox', 'otherheartorbloodcondition_sonfreetextbox',
    'otherheartorbloodcondition_grandparentfreetextbox', 'circulatory_otherheartbloodcurrently',
    'circulatory_howoldwereyouotherheartblood', 'circulatory_rxmedsforotherheartblood',
    'diagnosedhealthcondition_digestivecondition', 'digestivecondition_acidreflux_yes',
    'digestive_acidrefluxcurrently', 'digestive_howoldwereyouacidreflux',
    'digestive_rxmedsforacidreflux', 'digestivecondition_bowelobstruction_yes',
    'digestive_bowelobstructioncurrently', 'digestive_howoldwereyoubowelobstruction',
    'digestive_rxmedsforbowelobstruction', 'digestivecondition_celiacdisease_yes',
    'digestive_celiacdiseasecurrently', 'digestive_howoldwereyouceliacdisease',
    'digestive_rxmedsforceliacdisease', 'digestivecondition_colonpolyps_yes',
    'digestive_colonpolypscurrently', 'digestive_howoldwereyoucolonpolyps',
    'digestive_rxmedsforcolonpolyps', 'digestivecondition_crohnsdisease_yes',
    'digestive_crohnsdiseasecurrently', 'digestive_howoldwereyoucrohnsdisease',
    'digestive_rxmedsforcrohnsdisease', 'digestivecondition_diverticulitis_yes',
    'digestive_diverticulosiscurrently', 'digestive_howoldwereyoudiverticulosis',
    'digestive_rxmedsfordiverticulosis', 'digestivecondition_gallstones_yes',
    'digestive_gallstonescurrently', 'digestive_howoldwereyougallstones',
    'digestive_rxmedsforgallstones', 'digestivecondition_hemorrhoids_yes',
    'digestive_hemorrhoidscurrently', 'digestive_howoldwereyouhemorrhoids',
    'digestive_rxmedsforhemorrhoids', 'digestivecondition_hernia_yes',
    'digestive_herniacurrently', 'digestive_howoldwereyouhernia',
    'digestive_rxmedsforhernia', 'digestivecondition_irritablebowelsyndrome_yes',
    'digestive_irritablebowelcurrently', 'digestive_howoldwereyouirritablebowel',
    'digestive_rxmedsforirritablebowel', 'diagnosedhealthcondition_livercondition_yes',
    'digestive_liverconditioncurrently', 'digestive_howoldwereyoulivercondition',
    'digestive_rxmedsforlivercondition', 'digestivecondition_pancreatitis_yes',
    'digestive_pancreatitiscurrently', 'digestive_howoldwereyoupancreatitis',
    'digestive_rxmedsforpancreatitis', 'digestivecondition_pepticulcers_yes',
    'digestive_pepticulcerscurrently', 'digestive_howoldwereyoupepticulcers',
    'digestive_rxmedsforpepticulcers', 'digestivecondition_ulcerativecolitis_yes',
    'digestive_ulcerativecolitiscurrently', 'digestive_howoldwereyouulcerativecolitis',
    'digestive_rxmedsforulcerativecolitis', 'digestivecondition_otherdigestivecondition_yes',
    'otherdigestivecondition_freetextbox', 'otherdigestivecondition_motherfreetextbox',
    'otherdigestivecondition_fatherfreetextbox', 'otherdigestivecondition_siblingfreetextbox',
    'otherdigestivecondition_daughterfreetextbox', 'otherdigestivecondition_sonfreetextbox',
    'otherdigestivecondition_grandparentfreetextbox', 'digestive_otherdigestiveconditioncurrently',
    'digestive_howoldwereyouotherdigestivecondition', 'digestive_rxmedsforotherdigestivecondition',
    'diagnosedhealthcondition_endocrinecondition', 'endocrinecondition_hyperthyroidism_yes',
    'endocrine_hyperthyroidismcurrently', 'endocrine_howoldwereyouhyperthyroidism',
    'endocrine_rxmedsforhyperthyroidism', 'endocrinecondition_hypothyroidism_yes',
    'endocrine_hypothyroidismcurrently', 'endocrine_howoldwereyouhypothyroidism',
    'endocrine_rxmedsforhypothyroidism', 'endocrinecondition_prediabetes_yes',
    'endocrine_prediabetescurrently', 'endocrine_howoldwereyouprediabetes',
    'endocrine_rxmedsforprediabetes', 'endocrinecondition_type1diabetes_yes',
    'endocrine_type1diabetescurrently', 'endocrine_howoldwereyoutype1diabetes',
    'endocrine_rxmedsfortype1diabetes', 'endocrinecondition_type2diabetes_yes',
    'endocrine_type2diabetescurrently', 'endocrine_howoldwereyoutype2diabetes',
    'endocrine_rxmedsfortype2diabetes', 'endocrinecondition_otherdiabetes_yes',
    'otherdiabetes_freetextbox', 'otherdiabetes_motherfreetextbox',
    'otherdiabetes_fatherfreetextbox', 'otherdiabetes_siblingfreetextbox',
    'otherdiabetes_daughterfreetextbox', 'otherdiabetes_sonfreetextbox',
    'otherdiabetes_grandparentfreetextbox', 'endocrine_otherdiabetescurrently',
    'endocrine_howoldwereyouotherdiabetes', 'endocrine_rxmedsforotherdiabetes',
    'endocrinecondition_otherthyroid_yes', 'otherthyroid_freetextbox',
    'otherthyroid_motherfreetextbox', 'otherthyroid_fatherfreetextbox',
    'otherthyroid_siblingfreetextbox', 'otherthyroid_daughterfreetextbox',
    'otherthyroid_sonfreetextbox', 'otherthyroid_grandparentfreetextbox',
    'endocrine_otherthyroidcurrently', 'endocrine_howoldwereyouotherthyroid',
    'endocrine_rxmedsforotherthyroid', 'endocrinecondition_otherhormoneendocrine_yes',
    'otherhormoneendocrine_freetextbox', 'otherhormoneendocrine_motherfreetextbox',
    'otherhormoneendocrine_fatherfreetextbox', 'otherhormoneendocrine_siblingfreetextbox',
    'otherhormoneendocrine_daughterfreetextbox', 'otherhormoneendocrine_sonfreetextbox',
    'otherhormoneendocrine_grandparentfreetextbox', 'endocrine_otherhormone_endocrineconditioncurrently',
    'endocrine_howoldwereyouotherhormoneedocrinecondition', 'rxmedsforotherhormone_endocrinecondition_no',
    'diagnosedhealthcondition_kidneycondition', 'kidneycondition_acutekidneynodialysis_yes',
    'kidney_acutekidneynodialysiscurrently', 'kidney_howoldwereyouacutekidneynodialysis',
    'kidney_rxmedsforacutekidneynodialysis', 'kidneycondition_kidneywithdialysis_yes',
    'kidney_kidneywithdialysiscurrently', 'kidney_howoldwereyoukidneywithdialysis',
    'kidney_rxmedsforkidneywithdialysis', 'kidneycondition_kidneywithoutdialysis_yes',
    'kidney_kidneywithoutdialysiscurrently', 'kidney_howoldwereyoukidneywithoutdialysis',
    'kidney_rxmedsforkidneywithoutdialysis', 'kidneycondition_kidneystones_yes',
    'kidney_kidneystonescurrently', 'kidney_howoldwereyoukidneystones',
    'kidney_rxmedsforkidneystones', 'kidneycondition_otherkidneycondition_yes',
    'otherkidneycondition_freetextbox', 'otherkidneycondition_motherfreetextbox',
    'otherkidneycondition_fatherfreetextbox', 'otherkidneycondition_siblingfreetextbox',
    'otherkidneycondition_daughterfreetextbox', 'otherkidneycondition_sonfreetextbox',
    'otherkidneycondition_grandparentfreetextbox', 'kidney_otherkidneyconditioncurrently',
    'kidney_howoldwereyouotherkidneycondition', 'kidney_rxmedsforotherkidneycondition',
    'diagnosedhealthcondition_respiratorycondition', 'respiratorycondition_asthma_yes',
    'respiratory_asthmacurrently', 'respiratory_howoldwereyouasthma',
    'respiratory_rxmedsforasthma', 'respiratorycondition_chroniclungdisease_yes',
    'respiratory_chroniclungdiseasecurrently', 'respiratory_howoldwereyouchroniclungdisease',
    'respiratory_rxmedsforchroniclungdisease', 'respiratorycondition_sleepapnea_yes',
    'respiratory_sleepapneacurrently', 'respiratory_howoldwereyousleepapnea',
    'respiratory_rxmedsforsleepapnea', 'respiratorycondition_otherlungcondition_yes',
    'otherrespiratory_freetextbox', 'otherrespiratory_motherfreetextbox',
    'otherrespiratory_fatherfreetextbox', 'otherrespiratory_siblingfreetextbox',
    'otherrespiratory_daughterfreetextbox', 'otherrespiratory_sonfreetextbox',
    'otherrespiratory_grandparentfreetextbox', 'respiratory_otherlungconditioncurrently',
    'respiratory_howoldwereyouotherlungcondition', 'respiratory_rxmedsforotherlungcondition',
    'diagnosedhealthcondition_nervouscondition', 'nervouscondition_cerebralpalsy_yes',
    'nervoussystem_cerebralpalsycurrently', 'nervoussystem_howoldwereyoucerebralpalsy',
    'nervoussystem_rxmedsforcerebralpalsy', 'nervouscondition_chronicfatigue_yes',
    'nervoussystem_chronicfatiguecurrently', 'nervoussystem_howoldwereyouchronicfatigue',
    'nervoussystem_rxmedsforchronicfatigue', 'nervouscondition_concussion_yes',
    'nervoussystem_concussioncurrently', 'nervoussystem_howoldwereyouconcussion',
    'nervoussystem_rxmedsforconcussion', 'nervouscondition_dementia_yes',
    'nervoussystem_dementiacurrently', 'nervoussystem_howoldwereyoudementia',
    'nervoussystem_rxmedsfordementia', 'nervouscondition_epilepsyseizure_yes',
    'nervoussystem_epilepsycurrently', 'nervoussystem_howoldwereyouepilepsy',
    'nervoussystem_rxmedsforepilepsy', 'nervouscondition_insomnia_yes',
    'nervoussystem_insomniacurrently', 'nervoussystem_howoldwereyouinsomnia',
    'nervoussystem_rxmedsforinsomnia', 'nervouscondition_amyotrophiclateralsclerosis_yes',
    'nervoussystem_lougehrigsdiseasecurrently', 'nervoussystem_howoldwereyoulougehrigsdisease',
    'nervoussystem_rxmedsforlougehrigsdisease', 'nervouscondition_memoryloss_yes',
    'nervoussystem_memorylosscurrently', 'nervoussystem_howoldwereyoumemoryloss',
    'nervoussystem_rxmedsformemoryloss', 'nervouscondition_migraineheadaches_yes',
    'nervoussystem_migrainecurrently', 'nervoussystem_howoldwereyoumigraine',
    'nervoussystem_rxmedsformigraine', 'nervouscondition_multiplesclerosis_yes',
    'nervoussystem_multiplesclerosiscurrently', 'nervoussystem_howoldwereyoumultiplesclerosis',
    'nervoussystem_rxmedsformultiplesclerosis', 'nervouscondition_musculardystrophy_yes',
    'nervoussystem_musculardystrophycurrently', 'nervoussystem_howoldwereyoumusculardystrophy',
    'nervoussystem_rxmedsformusculardystrophy', 'nervouscondition_narcolepsy_yes',
    'nervoussystem_narcolepsycurrently', 'nervoussystem_howoldwereyounarcolepsy',
    'nervoussystem_rxmedsfornarcolepsy', 'nervouscondition_neuropathy_yes',
    'nervoussystem_neuropathycurrently', 'nervoussystem_howoldwereyouneuropathy',
    'nervoussystem_rxmedsforneuropathy', 'nervouscondition_parkinsons_yes',
    'nervoussystem_parkinsonsdiseasecurrently', 'nervoussystem_howoldwereyouparkinsonsdisease',
    'nervoussystem_rxmedsforparkinsonsdisease', 'nervouscondition_restlesslegssyndrome_yes',
    'nervoussystem_restlesslegcurrently', 'nervoussystem_howoldwereyourestlessleg',
    'nervoussystem_rxmedsforrestlessleg', 'nervouscondition_spinalcordinjury_yes',
    'nervoussystem_spinalcordinjurycurrently', 'nervoussystem_howoldwereyouspinalcordinjury',
    'nervoussystem_rxmedsforspinalcordinjury', 'nervouscondition_traumaticbraininjury_yes',
    'nervoussystem_traumaticbraininjurycurrently', 'nervoussystem_howoldwereyoutraumabraininjury',
    'nervoussystem_rxmedsfortraumabraininjury', 'nervouscondition_otherbrainnervoussystem_yes',
    'otherbrainnervoussystem_freetextbox', 'otherbrainnervoussystem_motherfreetextbox',
    'otherbrainnervoussystem_fatherfreetextbox', 'otherbrainnervoussystem_siblingfreetextbox',
    'otherbrainnervoussystem_daughterfreetextbox', 'otherbrainnervoussystem_sonfreetextbox',
    'otherbrainnervoussystem_grandparentfreetextbox', 'nervoussystem_otherbrainnervoussystemcurrently',
    'nervoussystem_howoldwereyouotherbrainnervoussystem', 'nervoussystem_rxmedsforotherbrainnervoussystem',
    'diagnosedhealthcondition_mentalcondition', 'mentalcondition_alcoholuse_yes',
    'mentalhealth_alcoholdisordercurrently', 'mentalhealth_howoldwereyoualcoholdisorder',
    'mentalhealth_rxmedsforalcoholdisorder', 'mentalcondition_anxietypanic_yes',
    'mentalhealth_anxietycurrently', 'mentalhealth_howoldwereyouanxiety',
    'mentalhealth_rxmedsforanxiety', 'mentalcondition_adhd_yes',
    'mentalhealth_adhdcurrently', 'mentalhealth_howoldwereyouadhd',
    'mentalhealth_rxmedsforadhd', 'mentalcondition_autism_yes',
    'mentalhealth_autismcurrently', 'mentalhealth_howoldwereyouautism',
    'mentalhealth_rxmedsforautism', 'mentalcondition_bipolar_yes',
    'mentalhealth_bipolarcurrently', 'mentalhealth_howoldwereyoubipolar',
    'mentalhealth_rxmedsforbipolar', 'mentalcondition_depression_yes',
    'mentalhealth_depressioncurrently', 'mentalhealth_howoldwereyoudepression',
    'mentalhealth_rxmedsfordepression', 'mentalcondition_druguse_yes',
    'mentalhealth_drugusedisordercurrently', 'mentalhealth_howoldwereyoudrugusedisorder',
    'mentalhealth_rxmedsfordrugusedisorder', 'mentalcondition_eatingdisorder_yes',
    'mentalhealth_eatingdisordercurrently', 'mentalhealth_howoldwereyoueatingdisorder',
    'mentalhealth_rxmedsforeatingdisorder', 'mentalcondition_personalitydisorder_yes',
    'mentalhealth_personalitydisordercurrently', 'mentalhealth_howoldwereyoupersonalitydisorder',
    'mentalhealth_rxmedsforpersonalitydisorder', 'mentalcondition_ptsd_yes',
    'mentalhealth_ptsdcurrently', 'mentalhealth_howoldwereyouptsd',
    'mentalhealth_rxmedsforptsd', 'mentalcondition_schizophrenia_yes',
    'mentalhealth_schizophreniacurrently', 'mentalhealth_howoldwereyouschizophrenia',
    'mentalhealth_rxmedsforschizophrenia', 'mentalcondition_socialphobia_yes',
    'mentalhealth_socialphobiacurrently', 'mentalhealth_howoldwereyousocialphobia',
    'mentalhealth_rxmedsforsocialphobia', 'mentalcondition_othermentalhealthsubstanceuse_yes',
    'othermentalhealthsubstanceuse_freetextbox', 'othermentalhealthsubstanceuse_motherfreetextbox',
    'othermentalhealthsubstanceuse_fatherfreetextbox', 'othermentalhealthsubstanceuse_siblingfreetextbox',
    'othermentalhealthsubstanceuse_daughterfreetextbox', 'othermentalhealthsubstanceuse_sonfreetextbox',
    'othermentalhealthsubstanceuse_grandparentfreetextb', 'mentalhealth_othermentalhealthsubstanceusecurrently',
    'mentalhealth_howoldwereyouothermentalhealthsubstanceuse', 'mentalhealth_rxmedsforothermentalhealthsubstanceuse',
    'diagnosedhealthcondition_skeletalmuscularcondition', 'skeletalmuscularcondition_carpaltunnel_yes',
    'skeletalmuscular_carpaltunnelcurrently', 'skeletalmuscular_howoldwereyoucarpaltunnel',
    'skeletalmuscular_rxmedsforcarpaltunnel', 'skeletalmuscularcondition_fibromyalgia_yes',
    'skeletalmuscular_fibromyalgiacurrently', 'skeletalmuscular_howoldwereyoufibromyalgia',
    'skeletalmuscular_rxmedsforfibromyalgia', 'skeletalmuscularcondition_fracturedbrokenbones_yes',
    'skeletalmuscular_fracturedbrokenbonecurrently', 'skeletalmuscular_howoldwereyoufracturedbrokenbone',
    'skeletalmuscular_rxmedsforfracturedbrokenbone', 'skeletalmuscularcondition_gout_yes',
    'skeletalmuscular_goutcurrently', 'skeletalmuscular_howoldwereyougout',
    'skeletalmuscular_rxmedsforgout', 'skeletalmuscularcondition_osteoarthritis_yes',
    'skeletalmuscular_osteoarthritiscurrently', 'skeletalmuscular_howoldwereyouosteoarthritis',
    'skeletalmuscular_rxmedsforosteoarthritis', 'skeletalmuscularcondition_osteoporosis_yes',
    'skeletalmuscular_osteoporosiscurrently', 'skeletalmuscular_howoldwereyouosteoporosis',
    'skeletalmuscular_rxmedsforosteoporosis', 'skeletalmuscularcondition_pseudogout_yes',
    'skeletalmuscular_pseudogoutcurrently', 'skeletalmuscular_howoldwereyoupseudogout',
    'skeletalmuscular_rxmedsforpseudogout', 'skeletalmuscularcondition_rheumatoidarthritis_yes',
    'skeletalmuscular_rheumatoidarthritiscurrently', 'skeletalmuscular_howoldwereyourheumatoidarthritis',
    'skeletalmuscular_rxmedsforrheumatoidarthritis', 'skeletalmuscularcondition_spinemusclebone_yes',
    'skeletalmuscular_spinemusclebonecurrently', 'skeletalmuscular_howoldwereyouspinemusclebone',
    'skeletalmuscular_rxmedsforspinemusclebone', 'skeletalmuscularcondition_systemiclupus_yes',
    'skeletalmuscular_lupuscurrently', 'skeletalmuscular_howoldwereyoulupus',
    'skeletalmuscular_rxmedsforlupus', 'skeletalmuscularcondition_otherarthritis_yes',
    'otherarthritis_freetextbox', 'otherarthritis_motherfreetextbox',
    'otherarthritis_fatherfreetextbox', 'otherarthritis_siblingfreetextbox',
    'otherarthritis_daughterfreetextbox', 'otherarthritis_sonfreetextbox',
    'otherarthritis_grandparentfreetextbox', 'skeletalmuscular_otherarthritiscurrently',
    'skeletalmuscular_howoldwereyouotherarthritis', 'skeletalmuscular_rxmedsforotherarthritis',
    'skeletalmuscularcondition_otherbonejointmuscle_yes', 'otherbonejointmuscle_freetextbox',
    'otherbonejointmuscle_motherfreetextbox', 'otherbonejointmuscle_fatherfreetextbox',
    'otherbonejointmuscle_siblingfreetextbox', 'otherbonejointmuscle_daughterfreetextbox',
    'otherbonejointmuscle_sonfreetextbox', 'otherbonejointmuscle_grandparentfreetextbox',
    'skeletalmuscular_otherbonejointmuscleconditioncurrently', 'skeletalmuscular_howoldwereyouotherbonejointmusclecondition',
    'skeletalmuscular_rxmedsforotherbonejointmusclecondition', 'diagnosedhealthcondition_visioncondition',
    'visioncondition_blindness_yes', 'hearingvision_blindnesscurrently',
    'hearingvision_howoldwereyoublindness', 'hearingvision_rxmedsforblindness',
    'visioncondition_cataracts_yes', 'hearingvision_cataractscurrently',
    'hearingvision_howoldwereyoucataracts', 'hearingvision_rxmedsforcataracts',
    'visioncondition_dryeyes_yes', 'hearingvision_dryeyescurrently',
    'hearingvision_howoldwereyoudryeyes', 'hearingvision_rxmedsfordryeyes',
    'visioncondition_farsightedness_yes', 'hearingvision_farsightednesscurrently',
    'hearingvision_howoldwereyoufarsightedness', 'hearingvision_rxmedsforfarsightedness',
    'visioncondition_nearsightedness_yes', 'hearingvision_nearsightednesscurrently',
    'hearingvision_howoldwereyounearsightedness', 'hearingvision_rxmedsfornearsightedness',
    'visioncondition_astigmatism_yes', 'hearingvision_astigmatismcurrently',
    'hearingvision_howoldwereyouastigmatism', 'hearingvision_rxmedsforastigmatism',
    'visioncondition_glaucoma_yes', 'hearingvision_glaucomacurrently',
    'hearingvision_howoldwereyouglaucoma', 'hearingvision_rxmedsforglaucoma',
    'visioncondition_maculardegeneration_yes', 'hearingvision_maculardegenerationcurrently',
    'hearingvision_howoldwereyoumaculardegeneration', 'hearingvision_rxmedsformaculardegeneration',
    'visioncondition_hearingloss_yes', 'hearingvision_hearinglosscurrently',
    'hearingvision_howoldwereyouhearingloss', 'hearingvision_rxmedsforhearingloss',
    'visioncondition_tinnitus_yes', 'hearingvision_tinnituscurrently',
    'hearingvision_howoldwereyoutinnitus', 'hearingvision_rxmedsfortinnitus',
    'visioncondition_otherhearingeyecondition_yes', 'otherhearingeye_freetextbox',
    'otherhearingeye_motherfreetextbox', 'otherhearingeye_fatherfreetextbox',
    'otherhearingeye_siblingfreetextbox', 'otherhearingeye_daughterfreetextbox',
    'otherhearingeye_sonfreetextbox', 'otherhearingeye_grandparentfreetextbox',
    'hearingvision_otherhearingeyeconditioncurrently', 'hearingvision_howoldwereyouotherhearingeyecondition',
    'hearingvision_rxmedsforotherhearingeyecondition', 'diagnosedhealthcondition_othercondition',
    'diagnosedhealthcondition_acne_yes', 'other_acnecurrently',
    'other_howoldwereyouacne', 'other_rxmedsforacne',
    'diagnosedhealthcondition_allergies_yes', 'other_allergiescurrently',
    'other_howoldwereyouallergies', 'other_rxmedsforallergies',
    'otherhealthcondition_endometriosis_yes', 'other_endometriosiscurrently',
    'other_howoldwereyouendometriosis', 'other_rxmedsforendometriosis',
    'otherhealthcondition_enlargedprostate_yes', 'other_enlargedprostatecurrently',
    'other_howoldwereyouenlargedprostate', 'other_rxmedsforenlargedprostate',
    'otherhealthcondition_fibroids_yes', 'other_fibroidscurrently',
    'other_howoldwereyoufibroids', 'other_rxmedsforfibroids',
    'otherhealthcondition_obesity_yes', 'other_obesitycurrently',
    'other_howoldwereyouobesity', 'other_rxmedsforobesity',
    'otherhealthcondition_polycysticovariansyndrome_yes', 'other_pcoscurrently',
    'other_howoldwereyoupcos', 'other_rxmedsforpcos',
    'otherhealthcondition_reactionstoanesthesia_yes', 'other_reactionsanesthesiacurrently',
    'other_howoldwereyoureactionsanesthesia', 'other_rxmedsforreactionsanesthesia',
    'diagnosedhealthcondition_skincondition_yes', 'other_skinconditioncurrently',
    'other_howoldwereyouskincondition', 'other_rxmedsforskincondition',
    'diagnosedhealthcondition_vitaminbdeficiency_yes', 'other_vitaminbdeficiencycurrently',
    'other_howoldwereyouvitaminbdeficiency', 'other_rxmedsforvitaminbdeficiency',
    'diagnosedhealthcondition_vitaminddeficiency_yes', 'other_vitaminddeficiencycurrently',
    'other_howoldwereyouvitaminddeficiency', 'other_rxmedsforvitaminddeficiency',
    'otherhealthcondition_otherhealthcondition_yes', 'otherdiagnosis_freetextbox',
    'otherdiagnosis_motherfreetextbox', 'otherdiagnosis_fatherfreetextbox',
    'otherdiagnosis_siblingfreetextbox', 'otherdiagnosis_daughterfreetextbox',
    'otherdiagnosis_sonfreetextbox', 'otherdiagnosis_grandparentfreetextbox',
    'other_otherdiagnosiscurrently', 'other_howoldwereyouotherdiagnosis',
    'other_rxmedsforotherdiagnosis', 'infectiousdiseases_infectiousdiseasecondition',
    'infectiousdiseases_chickenpoxcurrently', 'infectiousdiseases_howoldwereyouchickenpox',
    'infectiousdiseases_rxmedsforchickenpox', 'infectiousdiseases_chronicsinuscurrently',
    'infectiousdiseases_howoldwereyouchronicsinus', 'infectiousdiseases_rxmedsforchronicsinus',
    'infectiousdiseases_denguefevercurrently', 'infectiousdiseases_howoldwereyoudenguefever',
    'infectiousdiseases_rxmedsfordenguefever', 'infectiousdiseases_hepatitisacurrently',
    'infectiousdiseases_howoldwereyouhepatitisa', 'infectiousdiseases_rxmedsforhepatitisa',
    'infectiousdiseases_hepatitisbcurrently', 'infectiousdiseases_howoldwereyouhepatitisb',
    'infectiousdiseases_rxmedsforhepatitisb', 'infectiousdiseases_hepatitisccurrently',
    'infectiousdiseases_howoldwereyouhepatitisc', 'infectiousdiseases_rxmedsforhepatitisc',
    'infectiousdiseases_hivaidscurrently', 'infectiousdiseases_howoldwereyouhivaids',
    'infectiousdiseases_rxmedsforhivaids', 'infectiousdiseases_lymediseasecurrently',
    'infectiousdiseases_howoldwereyoulymedisease', 'infectiousdiseases_rxmedsforlymedisease',
    'infectiousdiseases_urinarytractcurrently', 'infectiousdiseases_howoldwereyouurinarytract',
    'infectiousdiseases_rxmedsforurinarytract', 'infectiousdiseases_yeastinfectioncurrently',
    'infectiousdiseases_howoldwereyouyeastinfection', 'infectiousdiseases_rxmedsforyeastinfection',
    'infectiousdiseases_sarscurrently', 'infectiousdiseases_howoldwereyousars',
    'infectiousdiseases_rxmedsforsars', 'infectiousdiseases_stiscurrently',
    'infectiousdiseases_howoldwereyoustis', 'infectiousdiseases_rxmedsforstis',
    'infectiousdiseases_shinglescurrently', 'infectiousdiseases_howoldwereyoushingles',
    'infectiousdiseases_rxmedsforshingles', 'infectiousdiseases_tuberculosiscurrently',
    'infectiousdiseases_howoldwereyoutuberculosis', 'infectiousdiseases_rxmedsfortuberculosis',
    'infectiousdiseases_westnileviruscurrently', 'infectiousdiseases_howoldwereyouwestnilevirus',
    'infectiousdiseases_rxmedsforwestnilevirus', 'infectiousdiseases_zikaviruscurrently',
    'infectiousdiseases_howoldwereyouzikavirus', 'infectiousdiseases_rxmedsforzikavirus',
    'otherinfectiousdisease_freetextbox', 'infectiousdiseases_otherinfectiousdiseasecurrently',
    'infectiousdiseases_howoldwereyouotherinfectiousdisease', 'infectiousdiseases_rxmedsforotherinfectiousdisease',
    'outro_text', 'pfhh_codetracking',
    'pfhh_tracked_changes' )
ORDER BY observation_source_value, observation_date
""")

DROP_QUERY_TMPL = Template("""
DELETE FROM `{{project}}.{{dataset}}.observation` AS o
WHERE observation_id IN (
    -- delete if we have backed up the data --
    SELECT observation_id
    FROM `{{project}}.{{sandbox}}.{{drop_table}}`)
""")


class CombinedPersonalFamilyHealthSurveySuppression(BaseCleaningRule):
    """
    Suppress rows by values in the observation_source_concept_id field and observation_date.
    """

    def __init__(self,
                 project_id,
                 dataset_id,
                 sandbox_dataset_id,
                 table_namer=None):
        """
        Initialize the class with proper info.

        Set the issue numbers, description and affected datasets.  As other
        tickets may affect this SQL, append them to the list of Jira Issues.
        """
        desc = (
            f'Remove records from the rdr dataset where '
            f'observation_source_concept_id and observation_date indicate the '
            f'record is likely from the combined Personal Family Medical History Survey.'
        )
        super().__init__(issue_numbers=ISSUE_NUMBERS,
                         description=desc,
                         affected_datasets=[cdr_consts.RDR],
                         project_id=project_id,
                         dataset_id=dataset_id,
                         sandbox_dataset_id=sandbox_dataset_id,
                         affected_tables=[OBSERVATION],
                         table_namer=table_namer)

    def get_query_specs(self):
        """
        Return a list of dictionary query specifications.

        :return:  A list of dictionaries.  Each dictionary contains a
            single query and a specification for how to execute that query.
            The specifications are optional but the query is required.
        """
        save_dropped_rows = {
            cdr_consts.QUERY:
                DROP_SELECTION_QUERY_TMPL.render(
                    project=self.project_id,
                    dataset=self.dataset_id,
                    sandbox=self.sandbox_dataset_id,
                    drop_table=self.sandbox_table_for(OBSERVATION)),
        }

        drop_rows_query = {
            cdr_consts.QUERY:
                DROP_QUERY_TMPL.render(
                    project=self.project_id,
                    dataset=self.dataset_id,
                    sandbox=self.sandbox_dataset_id,
                    drop_table=self.sandbox_table_for(OBSERVATION)),
        }

        return [save_dropped_rows, drop_rows_query]

    def setup_rule(self, client):
        """
        Function to run any data upload options before executing a query.
        """
        pass

    def get_table_counts(self, client, dataset, table_name):
        """
        Run required steps for validation setup

        Will validate this rule only removes the expected amount of data.  Will
        do this by getting the initial count of the observation table before the 
        rule is run.
        """
        sql = Template("""SELECT row_count 
        FROM `{{project}}.{{dataset}}.__TABLES__` 
        WHERE table_id = '{{table}}'""")

        prefilled_sql = sql.render(project=self.project_id,
                                   dataset=dataset,
                                   table=table_name)

        # run the query and save the results
        job = self.client.query(prefilled_sql)
        rows = job.result()

        for row in rows:
            return row['row_count']

    def setup_validation(self, client):
        """
        Run required steps for validation setup

        Will validate this rule only removes the expected amount of data.  Will
        do this by getting the initial count of the observation table before the 
        rule is run.
        """
        self.initial_obs_count = self.get_table_counts(client, self.dataset_id,
                                                       OBSERVATION)
        LOGGER.debug(
            f"initial observation row count is:  {self.initial_obs_count}")
        self.client = client

    def validate_rule(self):
        """
        Validates the cleaning rule which deletes or updates the data from the tables

        This abstract method was added to the base class after this rule was authored.
        This rule needs to implement logic to run validation on cleaning rules that will
        be updating or deleting the values.
        Until done no issue exists for this yet.
        """
        clean_obs_count = self.get_table_counts(self.client, self.dataset_id,
                                                OBSERVATION)
        sandbox_obs_count = self.get_table_counts(
            self.client, self.sandbox_dataset_id,
            self.sandbox_table_for(OBSERVATION))

        msg = (
            f'The sum of the cleaned observation table count, {clean_obs_count}, '
            f'and the sandboxed tablecount, {sandbox_obs_count}, is not '
            f'equal to the initial observation table count, {self.initial_obs_count}.'
        )
        assert self.initial_obs_count == (clean_obs_count +
                                          sandbox_obs_count), msg

    def get_sandbox_tablenames(self):
        sandbox_tables = []
        for table in self.affected_tables:
            sandbox_tables.append(self.sandbox_table_for(table))

        return sandbox_tables


if __name__ == '__main__':
    import cdr_cleaner.args_parser as parser
    import cdr_cleaner.clean_cdr_engine as clean_engine

    ARGS = parser.parse_args()

    if ARGS.list_queries:
        clean_engine.add_console_logging()
        query_list = clean_engine.get_query_list(
            ARGS.project_id, ARGS.dataset_id, ARGS.sandbox_dataset_id,
            [(CombinedPersonalFamilyHealthSurveySuppression,)])
        for query in query_list:
            LOGGER.info(query)
    else:
        clean_engine.add_console_logging(ARGS.console_log)
        clean_engine.clean_dataset(
            ARGS.project_id, ARGS.dataset_id, ARGS.sandbox_dataset_id,
            [(CombinedPersonalFamilyHealthSurveySuppression,)])
