key_file_path=$1
action=$2
input_dataset=$3
project=$4


source deid_env/bin/activate

# run the deid module
python aou.py --rules config/config.json --private_key ${key_file_path} --table config/tables/condition_occurrence.json --action ${action} --log deid_log.log --idataset ${input_dataset}
python aou.py --rules config/config.json --private_key ${key_file_path} --table config/tables/death.json --action ${action} --log deid_log.log --idataset ${input_dataset}
python aou.py --rules config/config.json --private_key ${key_file_path} --table config/tables/device_exposure.json --action ${action} --log deid_log.log --idataset ${input_dataset}
python aou.py --rules config/config.json --private_key ${key_file_path} --table config/tables/drug_exposure.json --action ${action} --log deid_log.log --idataset ${input_dataset}
python aou.py --rules config/config.json --private_key ${key_file_path} --table config/tables/measurement.json --action ${action} --log deid_log.log --idataset ${input_dataset}
python aou.py --rules config/config.json --private_key ${key_file_path} --table config/tables/observation.json --action ${action} --log deid_log.log --idataset ${input_dataset}
python aou.py --rules config/config.json --private_key ${key_file_path} --table config/tables/observation_period.json --action ${action} --log deid_log.log --idataset ${input_dataset}
python aou.py --rules config/config.json --private_key ${key_file_path} --table config/tables/person.json --action ${action} --log deid_log.log --idataset ${input_dataset}
python aou.py --rules config/config.json --private_key ${key_file_path} --table config/tables/procedure_occurrence.json --action ${action} --log deid_log.log --idataset ${input_dataset}
python aou.py --rules config/config.json --private_key ${key_file_path} --table config/tables/specimen.json --action ${action} --log deid_log.log --idataset ${input_dataset}
python aou.py --rules config/config.json --private_key ${key_file_path} --table config/tables/visit_occurrence.json --action ${action} --log deid_log.log --idataset ${input_dataset}

# copy dataset tables that do not need de-identified.
bq cp -f ${project}:${input_dataset}._mapping_condition_occurrence ${project}:${input_dataset}_deid._mapping_condition_occurrence
bq cp -f ${project}:${input_dataset}._mapping_device_exposure ${project}:${input_dataset}_deid._mapping_device_exposure
bq cp -f ${project}:${input_dataset}._mapping_drug_exposure ${project}:${input_dataset}_deid._mapping_drug_exposure
bq cp -f ${project}:${input_dataset}._mapping_measurement ${project}:${input_dataset}_deid._mapping_measurement
bq cp -f ${project}:${input_dataset}._mapping_observation ${project}:${input_dataset}_deid._mapping_observation
bq cp -f ${project}:${input_dataset}._mapping_procedure_occurrence ${project}:${input_dataset}_deid._mapping_procedure_occurrence
bq cp -f ${project}:${input_dataset}._mapping_visit_occurrence ${project}:${input_dataset}_deid._mapping_visit_occurrence
bq cp -f ${project}:${input_dataset}.achilles_analysis ${project}:${input_dataset}_deid.achilles_analysis
bq cp -f ${project}:${input_dataset}.achilles_heel_results ${project}:${input_dataset}_deid.achilles_heel_results
bq cp -f ${project}:${input_dataset}.achilles_results ${project}:${input_dataset}_deid.achilles_results
bq cp -f ${project}:${input_dataset}.achilles_results_derived ${project}:${input_dataset}_deid.achilles_results_derived
bq cp -f ${project}:${input_dataset}.achilles_results_dist ${project}:${input_dataset}_deid.achilles_results_dist
bq cp -f ${project}:${input_dataset}.attribute_definition ${project}:${input_dataset}_deid.attribute_definition
bq cp -f ${project}:${input_dataset}.care_site ${project}:${input_dataset}_deid.care_site
bq cp -f ${project}:${input_dataset}.cdm_source ${project}:${input_dataset}_deid.cdm_source
bq cp -f ${project}:${input_dataset}.cohort ${project}:${input_dataset}_deid.cohort
bq cp -f ${project}:${input_dataset}.cohort_attribute ${project}:${input_dataset}_deid.cohort_attribute
bq cp -f ${project}:${input_dataset}.cohort_definition ${project}:${input_dataset}_deid.cohort_definition
bq cp -f ${project}:${input_dataset}.concept ${project}:${input_dataset}_deid.concept
bq cp -f ${project}:${input_dataset}.concept_ancestor ${project}:${input_dataset}_deid.concept_ancestor
bq cp -f ${project}:${input_dataset}.concept_class ${project}:${input_dataset}_deid.concept_class
bq cp -f ${project}:${input_dataset}.concept_relationship ${project}:${input_dataset}_deid.concept_relationship
bq cp -f ${project}:${input_dataset}.concept_synonym ${project}:${input_dataset}_deid.concept_synonym
bq cp -f ${project}:${input_dataset}.condition_era ${project}:${input_dataset}_deid.condition_era
bq cp -f ${project}:${input_dataset}.cost ${project}:${input_dataset}_deid.cost
bq cp -f ${project}:${input_dataset}.device_cost ${project}:${input_dataset}_deid.device_cost
bq cp -f ${project}:${input_dataset}.domain ${project}:${input_dataset}_deid.domain
bq cp -f ${project}:${input_dataset}.dose_era ${project}:${input_dataset}_deid.dose_era
bq cp -f ${project}:${input_dataset}.drug_cost ${project}:${input_dataset}_deid.drug_cost
bq cp -f ${project}:${input_dataset}.drug_era ${project}:${input_dataset}_deid.drug_era
bq cp -f ${project}:${input_dataset}.drug_strength ${project}:${input_dataset}_deid.drug_strength
bq cp -f ${project}:${input_dataset}.fact_relationship ${project}:${input_dataset}_deid.fact_relationship
bq cp -f ${project}:${input_dataset}.location ${project}:${input_dataset}_deid.location
bq cp -f ${project}:${input_dataset}.payer_plan_period ${project}:${input_dataset}_deid.payer_plan_period
bq cp -f ${project}:${input_dataset}.procedure_cost ${project}:${input_dataset}_deid.procedure_cost
bq cp -f ${project}:${input_dataset}.provider ${project}:${input_dataset}_deid.provider
bq cp -f ${project}:${input_dataset}.relationship ${project}:${input_dataset}_deid.relationship
bq cp -f ${project}:${input_dataset}.source_to_concept_map ${project}:${input_dataset}_deid.source_to_concept_map
bq cp -f ${project}:${input_dataset}.visit_cost ${project}:${input_dataset}_deid.visit_cost
bq cp -f ${project}:${input_dataset}.vocabulary ${project}:${input_dataset}_deid.vocabulary
