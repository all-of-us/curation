from datasteward_df import common

date_fields = {
    common.OBSERVATION_PERIOD: 'observation_period_start_date',
    common.VISIT_OCCURRENCE: 'visit_start_date',
    common.CONDITION_OCCURRENCE: 'condition_start_date',
    common.PROCEDURE_OCCURRENCE: 'procedure_date',
    common.DRUG_EXPOSURE: 'drug_exposure_start_date',
    common.OBSERVATION: 'observation_date',
    common.DRUG_ERA: 'drug_era_start_date',
    common.CONDITION_ERA: 'condition_era_start_date',
    common.MEASUREMENT: 'measurement_date',
    common.DEVICE_EXPOSURE: 'device_exposure_start_date'
}

person = common.PERSON
