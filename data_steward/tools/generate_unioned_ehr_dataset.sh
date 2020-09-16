#!/usr/bin/env bash
set -ex

# Generate unioned_ehr dataset
# Loads vocabulary from specified dataset and unioned EHR tables from specified dataset

deactivation='false'
pids_project_id='test_project'
pids_dataset_id='test_dataset'
pids_table='test_table'

USAGE="
Usage: generate_unioned_ehr_dataset.sh
  --key_file <path to key file>
  --vocab_dataset <vocab dataset>
  --ehr_snapshot <EHR dataset>
  --dataset_release_tag <release tag for the CDR>
  [--deactivation <choose b/w true|false to run delete_deactivated participants: default is ${deactivation}>]
  [--pids_project_id <Identifies the project where the pids table is stored: default is ${pids_project_id}>]
  [--pids_dataset_id <Identifies the dataset where the pids table is stored: default is ${pids_dataset_id}>]
  [--pids_table <Identifies the table where the pids are stored: default is ${pids_table} >]
"

echo
while true; do
  case "$1" in
  --key_file)
    key_file=$2
    shift 2
    ;;
  --vocab_dataset)
    vocab_dataset=$2
    shift 2
    ;;
  --ehr_snapshot)
    ehr_snapshot=$2
    shift 2
    ;;
  --dataset_release_tag)
    dataset_release_tag=$2
    shift 2
    ;;
  --deactivation)
    deactivation=$2
    shift 2
    ;;
  --pids_project_id)
    pids_project_id=$2
    shift 2
    ;;
  --pids_dataset_id)
    pids_dataset_id=$2
    shift 2
    ;;
  --pids_table)
    pids_table=$2
    shift 2
    ;;
  --)
    shift
    break
    ;;
  *) break ;;
  esac
done

if [[ -z "${key_file}" ]] || [[ -z "${vocab_dataset}" ]] || [[ -z "${ehr_snapshot}" ]] || [[ -z "${dataset_release_tag}" ]] \
 || [[ -z "${deactivation}" ]] || [[ -z "${pids_project_id}" ]] || [[ -z "${pids_dataset_id}" ]] || [[ -z "${pids_table}" ]]; then
  echo "${USAGE}"
  exit 1
fi

app_id=$(python -c 'import json,sys;obj=json.load(sys.stdin);print(obj["project_id"]);' < "${key_file}")

ROOT_DIR=$(git rev-parse --show-toplevel)
DATA_STEWARD_DIR="${ROOT_DIR}/data_steward"
TOOLS_DIR="${DATA_STEWARD_DIR}/tools"
CLEANER_DIR="${DATA_STEWARD_DIR}/cdr_cleaner"

echo "ehr_snapshot --> ${ehr_snapshot}"
echo "app_id --> ${app_id}"
echo "key_file --> ${key_file}"
echo "vocab_dataset --> ${vocab_dataset}"
echo "dataset_release_tag --> ${dataset_release_tag}"
echo "deactivation --> ${deactivation}"
echo "pids_project_id --> ${pids_project_id}"
echo "pids_dataset_id --> ${pids_dataset_id}"
echo "pids_table --> ${pids_table}"

export GOOGLE_APPLICATION_CREDENTIALS="${key_file}"
export GOOGLE_CLOUD_PROJECT="${app_id}"

#set application environment (ie dev, test, prod)
gcloud auth activate-service-account --key-file=${key_file}
gcloud config set project ${app_id}

source "${TOOLS_DIR}/set_path.sh"

echo "-------------------------->Take a Snapshot of Unioned EHR Submissions (step 5)"
source_prefix="unioned_ehr_"
unioned_ehr_dataset="${dataset_release_tag}_unioned_ehr"
unioned_ehr_dataset_backup="${unioned_ehr_dataset}_backup"
unioned_ehr_dataset_staging="${unioned_ehr_dataset}_staging"

#---------------------------------------------------------------------
# Step 1 Create an empty dataset
bq mk --dataset --description "copy ehr_union from ${ehr_snapshot}" --label "phase:backup" --label "release_tag:${dataset_release_tag}" --label "de_identified:false"${app_id}:${unioned_ehr_dataset_backup}

#----------------------------------------------------------------------
# Step 2 Create the clinical tables for unioned EHR data set
python "${DATA_STEWARD_DIR}/cdm.py" ${unioned_ehr_dataset_backup}

#----------------------------------------------------------------------
# Step 3 Copy OMOP vocabulary to unioned EHR data set
python "${DATA_STEWARD_DIR}/cdm.py" --component vocabulary ${unioned_ehr_dataset_backup}

"${TOOLS_DIR}/table_copy.sh" --source_app_id ${app_id} --target_app_id ${app_id} --source_dataset ${vocab_dataset} --target_dataset ${unioned_ehr_dataset_backup} --sync false

#----------------------------------------------------------------------
# Step 4 copy unioned ehr clinical tables tables
"${TOOLS_DIR}/table_copy.sh" --source_app_id ${app_id} --target_app_id ${app_id} --source_dataset ${ehr_snapshot} --source_prefix ${source_prefix} --target_dataset ${unioned_ehr_dataset_backup} --sync false

#----------------------------------------------------------------------
# Step 5 copy mapping tables tables
"${TOOLS_DIR}/table_copy.sh" --source_app_id ${app_id} --target_app_id ${app_id} --source_dataset ${ehr_snapshot} --source_prefix _mapping_ --target_dataset ${unioned_ehr_dataset_backup} --target_prefix _mapping_ --sync false

echo "removing tables copies unintentionally"
bq rm -f ${unioned_ehr_dataset}._mapping_ipmc_nu_condition_occurrence
bq rm -f ${unioned_ehr_dataset}._mapping_ipmc_nu_device_exposure
bq rm -f ${unioned_ehr_dataset}._mapping_ipmc_nu_drug_exposure
bq rm -f ${unioned_ehr_dataset}._mapping_ipmc_nu_fact_relationship
bq rm -f ${unioned_ehr_dataset}._mapping_ipmc_nu_measurement
bq rm -f ${unioned_ehr_dataset}._mapping_ipmc_nu_note
bq rm -f ${unioned_ehr_dataset}._mapping_ipmc_nu_observation
bq rm -f ${unioned_ehr_dataset}._mapping_ipmc_nu_procedure_occurrence
bq rm -f ${unioned_ehr_dataset}._mapping_ipmc_nu_specimen
bq rm -f ${unioned_ehr_dataset}._mapping_ipmc_nu_visit_occurrence

# Run cleaning rules on unioned_ehr_dataset
# create an intermediary table to apply cleaning rules on
bq mk --dataset --description "intermediary dataset to apply cleaning rules on ${unioned_ehr_dataset_backup}" --label "phase:staging" --label "release_tag:${dataset_release_tag}" --label "de_identified:false" ${app_id}:${unioned_ehr_dataset_staging}

"${TOOLS_DIR}/table_copy.sh" --source_app_id ${app_id} --target_app_id ${app_id} --source_dataset ${unioned_ehr_dataset_backup} --target_dataset ${unioned_ehr_dataset_staging}

export UNIONED_DATASET_ID="${unioned_ehr_dataset_staging}"
export BIGQUERY_DATASET_ID="${unioned_ehr_dataset_staging}"
data_stage='unioned'

# Remove de-activated participants
if [ ${deactivation} == 'true' ];
then
  python "${CLEANER_DIR}/cleaning_rules/remove_ehr_data_past_deactivation_date.py" --project-id ${app_id} --pids-project-id ${pids_project_id} --pids-dataset-id ${pids_dataset_id} --pids-table ${pids_table}
fi

# run cleaning_rules on a dataset
python "${CLEANER_DIR}/clean_cdr.py" --data_stage ${data_stage} -s 2>&1 | tee unioned_cleaning_log_"${unioned_ehr_dataset_staging}".txt

# Create a snapshot dataset with the result
python "${TOOLS_DIR}/snapshot_by_query.py" -p "${app_id}" -d "${unioned_ehr_dataset_staging}" -n "${unioned_ehr_dataset}"

bq update --description "${version} clean version of ${unioned_ehr_dataset_backup}" ${app_id}:${unioned_ehr_dataset}

#copy sandbox dataset
"${TOOLS_DIR}/table_copy.sh" --source_app_id ${app_id} --target_app_id ${app_id} --source_dataset "${unioned_ehr_dataset_staging}_sandbox" --target_dataset "${unioned_ehr_dataset}_sandbox"

bq rm -r -d "${unioned_ehr_dataset_staging}_sandbox"
bq rm -r -d "${unioned_ehr_dataset_staging}"

unset PYTHONPATH

set +ex
