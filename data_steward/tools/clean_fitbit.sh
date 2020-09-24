#!/usr/bin/env bash
set -ex
# This Script automates the process of de-identification of the fitbit dataset
# This script expects you to use the venv in curation directory

USAGE="
Usage: clean_fitbit.sh
  --key_file <path to key file>
  --fitbit_dataset <fitbit_dataset_id>
  --combined_dataset <combined_dataset_id>
  --mapping_dataset <mapping_dataset_id>
  --mapping_table <mapping_table_id>
  --dataset_release_tag <release tag for the CDR>
"

while true; do
  case "$1" in
  --combined_dataset)
    combined_dataset=$2
    shift 2
    ;;
  --key_file)
    key_file=$2
    shift 2
    ;;
  --fitbit_dataset)
    fitbit_dataset=$2
    shift 2
    ;;
  --dataset_release_tag)
    dataset_release_tag=$2
    shift 2
    ;;
  --mapping_dataset)
    mapping_dataset=$2
    shift 2
    ;;
  --mapping_table)
    mapping_table=$2
    shift 2
    ;;
  --)
    shift
    break
    ;;
  *) break ;;
  esac
done

if [[ -z "${key_file}" ]] || [[ -z "${combined_dataset}" ]] || [[ -z "${fitbit_dataset}" ]] || [[ -z "${dataset_release_tag}" ]] || [[ -z "${mapping_dataset}" ]] || [[ -z "${mapping_table}" ]]; then
  echo "${USAGE}"
  exit 1
fi

echo "key_file --> ${key_file}"
echo "combined_dataset --> ${combined_dataset}"
echo "fitbit_dataset --> ${fitbit_dataset}"
echo "mapping_dataset --> ${mapping_dataset}"
echo "mapping_table --> ${mapping_table}"

APP_ID=$(python -c 'import json,sys;obj=json.load(sys.stdin);print(obj["project_id"]);' < "${key_file}")
export GOOGLE_APPLICATION_CREDENTIALS="${key_file}"
export GOOGLE_CLOUD_PROJECT="${APP_ID}"

#set application environment (ie dev, test, prod)
gcloud auth activate-service-account --key-file="${key_file}"
gcloud config set project "${APP_ID}"

fitbit_deid_dataset="R${fitbit_dataset}_deid"
ROOT_DIR=$(git rev-parse --show-toplevel)
DATA_STEWARD_DIR="${ROOT_DIR}/data_steward"
TOOLS_DIR="${DATA_STEWARD_DIR}/tools"
CLEANER_DIR="${DATA_STEWARD_DIR}/cdr_cleaner"
CLEAN_DEID_DIR="${CLEANER_DIR}/cleaning_rules/deid"

export BIGQUERY_DATASET_ID="${fitbit_dataset}"
export PYTHONPATH="${PYTHONPATH}:${CLEAN_DEID_DIR}:${DATA_STEWARD_DIR}"

# create empty fitbit de-id dataset
bq mk --dataset --description "${dataset_release_tag} de-identified version of ${fitbit_dataset}" --label "phase:staging" --label "release_tag:${dataset_release_tag}" --label "de_identified:false"  "${APP_ID}":"${fitbit_deid_dataset}"
"${TOOLS_DIR}"/table_copy.sh --source_app_id "${APP_ID}" --target_app_id "${APP_ID}" --source_dataset "${fitbit_dataset}" --target_dataset "${fitbit_deid_dataset}"
# Use the below command if copy fails
#transfer_params='{"source_dataset_id":"'${fitbit_dataset}'","source_project_id":"'${APP_ID}'"'
#bq mk --transfer_config --project_id="${APP_ID}" --data_source="cross_region_copy" --target_dataset="${fitbit_deid_dataset}" --display_name='Create Fitbit Deid' --params="${transfer_params}"

# create empty fitbit sandbox dataset
sandbox_dataset="${fitbit_deid_dataset}_sandbox"
bq mk --dataset --description "Sandbox created for storing records affected by the cleaning rules applied to ${fitbit_deid_dataset}" --label "phase:sandbox" --label "release_tag:${dataset_release_tag}" --label "de_identified:false" "${APP_ID}":"${sandbox_dataset}"

# Create logs dir
LOGS_DIR="${DATA_STEWARD_DIR}/logs"
mkdir -p "${LOGS_DIR}"

# Apply cleaning rules
python "${CLEAN_DEID_DIR}/remove_fitbit_data_if_max_age_exceeded.py" --project_id "${APP_ID}" --dataset_id "${fitbit_deid_dataset}" --sandbox_dataset_id "${sandbox_dataset}" --combined_dataset_id "${combined_dataset}" -s 2>&1 | tee -a "${LOGS_DIR}"/fitbit_log.txt
python "${CLEAN_DEID_DIR}/pid_rid_map.py" --project_id "${APP_ID}" --dataset_id "${fitbit_deid_dataset}" --sandbox_dataset_id "${sandbox_dataset}" --mapping_dataset_id "${mapping_dataset}" --mapping_table_id "${mapping_table}" -s 2>&1 | tee -a "${LOGS_DIR}"/fitbit_log.txt
python "${CLEAN_DEID_DIR}/fitbit_dateshift.py" --project_id "${APP_ID}" --dataset_id "${fitbit_deid_dataset}" --sandbox_dataset_id "${sandbox_dataset}" --mapping_dataset_id "${mapping_dataset}" --mapping_table_id "${mapping_table}" -s 2>&1 | tee -a "${LOGS_DIR}"/fitbit_log.txt

bq update --set_label "phase:clean" --set_label "de_identified:true" "${APP_ID}":"${fitbit_deid_dataset}"

unset PYTHONPATH

set +ex
