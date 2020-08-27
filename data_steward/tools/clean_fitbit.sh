#!/usr/bin/env bash
set -ex
# This Script automates the process of de-identification of the fitbit dataset
# This script expects you to use the venv in curation directory

USAGE="
Usage: clean_fitbit.sh
  --key_file <path to key file>
  --fitbit_dataset <fitbit_dataset_id>
  --combined_dataset <combined_dataset_id>
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
  --)
    shift
    break
    ;;
  *) break ;;
  esac
done

if [[ -z "${key_file}" ]] || [[ -z "${combined_dataset}" ]] || [[ -z "${fitbit_dataset}" ]] || [[ -z "${dataset_release_tag}" ]]; then
  echo "${USAGE}"
  exit 1
fi

echo "key_file --> ${key_file}"
echo "combined_dataset --> ${combined_dataset}"
echo "fitbit_dataset --> ${fitbit_dataset}"

APP_ID=$(python -c 'import json,sys;obj=json.load(sys.stdin);print(obj["project_id"]);' < "${key_file}")
export GOOGLE_APPLICATION_CREDENTIALS="${key_file}"
export GOOGLE_CLOUD_PROJECT="${APP_ID}"

#set application environment (ie dev, test, prod)
gcloud auth activate-service-account --key-file="${key_file}"
gcloud config set project "${APP_ID}"

fitbit_deid_dataset="${fitbit_dataset}_deid"
registered_fitbit_deid="R${fitbit_deid_dataset}"
ROOT_DIR=$(git rev-parse --show-toplevel)
DATA_STEWARD_DIR="${ROOT_DIR}/data_steward"
TOOLS_DIR="${DATA_STEWARD_DIR}/tools"
CLEANER_DIR="${DATA_STEWARD_DIR}/cdr_cleaner"
CLEAN_DEID_DIR="${CLEANER_DIR}/cleaning_rules/deid"

export BIGQUERY_DATASET_ID="${fitbit_dataset}"
export PYTHONPATH="${PYTHONPATH}:${CLEAN_DEID_DIR}:${DATA_STEWARD_DIR}"

# create empty fitbit de-id dataset
bq mk --dataset --description "${dataset_release_tag} de-identified version of ${fitbit_dataset}" "${APP_ID}":"${registered_fitbit_deid}"
"${TOOLS_DIR}"/table_copy.sh --source_app_id "${APP_ID}" --target_app_id "${APP_ID}" --source_dataset "${fitbit_dataset}" --target_dataset "${registered_fitbit_deid}"
# Use the below command if copy fails
#transfer_params='{"source_dataset_id":"'${registered_fitbit_deid}'","source_project_id":"'${APP_ID}'"'
#bq mk --transfer_config --project_id="${APP_ID}" --data_source="cross_region_copy" --target_dataset="${registered_fitbit_deid}" --display_name='Create Fitbit Deid' --params="${transfer_params}"

# create empty fitbit sandbox dataset
sandbox_dataset="${registered_fitbit_deid}_sandbox"
bq mk --dataset --description "${dataset_release_tag} sandbox dataset for ${registered_fitbit_deid}" "${APP_ID}":"${sandbox_dataset}"

# Create logs dir
mkdir -p ../logs

# Apply cleaning rules
python "${CLEAN_DEID_DIR}/remove_fitbit_data_if_max_age_exceeded.py" --project_id "${APP_ID}" --dataset_id "${registered_fitbit_deid}" --sandbox_dataset_id "${sandbox_dataset}" --combined_dataset_id "${combined_dataset}" -s 2>&1 | tee ../logs/fitbit_max_age_log.txt
python "${CLEAN_DEID_DIR}/pid_rid_map.py" --project_id "${APP_ID}" --dataset_id "${registered_fitbit_deid}" --sandbox_dataset_id "${sandbox_dataset}" --combined_dataset_id "${combined_dataset}" -s 2>&1 | tee ../logs/fitbit_pid_rid_log.txt

unset PYTHONPATH

set +ex
