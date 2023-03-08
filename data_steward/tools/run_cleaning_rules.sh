#!/usr/bin/env bash
set -ex

USAGE="
Usage: run_cleaning_rules.sh
  --key_file <path to key file>
  --dataset <dataset name to apply cleaning rules>
  --result_dataset <Dataset name to copy result dataset>
  --data_stage <Dataset stage>
"

while true; do
  case "$1" in
  --dataset)
    dataset=$2
    shift 2
    ;;
  --key_file)
    key_file=$2
    shift 2
    ;;
  --result_dataset)
    result_dataset=$2
    shift 2
    ;;
  --data_stage)
    data_stage=$2
    shift 2
    ;;
  --)
    shift
    break
    ;;
  *) break ;;
  esac
done

if [[ -z "${key_file}" ]] || [[ -z "${dataset}" ]] || [[ -z "${result_dataset}" ]] || [[ -z "${data_stage}" ]]; then
  echo "$USAGE"
  exit 1
fi

ROOT_DIR=$(git rev-parse --show-toplevel)
DATA_STEWARD_DIR="${ROOT_DIR}/data_steward"
TOOLS_DIR="${DATA_STEWARD_DIR}/tools"
CLEANER_DIR="${DATA_STEWARD_DIR}/cdr_cleaner"

app_id=$(python -c 'import json,sys;obj=json.load(sys.stdin);print(obj["project_id"]);' < "${key_file}")

today=$(date '+%Y%m%d')

echo "dataset --> ${dataset}"
echo "app_id --> ${app_id}"
echo "key_file --> ${key_file}"
echo "result_dataset --> ${result_dataset}"
echo "Data Stage --> ${data_stage}"

export GOOGLE_APPLICATION_CREDENTIALS="${key_file}"
export GOOGLE_CLOUD_PROJECT="${app_id}"

#set application environment (ie dev, test, prod)
gcloud auth activate-service-account --key-file=${key_file}
gcloud config set project ${app_id}

source "${TOOLS_DIR}/set_path.sh"

#--------------------------------------------------------
export DATASET="${dataset}"
export DATASET_STAGING="${dataset}_staging"
export DATASET_STAGING_SANDBOX="${dataset}_staging_sandbox"
export DATASET_SANDBOX="${dataset}_sandbox"
export BIGQUERY_DATASET_ID="${DATASET_STAGING}"

# snapshotting dataset to apply cleaning rules
bq mk --dataset --description "Snapshot of ${DATASET}" --label "owner:curation" --label "phase:staging"  ${app_id}:${DATASET_STAGING}

"${TOOLS_DIR}/table_copy.sh" --source_app_id ${app_id} --target_app_id ${app_id} --source_dataset ${DATASET} --target_dataset ${DATASET_STAGING}

# Setting the environment variable for cleaning reules to use, using data_stage
if [ "${data_stage}" == 'unioned' ];
then
    export UNIONED_DATASET_ID="${DATASET_STAGING}"
elif [ "${data_stage}" == 'rdr' ];
then
    export RDR_DATASET_ID="${DATASET_STAGING}"
elif [ "${data_stage}" == 'combined' ];
then
    export COMBINED_DATASET_ID="${DATASET_STAGING}"
elif [ "${data_stage}" == 'deid_base' ];
then
    export COMBINED_DEID_DATASET_ID="${DATASET_STAGING}"
elif [ "${data_stage}" == 'deid_clean' ];
then
    export COMBINED_DEID_CLEAN_DATASET_ID="${DATASET_STAGING}"
elif [ "${data_stage}" == 'fitbit' ]; 
then
    export FITBIT_DATASET_ID="${DATASET_STAGING}"
fi

bq mk --dataset --description "Sandbox created for storing records affected by the cleaning rules applied to ${DATASET_STAGING}" --label "owner:curation" --label "phase:sandbox" --label "de_identified:false" "${app_id}":"${DATASET_STAGING_SANDBOX}"

# run cleaning_rules on a dataset
python "${CLEANER_DIR}/clean_cdr.py" --project_id "${app_id}" --dataset_id "${DATASET}" --sandbox_dataset_id "${DATASET_STAGING_SANDBOX}" --data_stage "${data_stage}" -s 2>&1 | tee cleaning_rules_log.txt

# Create a snapshot dataset with the result
python "${TOOLS_DIR}/snapshot_by_query.py" -p "${app_id}" -d "${DATASET_STAGING}" -n "${result_dataset}"

# Snapshot the staging sandbox dataset to store it with 
"${TOOLS_DIR}/table_copy.sh" --source_app_id ${app_id} --target_app_id ${app_id} --source_dataset "${DATASET_STAGING_SANDBOX}" --target_dataset "${DATASET_SANDBOX}"

# Update sandbox description
bq update --description "Sandbox created for storing records affected by the cleaning rules applied to ${DATASET}" -- set_label "owner:curation" --set_label "phase:sandbox" "${app_id}":"${DATASET_SANDBOX}"

# Remove staging datasets
bq rm -r -d "${DATASET_STAGING_SANDBOX}"
bq rm -r -d "${DATASET_STAGING}"

unset PYTHONPATH
