#!/usr/bin/env bash
set -ex
# This Script automates the process of de-identification of the combined_dataset
# This script expects you are using the venv in curation directory

USAGE="
Usage: deid_runner.sh
  --key_file <path to key file>
  --cdr_id <combined_dataset name>
  --vocab_dataset <vocabulary dataset name>
  --dataset_release_tag <release tag for the CDR>
"

while true; do
  case "$1" in
  --cdr_id)
    cdr_id=$2
    shift 2
    ;;
  --key_file)
    key_file=$2
    shift 2
    ;;
  --vocab_dataset)
    vocab_dataset=$2
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

if [[ -z "${key_file}" ]] || [[ -z "${cdr_id}" ]] || [[ -z "${vocab_dataset}" ]] || [[ -z "${dataset_release_tag}" ]]; then
  echo "${USAGE}"
  exit 1
fi

echo "key_file --> ${key_file}"
echo "cdr_id --> ${cdr_id}"
echo "vocab_dataset --> ${vocab_dataset}"

APP_ID=$(python -c 'import json,sys;obj=json.load(sys.stdin);print(obj["project_id"]);' < "${key_file}")
export GOOGLE_APPLICATION_CREDENTIALS="${key_file}"
export GOOGLE_CLOUD_PROJECT="${APP_ID}"

#set application environment (ie dev, test, prod)
gcloud auth activate-service-account --key-file="${key_file}"
gcloud config set project "${APP_ID}"

cdr_deid="${cdr_id}_deid"
ROOT_DIR=$(git rev-parse --show-toplevel)
DATA_STEWARD_DIR="${ROOT_DIR}/data_steward"
TOOLS_DIR="${DATA_STEWARD_DIR}/tools"
DEID_DIR="${DATA_STEWARD_DIR}/deid"
CLEANER_DIR="${DATA_STEWARD_DIR}/cdr_cleaner"
HANDOFF_DATE=$(date --date='1 day' '+%Y-%m-%d')

#------Create de-id virtual environment----------
virtualenv -p "$(which python3.7)" "${DATA_STEWARD_DIR}/curation_venv"

source "${DATA_STEWARD_DIR}/curation_venv/bin/activate"

# install the requirements in the virtualenv
pip install -r "${DATA_STEWARD_DIR}/requirements.txt"
pip install -r "${DEID_DIR}/requirements.txt"

export BIGQUERY_DATASET_ID="${cdr_deid}"
export PYTHONPATH="${PYTHONPATH}:${DEID_DIR}:${DATA_STEWARD_DIR}"

# Version is the most recent tag accessible from the current branch
version=$(git describe --abbrev=0 --tags)

# create empty de-id dataset
bq mk --dataset --description "${version} deidentified version of ${cdr_id}" "${APP_ID}":"${cdr_deid}"

# create the clinical tables
python "${DATA_STEWARD_DIR}/cdm.py" "${cdr_deid}"

# copy OMOP vocabulary
python "${DATA_STEWARD_DIR}/cdm.py" --component vocabulary "${cdr_deid}"
"${TOOLS_DIR}"/table_copy.sh --source_app_id "${APP_ID}" --target_app_id "${APP_ID}" --source_dataset "${vocab_dataset}" --target_dataset "${cdr_deid}"

# apply deidentification on combined dataset
python "${TOOLS_DIR}/run_deid.py" --idataset "${cdr_id}" -p "${key_file}" -a submit --interactive -c

# generate ext tables in deid dataset
python "${TOOLS_DIR}/generate_ext_tables.py" -p "${APP_ID}" -d "${cdr_deid}" -c "${cdr_id}" -s

cdr_deid_base_staging="${cdr_deid}_base_staging"
cdr_deid_base="${cdr_deid}_base"
cdr_deid_clean_staging="${cdr_deid}_clean_staging"
cdr_deid_clean="${cdr_deid}_clean"

# Copy cdr_metadata table
python "${TOOLS_DIR}/add_cdr_metadata.py" --component "copy" --project_id ${app_id} --target_dataset ${cdr_deid} --source_dataset ${cdr_id}

# create empty de-id_clean dataset to apply cleaning rules
bq mk --dataset --description "Intermediary dataset to apply cleaning rules on ${cdr_deid}" ${APP_ID}:${cdr_deid_base_staging}

# copy de_id dataset to a clean version
"${TOOLS_DIR}"/table_copy.sh --source_app_id "${APP_ID}" --target_app_id "${APP_ID}" --source_dataset "${cdr_deid}" --target_dataset "${cdr_deid_base_staging}"

export BIGQUERY_DATASET_ID="${cdr_deid_base_staging}"
export COMBINED_DEID_DATASET_ID="${cdr_deid_base_staging}"
data_stage='deid_base'

# run cleaning_rules on a dataset
python "${CLEANER_DIR}/clean_cdr.py" --data_stage ${data_stage} -s 2>&1 | tee deid_base_cleaning_log.txt

# Create a snapshot dataset with the result
python "${TOOLS_DIR}/snapshot_by_query.py" -p "${APP_ID}" -d "${cdr_deid_base_staging}" -n "${cdr_deid_base}"

bq update --description "${version} De-identified Base version of ${cdr_id}" ${APP_ID}:${cdr_deid_base}

# Add qa_handoff_date to cdr_metadata table 
python "${TOOLS_DIR}/add_cdr_metadata.py" --component "insert" --project_id ${app_id} --target_dataset ${cdr_deid_base} --qa_handoff_date ${HANDOFF_DATE}

#copy sandbox dataset
"${TOOLS_DIR}/table_copy.sh" --source_app_id ${app_id} --target_app_id ${app_id} --source_dataset "${cdr_deid_base_staging}_sandbox" --target_dataset "${cdr_deid_base}_sandbox"

# remove intemideary datasets
bq rm -r -d "${cdr_deid_base_staging}_sandbox"
bq rm -r -d "${cdr_deid_base_staging}"

# create empty de-id_clean dataset to apply cleaning rules
bq mk --dataset --description "Intermediary dataset to apply cleaning rules on ${cdr_deid_base}" ${APP_ID}:${cdr_deid_clean_staging}

# copy de_id dataset to a clean version
"${TOOLS_DIR}/table_copy.sh" --source_app_id "${APP_ID}" --target_app_id "${APP_ID}" --source_dataset "${cdr_deid_base}" --target_dataset "${cdr_deid_clean_staging}"

export BIGQUERY_DATASET_ID="${cdr_deid_clean_staging}"
export COMBINED_DEID_CLEAN_DATASET_ID="${cdr_deid_clean_staging}"
data_stage='deid_clean'

# run cleaning_rules on a dataset
python "${CLEANER_DIR}/clean_cdr.py" --data_stage ${data_stage} -s 2>&1 | tee deid_clean_cleaning_log.txt

# Create a snapshot dataset with the result
python "${TOOLS_DIR}/snapshot_by_query.py" -p "${APP_ID}" -d "${cdr_deid_clean_staging}" -n "${cdr_deid_clean}"

bq update --description "${version} De-identified Clean version of ${cdr_deid_base}" ${APP_ID}:${cdr_deid_clean}

#copy sandbox dataset
"${TOOLS_DIR}/table_copy.sh" --source_app_id ${app_id} --target_app_id ${app_id} --source_dataset "${cdr_deid_clean_staging}_sandbox" --target_dataset "${cdr_deid_clean}_sandbox"

# remove intemideary datasets
bq rm -r -d "${cdr_deid_clean_staging}_sandbox"
bq rm -r -d "${cdr_deid_clean_staging}"

# deactivate virtual environment
unset PYTHONPATH
deactivate

set +ex
