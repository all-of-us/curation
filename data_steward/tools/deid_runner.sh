#!/usr/bin/env bash
set -ex
# This Script automates the process of de-identification of the combined_dataset
# This script expects you are using the venv in curation directory

USAGE="
Usage: deid_runner.sh
  --key_file <path to key file>
  --cdr_id <combined_dataset name>
  --run_as <service account email for impersonation>
  --pmi_email <pmi-ops account email>
  --deid_questionnaire_response_map_dataset <deid questionnaire response map dataset name>
  --vocab_dataset <vocabulary dataset name>
  --dataset_release_tag <release tag for the CDR>
  --deid_max_age <integer maximum age for de-identified participants>
  --clean_survey_dataset_id <clean_survey_dataset_id>
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
  --run_as)
    run_as=$2
    shift 2
    ;;
  --pmi_email)
    pmi_email=$2
    shift 2
    ;;
  --deid_questionnaire_response_map_dataset)
    deid_questionnaire_response_map_dataset=$2
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
  --deid_max_age)
    deid_max_age=$2
    shift 2
    ;;
  --)
    shift
    break
    ;;
  *) break ;;
  esac
done

if [[ -z "${key_file}" ]] || [[ -z "${cdr_id}" ]] || [[ -z "${run_as}" ]] || [[ -z "${pmi_email}" ]] || \
   [[ -z "${deid_questionnaire_response_map_dataset}" ]] || [[ -z "${vocab_dataset}" ]] || \
   [[ -z "${dataset_release_tag}" ]] || [[ -z "${deid_max_age}" ]]; then
  echo "${USAGE}"
  exit 1
fi

echo "key_file --> ${key_file}"
echo "cdr_id --> ${cdr_id}"
echo "vocab_dataset --> ${vocab_dataset}"

APP_ID=$(python -c 'import json,sys;obj=json.load(sys.stdin);print(obj["quota_project_id"]);' < "${key_file}")
export PROJECT_ID="${APP_ID}"
export GOOGLE_APPLICATION_CREDENTIALS="${key_file}"
export GOOGLE_CLOUD_PROJECT="${APP_ID}"

#set application environment (ie dev, test, prod)
# gcloud auth activate-service-account --key-file="${key_file}"
gcloud config set project "${APP_ID}"

registered_cdr_deid="R${dataset_release_tag}_deid"
registered_cdr_deid_sandbox="${dataset_release_tag}_deid_sandbox"
ROOT_DIR=$(git rev-parse --show-toplevel)
DATA_STEWARD_DIR="${ROOT_DIR}/data_steward"
TOOLS_DIR="${DATA_STEWARD_DIR}/tools"
DEID_DIR="${DATA_STEWARD_DIR}/deid"
CLEANER_DIR="${DATA_STEWARD_DIR}/cdr_cleaner"
HANDOFF_DATE="$(date -v +2d +'%Y-%m-%d')"
data_stage="registered_tier_deid"
combined_dataset="${dataset_release_tag}_combined"

export BIGQUERY_DATASET_ID="${registered_cdr_deid}"
export PYTHONPATH="${PYTHONPATH}:${DEID_DIR}:${DATA_STEWARD_DIR}"

# Version is the most recent tag accessible from the current branch
version=$(git describe --abbrev=0 --tags)

# create empty dataset for reg_{combined dataaset}
# bq mk --dataset --description "Copy of ${combined_dataset}" --label "owner:curation" --label "phase:clean" --label "data_tier:registered" --label "release_tag:${dataset_release_tag}" --label "de_identified:false" "${APP_ID}":"${cdr_id}"

# create empty de-id dataset
#bq mk --dataset --description "${version} deidentified version of ${cdr_id}" --label "owner:curation" --label "phase:clean" --label "data_tier:registered" --label "release_tag:${dataset_release_tag}" --label "de_identified:true" "${APP_ID}":"${registered_cdr_deid}"

# create the clinical tables
#python "${DATA_STEWARD_DIR}/cdm.py" "${registered_cdr_deid}"

#copy tables
# "${TOOLS_DIR}"/table_copy.sh --source_app_id ${APP_ID} --target_app_id ${APP_ID} --source_dataset ${combined_dataset} --target_dataset ${cdr_id} --sync false

# copy OMOP vocabulary
#python "${DATA_STEWARD_DIR}/cdm.py" --component vocabulary "${registered_cdr_deid}"
#"${TOOLS_DIR}"/table_copy.sh --source_app_id "${APP_ID}" --target_app_id "${APP_ID}" --source_dataset "${vocab_dataset}" --target_dataset "${registered_cdr_deid}"

# apply de-identification on registered tier dataset
#python "${TOOLS_DIR}/run_deid.py" --idataset "${cdr_id}" --private_key "${key_file}" --action submit --interactive --console-log --age_limit "${deid_max_age}" --odataset "${registered_cdr_deid}" --run_as "${run_as}" 2>&1 | tee deid_run.txt

# create empty sandbox dataset for the deid
#bq mk --dataset --force --description "${version} sandbox dataset to apply cleaning rules on ${registered_cdr_deid}" --label "owner:curation" --label "phase:sandbox" --label "data_tier:registered" --label "release_tag:${dataset_release_tag}" --label "de_identified:true" "${APP_ID}":"${registered_cdr_deid_sandbox}"

# clear GOOGLE_APPLICATION_CREDENTIALS environment variable inorder to make impersonation work in clean_engine
unset GOOGLE_APPLICATION_CREDENTIALS
gcloud config set account "${pmi_email}"

# apply de-identification rules on registered tier dataset
python "${CLEANER_DIR}/clean_cdr.py" --project_id "${APP_ID}" --dataset_id "${registered_cdr_deid}" --run_as "${run_as}" --sandbox_dataset_id "${registered_cdr_deid_sandbox}" --data_stage ${data_stage} --mapping_dataset_id "${cdr_id}" --deid_questionnaire_response_map_dataset "${deid_questionnaire_response_map_dataset}" -s 2>&1 | tee registered_tier_cleaning_log.txt

# Add GOOGLE_APPLICATION_CREDENTIALS environment variable
export GOOGLE_APPLICATION_CREDENTIALS="${key_file}"
# gcloud auth activate-service-account --key-file="${key_file}"

# Copy cdr_metadata table
python "${TOOLS_DIR}/add_cdr_metadata.py" --component "copy" --project_id "${APP_ID}" --target_dataset "${registered_cdr_deid}" --source_dataset "${cdr_id}"

# Add qa_handoff_date to cdr_metadata table
python "${TOOLS_DIR}/add_cdr_metadata.py" --component "insert" --project_id "${APP_ID}" --target_dataset "${registered_cdr_deid}" --qa_handoff_date "${HANDOFF_DATE}"

unset PYTHONPATH

set +ex
