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
  --cope_survey_dataset <dataset where RDR provided cope survey mapping table is loaded>
  --cope_survey_table_name <name of the cope survey mappig table>
  --deid_max_age <integer maximum age for de-identified participants>
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
  --cope_survey_dataset)
    cope_survey_dataset=$2
    shift 2
    ;;
  --cope_survey_table_name)
    cope_survey_table_name=$2
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

if [[ -z "${key_file}" ]] || [[ -z "${cdr_id}" ]] || [[ -z "${vocab_dataset}" ]] || [[ -z "${dataset_release_tag}" ]] || [[ -z "${cope_survey_dataset}" ]] || [[ -z "${cope_survey_table_name}" ]] || [[ -z "${deid_max_age}" ]]; then
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

registered_cdr_deid="R${dataset_release_tag}_deid"
registered_cdr_deid_sandbox="${registered_cdr_deid}_sandbox"
ROOT_DIR=$(git rev-parse --show-toplevel)
DATA_STEWARD_DIR="${ROOT_DIR}/data_steward"
TOOLS_DIR="${DATA_STEWARD_DIR}/tools"
DEID_DIR="${DATA_STEWARD_DIR}/deid"
CLEANER_DIR="${DATA_STEWARD_DIR}/cdr_cleaner"
HANDOFF_DATE="$(date -v +1d +'%Y-%m-%d')"
data_stage="registered_tier_died"

export BIGQUERY_DATASET_ID="${registered_cdr_deid}"
export PYTHONPATH="${PYTHONPATH}:${DEID_DIR}:${DATA_STEWARD_DIR}"

# Version is the most recent tag accessible from the current branch
version=$(git describe --abbrev=0 --tags)

# create empty de-id dataset
bq mk --dataset --description "${version} deidentified version of ${cdr_id}" --label "phase:backup" --label "release_tag:${dataset_release_tag}" --label "de_identified:true" "${APP_ID}":"${registered_cdr_deid}"

# create the clinical tables
python "${DATA_STEWARD_DIR}/cdm.py" "${registered_cdr_deid}"

# copy OMOP vocabulary
python "${DATA_STEWARD_DIR}/cdm.py" --component vocabulary "${registered_cdr_deid}"
"${TOOLS_DIR}"/table_copy.sh --source_app_id "${APP_ID}" --target_app_id "${APP_ID}" --source_dataset "${vocab_dataset}" --target_dataset "${registered_cdr_deid}"

# apply de-identification on registered tier dataset
python "${TOOLS_DIR}/run_deid.py" --idataset "${cdr_id}" --private_key "${key_file}" --action submit --interactive --console-log --age_limit "${deid_max_age}" --odataset "${registered_cdr_deid}" 2>&1 | tee deid_run.txt

# create empty sandbox dataset for the deid
bq mk --dataset --force --description "${version} sandbox dataset to apply cleaning rules on ${registered_cdr_deid}" --label "phase:sandbox" --label "release_tag:${dataset_release_tag}" --label "de_identified:true" "${APP_ID}":"${registered_cdr_deid_sandbox}"

# apply de-identification rules on registered tier dataset 
python "${CLEANER_DIR}/clean_cdr.py" --project_id "${APP_ID}" --dataset_id "${registered_cdr_deid}" --sandbox_dataset_id "${registered_cdr_deid_sandbox}" --data_stage ${data_stage} --mapping_dataset_id "${cdr_id}"  --cope_survey_dataset "${cope_survey_dataset}" --cope_survey_table "${cope_survey_table_name}" -s 2>&1 | tee registered_tier_cleaning_log.txt

cdr_deid_base_staging="${registered_cdr_deid}_base_staging"
cdr_deid_base_staging_sandbox="${registered_cdr_deid}_base_staging_sandbox"
cdr_deid_base_sandbox="${registered_cdr_deid}_base_sandbox"
cdr_deid_base="${registered_cdr_deid}_base"
cdr_deid_clean_staging="${registered_cdr_deid}_clean_staging"
cdr_deid_clean_staging_sandbox="${registered_cdr_deid}_clean_staging_sandbox"
cdr_deid_clean_sandbox="${registered_cdr_deid}_clean_sandbox"
cdr_deid_clean="${registered_cdr_deid}_clean"

# Copy cdr_metadata table
python "${TOOLS_DIR}/add_cdr_metadata.py" --component "copy" --project_id ${APP_ID} --target_dataset ${registered_cdr_deid} --source_dataset ${cdr_id}

# create empty de-id_base dataset to apply cleaning rules
bq mk --dataset --description "Intermediary dataset to apply cleaning rules on ${registered_cdr_deid}" --label "phase:staging" --label "release_tag:${dataset_release_tag}" --label "de_identified:true" ${APP_ID}:${cdr_deid_base_staging}

# create empty sandbox dataset to apply cleaning rules on staging dataset
bq mk --dataset --description "Sandbox created for storing records affected by the cleaning rules applied to ${cdr_deid_base_staging}" --label "phase:sandbox" --label "release_tag:${dataset_release_tag}" --label "de_identified:true" "${APP_ID}":"${cdr_deid_base_staging_sandbox}"

# copy de_id dataset to a clean version
"${TOOLS_DIR}"/table_copy.sh --source_app_id "${APP_ID}" --target_app_id "${APP_ID}" --source_dataset "${registered_cdr_deid}" --target_dataset "${cdr_deid_base_staging}"

export BIGQUERY_DATASET_ID="${cdr_deid_base_staging}"
export COMBINED_DEID_DATASET_ID="${cdr_deid_base_staging}"
data_stage='deid_base'

# run cleaning_rules on deid base staging dataset
python "${CLEANER_DIR}/clean_cdr.py" --project_id "${APP_ID}" --dataset_id "${cdr_deid_base_staging}" --sandbox_dataset_id "${cdr_deid_base_staging_sandbox}" --data_stage ${data_stage} -s 2>&1 | tee deid_base_cleaning_log.txt

# Create a snapshot dataset with the result
python "${TOOLS_DIR}/snapshot_by_query.py" --project_id "${APP_ID}" --dataset_id "${cdr_deid_base_staging}" --snapshot_dataset_id "${cdr_deid_base}"

bq update --description "${version} De-identified Base version of ${cdr_id}" --set_label "phase:clean" --set_label "release_tag:${dataset_release_tag}" --set_label "de_identified:true" ${APP_ID}:${cdr_deid_base}

# Add qa_handoff_date to cdr_metadata table
python "${TOOLS_DIR}/add_cdr_metadata.py" --component "insert" --project_id ${APP_ID} --target_dataset ${cdr_deid_base} --qa_handoff_date ${HANDOFF_DATE}

#copy sandbox dataset
"${TOOLS_DIR}/table_copy.sh" --source_app_id ${APP_ID} --target_app_id ${APP_ID} --source_dataset "${cdr_deid_base_staging_sandbox}" --target_dataset "${cdr_deid_base_sandbox}"

# Update sandbox description
bq update --description "Sandbox created for storing records affected by the cleaning rules applied to ${cdr_deid_base}" --set_label "phase:sandbox" --set_label "release_tag:${dataset_release_tag}" --set_label "de_identified:true" "${APP_ID}":"${cdr_deid_base_sandbox}"

# remove intermediary datasets
bq rm -r -d "${cdr_deid_base_staging_sandbox}"
bq rm -r -d "${cdr_deid_base_staging}"

# create empty de-id_clean dataset to apply cleaning rules
bq mk --dataset --description "Intermediary dataset to apply cleaning rules on ${cdr_deid_base}" --label "phase:staging" --label "release_tag:${dataset_release_tag}" --label "de_identified:true" ${APP_ID}:${cdr_deid_clean_staging}

# create empty sandbox dataset to apply cleaning rules on staging dataset
bq mk --dataset --description "Sandbox created for storing records affected by the cleaning rules applied to ${cdr_deid_clean_staging}" --label "phase:sandbox" --label "release_tag:${dataset_release_tag}" --label "de_identified:true" "${APP_ID}":"${cdr_deid_clean_staging_sandbox}"

# copy de_id dataset to a clean version
"${TOOLS_DIR}/table_copy.sh" --source_app_id "${APP_ID}" --target_app_id "${APP_ID}" --source_dataset "${cdr_deid_base}" --target_dataset "${cdr_deid_clean_staging}"

export BIGQUERY_DATASET_ID="${cdr_deid_clean_staging}"
export COMBINED_DEID_CLEAN_DATASET_ID="${cdr_deid_clean_staging}"
data_stage='deid_clean'

# run cleaning_rules on deid clean staging dataset
python "${CLEANER_DIR}/clean_cdr.py" --project_id "${APP_ID}" --dataset_id "${cdr_deid_clean_staging}" --sandbox_dataset_id "${cdr_deid_clean_staging_sandbox}" --data_stage ${data_stage} -s 2>&1 | tee deid_clean_cleaning_log.txt

# Create a snapshot dataset with the result
python "${TOOLS_DIR}/snapshot_by_query.py" --project_id "${APP_ID}" --dataset_id "${cdr_deid_clean_staging}" --snapshot_dataset_id "${cdr_deid_clean}"

bq update --description "${version} De-identified Clean version of ${cdr_deid_base}" --set_label "phase:clean" --set_label "release_tag:${dataset_release_tag}" --set_label "de_identified:true" ${APP_ID}:${cdr_deid_clean}

#copy sandbox dataset
"${TOOLS_DIR}/table_copy.sh" --source_app_id ${APP_ID} --target_app_id ${APP_ID} --source_dataset "${cdr_deid_clean_staging_sandbox}" --target_dataset "${cdr_deid_clean_sandbox}"

# Update sandbox description
bq update --description "Sandbox created for storing records affected by the cleaning rules applied to ${cdr_deid_clean}" --set_label "phase:sandbox" --set_label "release_tag:${dataset_release_tag}" --set_label "de_identified:true" "${APP_ID}":"${cdr_deid_clean_sandbox}"

# remove intermediary datasets
bq rm -r -d "${cdr_deid_clean_staging_sandbox}"
bq rm -r -d "${cdr_deid_clean_staging}"

unset PYTHONPATH

set +ex
