#!/usr/bin/env bash
set -ex
# This Script automates the process of combining ehr and rdr datasets

USAGE="
Usage: generate_combined_dataset.sh
  --key_file <path to key file>
  --vocab_dataset <vocab dataset>
  --unioned_ehr_dataset <unioned dataset>
  --rdr_dataset <RDR dataset>
  --dataset_release_tag <release tag for the CDR>
  --ehr_cutoff <ehr_cut_off date format yyyy-mm-dd>
  --rdr_export_date <date RDR export is run format yyyy-mm-dd>
"

while true; do
  case "$1" in
  --key_file)
    key_file=$2
    shift 2
    ;;
  --unioned_ehr_dataset)
    unioned_ehr_dataset=$2
    shift 2
    ;;
  --vocab_dataset)
    vocab_dataset=$2
    shift 2
    ;;
  --rdr_dataset)
    rdr_dataset=$2
    shift 2
    ;;
  --dataset_release_tag)
    dataset_release_tag=$2
    shift 2
    ;;
  --ehr_cutoff)
    ehr_cutoff=$2
    shift 2
    ;;
  --rdr_export_date)
    rdr_export_date=$2
    shift 2
    ;;
  --)
    shift
    break
    ;;
  *) break ;;
  esac
done

if [[ -z "${key_file}" ]] || [[ -z "${unioned_ehr_dataset}" ]] || [[ -z "${vocab_dataset}" ]] || [[ -z "${rdr_dataset}" ]] || [[ -z "${dataset_release_tag}" ]] || [[ -z "${ehr_cutoff}" ]] || [[ -z "${rdr_export_date}" ]]; then
  echo "$USAGE"
  exit 1
fi

ROOT_DIR=$(git rev-parse --show-toplevel)
DATA_STEWARD_DIR="${ROOT_DIR}/data_steward"
TOOLS_DIR="${DATA_STEWARD_DIR}/tools"
CLEANER_DIR="${DATA_STEWARD_DIR}/cdr_cleaner"
app_id=$(python -c 'import json,sys;obj=json.load(sys.stdin);print(obj["project_id"]);' <"${key_file}")
today=$(date '+%Y-%m-%d')

echo "unioned_ehr_dataset --> ${unioned_ehr_dataset}"
echo "rdr_dataset --> ${rdr_dataset}"
echo "key_file --> ${key_file}"
echo "vocab_dataset --> ${vocab_dataset}"
echo "ehr_cutoff_date --> ${ehr_cutoff}"
echo "rdr_export_date --> ${rdr_export_date}"
echo "cdr generation date --> ${today}"

export GOOGLE_APPLICATION_CREDENTIALS="${key_file}"
export GOOGLE_CLOUD_PROJECT="${app_id}"

#set application environment (ie dev, test, prod)
gcloud auth activate-service-account --key-file=${key_file}
gcloud config set project ${app_id}

#---------Create curation virtual environment----------
# create a new environment in directory curation_venv
virtualenv -p "$(which python3.7)" "${DATA_STEWARD_DIR}/curation_venv"

# activate it
source "${DATA_STEWARD_DIR}/curation_venv/bin/activate"

# install the requirements in the virtualenv
pip install -r "${DATA_STEWARD_DIR}/requirements.txt"

# shellcheck source=src/set_path.sh
source "${TOOLS_DIR}/set_path.sh"

#--------------------------------------------------------
#Combine RDR and Unioned EHR data (step 6 in playbook)
tag=$(git describe --abbrev=0 --tags)
version=${tag}

combined="${dataset_release_tag}_combined"
combined_backup="${combined}_backup"
combined_staging="${combined}_staging"

export RDR_DATASET_ID="${rdr_dataset}"
export UNIONED_DATASET_ID="${unioned_ehr_dataset}"
export COMBINED_DATASET_ID="${combined_backup}"
export BIGQUERY_DATASET_ID="${unioned_ehr_dataset}"

bq mk --dataset --description "${version} combined raw version of  ${rdr_dataset} + ${unioned_ehr_dataset}" ${app_id}:${combined_backup}

#Create the clinical tables for unioned EHR data set
python "${DATA_STEWARD_DIR}/cdm.py" ${combined_backup}

#Copy OMOP vocabulary to CDR EHR data set
python "${DATA_STEWARD_DIR}/cdm.py" --component vocabulary ${combined_backup}
"${TOOLS_DIR}/table_copy.sh" --source_app_id ${app_id} --target_app_id ${app_id} --source_dataset ${vocab_dataset} --target_dataset ${combined_backup}

#Combine EHR and PPI data sets
python "${TOOLS_DIR}/combine_ehr_rdr.py"

# Add cdr_meta data table
python "${TOOLS_DIR}/add_cdr_metadata.py" --component "create" --project_id ${app_id} --target_dataset ${combined_backup}

# Add data to cdr_metadata table
python "${TOOLS_DIR}/add_cdr_metadata.py" --component "insert" --project_id ${app_id} --target_dataset ${combined_backup} \
--etl_version ${version} --ehr_source ${unioned_ehr_dataset} --ehr_cutoff_date ${ehr_cutoff} --rdr_source ${rdr_dataset} --cdr_generation_date ${today} --vocabulary_version ${vocab_dataset}

# create an intermediary table to apply cleaning rules on
bq mk --dataset --description "intermediary dataset to apply cleaning rules on ${combined_backup}" ${app_id}:${combined_staging}

"${TOOLS_DIR}/table_copy.sh" --source_app_id ${app_id} --target_app_id ${app_id} --source_dataset ${combined_backup} --target_dataset ${combined_staging}

export COMBINED_DATASET_ID="${combined_staging}"
export BIGQUERY_DATASET_ID="${combined_staging}"
data_stage='combined'

# run cleaning_rules on a dataset
python "${CLEANER_DIR}/clean_cdr.py" --data_stage ${data_stage} -s 2>&1 | tee combined_cleaning_log_"${combined}".txt

# Create a snapshot dataset with the result
python "${TOOLS_DIR}/snapshot_by_query.py" -p "${app_id}" -d "${combined_staging}" -n "${combined}"

bq update --description "${version} combined clean version of ${rdr_dataset} + ${unioned_ehr_dataset}" ${app_id}:${combined}

#copy sandbox dataset
"${TOOLS_DIR}/table_copy.sh" --source_app_id ${app_id} --target_app_id ${app_id} --source_dataset "${combined_staging}_sandbox" --target_dataset "${combined}_sandbox"

bq rm -r -d "${combined_staging}_sandbox"
bq rm -r -d "${combined_staging}"

dbrowser="${combined}_dbrowser"

# Create a dataset for data browser team
bq mk --dataset --description "intermediary dataset to apply cleaning rules on ${combined_backup}" ${app_id}:${dbrowser}

"${TOOLS_DIR}/table_copy.sh" --source_app_id ${app_id} --target_app_id ${app_id} --source_dataset ${combined} --target_dataset ${dbrowser}

# Deactivate venv and unset PYTHONPATH
unset PYTHOPATH
deactivate

set +ex
