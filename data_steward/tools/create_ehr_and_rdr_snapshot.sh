#!/usr/bin/env bash
set -ex
# This Script automates the process of generating the ehr_snapshot

USAGE="
Usage: create_ehr_snapshot.sh
  --key_file <path to key file>
  --ehr_dataset <EHR dataset ID>
  --rdr_dataset <RDR dataset ID>
  --dataset_release_tag <release tag for the CDR>
"

while true; do
  case "$1" in
  --key_file)
    key_file=$2
    shift 2
    ;;
  --ehr_dataset)
    ehr_dataset=$2
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
  --)
    shift
    break
    ;;
  *) break ;;
  esac
done

if [[ -z "${key_file}" ]] || [[ -z "${ehr_dataset}" ]] || [[ -z "${rdr_dataset}" ]] || [[ -z "${dataset_release_tag}" ]]; then
  echo "Specify the key file location and ehr_dataset ID, rdr_dataset ID and Dataset release tag . $USAGE"
  exit 1
fi

app_id=$(python -c 'import json,sys;obj=json.load(sys.stdin);print(obj["project_id"]);' < "${key_file}")

echo "ehr_dataset --> ${ehr_dataset}"
echo "rdr_dataset --> ${rdr_dataset}"
echo "app_id --> ${app_id}"
echo "key_file --> ${key_file}"
echo "dataset_release_tag --> ${dataset_release_tag}"

ROOT_DIR=$(git rev-parse --show-toplevel)
DATA_STEWARD_DIR="${ROOT_DIR}/data_steward"
TOOLS_DIR="${DATA_STEWARD_DIR}/tools"

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
#------------------------------------------------------

echo "-------------------------->Snapshotting EHR Dataset (step 4)"
ehr_snapshot="${dataset_release_tag}_ehr"
echo "ehr_snapshot --> ${ehr_snapshot}"

bq mk --dataset --description "snapshot of latest EHR dataset ${ehr_dataset}" ${app_id}:${ehr_snapshot}

#copy tables
"${TOOLS_DIR}/table_copy.sh" --source_app_id ${app_id} --target_app_id ${app_id} --source_dataset ${ehr_dataset} --target_dataset ${ehr_snapshot} --sync false

echo "--------------------------> Snapshotting  and cleaning RDR Dataset (step 5)"
rdr_snapshot="${dataset_release_tag}_rdr"
rdr_snapshot_staging="${rdr_snapshot}_staging"

bq mk --dataset --description "snapshot of latest RDR dataset ${rdr_dataset}" ${app_id}:${rdr_snapshot_staging}

#copy tables
"${TOOLS_DIR}/table_copy.sh" --source_app_id ${app_id} --target_app_id ${app_id} --source_dataset ${rdr_dataset} --target_dataset ${rdr_snapshot_staging} --sync false

#set BIGQUERY_DATASET_ID variable to dataset name where the vocabulary exists
export BIGQUERY_DATASET_ID="${rdr_snapshot_staging}"
export RDR_DATASET_ID="${rdr_snapshot_staging}"
echo "Cleaning the RDR data"
data_stage="rdr"

python "${CLEANER_DIR}/clean_cdr.py" --data_stage ${data_stage} -s

# Create a snapshot dataset with the result
python "${TOOLS_DIR}/snapshot_by_query.py" -p "${app_id}" -d "${rdr_snapshot_staging}" -n "${rdr_snapshot}"

#copy sandbox dataset
"${TOOLS_DIR}/table_copy.sh" --source_app_id ${app_id} --target_app_id ${app_id} --source_dataset "${rdr_snapshot_staging}_sandbox" --target_dataset "${rdr_snapshot}_sandbox"

bq rm -r -d "${rdr_snapshot_staging}_sandbox" 
bq rm -r -d "${rdr_snapshot_staging}" 

echo "Done."

# deactivate venv and unset PYTHONPATH
deactivate
unset PYTHONPATH

set +ex