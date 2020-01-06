#!/usr/bin/env bash
set -ex
# This Script automates the process of generating the ehr_snapshot

USAGE="
Usage: create_ehr_snapshot.sh
  --key_file <path to key file>
  --ehr_dataset <EHR dataset ID>
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
  --)
    shift
    break
    ;;
  *) break ;;
  esac
done

if [[ -z "${key_file}" ]] || [[ -z "${ehr_dataset}" ]]; then
  echo "Specify the key file location and ehr_dataset ID. $USAGE"
  exit 1
fi

today=$(date '+%Y%m%d')
current_dir=$(pwd)
app_id=$(python -c 'import json,sys;obj=json.load(sys.stdin);print(obj["project_id"]);' < "${key_file}")

echo "today --> ${today}"
echo "ehr_dataset --> ${ehr_dataset}"
echo "app_id --> ${app_id}"
echo "key_file --> ${key_file}"
echo "current_dir --> ${current_dir}"

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

echo "-------------------------->Take a Snapshot of EHR Dataset (step 4)"
ehr_snap_dataset="ehr${today}"
echo "ehr_snap_dataset --> $ehr_snap_dataset"

bq mk --dataset --description "snapshot of EHR dataset ${ehr_dataset}" ${app_id}:${ehr_snap_dataset}

#copy tables
"${TOOLS_DIR}/table_copy.sh" --source_app_id ${app_id} --target_app_id ${app_id} --source_dataset ${ehr_dataset} --target_dataset ${ehr_snap_dataset} --sync false

deactivate
unset PYTHONPATH
