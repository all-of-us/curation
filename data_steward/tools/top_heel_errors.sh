#!/usr/bin/env bash
set -ex
# Fetch the most prevalent achilles heel errors in a dataset

all_hpo=
OUTPUT_FILENAME="heel_errors.csv"

USAGE="
Usage: top_heel_errors.sh
  --key_file <path to key file>
  --dataset_id <EHR dataset>
  [--all_hpo]
"
while true; do
  case "$1" in
  --dataset_id)
    dataset_id=$2
    shift 2
    ;;
  --key_file)
    key_file=$2
    shift 2
    ;;
  --all_hpo)
    all_hpo=1
    shift
    ;;
  --)
    shift
    break
    ;;
  *) break ;;
  esac
done


if [[ -z "${key_file}" ]] || [[ -z "${dataset_id}" ]]; then
  echo "Specify the key file location, Application ID and Dataset ID. $USAGE"
  exit 1
fi

app_id=$(python -c 'import json,sys;obj=json.load(sys.stdin);print(obj["project_id"]);' < "${key_file}")

echo "dataset_id --> ${dataset_id}"
echo "app_id --> ${app_id}"
echo "key_file --> ${key_file}"
echo "all_hpo --> ${all_hpo}"

export GOOGLE_APPLICATION_CREDENTIALS="${key_file}"
export GOOGLE_CLOUD_PROJECT="${app_id}"
export BIGQUERY_DATASET_ID="${dataset_id}"

#set application environment (ie dev, test, prod)
gcloud auth activate-service-account --key-file=${key_file}
gcloud config set project ${app_id}

#-------Set python path to add the modules and lib--------
ROOT_DIR=$(git rev-parse --show-toplevel)
DATA_STEWARD_DIR="${ROOT_DIR}/data_steward"
TOOLS_DIR="${DATA_STEWARD_DIR}/tools"

virtualenv -p $(which python3.7) "${DATA_STEWARD_DIR}/curation_venv"

# activate it
source "${DATA_STEWARD_DIR}/curation_venv/bin/activate"

# install the requirements in the virtualenv
pip install -r "${DATA_STEWARD_DIR}/requirements.txt"

source "${TOOLS_DIR}/set_path.sh"

#----------------Run the heel errors script------------------
ALL_HPO_OPT=
if [[ "${all_hpo}" -eq "1" ]]; then
  ALL_HPO_OPT="--all_hpo"
fi

python "${TOOLS_DIR}/top_heel_errors.py" --app_id ${app_id} --dataset_id ${dataset_id} ${ALL_HPO_OPT} ${OUTPUT_FILENAME}

#----------cleanup-------------------
unset PYTHONPATH
deactivate
