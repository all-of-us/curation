#!/usr/bin/env bash

# This Script automates the process of de-identification of the combined_dataset
# This script expects you are using the venv in curation directory

USAGE="
Usage: deid_runner.sh
  --key_file <path to key file>
  --cdr_id <combined_dataset name>
  --vocab_dataset <vocabulary dataset name>
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
  --)
    shift
    break
    ;;
  *) break ;;
  esac
done

if [[ -z "${key_file}" ]] || [[ -z "${cdr_id}" ]] || [[ -z "${vocab_dataset}" ]]; then
  echo "Specify the key file location and input dataset name application id and vocab dataset name. $USAGE"
  exit 1
fi

echo "key_file --> ${key_file}"
echo "cdr_id --> ${cdr_id}"
echo "vocab_dataset --> ${vocab_dataset}"

APP_ID=$(cat "${key_file}" | python -c 'import json,sys;obj=json.load(sys.stdin);print(obj["project_id"]);')
export GOOGLE_APPLICATION_CREDENTIALS="${key_file}"
export APPLICATION_ID="${APP_ID}"

#set application environment (ie dev, test, prod)
gcloud auth activate-service-account --key-file="${key_file}"
gcloud config set project "${APP_ID}"

cdr_deid="${cdr_id}_deid"
cdr_deid_clean="${cdr_deid}_clean"

#------Create de-id virtual environment----------
set -e

# create a new environment in directory deid_env
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
DEID_DIR="${DIR}/deid"
GCLOUD_PATH=$(which gcloud)
CLOUDSDK_ROOT_DIR=${GCLOUD_PATH%/bin/gcloud}
GAE_SDK_ROOT="${CLOUDSDK_ROOT_DIR}/platform/google_appengine"
GAE_SDK_APPENGINE="${GAE_SDK_ROOT}/google/appengine"
GAE_SDK_NET="${GAE_SDK_ROOT}/google/net"
VENV_DIR="${DIR}/deid_venv"

virtualenv --python=$(which python) "${VENV_DIR}"

source ${VENV_DIR}/bin/activate

# install the requirements in the virtualenv
pip install -r "${DIR}/requirements.txt"
pip install -r "${DEID_DIR}/requirements.txt"

VENV_LIB_GOOGLE="$(python -c "import google as _; print(_.__path__[-1])")"

cp -R "${GAE_SDK_APPENGINE}" "${VENV_LIB_GOOGLE}"
cp -R "${GAE_SDK_NET}" "${VENV_LIB_GOOGLE}"

export BIGQUERY_DATASET_ID="${cdr_deid}"
export PYTHONPATH="${PYTHONPATH}:${DEID_DIR}:${DIR}"

# Get Git version tag
tag=$(git describe --abbrev=0 --tags)
version=${tag}

# create empty de-id dataset
bq mk --dataset --description "${version} deidentified base version of ${cdr_id}" "${APP_ID}":"${cdr_deid}"

# create the clinical tables
python "${DIR}/cdm.py" "${cdr_deid}"

# copy OMOP vocabulary
python "${DIR}/cdm.py" --component vocabulary "${cdr_deid}"
"${DIR}"/tools/table_copy.sh --source_app_id "${APP_ID}" --target_app_id "${APP_ID}" --source_dataset "${vocab_dataset}" --target_dataset "${cdr_deid}"

# apply deidentification on combined dataset
python "${DIR}/run_deid.py" --idataset "${cdr_id}" -p "${key_file}" -a submit --interactive |& tee -a deid_output.txt

# create empty de-id_clean dataset to apply cleaning rules
bq mk --dataset --description "${version} deidentified clean version of ${cdr_id}" "${APP_ID}":"${cdr_deid_clean}"

# copy de_id dataset to a clean version
"${DIR}"/tools/table_copy.sh --source_app_id "${APP_ID}" --target_app_id "${APP_ID}" --source_dataset "${cdr_deid}" --target_dataset "${cdr_deid_clean}"

# deactivate virtual environment
deactivate
