#!/usr/bin/env bash

# This Script automates the process of de-identification of the combined_dataset
# This script expects you are using the venv in curation directory

USAGE="
Usage: deid_runner.sh
  --key_file <path to key file>
  --app_id <application id>
  --cdr_id <combined_dataset name>
  --vocab_dataset <vocabulary dataset name>
"

while true; do
  case "$1" in
  --cdr_id)
    cdr_id=$2
    shift 2
    ;;
  --app_id)
    app_id=$2
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

if [[ -z "${key_file}" ]] || [[ -z "${cdr_id}" ]] || [[ -z "${app_id}" ]] || [[ -z "${cdr_id}" ]]; then
  echo "Specify the key file location and input dataset name application id and vocab dataset name. $USAGE"
  exit 1
fi

current_dir=$(pwd)

echo "app_id --> ${app_id}"
echo "key_file --> ${key_file}"
echo "cdr_id --> ${cdr_id}"
echo "vocab_dataset --> ${vocab_dataset}"
echo "currend Dir --> ${current_dir}"

export GOOGLE_APPLICATION_CREDENTIALS="${key_file}"
export APPLICATION_ID="${app_id}"

#set application environment (ie dev, test, prod)
gcloud auth activate-service-account --key-file="${key_file}"
gcloud config set project "${app_id}"

cdr_deid="${cdr_id}_deid"

#------Create de-id virtual environment----------
set -e
# create a new environment in directory deid_env

GCLOUD_PATH=$(which gcloud)
CLOUDSDK_ROOT_DIR=${GCLOUD_PATH%/bin/gcloud}
GAE_SDK_ROOT="${CLOUDSDK_ROOT_DIR}/platform/google_appengine"

# activate it
source ../venv/bin/activate
# install the requirements in the virtualenv
pip install -r requirements.txt -t ../lib/
pip install -r deid/requirements.txt

cp -R "${GAE_SDK_ROOT}"/google/appengine ../venv/lib/python2.7/site-packages/google/
cp -R "${GAE_SDK_ROOT}"/google/net ../venv/lib/python2.7/site-packages/google/

export BIGQUERY_DATASET_ID="${cdr_deid}"

# create empty de-id dataset
bq mk --dataset --description "${version} deid ${cdr}" ${app_id}:${cdr_deid}

#Create the clinical tables for unioned EHR data set
PYTHONPATH=$PYTHONPATH:./:./lib python cdm.py ${cdr_deid}

#Copy OMOP vocabulary to CDR EHR data set
PYTHONPATH=$PYTHONPATH:./:./lib python cdm.py --component vocabulary ${cdr_deid}
tools/table_copy.sh --source_app_id ${app_id} --target_app_id ${app_id} --source_dataset ${vocab_dataset} --target_dataset ${cdr_deid}

PYTHONPATH=$PYTHONPATH:./:./lib python run_deid.py --idataset "${cdr_id}" -p "${key_file}" -a submit --interactive |& tee -a deid_output.txt

# deavtivate the virtual environment
deactivate
