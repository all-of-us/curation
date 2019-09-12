#!/usr/bin/env bash

# This Script automates the process of generating the ehr_snapshot

ehr_dataset="auto_pipeline_input"
app_id="aou-res-curation-test"

USAGE="
Usage: create_ehr_snapshot.sh
  --key_file <path to key file>
  --app_id <application id>
  [--ehr_dataset <EHR dataset: default is ${ehr_dataset}>]
"

while true; do
  case "$1" in
  --app_id)
    app_id=$2
    shift 2
    ;;
  --ehr_dataset)
    ehr_dataset=$2
    shift 2
    ;;
  --key_file)
    key_file=$2
    shift 2
    ;;
  --)
    shift
    break
    ;;
  *) break ;;
  esac
done

if [[ -z "${key_file}" ]] || [[ -z "${app_id}" ]]; then
  echo "Specify the key file location and application ID. $USAGE"
  exit 1
fi

today=$(date '+%Y%m%d')
current_dir=$(pwd)

echo "today --> ${today}"
echo "ehr_dataset --> ${ehr_dataset}"
echo "app_id --> ${app_id}"
echo "key_file --> ${key_file}"
echo "current_dir --> ${current_dir}"

export GOOGLE_APPLICATION_CREDENTIALS="${key_file}"
export APPLICATION_ID="${app_id}"

#set application environment (ie dev, test, prod)
gcloud auth activate-service-account --key-file=${key_file}
gcloud config set project ${app_id}

#---------Create curation virtual environment----------
set -e
# create a new environment in directory curation_env
virtualenv -p $(which python2.7) curation_env

# activate it
source curation_env/bin/activate

# install the requirements in the virtualenv
pip install -t ../lib -r ../requirements.txt

source set_path.sh
#------------------------------------------------------

echo "-------------------------->Take a Snapshot of EHR Dataset (step 4)"
ehr_snap_dataset="ehr${today}"
echo "ehr_snap_dataset --> $ehr_snap_dataset"

bq mk --dataset --description "snapshot of EHR dataset ${ehr_dataset}" ${app_id}:${ehr_snap_dataset}

#copy tables
echo "table_copy.sh --source_app_id ${app_id} --target_app_id ${app_id} --source_dataset ${ehr_dataset} --target_dataset ${ehr_snap_dataset}"
./table_copy.sh --source_app_id ${app_id} --target_app_id ${app_id} --source_dataset ${ehr_dataset} --target_dataset ${ehr_snap_dataset} --sync false

deactivate
unset PYTHONPATH
