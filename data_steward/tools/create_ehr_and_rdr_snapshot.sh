#!/usr/bin/env bash

# This Script automates the process of generating the ehr_snapshot

ehr_dataset="auto_pipeline_input"
app_id="aou-res-curation-test"

USAGE="
Usage: create_ehr_snapshot.sh
  --key_file <path to key file>
  --rdr <rdr dataset name>
  --release <version identifier>
  [--ehr_dataset <EHR dataset: default is ${ehr_dataset}>]
"

while true; do
  case "$1" in
  --ehr_dataset)
    ehr_dataset=$2
    shift 2
    ;;
  --key_file)
    key_file=$2
    shift 2
    ;;
  --rdr)
    rdr=$2
    shift 2
    ;;
  --identifier)
    identifier=$2
    shift 2
    ;;
  --)
    shift
    break
    ;;
  *) break ;;
  esac
done

if [[ -z "${key_file}" ]] || [[ -z "${app_id}" ]] || [[ -z "${rdr}" ]] || [[ -z "${identifier}" ]]; then
  echo "Specify the key file location and application ID RDr Dataset id, Quater identifier and release identifier . $USAGE"
  exit 1
fi

current_dir=$(pwd)
app_id=$(cat "${key_file}" | python -c 'import json,sys;obj=json.load(sys.stdin);print(obj["project_id"]);')

echo "ehr_dataset --> ${ehr_dataset}"
echo "app_id --> ${app_id}"
echo "key_file --> ${key_file}"
echo "current_dir --> ${current_dir}"
echo "rdr --> ${rdr}"
echo "identifier --> ${identifier}"

export GOOGLE_APPLICATION_CREDENTIALS="${key_file}"
export GOOGLE_CLOUD_PROJECT="${app_id}"

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

echo "-------------------------->Snapshotting EHR Dataset (step 4)"
ehr_snapshot="${identifier}_ehr"
echo "ehr_snap_dataset --> ${ehr_snapshot}"

bq mk --dataset --description "snapshot of EHR dataset ${ehr_dataset}" ${app_id}:${ehr_snapshot}

#copy tables
echo "table_copy.sh --source_app_id ${app_id} --target_app_id ${app_id} --source_dataset ${ehr_dataset} --target_dataset ${ehr_snapshot}"
./table_copy.sh --source_app_id ${app_id} --target_app_id ${app_id} --source_dataset ${ehr_dataset} --target_dataset ${ehr_snapshot} --sync false

echo "--------------------------> Snapshotting RDR Dataset (step 5)"
rdr_snapshot="${identifier}_rdr"
echo "rdr_snapshot --> ${rdr_snapshot}"

bq mk --dataset --description "snapshot of RDR dataset ${rdr}" ${app_id}:${rdr_snapshot}

#copy tables
echo "table_copy.sh --source_app_id ${app_id} --target_app_id ${app_id} --source_dataset ${rdr} --target_dataset ${rdr_snapshot}"
./table_copy.sh --source_app_id ${app_id} --target_app_id ${app_id} --source_dataset ${rdr} --target_dataset ${rdr_snapshot} --sync false

deactivate
unset PYTHONPATH
