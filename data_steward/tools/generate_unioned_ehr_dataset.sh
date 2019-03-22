#!/usr/bin/env bash
echo "begining"
# This script automates the curation playbook (https://docs.google.com/document/d/1QnwUhJmrVc9JNt64goeRw-K7490gbaF7DsYhTTt9tUo/edit#heading=h.k24j7tgoprtn)

app_id="aou-res-curation-test"
vocab_dataset="vocabulary20190423"

USAGE="
Usage: generate_unioned_ehr_dataset.sh
  --key_file <path to key file>
  --app_id <application id>
  --ehr_snap_dataset <EHR dataset: default is ${ehr_snap_dataset}>
  [--vocab_dataset <vocab dataset: default is ${vocab_dataset}>]
"

echo
while true; do
  case "$1" in
    --app_id) app_id=$2; shift 2;;
    --ehr_snap_dataset) ehr_snap_dataset=$2; shift 2;;
    --vocab_dataset) vocab_dataset=$2; shift 2;;
    --key_file) key_file=$2; shift 2;;
    -- ) shift; break ;;
    * ) break ;;
  esac
done

if [[ -z "${key_file}" ]] || [[ -z "${app_id}" ]]
then
  echo "Specify the key file location and application ID. $USAGE"
  exit 1
fi

today=$(date '+%Y%m%d')
current_dir=$(pwd)

echo "today --> ${today}"
echo "ehr_snap_dataset --> ${ehr_snap_dataset}"
echo "app_id --> ${app_id}"
echo "key_file --> ${key_file}"
echo "vocab_dataset --> ${vocab_dataset}"

export GOOGLE_APPLICATION_CREDENTIALS="${key_file}"
export APPLICATION_ID="${app_id}"

#set application environment (ie dev, test, prod)
gcloud auth activate-service-account --key-file=${key_file}
gcloud config set project ${app_id}


#---------Create curation virtual environment----------
set -e
# create a new environment in directory curation_env
virtualenv  -p $(which python2.7) curation_env

# activate it
source curation_env/bin/activate

# install the requirements in the virtualenv
pip install -t ../lib -r ../requirements.txt

source set_path.sh

echo "-------------------------->Take a Snapshot of Unioned EHR Submissions (step 5)"
source_prefix="unioned_ehr_"
unioned_ehr_dataset="unioned_ehr${today}"
echo "unioned_ehr_dataset --> $unioned_ehr_dataset"

#---------------------------------------------------------------------
# Step 1 Create an empty dataset
echo "bq mk --dataset --description "copy ehr_union from ${ehr_snap_dataset}" ${app_id}:$unioned_ehr_dataset"
bq mk --dataset --description "copy ehr_union from ${ehr_snap_dataset}" ${app_id}:${unioned_ehr_dataset}

#----------------------------------------------------------------------
# Step 2 Create the clinical tables for unioned EHR data set
echo "python cdm.py ${unioned_ehr_dataset}"
python ../cdm.py ${unioned_ehr_dataset}

#----------------------------------------------------------------------
# Step 3 Copy OMOP vocabulary to unioned EHR data set
echo "python cdm.py --component vocabulary $unioned_ehr_dataset"
python ../cdm.py --component vocabulary ${unioned_ehr_dataset}

echo "table_copy.sh --source_app_id ${app_id} --target_app_id ${app_id} --source_dataset ${vocab_dataset} --target_dataset ${unioned_ehr_dataset}"
./table_copy.sh --source_app_id ${app_id} --target_app_id ${app_id} --source_dataset ${vocab_dataset} --target_dataset ${unioned_ehr_dataset}

#----------------------------------------------------------------------
# Step 4 copy unioned ehr clinical tables tables
echo "table_copy.sh --source_app_id ${app_id} --target_app_id ${app_id} --source_dataset ${ehr_snap_dataset} --source_prefix ${source_prefix} --target_dataset ${unioned_ehr_dataset}"
./table_copy.sh --source_app_id ${app_id} --target_app_id ${app_id} --source_dataset ${ehr_snap_dataset} --source_prefix ${source_prefix} --target_dataset ${unioned_ehr_dataset}

#----------------------------------------------------------------------
# Step 5 copy mapping tables tables
echo "table_copy.sh --source_app_id ${app_id} --target_app_id ${app_id} --source_dataset ${ehr_snap_dataset} --source_prefix _mapping_ --target_dataset ${unioned_ehr_dataset} --target_prefix _mapping_"
./table_copy.sh --source_app_id ${app_id} --target_app_id ${app_id} --source_dataset ${ehr_snap_dataset} --source_prefix _mapping_ --target_dataset ${unioned_ehr_dataset} --target_prefix _mapping_

unset PYTHONPATH
deactivate

rm -rf curation_env

