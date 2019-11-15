#!/usr/bin/env bash

# Generate unioned_ehr dataset
# Loads vocabulary from specified dataset and unioned EHR tables from specified dataset

USAGE="
Usage: generate_unioned_ehr_dataset.sh
  --key_file <path to key file>
  --vocab_dataset <vocab dataset>
  --ehr_snapshot <EHR dataset>
  --identifier <version identifier>
"

echo
while true; do
  case "$1" in
  --key_file)
    key_file=$2
    shift 2
    ;;
  --vocab_dataset)
    vocab_dataset=$2
    shift 2
    ;;
  --ehr_snapshot)
    ehr_snapshot=$2
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

if [[ -z "${key_file}" ]] || [[ -z "${vocab_dataset}" ]] || [[ -z "${ehr_snapshot}" ]] || [[ -z "${identifier}" ]]; then
  echo "$USAGE"
  exit 1
fi

today=$(date '+%Y%m%d')
current_dir=$(pwd)
app_id=$(cat "${key_file}" | python -c 'import json,sys;obj=json.load(sys.stdin);print(obj["project_id"]);')

echo "ehr_snapshot --> ${ehr_snapshot}"
echo "app_id --> ${app_id}"
echo "key_file --> ${key_file}"
echo "vocab_dataset --> ${vocab_dataset}"
echo "identifier --> ${identifier}"

export GOOGLE_APPLICATION_CREDENTIALS="${key_file}"
export GOOGLE_CLOUD_PROJECT="${app_id}"

#set application environment (ie dev, test, prod)
gcloud auth activate-service-account --key-file=${key_file}
gcloud config set project ${app_id}

#---------Create curation virtual environment----------
set -e
# create a new environment in directory curation_env
virtualenv -p $(which python3.7) curation_env

# activate it
source curation_env/bin/activate

# install the requirements in the virtualenv
pip install -r ../requirements.txt

source set_path.sh

echo "-------------------------->Take a Snapshot of Unioned EHR Submissions (step 5)"
source_prefix="unioned_ehr_"
unioned_ehr_dataset="${identifier}_unioned_ehr"
echo "unioned_ehr_dataset --> ${unioned_ehr_dataset}"

#---------------------------------------------------------------------
# Step 1 Create an empty dataset
echo "bq mk --dataset --description "copy ehr_union from ${ehr_snapshot}" ${app_id}:$unioned_ehr_dataset"
bq mk --dataset --description "copy ehr_union from ${ehr_snapshot}" ${app_id}:${unioned_ehr_dataset}

#----------------------------------------------------------------------
# Step 2 Create the clinical tables for unioned EHR data set
echo "python cdm.py ${unioned_ehr_dataset}"
python ../cdm.py ${unioned_ehr_dataset}

#----------------------------------------------------------------------
# Step 3 Copy OMOP vocabulary to unioned EHR data set
echo "python cdm.py --component vocabulary $unioned_ehr_dataset"
python ../cdm.py --component vocabulary ${unioned_ehr_dataset}

echo "table_copy.sh --source_app_id ${app_id} --target_app_id ${app_id} --source_dataset ${vocab_dataset} --target_dataset ${unioned_ehr_dataset}"
./table_copy.sh --source_app_id ${app_id} --target_app_id ${app_id} --source_dataset ${vocab_dataset} --target_dataset ${unioned_ehr_dataset} --sync false

#----------------------------------------------------------------------
# Step 4 copy unioned ehr clinical tables tables
echo "table_copy.sh --source_app_id ${app_id} --target_app_id ${app_id} --source_dataset ${ehr_snapshot} --source_prefix ${source_prefix} --target_dataset ${unioned_ehr_dataset}"
./table_copy.sh --source_app_id ${app_id} --target_app_id ${app_id} --source_dataset ${ehr_snapshot} --source_prefix ${source_prefix} --target_dataset ${unioned_ehr_dataset} --sync false

#----------------------------------------------------------------------
# Step 5 copy mapping tables tables
echo "table_copy.sh --source_app_id ${app_id} --target_app_id ${app_id} --source_dataset ${ehr_snapshot} --source_prefix _mapping_ --target_dataset ${unioned_ehr_dataset} --target_prefix _mapping_"
./table_copy.sh --source_app_id ${app_id} --target_app_id ${app_id} --source_dataset ${ehr_snapshot} --source_prefix _mapping_ --target_dataset ${unioned_ehr_dataset} --target_prefix _mapping_ --sync false

echo "removing tables copies unintentionally"
bq rm -f ${unioned_ehr_dataset}._mapping_ipmc_nu_condition_occurrence
bq rm -f ${unioned_ehr_dataset}._mapping_ipmc_nu_device_exposure
bq rm -f ${unioned_ehr_dataset}._mapping_ipmc_nu_drug_exposure
bq rm -f ${unioned_ehr_dataset}._mapping_ipmc_nu_fact_relationship
bq rm -f ${unioned_ehr_dataset}._mapping_ipmc_nu_measurement
bq rm -f ${unioned_ehr_dataset}._mapping_ipmc_nu_note
bq rm -f ${unioned_ehr_dataset}._mapping_ipmc_nu_observation
bq rm -f ${unioned_ehr_dataset}._mapping_ipmc_nu_procedure_occurrence
bq rm -f ${unioned_ehr_dataset}._mapping_ipmc_nu_specimen
bq rm -f ${unioned_ehr_dataset}._mapping_ipmc_nu_visit_occurrence

unset PYTHONPATH
deactivate
