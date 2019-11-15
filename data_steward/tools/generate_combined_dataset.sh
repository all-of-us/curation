#!/usr/bin/env bash

# This Script automates the process of combining ehr and rdr datasets

USAGE="
Usage: generate_combined_dataset.sh
  --key_file <path to key file>
  --vocab_dataset <vocab dataset>
  --unioned_ehr_dataset <unioned dataset>
  --rdr_dataset <RDR dataset>
  --identifier <version identifier> 
"

while true; do
  case "$1" in
  --unioned_ehr_dataset)
    unioned_ehr_dataset=$2
    shift 2
    ;;
  --vocab_dataset)
    vocab_dataset=$2
    shift 2
    ;;
  --key_file)
    key_file=$2
    shift 2
    ;;
  --rdr_dataset)
    rdr_dataset=$2
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

if [[ -z "${key_file}" ]] || [[ -z "${app_id}" ]] || [[ -z "${vocab_dataset}" ]] || [[ -z "${unioned_ehr_dataset}" ]] || [[ -z "${identifier}" ]]; then
  echo "$USAGE"
  exit 1
fi

current_dir=$(pwd)
app_id=$(cat "${key_file}" | python -c 'import json,sys;obj=json.load(sys.stdin);print(obj["project_id"]);')

echo "today --> ${today}"
echo "unioned_ehr_dataset --> ${unioned_ehr_dataset}"
echo "app_id --> ${app_id}"
echo "key_file --> ${key_file}"
echo "vocab_dataset --> ${vocab_dataset}"
echo "rdr_dataset --> ${rdr_dataset}"
echo "identifier --> ${identifier}"

export GOOGLE_APPLICATION_CREDENTIALS="${key_file}"
export GOOGLE_CLOUD_PROJECT="${app_id}"

#set application environment (ie dev, test, prod)
gcloud auth activate-service-account --key-file=${key_file}
gcloud config set project ${app_id}

#---------Create curation virtual environment----------
set -e
# create a new environment in directory curation_env
virtualenv  -p $(which python3.7) curation_env

# activate it
source curation_env/bin/activate

# install the requirements in the virtualenv
pip install -r ../requirements.txt

source set_path.sh

#--------------------------------------------------------
#Combine RDR and Unioned EHR data (step 6 in playbook)
tag=$(git describe --abbrev=0 --tags)
version=${tag}

combined_backup="${identifier}_combined_backup"
combined_staging="${identifier}_combined_staging"
combined="${identifier}_combined"

export RDR_DATASET_ID="${rdr_dataset}"
export UNIONED_DATASET_ID="${unioned_ehr_dataset}"
export EHR_RDR_DATASET_ID="${combined_backup}"
export BIGQUERY_DATASET_ID="${unioned_ehr_dataset}"

bq mk --dataset --description "${version} combined raw version of ${rdr_dataset} + ${unioned_ehr_dataset}" ${app_id}:${combined_backup}

#Create the clinical tables for unioned EHR data set
cd ..
python cdm.py ${combined_backup}

#Copy OMOP vocabulary to CDR EHR data set
python cdm.py --component vocabulary ${combined_backup}
tools/table_copy.sh --source_app_id ${app_id} --target_app_id ${app_id} --source_dataset ${vocab_dataset} --target_dataset ${combined_backup}

#Combine EHR and PPI data sets
python tools/combine_ehr_rdr.py

# create an intermediary table to apply cleaning rules on
bq mk --dataset --description "intermediary dataset to apply cleaning rules on ${combined_backup}" ${app_id}:${combined_staging}

echo "table_copy.sh --source_app_id ${app_id} --target_app_id ${app_id} --source_dataset ${combined_backup} --target_dataset ${combined_staging}"
./table_copy.sh --source_app_id ${app_id} --target_app_id ${app_id} --source_dataset ${combined_backup} --target_dataset ${combined_staging}

export EHR_RDR_DATASET_ID="${combined_staging}"
export BIGQUERY_DATASET_ID="${combined_staging}"
data_stage='combined'

cd ../cdr_cleaner/

# run cleaning_rules on a dataset
python clean_cdr.py --data_stage ${data_stage} -s 2>&1 | tee combined_cleaning_log_"${today}".txt

cd ../tools/

# Create a snapshot dataset with the result
python snapshot_by_query.py -p "${app_id}" -d "${combined_staging}" -n "${combined}"

bq update --description "${version} combined clean version of ${rdr_dataset} + ${unioned_ehr_dataset}" ${app_id}:${combined}

#copy sandbox dataset
./table_copy.sh --source_app_id ${app_id} --target_app_id ${app_id} --source_dataset "${combined_staging}_sandbox" --target_dataset "${combined}_sandbox"

dbrowser="${identifier}_dbrowser"

# Create a dataset for data browser team
bq mk --dataset --description "intermediary dataset to apply cleaning rules on ${combined_backup}" ${app_id}:${dbrowser}

unset PYTHOPATH
deactivate
