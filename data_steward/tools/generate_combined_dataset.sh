#!/usr/bin/env bash

# This Script automates the process of combining ehr and rdr datasets

rdr_dataset="test_rdr"

USAGE="
Usage: generate_combined_dataset.sh
  --key_file <path to key file>
  --app_id <application id>
  --vocab_dataset <vocab dataset>
  --unioned_ehr_dataset <unioned dataset>
  [--rdr_dataset <RDR dataset: default is ${rdr_dataset}>]
"

while true; do
  case "$1" in
    --app_id) app_id=$2; shift 2;;
    --unioned_ehr_dataset) unioned_ehr_dataset=$2; shift 2;;
    --vocab_dataset) vocab_dataset=$2; shift 2;;
    --key_file) key_file=$2; shift 2;;
    --rdr_dataset) rdr_dataset=$2; shift 2;;
    -- ) shift; break ;;
    * ) break ;;
  esac
done

if [[ -z "${key_file}" ]] || [[ -z "${app_id}" ]] || [[ -z "${vocab_dataset}" ]]  || [[ -z "${unioned_ehr_dataset}" ]]
then
  echo "$USAGE"
  exit 1
fi

today=$(date '+%Y%m%d')
current_dir=$(pwd)

echo "today --> ${today}"
echo "unioned_ehr_dataset --> ${unioned_ehr_dataset}"
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

#--------------------------------------------------------
#Combine RDR and Unioned EHR data (step 6 in playbook)
cdr="combined${today}_base"
tag=$(git describe --abbrev=0 --tags)
version=${tag}

export RDR_DATASET_ID="${rdr_dataset}"
export UNIONED_DATASET_ID="${unioned_ehr_dataset}"
export EHR_RDR_DATASET_ID="${cdr}"
export BIGQUERY_DATASET_ID="${unioned_ehr_dataset}"

bq mk --dataset --description "${version} combine_ehr_rdr base version  ${rdr_dataset} + ${unioned_ehr_dataset}" ${app_id}:${cdr}

#Create the clinical tables for unioned EHR data set
cd ..
python cdm.py ${cdr}

#Copy OMOP vocabulary to CDR EHR data set
python cdm.py --component vocabulary ${cdr}
tools/table_copy.sh --source_app_id ${app_id} --target_app_id ${app_id} --source_dataset ${vocab_dataset} --target_dataset ${cdr}

#Combine EHR and PPI data sets
python tools/combine_ehr_rdr.py

unset PYTHOPATH
deactivate
