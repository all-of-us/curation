#!/usr/bin/env bash

# This script automates the curation playbook (https://docs.google.com/document/d/1QnwUhJmrVc9JNt64goeRw-K7490gbaF7DsYhTTt9tUo/edit#heading=h.k24j7tgoprtn)
# It assuming steps 1-3 are already done via a daily cron job. This script automates 4-7.

app_id="aou-res-curation-test"
vocab_dataset="vocabulary20190423"
result_bucket="drc_curation_internal_test"

USAGE="
Usage: run_deid.sh
  --key_file <path to key file>
  --app_id <application id>
  --deid_config <path to deid config json file>
  [--cdr_id <EHR dataset: default empty>]
  [--vocab_dataset <vocab dataset: default is ${vocab_dataset}>]
"

while true; do
  case "$1" in
    --app_id) app_id=$2; shift 2;;
    --cdr_id) cdr_id=$2; shift 2;;
    --vocab_dataset) vocab_dataset=$2; shift 2;;
    --key_file) key_file=$2; shift 2;;
    --deid_config) deid_config=$2; shift 2;;
    -- ) shift; break ;;
    * ) break ;;
  esac
done

if [[ -z "${key_file}" ]] || [[ -z "${app_id}" ]] || [[ -z "${deid_config}" ]]
then
  echo "Specify the key file location, application ID and deid config file. $USAGE"
  exit 1
fi

current_dir=$(pwd)

echo "app_id --> ${app_id}"
echo "key_file --> ${key_file}"
echo "cdr_id --> ${cdr_id}"
echo "vocab_dataset --> ${vocab_dataset}"
echo "deid_config --> ${deid_config}"
echo "current_dir --> ${current_dir}"

cdr_deid="${cdr_id}_deid"

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

# Append app engine to python path
source set_path.sh


export BIGQUERY_DATASET_ID="${cdr_deid}"

# create empty de-id dataset
bq mk --dataset --description "${version} deid ${cdr}" ${app_id}:${cdr_deid}

#Create the clinical tables for unioned EHR data set
python cdm.py ${cdr_deid}

#Copy OMOP vocabulary to CDR EHR data set
python cdm.py --component vocabulary ${cdr_deid}
tools/table_copy.sh --source_app_id ${app_id} --target_app_id ${app_id} --source_dataset ${vocab_dataset} --target_dataset ${cdr_deid}

#The location and care_site tables must be deleted (due to this bug):
bq rm -f --table ${app_id}:${cdr_deid}.location
bq rm -f --table ${app_id}:${cdr_deid}.care_site

#Close virtual environment and remove
deactivate
unset PYTHONPATH
cd ../deid

#NOTE: Create a copy of config.json

#------Create de-id virtual environment----------
set -e
# create a new environment in directory deid_env
virtualenv -p $(which python2.7) deid_env

# activate it
source deid_env/bin/activate

# install the requirements in the virtualenv
pip install -r requirements.txt
#------------------------------------------------

cd src
set -e

python deid.py --i_dataset ${cdr_id} --table person --o_dataset ${cdr_deid} --config ${deid_config} --log
python deid.py --i_dataset ${cdr_id} --table visit_occurrence --o_dataset ${cdr_deid} --config ${deid_config} --log
python deid.py --i_dataset ${cdr_id} --table condition_occurrence --o_dataset ${cdr_deid} --config ${deid_config} --log
python deid.py --i_dataset ${cdr_id} --table drug_exposure --o_dataset ${cdr_deid} --config ${deid_config} --log
python deid.py --i_dataset ${cdr_id} --table procedure_occurrence --o_dataset ${cdr_deid} --config ${deid_config} --log
python deid.py --i_dataset ${cdr_id} --table device_exposure --o_dataset ${cdr_deid} --config ${deid_config} --log
python deid.py --i_dataset ${cdr_id} --table death --o_dataset ${cdr_deid} --config ${deid_config} --log
python deid.py --i_dataset ${cdr_id} --table measurement --o_dataset ${cdr_deid} --config ${deid_config} --log
python deid.py --i_dataset ${cdr_id} --table observation --o_dataset ${cdr_deid} --config ${deid_config} --log
python deid.py --i_dataset ${cdr_id} --table location --o_dataset ${cdr_deid} --config ${deid_config} --log
python deid.py --i_dataset ${cdr_id} --table care_site --o_dataset ${cdr_deid} --config ${deid_config} --log

#Close virtual environment and remove
deactivate

rm -rf deid_env