#!/usr/bin/env bash

# This script automates the curation playbook (https://docs.google.com/document/d/1QnwUhJmrVc9JNt64goeRw-K7490gbaF7DsYhTTt9tUo/edit#heading=h.k24j7tgoprtn)
# It assuming steps 1-3 are already done via a daily cron job. This script automates 4-7.

ehr_dataset="auto_pipeline_input"
app_id="aou-res-curation-test"
vocab_dataset="vocabulary20180104"
rdr_dataset="test_rdr"
result_bucket="drc_curation_internal_test"
dataset_prefix=""

USAGE="
Usage: run_curation_pipeline.sh
  --key_file <path to key file>
  --app_id <application id>
  --deid_config <path to deid config json file>
  [--ehr_dataset <EHR dataset: default is ${ehr_dataset}>]
  [--vocab_dataset <vocab dataset: default is ${vocab_dataset}>]
  [--dataset_prefix <prefix on output datasets: default empty>]
  [--rdr_dataset <RDR dataset: default is ${rdr_dataset}>]
  [--result_bucket <Internal bucket: default is ${result_bucket}>]
"

while true; do
  case "$1" in
    --app_id) app_id=$2; shift 2;;
    --ehr_dataset) ehr_dataset=$2; shift 2;;
    --vocab_dataset) vocab_dataset=$2; shift 2;;
    --key_file) key_file=$2; shift 2;;
    --rdr_dataset) rdr_dataset=$2; shift 2;;
    --dataset_prefix) dataset_prefix=$2; shift 2;;
    --result_bucket) result_bucket=$2; shift 2;;
    --deid_config) deid_config=$2; shift 2;;
    -- ) shift; break ;;
    * ) break ;;
  esac
done

if [ -z "${key_file}" ] || [ -z "${app_id}" ] || [ -z "${deid_config}" ]
then
  echo "Specify the key file location, application ID and deid config file. $USAGE"
  exit 1
fi

today=$(date '+%Y%m%d')
current_dir=$(pwd)

echo "today --> ${today}"
echo "ehr_dataset --> ${ehr_dataset}"
echo "app_id --> ${app_id}"
echo "key_file --> ${key_file}"
echo "bq_rdr_dataaset --> ${rdr_dataset}"
echo "vocab_dataset --> ${vocab_dataset}"
echo "result_bucket --> ${result_bucket}"
echo "dataset_prefix --> ${dataset_prefix}"
echo "deid_config --> ${deid_config}"
echo "current_dir --> ${current_dir}"

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
pip install -t lib -r requirements.txt

source tools/set_path.sh
#------------------------------------------------------

########################################################
#Take a Snapshot of EHR Dataset (step 4)
########################################################
echo "-------------------------->Take a Snapshot of EHR Dataset (step 4)"
ehr_snap_dataset="${dataset_prefix}ehr${today}"
echo "ehr_snap_dataset --> $ehr_snap_dataset"

bq mk --dataset --description "snapshot of EHR dataset ${ehr_dataset}" ${app_id}:${ehr_snap_dataset}

#copy tables
echo "tools/table_copy.sh --source_app_id ${app_id} --target_app_id ${app_id} --source_dataset ${ehr_dataset} --target_dataset ${ehr_snap_dataset}"
tools/table_copy.sh --source_app_id ${app_id} --target_app_id ${app_id} --source_dataset ${ehr_dataset} --target_dataset ${ehr_snap_dataset}


########################################################
#Take a Snapshot of Unioned EHR Submissions (step 5)
########################################################
echo "-------------------------->Take a Snapshot of Unioned EHR Submissions (step 5)"
source_prefix="unioned_ehr_"
unioned_ehr_dataset="${dataset_prefix}unioned_ehr${today}"

echo "bq mk --dataset --description "copy ehr_union from ${ehr_snap_dataset}" ${app_id}:$unioned_ehr_dataset"
bq mk --dataset --description "copy ehr_union from ${ehr_snap_dataset}" ${app_id}:${unioned_ehr_dataset}

#Create the clinical tables for unioned EHR data set
echo "python cdm.py $unioned_ehr_dataset"
python cdm.py ${unioned_ehr_dataset}


#Copy OMOP vocabulary to unioned EHR data set
echo "python cdm.py --component vocabulary $unioned_ehr_dataset"
python cdm.py --component vocabulary ${unioned_ehr_dataset}
echo "tools/table_copy.sh --source_app_id ${app_id} --target_app_id ${app_id} --source_dataset ${vocab_dataset} --target_dataset ${unioned_ehr_dataset}"
tools/table_copy.sh --source_app_id ${app_id} --target_app_id ${app_id} --source_dataset ${vocab_dataset} --target_dataset ${unioned_ehr_dataset}

#copy unioned ehr clinical tables tables
echo "tools/table_copy.sh --source_app_id ${app_id} --target_app_id ${app_id} --source_dataset ${ehr_snap_dataset} --source_prefix ${source_prefix} --target_dataset ${unioned_ehr_dataset}"
tools/table_copy.sh --source_app_id ${app_id} --target_app_id ${app_id} --source_dataset ${ehr_snap_dataset} --source_prefix ${source_prefix} --target_dataset ${unioned_ehr_dataset}
#copy mapping tables tables
echo "tools/table_copy.sh --source_app_id ${app_id} --target_app_id ${app_id} --source_dataset ${ehr_snap_dataset} --source_prefix _mapping_ --target_dataset ${unioned_ehr_dataset} --target_prefix _mapping_"
tools/table_copy.sh --source_app_id ${app_id} --target_app_id ${app_id} --source_dataset ${ehr_snap_dataset} --source_prefix _mapping_ --target_dataset ${unioned_ehr_dataset} --target_prefix _mapping_

########################################################
#Combine RDR and Unioned EHR data (step 6)
########################################################
cdr="${dataset_prefix}combined${today}"
version="v0-2-rc3"

export RDR_DATASET_ID="${rdr_dataset}"
export UNIONED_DATASET_ID="${unioned_ehr_dataset}"
export EHR_RDR_DATASET_ID="${cdr}"
export BIGQUERY_DATASET_ID="${unioned_ehr_dataset}"

bq mk --dataset --description "${version} combine_ehr_rdr ${rdr_dataset} + ${unioned_ehr_dataset}" ${app_id}:${cdr}

#Create the clinical tables for unioned EHR data set
python cdm.py ${cdr}

#Copy OMOP vocabulary to CDR EHR data set
python cdm.py --component vocabulary ${cdr}
tools/table_copy.sh --source_app_id ${app_id} --target_app_id ${app_id} --source_dataset ${vocab_dataset} --target_dataset ${cdr}

#Combine EHR and PPI data sets
python tools/combine_ehr_rdr.py

#Run Achilles
export BIGQUERY_DATASET_ID="${cdr}"
export BUCKET_NAME_NYC="test-bucket"

python tools/run_achilles_and_export.py --bucket=${result_bucket} --folder=${cdr}


########################################################
# De-identify the combined dataset (step 7)
########################################################
version="v0-2-rc4"
cdr_deid="${cdr}_deid"

export BIGQUERY_DATASET_ID="${cdr_deid}"

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
cd deid

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

python deid.py --i_dataset ${cdr} --table person --o_dataset ${cdr_deid} --config ${deid_config} --log
python deid.py --i_dataset ${cdr} --table visit_occurrence --o_dataset ${cdr_deid} --config ${deid_config} --log
python deid.py --i_dataset ${cdr} --table condition_occurrence --o_dataset ${cdr_deid} --config ${deid_config} --log
python deid.py --i_dataset ${cdr} --table drug_exposure --o_dataset ${cdr_deid} --config ${deid_config} --log
python deid.py --i_dataset ${cdr} --table procedure_occurrence --o_dataset ${cdr_deid} --config ${deid_config} --log
python deid.py --i_dataset ${cdr} --table device_exposure --o_dataset ${cdr_deid} --config ${deid_config} --log
python deid.py --i_dataset ${cdr} --table death --o_dataset ${cdr_deid} --config ${deid_config} --log
python deid.py --i_dataset ${cdr} --table measurement --o_dataset ${cdr_deid} --config ${deid_config} --log
python deid.py --i_dataset ${cdr} --table observation --o_dataset ${cdr_deid} --config ${deid_config} --log
python deid.py --i_dataset ${cdr} --table location --o_dataset ${cdr_deid} --config ${deid_config} --log
python deid.py --i_dataset ${cdr} --table care_site --o_dataset ${cdr_deid} --config ${deid_config} --log

#Close virtual environment and remove
deactivate

cd ../..
#Switch to curation virtual env
unset PYTHONPATH
source curation_env/bin/activate
#Run Achilles
source tools/set_path.sh
python tools/run_achilles_and_export.py --bucket=${result_bucket} --folder=${cdr_deid}
