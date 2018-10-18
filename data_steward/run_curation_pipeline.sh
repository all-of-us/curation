#!/usr/bin/env bash

# This script automates the curation playbook (https://docs.google.com/document/d/1QnwUhJmrVc9JNt64goeRw-K7490gbaF7DsYhTTt9tUo/edit#heading=h.k24j7tgoprtn)
# It assuming steps 1-3 are already done via a daily cron job. This script automates 4-7.

today=$(date '+%Y%m%d')
echo "today --> $today"

USAGE="run_curation_pipeline.sh --key_file <Keyfile Path> --app_id <Application ID> [--bq_dataset <BigQuery EHR Dataset: default is prod_drc_dataset>]
[--vocabulary20180104 <BigQuery Vocab Dataset: default is vocabulary20180104>] [--dataset_prefix <prefix on output datasets: default empty>]
[--bq_rdr_dataset <RDR dataset>] [--result_bucket <Internal bucket>]"

dataset_prefix=""

while true; do
  case "$1" in
    --app_id) application_id=$2; shift 2;;
    --bq_dataset) bq_drc_dataset=$2; shift 2;;
    --bq_vocab_dataset) bq_vocab_dataset=$2; shift 2;;
    --key_file) key_file=$2; shift 2;;
    --bq_rdr_dataset) rdr_dataset=$2; shift 2;;
    --dataset_prefix) dataset_prefix=$2; shift 2;;
    --result_bucket) result_bucket=$2; shift 2;;
    -- ) shift; break ;;
    * ) break ;;
  esac
done

if [ -z "${key_file}" ] || [ -z "${application_id}" ]
then
  echo "Please specify the location of your GS Utils key path AND application ID. Usage: $USAGE"
  exit 1
fi

bq_drc_dataset=$([[ "${bq_drc_dataset}" ]] && echo "${bq_drc_dataset}" || echo "auto_pipeline_input")
application_id=$([[ "${application_id}" ]] && echo "${application_id}" || echo "aou-res-curation-test")
bq_vocab_dataset=$([[ "${bq_vocab_dataset}" ]] && echo "${bq_vocab_dataset}" || echo "vocabulary20180104")
bq_rdr_dataset=$([[ "${bq_rdr_dataset}" ]] && echo "${bq_rdr_dataset}" || echo "test_rdr")
result_bucket=$([[ "${result_bucket}" ]] && echo "${result_bucket}" || echo "drc_curation_internal_test")
current_dir=$(pwd)

echo "bq_drc_dataset --> ${bq_drc_dataset}"
echo "application_id --> ${application_id}"
echo "key_file --> ${key_file}"
echo "bq_rdr_dataaset --> ${bq_rdr_dataset}"
echo "bq_vocab_dataset --> ${bq_vocab_dataset}"
echo "result_bucket --> ${result_bucket}"
echo "dataset_prefix --> ${dataset_prefix}"

export GOOGLE_APPLICATION_CREDENTIALS="${key_file}"
export APPLICATION_ID="${application_id}"
#Each person needs to set this to the path of their own gcloud sdk
#path_to_gcloud_sdk="/Users/ksdkalluri/google-cloud-sdk/platform/google_appengine:${current_dir}/lib"
#export PYTHONPATH="$PATH:${path_to_gcloud_sdk}"

#Each person needs to set this to the path of their own gcloud sdk
#source tools/set_path.sh

#set application environment (ie dev, test, prod)
gcloud config set project $application_id


#---------Create curation virtual environment----------
set -e
# create a new environment in directory curation_env
virtualenv  -p `which python2.7` curation_env

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
ehr_dataset="${dataset_prefix}ehr${today}"
echo "ehr_dataset --> $ehr_dataset"

bq mk --dataset --description "snapshot prod_drc_dataset" ${application_id}:${ehr_dataset}

#copy tables
echo "tools/table_copy.sh --source_app_id ${application_id} --target_app_id ${application_id} --source_dataset ${bq_drc_dataset} --target_dataset ${ehr_dataset}"
tools/table_copy.sh --source_app_id ${application_id} --target_app_id ${application_id} --source_dataset ${bq_drc_dataset} --target_dataset ${ehr_dataset}


########################################################
#Take a Snapshot of Unioned EHR Submissions (step 5)
########################################################
echo "-------------------------->Take a Snapshot of Unioned EHR Submissions (step 5)"
rdr_dataset="${bq_rdr_dataset}"
source_prefix="unioned_ehr_"
unioned_ehr_dataset="${dataset_prefix}unioned_ehr${today}"

echo "bq mk --dataset --description "copy ehr_union from ${bq_drc_dataset}" ${application_id}:$unioned_ehr_dataset"
bq mk --dataset --description "copy ehr_union from ${bq_drc_dataset}" ${application_id}:$unioned_ehr_dataset

#Create the clinical tables for unioned EHR data set
echo "python cdm.py $unioned_ehr_dataset"
python cdm.py $unioned_ehr_dataset


#Copy OMOP vocabulary to unioned EHR data set
echo "python cdm.py --component vocabulary $unioned_ehr_dataset"
python cdm.py --component vocabulary $unioned_ehr_dataset
echo "tools/table_copy.sh --source_app_id ${application_id} --target_app_id ${application_id} --source_dataset ${bq_vocab_dataset} --target_dataset ${unioned_ehr_dataset}"
tools/table_copy.sh --source_app_id ${application_id} --target_app_id ${application_id} --source_dataset ${bq_vocab_dataset} --target_dataset ${unioned_ehr_dataset}

#copy unioned ehr clinical tables tables
echo "tools/table_copy.sh --source_app_id ${application_id} --target_app_id ${application_id} --source_dataset ${bq_drc_dataset} --source_prefix ${source_prefix} --target_dataset ${unioned_ehr_dataset}"
tools/table_copy.sh --source_app_id ${application_id} --target_app_id ${application_id} --source_dataset ${bq_drc_dataset} --source_prefix ${source_prefix} --target_dataset ${unioned_ehr_dataset}
#copy mapping tables tables
echo "tools/table_copy.sh --source_app_id ${application_id} --target_app_id ${application_id} --source_dataset ${bq_drc_dataset} --source_prefix _mapping_ --target_dataset ${unioned_ehr_dataset} --target_prefix _mapping_"
tools/table_copy.sh --source_app_id ${application_id} --target_app_id ${application_id} --source_dataset ${bq_drc_dataset} --source_prefix _mapping_ --target_dataset ${unioned_ehr_dataset} --target_prefix _mapping_

#Close virtual environment and remove
#deactivate

########################################################
#Combine RDR and Unioned EHR data (step 6)
########################################################
cdr="${dataset_prefix}combined${today}"
version="v0-2-rc3"

export RDR_DATASET_ID="${rdr_dataset}"
export UNIONED_DATASET_ID="${unioned_ehr_dataset}"
export EHR_RDR_DATASET_ID="${cdr}"
export BIGQUERY_DATASET_ID="${unioned_ehr_dataset}"

bq mk --dataset --description "${version} combine_ehr_rdr ${rdr_dataset} + ${unioned_ehr_dataset}" ${application_id}:${cdr}

#Create the clinical tables for unioned EHR data set
python cdm.py $cdr

#Copy OMOP vocabulary to CDR EHR data set
python cdm.py --component vocabulary ${cdr}
tools/table_copy.sh --source_app_id ${application_id} --target_app_id ${application_id} --source_dataset ${bq_vocab_dataset} --target_dataset ${cdr}

#Combine EHR and PPI data sets
python tools/combine_ehr_rdr.py

#Run Achilles
export BIGQUERY_DATASET_ID="${cdr}"
#export GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json
export BUCKET_NAME_NYC="test-bucket"

python tools/run_achilles_and_export.py --bucket=${result_bucket} --folder=${cdr}

#Close virtual environment and remove
deactivate

########################################################
# De-identify the combined dataset (step 7)
########################################################
version="v0-2-rc4"
cdr_deid="${cdr}_deid"

export BIGQUERY_DATASET_ID="${cdr_deid}"

bq mk --dataset --description "${version} deid ${cdr}" ${application_id}:${cdr_deid}

#Create the clinical tables for unioned EHR data set
python cdm.py $cdr_deid

#Copy OMOP vocabulary to CDR EHR data set
python cdm.py --component vocabulary ${cdr_deid}
tools/table_copy.sh --source_app_id ${application_id} --target_app_id ${application_id} --source_dataset ${bq_vocab_dataset} --target_dataset ${cdr_deid}

#The location and care_site tables must be deleted (due to this bug):
bq rm -f --table ${application_id}:${cdr_deid}.location
bq rm -f --table ${application_id}:${cdr_deid}.care_site

unset PYTHONPATH
cd deid

#NOTE: Create a copy of config.json

#------Create de-id virtual environment----------
set -e
# create a new environment in directory deid_env
virtualenv -p `which python2.7` deid_env

# activate it
source deid_env/bin/activate

# install the requirements in the virtualenv
pip install -r requirements.txt
#------------------------------------------------

cd src
set -e

python deid.py --i_dataset ${cdr} --table person --o_dataset ${cdr_deid} --config ../test_config.json --log
python deid.py --i_dataset ${cdr} --table visit_occurrence --o_dataset ${cdr_deid} --config ../test_config.json --log
python deid.py --i_dataset ${cdr} --table condition_occurrence --o_dataset ${cdr_deid} --config ../test_config.json --log
python deid.py --i_dataset ${cdr} --table drug_exposure --o_dataset ${cdr_deid} --config ../test_config.json --log
python deid.py --i_dataset ${cdr} --table procedure_occurrence --o_dataset ${cdr_deid} --config ../test_config.json --log
python deid.py --i_dataset ${cdr} --table device_exposure --o_dataset ${cdr_deid} --config ../test_config.json --log
python deid.py --i_dataset ${cdr} --table death --o_dataset ${cdr_deid} --config ../test_config.json --log
python deid.py --i_dataset ${cdr} --table measurement --o_dataset ${cdr_deid} --config ../test_config.json --log
python deid.py --i_dataset ${cdr} --table observation --o_dataset ${cdr_deid} --config ../test_config.json --log
python deid.py --i_dataset ${cdr} --table location --o_dataset ${cdr_deid} --config ../test_config.json --log
python deid.py --i_dataset ${cdr} --table care_site --o_dataset ${cdr_deid} --config ../test_config.json --log

#Close virtual environment and remove
deactivate

cd ../..
#Switch to curation virtual env
unset PYTHONPATH
source curation_env/bin/activate
#Run Achilles
source tools/set_path.sh
python tools/run_achilles_and_export.py --bucket=${result_bucket} --folder=${cdr_deid}

unset PYTHONPATH
deactivate
