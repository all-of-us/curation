#!/usr/bin/env bash

# This script automates the curation playbook (https://docs.google.com/document/d/1QnwUhJmrVc9JNt64goeRw-K7490gbaF7DsYhTTt9tUo/edit#heading=h.k24j7tgoprtn)
# It assuming steps 1-3 are already done via a daily cron job. This script automates 4-7.

today=$(date '+%Y%m%d')
echo "today --> $today"

USAGE="run_curation_pipeline.sh --gsutil_key <Path to Google Keyfile Path> --app_id <Application ID> [--bq_dataset <BigQuery EHR Dataset: default is prod_drc_dataset>] [--vocabulary20180104 <BigQuery Vocab Dataset: default is vocabulary20180104>] "

while true; do
  case "$1" in
    --app_id) application_id=$2; shift 2;;
    --bq_dataset) bq_drc_dataset=$2; shift 2;;
    --bq_vocab_dataset) bq_vocab_dataset=$2; shift 2;;
    --gsutil_key) gsutil_key=$2; shift 2;;
    -- ) shift; break ;;
    * ) break ;;
  esac
done

if [ -z "${gsutil_key}" ] || [ -z "${application_id}" ]
then
  echo "Please specify the location of your GS Utils key path AND application ID. Usage: $USAGE"
  exit 1
fi

bq_drc_dataset=$([[ "${bq_drc_dataset}" ]] && echo "${bq_drc_dataset}" || echo "prod_drc_dataset")
application_id=$([[ "${application_id}" ]] && echo "${application_id}" || echo "aou-res-curation-test")
bq_vocab_dataset=$([[ "${bq_vocab_dataset}" ]] && echo "${bq_vocab_dataset}" || echo "vocabulary20180104")
current_dir=$(pwd)

echo "bq_drc_dataset --> ${bq_drc_dataset}"
echo "application_id --> ${application_id}"
echo "gsutil_key --> ${gsutil_key}"
echo "bq_vocab_dataset --> ${bq_vocab_dataset}"

export GOOGLE_APPLICATION_CREDENTIALS="${gsutil_key}"
export APPLICATION_ID="${application_id}"
#Each person needs to set this to the path of their own gcloud sdk
path_to_gcloud_sdk="~/Dev/google-cloud-sdk/platform/google_appengine:${current_dir}/lib"
export PYTHONPATH="$PATH:${path_to_gcloud_sdk}"

#set application environment (ie dev, test, prod)
gcloud config set project $application_id


#---------Create curation virtual environment----------
set -e
# create a new environment in directory curation_env
virtualenv curation_env

# activate it
source curation_env/bin/activate

# install the requirements in the virtualenv
pip install -t lib -r requirements.txt
#------------------------------------------------------

########################################################
#Take a Snapshot of EHR Dataset (step 4)
########################################################
echo "-------------------------->Take a Snapshot of EHR Dataset (step 4)"
ehr_dataset="ehr$today"
echo "ehr_dataset --> $ehr_dataset"

bq mk --dataset --description "snapshot prod_drc_dataset" ${application_id}:${ehr_dataset}

#copy tables
echo "tools/table_copy.sh --source_app_id ${application_id} --target_app_id ${application_id} --source_dataset ${bq_drc_dataset} --target_dataset ${ehr_dataset}"
tools/table_copy.sh --source_app_id ${application_id} --target_app_id ${application_id} --source_dataset ${bq_drc_dataset} --target_dataset ${ehr_dataset}


########################################################
#Take a Snapshot of Unioned EHR Submissions (step 5)
########################################################
echo "-------------------------->Take a Snapshot of Unioned EHR Submissions (step 5)"
rdr_dataset="rdr$today"
source_prefix="unioned_ehr_"
unioned_ehr_dataset="unioned_$ehr_dataset"

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
deactivate

########################################################
#Combine RDR and Unioned EHR data (step 6)
########################################################
cdr="combined$today"
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

python run_achilles_and_export.py --bucket=drc-curation-internal --folder=${cdr}

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
bq rm --table ${application_id}:${cdr_deid}.location
bq rm --table ${application_id}:${cdr_deid}.care_site

cd deid

#NOTE: Create a copy of config.json

#------Create de-id virtual environment----------
set -e
# create a new environment in directory deid_env
virtualenv deid_env

# activate it
source deid_env/bin/activate

# install the requirements in the virtualenv
pip install -r requirements.txt
#------------------------------------------------

cd src
set -e

source deid_env/bin/activate

python deid.py --i_dataset ${cdr} --table person --o_dataset ${cdr_deid} --config ../prod_config.json --log
python deid.py --i_dataset ${cdr} --table visit_occurrence --o_dataset ${cdr_deid} --config ../prod_config.json --log
python deid.py --i_dataset ${cdr} --table condition_occurrence --o_dataset ${cdr_deid} --config ../prod_config.json --log
python deid.py --i_dataset ${cdr} --table drug_exposure --o_dataset ${cdr_deid} --config ../prod_config.json --log
python deid.py --i_dataset ${cdr} --table procedure_occurrence --o_dataset ${cdr_deid} --config ../prod_config.json --log
python deid.py --i_dataset ${cdr} --table device_exposure --o_dataset ${cdr_deid} --config ../prod_config.json --log
python deid.py --i_dataset ${cdr} --table death --o_dataset ${cdr_deid} --config ../prod_config.json --log
python deid.py --i_dataset ${cdr} --table measurement --o_dataset ${cdr_deid} --config ../prod_config.json --log
python deid.py --i_dataset ${cdr} --table observation --o_dataset ${cdr_deid} --config ../prod_config.json --log
python deid.py --i_dataset ${cdr} --table location --o_dataset ${cdr_deid} --config ../prod_config.json --log
python deid.py --i_dataset ${cdr} --table care_site --o_dataset ${cdr_deid} --config ../prod_config.json --log

#Close virtual environment and remove
deactivate

#Switch to curation virtual env
source curation_env/bin/activate
#Run Achilles
python run_achilles_and_export.py --bucket=drc-curation-internal --folder=${cdr_deid}

deactivate
