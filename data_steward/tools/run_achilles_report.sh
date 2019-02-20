#!/usr/bin/env bash

dataset="test_rdr"
result_bucket="drc_curation_internal_test"
vocab_dataset="vocabulary20180104"

USAGE="
Usage: run_achilles_report.sh
  --key_file <path to key file>
  --app_id <application id>
  [--dataset <Dataset ID: default is ${dataset}>]
  [--result_bucket <Internal bucket: default is ${result_bucket}>]
  [--vocab_dataset <vocab dataset: default is ${vocab_dataset}>]
"

while true; do
  case "$1" in
    --app_id) app_id=$2; shift 2;;
    --key_file) key_file=$2; shift 2;;
    --dataset) dataset=$2; shift 2;;
    --vocab_dataset) vocab_dataset=$2; shift 2;;
    --result_bucket) result_bucket=$2; shift 2;;
    -- ) shift; break ;;
    * ) break ;;
  esac
done

if [ -z "${key_file}" ] || [ -z "${app_id}" ]
then
  echo "Specify the key file location and application ID. $USAGE"
  exit 1
fi

echo "app_id --> ${app_id}"
echo "key_file --> ${key_file}"
echo "dataset --> ${dataset}"
echo "vocab_dataset --> ${vocab_dataset}"
echo "result_bucket --> ${result_bucket}"


export GOOGLE_APPLICATION_CREDENTIALS="${key_file}"
export APPLICATION_ID="${app_id}"

#set application environment (ie dev, test, prod)
gcloud auth activate-service-account --key-file=${key_file}
gcloud config set project ${app_id}


#---------Create curation virtual environment----------
set -e

cd ../../
# create a new environment in directory curation_env
virtualenv  -p $(which python2.7) curation_env


# activate it
source curation_env/bin/activate

# install the requirements in the virtualenv
cd data_steward
pip install -t lib -r requirements.txt

cd tools
source set_path.sh
#------------------------------------------------------

export BIGQUERY_DATASET_ID="${dataset}"
export BUCKET_NAME_NYC="test-bucket"

# copy vocabulary tables to the rdr dataset to run the achilles analysis.
./table_copy.sh --source_app_id ${app_id} --target_app_id ${app_id} --source_dataset ${vocab_dataset} --target_dataset ${dataset}

# Run Achilles analysis
python run_achilles_and_export.py --bucket=${result_bucket} --folder=${dataset}

unset PYTHONPATH
deactivate
