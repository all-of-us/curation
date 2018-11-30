#!/usr/bin/env bash

USAGE="
Usage: run_consolidated_report.sh
--key_file <path to key file>
--app_id <application id>
--bucket_name <bucket name>
--dataaset <daataset id>"
while true; do
  case "$1" in
    --key_file) key_file=$2; shift 2;;
    --app_id) app_id=$2; shift 2;;
    --bucket_name) bucket_name=$2; shift 2;;
    --dataset) dataset=$2; shift 2;;
    -- ) shift; break ;;
    * ) break ;;
  esac
done

if [ -z "${key_file}" ] || [ -z "${app_id}" ] || [ -z "${bucket_name}" ] || [ -z "${dataset}" ]
then
  echo "Specify the key file location, application_id, bucket_name and dataset. $USAGE"
  exit 1
fi

echo "key_file --> ${key_file}"
echo "app_id --> ${app_id}"
echo "bucket_name --> ${bucket_name}"
echo "dataset --> ${dataset}"


export GOOGLE_APPLICATION_CREDENTIALS="${key_file}"
export APPLICATION_ID="${app_id}"
export DRC_BUCKET_NAME="${bucket_name}"
export BIGQUERY_DATASET_ID="${dataset}"

gcloud auth activate-service-account --key-file=${key_file}
gcloud config set project ${app_id}

cd ../..

#Copy the curation report directory from resources to consolidated achilles report
cp -R resources/curation_report/  tools/consolidated_achilles_report/curation_report/

cd tools/Consolidated_achilles_report/

#---------Create reports virtual environment----------
set -e
# create a new environment in directory curation_env
virtualenv  -p $(which python2.7) report_env

# activate the report_env virtual environment
source report_env/bin/activate

# install the requirements in the virtualenv
pip install -t lib -r requirements.txt

# Add the google appengine sdk to the PYTHONPATH
source ../set_path.sh

# Run Query, Gets latest submissions and downloads the curation reports
python main.py

# Unset the PYTHONPATH set during the venv installation
unset PYTHONPATH

#deacticate virtual environment
deactivate

#change the directory to curation_report
cd curation_report

#run server.py to serve the curation report locally
python server.py

