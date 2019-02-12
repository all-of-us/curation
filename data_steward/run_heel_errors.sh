#!/usr/bin/env bash

report_for="hpo"

USAGE="
Usage: run_heel_errors.sh
  --key_file <path to key file>
  --app_id <application id>
  --dataset_id <EHR dataset>
  [--report_for <Report For: default is ${report_for}>]
"
while true; do
  case "$1" in
    --app_id) app_id=$2; shift 2;;
    --dataset_id) dataset_id=$2; shift 2;;
    --key_file) key_file=$2; shift 2;;
    --report_for) report_for=$2; shift 2;;
    -- ) shift; break ;;
    * ) break ;;
  esac
done


echo "dataset_id --> ${dataset_id}"
echo "app_id --> ${app_id}"
echo "key_file --> ${key_file}"
echo "report_for --> ${report_for}"

if [ -z "${key_file}" ] || [ -z "${app_id}" ] || [ -z "${dataset_id}" ]
then
  echo "Specify the key file location, Application ID and Dataset ID. $USAGE"
  exit 1
fi

echo "dataset_id --> ${dataset_id}"
echo "app_id --> ${app_id}"
echo "key_file --> ${key_file}"
echo "report_for --> ${report_for}"

export GOOGLE_APPLICATION_CREDENTIALS="${key_file}"
export APPLICATION_ID="${app_id}"
export BIGQUERY_DATASET_ID="${dataset_id}"

#set application environment (ie dev, test, prod)
gcloud auth activate-service-account --key-file=${key_file}
gcloud config set project ${app_id}

#---------Create curation virtual environment----------
set -e
# --------create a new environment------------
virtualenv  -p $(which python2.7) heel_env

# ---------activate venv-------------
source heel_env/bin/activate

#-------install the requirements in the virtualenv--------
pip install -t lib -r requirements.txt

#-------Set python path to add the modules and lib--------
source tools/set_path.sh

#----------------Run the heel errors script------------------
python tools/common_heel_errors.py ${report_for} ${dataset_id}

#-----------Deactivate the venv and unset the python path
deactivate
unset PYTHONPATH