#!/usr/bin/env bash
report_for="results"
USAGE="
Usage: tools/consolidated_reports/run_consolidated_report.sh
--key_file <path to key file>
--app_id <application id>
--bucket_name <bucket name>
--dataset <dataset id>
--report_for <name of reports, choose b/w results and achilles: default is ${report_for} >"
while true; do
  case "$1" in
    --key_file) key_file=$2; shift 2;;
    --app_id) app_id=$2; shift 2;;
    --bucket_name) bucket_name=$2; shift 2;;
    --dataset) dataset=$2; shift 2;;
    --report_for) report_for=$2; shift 2;;
    -- ) shift; break ;;
    * ) break ;;
  esac
done

if [[ -z "${key_file}" ]] || [[ -z "${app_id}" ]] || [[ -z "${bucket_name}" ]] || [[ -z "${dataset}" ]] || [[ -z "${report_for}" ]]
then
  echo "Specify the key file location, application_id, bucket_name, dataset and report selection. $USAGE"
  exit 1
fi

echo "key_file --> ${key_file}"
echo "app_id --> ${app_id}"
echo "bucket_name --> ${bucket_name}"
echo "dataset --> ${dataset}"
echo "report_for --> ${report_for}"


export GOOGLE_APPLICATION_CREDENTIALS="${key_file}"
export APPLICATION_ID="${app_id}"
export DRC_BUCKET_NAME="${bucket_name}"
export BIGQUERY_DATASET_ID="${dataset}"

VENV_BIN="bin"
if [[ "$OSTYPE" == "msys" ]]; then
    VENV_BIN="Scripts"
fi

gcloud auth activate-service-account --key-file=${key_file}
gcloud config set project ${app_id}

REPORT="get_all_results_html"
if [[ "$report_for" == "achilles" ]];
then
    REPORT="get_all_achilles_reports"
fi

set -e
# create a new environment in directory curation_venv
virtualenv curation_venv

# activate the report_env virtual environment
source "curation_venv/${VENV_BIN}/activate"

# install the requirements in the virtualenv
pip install -t lib -r requirements.txt

# Add the google appengine sdk to the PYTHONPATH
source tools/set_path.sh

#Copy the curation report directory from resources to consolidated achilles report
cp -R resources/curation_report/  tools/consolidated_reports/curation_report/

cd tools/consolidated_reports/

# Run Query, Gets latest submissions and downloads the curation reports
python ${REPORT}.py

# Unset the PYTHONPATH set during the venv installation
unset PYTHONPATH

#deacticate virtual environment
deactivate

#exit here if reports are for results.html
if [[ "$report_for" == "results" ]]; then
    exit
fi

cd curation_report

#run server.py to serve the curation report locally
python server.py