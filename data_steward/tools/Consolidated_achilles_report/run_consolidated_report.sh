#!/usr/bin/env bash

USAGE="
Usage: run_consolidated_report.sh
--key_file <path to key file>"

while true; do
  case "$1" in
    --key_file) key_file=$2; shift 2;;
    -- ) shift; break ;;
    * ) break ;;
  esac
done

if [ -z "${key_file}" ]
then
  echo "Specify the key file location. $USAGE"
  exit 1
fi

echo "key_file --> ${key_file}"

export GOOGLE_APPLICATION_CREDENTIALS="${key_file}"
#export APPLICATION_ID="${app_id}"

gcloud auth activate-service-account --key-file=${key_file}
#gcloud config set project ${app_id}

cd ../..

cp -R resources/curation_report/  /tools/Consolidated_achilles_report/curation_report/

cd tools/Consolidated_achilles_report/
#---------Create reports virtual environment----------
set -e
# create a new environment in directory curation_env
virtualenv  -p $(which python2.7) report_env

# activate it
source report_env/bin/activate

# install the requirements in the virtualenv
pip install -r requirements.txt

report_en="/Users/ksdkalluri/curation/data_steward/tools/Consolidated_achilles_report/report_env"
#Exporting path of the created virtual environment
export PYTHONPATH=$PYTHONPATH:${report_en}

# Run Query, Gets latest submissions and downloads the curation reports
python main.py

#change the directory to curation_report
cd curation_report

#run server.py to serve the curation report locally
python server.py

#deacticate virtual environment
deactivate


