#!/usr/bin/env bash

# Fetch the most prevalent achilles heel errors in a dataset

all_hpo=
OUTPUT_FILENAME="heel_errors.csv"

USAGE="
Usage: top_heel_errors.sh
  --key_file <path to key file>
  --app_id <application id>
  --dataset_id <EHR dataset>
  [--all_hpo]
"
while true; do
  case "$1" in
  --app_id)
    app_id=$2
    shift 2
    ;;
  --dataset_id)
    dataset_id=$2
    shift 2
    ;;
  --key_file)
    key_file=$2
    shift 2
    ;;
  --all_hpo)
    all_hpo=1
    shift
    ;;
  --)
    shift
    break
    ;;
  *) break ;;
  esac
done

echo "dataset_id --> ${dataset_id}"
echo "app_id --> ${app_id}"
echo "key_file --> ${key_file}"
echo "all_hpo --> ${all_hpo}"

if [[ -z "${key_file}" ]] || [[ -z "${app_id}" ]] || [[ -z "${dataset_id}" ]]; then
  echo "Specify the key file location, Application ID and Dataset ID. $USAGE"
  exit 1
fi

export GOOGLE_APPLICATION_CREDENTIALS="${key_file}"
export GOOGLE_CLOUD_PROJECT="${app_id}"
export BIGQUERY_DATASET_ID="${dataset_id}"

#set application environment (ie dev, test, prod)
gcloud auth activate-service-account --key-file=${key_file}
gcloud config set project ${app_id}

#-------Set python path to add the modules and lib--------
set -e
BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && cd .. && pwd)"
cd ${BASE_DIR}
VIRTUAL_ENV=${BASE_DIR}/top_heel_errors_env
virtualenv -p $(which python2.7) ${VIRTUAL_ENV}
source tools/set_path.sh
set +e

# ---------activate venv-------------
source ${VIRTUAL_ENV}/bin/activate

#-------install the requirements in the virtualenv--------
pip install -t lib -r requirements.txt

#----------------Run the heel errors script------------------
ALL_HPO_OPT=
if [[ "${all_hpo}" -eq "1" ]]; then
  ALL_HPO_OPT="--all_hpo"
fi

cd tools
python top_heel_errors.py --app_id ${app_id} --dataset_id ${dataset_id} ${ALL_HPO_OPT} ${OUTPUT_FILENAME}

#----------cleanup-------------------
rm -rf ${VIRTUAL_ENV}
export PYTHONPATH=
deactivate
