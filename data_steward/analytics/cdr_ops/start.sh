#!/usr/bin/env bash

USAGE="start.sh --key_file <Path to service account key>"

while true; do
  case "$1" in
    --key_file) KEY_FILE=$2; shift 2;;
    --app_id) APP_ID=$2; shift 2;;
    -- ) shift; break ;;
    * ) break ;;
  esac
done

if [[ -z "${KEY_FILE}" ]]
then
  echo "Specify key file location. Usage: $USAGE"
  exit 1
fi

VENV_NAME='cdr_ops_env'
VENV_PATH="${HOME}/${VENV_NAME}"

BIN_PATH="${VENV_PATH}/bin"
if test -d "${VENV_PATH}/Scripts"
then
    # Windows
    BIN_PATH="${VENV_PATH}/Scripts"
fi

export GOOGLE_APPLICATION_CREDENTIALS="${KEY_FILE}"
PROJECT_ID=$(cat ${KEY_FILE} | python -c 'import json,sys;obj=json.load(sys.stdin);print(obj["project_id"]);')

gcloud config set project "${PROJECT_ID}"

python -m virtualenv ${VENV_PATH}

source "${BIN_PATH}/activate"

python -m pip install -U pip
python -m pip install -U -r requirements.txt
jupyter notebook
