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
  echo "Usage: $USAGE"
  exit 1
fi
NOTEBOOKS_DIR="$( cd "$(dirname "$0")" ; pwd -P )"
BASE_DIR="$( cd "${NOTEBOOKS_DIR}" && cd .. && pwd )"
export PYTHONPATH="${PYTHONPATH}:${NOTEBOOKS_DIR}"
export GOOGLE_APPLICATION_CREDENTIALS="${KEY_FILE}"

export APPLICATION_ID=$(cat ${KEY_FILE} | python -c 'import json,sys;obj=json.load(sys.stdin);print(obj["project_id"]);')
ACCOUNT=$(cat ${KEY_FILE} | python -c 'import json,sys;obj=json.load(sys.stdin);print(obj["client_email"]);')

echo "Activating service account ${ACCOUNT} for application ID ${APPLICATION_ID}..."

gcloud auth activate-service-account ${ACCOUNT} --key-file=${KEY_FILE}
gcloud config set project "${APPLICATION_ID}"

VENV_NAME='cdr_ops_env'
VENV_PATH="${HOME}/${VENV_NAME}"
WHICH_PYTHON=$(which python)
PYTHON_VERSION=$(${WHICH_PYTHON} --version 2>&1)

echo "Creating a ${PYTHON_VERSION} virtual environment in ${VENV_PATH}..."

BIN_PATH="${VENV_PATH}/bin"
if test -d "${VENV_PATH}/Scripts"
then
    # Windows
    BIN_PATH="${VENV_PATH}/Scripts"
fi

virtualenv --python=$(which python) ${VENV_PATH}
source "${BIN_PATH}/activate"
echo "Which python: $(which python)"
python -m pip install -U pip
python -m pip install -U -r requirements.txt
jupyter notebook
