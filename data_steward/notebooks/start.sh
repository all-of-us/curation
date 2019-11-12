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

# Determine separator to use for directories and PYTHONPATH
# only tested on Git Bash for Windows
SEP=':'
if [[ "$OSTYPE" == "msys" ]]; then
  SEP=';'
fi

NOTEBOOKS_DIR="$( cd "$(dirname "$0")" ; pwd -P )"
BASE_DIR="$( cd "${NOTEBOOKS_DIR}" && cd .. && pwd )"
export PYTHONPATH="${PYTHONPATH}${SEP}${BASE_DIR}"
export GOOGLE_APPLICATION_CREDENTIALS="${KEY_FILE}"

export APPLICATION_ID=$(cat ${KEY_FILE} | python -c 'import json,sys;obj=json.load(sys.stdin);print(obj["project_id"]);')
export PROJECT_ID="${APPLICATION_ID}"

ACCOUNT=$(cat ${KEY_FILE} | python -c 'import json,sys;obj=json.load(sys.stdin);print(obj["client_email"]);')

echo "Activating service account ${ACCOUNT} for application ID ${APPLICATION_ID}..."

gcloud auth activate-service-account ${ACCOUNT} --key-file=${KEY_FILE}
gcloud config set project "${APPLICATION_ID}"

PYTHON_CMD='python2.7'
VENV_NAME='cdr_ops_env'
VENV_PATH="${HOME}/${VENV_NAME}"

echo "Creating virtual environment in ${VENV_PATH}..."

if ! type ${PYTHON_CMD} > /dev/null;
then
  PYTHON_CMD=$(which python)
  PYTHON_VERSION=$(${PYTHON_CMD} --version 2>&1)
  echo "Command ${PYTHON_CMD} was not found. Attempting to use system default (${PYTHON_VERSION}) which may NOT work..."
fi

BIN_PATH="${VENV_PATH}/bin"
if test -d "${VENV_PATH}/Scripts"
then
    # Windows
    BIN_PATH="${VENV_PATH}/Scripts"
fi

virtualenv --python=${PYTHON_CMD} ${VENV_PATH}
source "${BIN_PATH}/activate"
echo "Which python: $(which python)"
python -m pip install -U pip
python -m pip install -U -r "${BASE_DIR}/requirements.txt"
python -m pip install -U -r "${NOTEBOOKS_DIR}/requirements.txt"

# The path set to /c/path/to/file gets converted to C:\\c\\path\\to\\file
# instead of C:\\path\\to\\file, requiring the following fix for Git Bash for Windows
if [[ "$OSTYPE" == "msys" ]]; then
  for i in $(echo "${PYTHONPATH//;/ }")
  do
      PATH_VAR="$(echo "${i}" | tail -c +3)"
      export PYTHONPATH="${PYTHONPATH}${SEP}${PATH_VAR}"
      echo "Added path ${PATH_VAR}"
  done
fi

jupyter nbextension enable --py --sys-prefix qgrid
jupyter nbextension enable --py --sys-prefix widgetsnbextension
jupyter notebook --notebook-dir=${BASE_DIR} --config=${NOTEBOOKS_DIR}/jupyter_notebook_config.py
