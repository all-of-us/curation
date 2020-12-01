#!/bin/bash

set -x

USAGE="
Usage: automate_all_tests.sh
  --key_file <path to key file>
  --branch <branch to checkout and build>
  --env_path <path to GCE compatible environment directory>
"

while true; do
  case "$1" in
  --branch)
    BRANCH=$2
    shift 2
    ;;
  --key_file)
    KEY_FILE=$2
    shift 2
    ;;
  --env_path)
    ENV_PATH=$2
    shift 2
    ;;
  --)
    shift
    break
    ;;
  *) break ;;
  esac
done

if [[ -z "${BRANCH}" ]] || [[ -z "${KEY_FILE}" ]] || [[ -z "${ENV_PATH}" ]]; then
  echo "${USAGE}"
  exit 1
fi

ROOT_DIR=$(git rev-parse --show-toplevel)
DATA_STEWARD_DIR="${ROOT_DIR}/data_steward"
DEID_DIR="${DATA_STEWARD_DIR}/deid"
export GH_USERNAME=$(whoami)

LOG_FILE="${ROOT_DIR}/logs/$(date '+%Y_%m_%d')_${BRANCH}.log"
echo "Redirecting output streams to log file: ${LOG_FILE}"
exec 1>>${LOG_FILE}
exec 2>>${LOG_FILE}

source "${ENV_PATH}/bin/activate"
python --version

# get repository updates
git fetch origin
git checkout ${BRANCH}
git merge origin/${BRANCH}

# ensure most recent dependencies are installed
if [[ ${GH_USERNAME} == "curapp" ]];
then
    proxy="--proxy http://10.0.0.40.3128"
    if [[ $(python --version) == "Python 3.6.8" ]];
    then
        echo "Monkey patching to run tests and record coverage results."
        sed -i 's/hashlib.md5()/hashlib.md5(usedforsecurity=False)/g' "${ENV_PATH}/lib64/python3.6/site-packages/coverage/misc.py"
    fi
else
    proxy=""

fi

pip ${proxy} install --upgrade pip
pip ${proxy} install -r "${DATA_STEWARD_DIR}/requirements.txt"
pip ${proxy} install -r "${DATA_STEWARD_DIR}/dev_requirements.txt"
pip ${proxy} install -r "${DEID_DIR}/requirements.txt"
PYTHONPATH=$ROOT_DIR:$DATA_STEWARD_DIR:$PYTHONPATH

# setting required credentials
export GOOGLE_CLOUD_PROJECT=aou-res-curation-test
export APPLICATION_ID=aou-res-curation-test
export GOOGLE_APPLICATION_CREDENTIALS="${KEY_FILE}"
gcloud auth activate-service-account --key-file="${KEY_FILE}"
gcloud config set project aou-res-curation-test
gcloud config list
echo $GOOGLE_APPLICATION_CREDENTIALS

# environment, bucket, and dataset setup
source "${DATA_STEWARD_DIR}/init_env.sh"

# have to reset here because init_env.sh overrides the previous setting
export GOOGLE_APPLICATION_CREDENTIALS="${KEY_FILE}"
PYTHONPATH=$PYTHONPATH python "${DATA_STEWARD_DIR}/ci/setup.py"

echo "Running unit tests"
source "${ROOT_DIR}/tests/run_tests.sh" -s unit

echo "Running integration tests"
source "${ROOT_DIR}/tests/run_tests.sh" -s integration

set +x
