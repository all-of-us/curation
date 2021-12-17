#!/bin/bash -e

set -e

UNIT_NAME="unit"
INTEGRATION_NAME="integration"

BASE_DIR="$(git rev-parse --show-toplevel)"

export PYTHONPATH=:"${BASE_DIR}":"${BASE_DIR}/data_steward":"${BASE_DIR}/tests":${PYTHONPATH}

function usage() {
  echo "Usage: run_test.sh " \
    "[unit|integration]" \
    "[Set env CURATION_TESTS_FILEPATH for specific tests, usage in runner.py]" >&2
  exit 1
}

# these will be flipped to 1 when that command's name is seen
run_unit=0
run_integration=0

# this is used to track which command we're currently building flags for
current_cmd=""

run_args=("${@}")
unit_args=()
integration_args=()

echo "${run_args[@]}"

for i in "${run_args[@]}"; do
  case $i in
  help)
    usage
    exit 0
    ;;
  unit)
    if [ $run_unit -eq 1 ]; then
      echo "\"${UNIT_NAME}\" already specified once"
      exit 1
    fi
    run_unit=1
    current_cmd="${UNIT_NAME}"
    shift
    ;;
  integration)
    if [ $run_integration -eq 1 ]; then
      echo "\"${INTEGRATION_NAME}\" already specified once"
      exit 1
    fi
    run_integration=1
    current_cmd="${INTEGRATION_NAME}"
    shift
    ;;
  *)
    usage
    echo "Unknown option ${i}"
    echo "run_args=${run_args[*]}"
    exit 1
  esac
done

if [ "${run_unit}" -ne 1 ] && [ "${run_integration}" -ne 1 ]; then
  usage
  exit 0
fi

if [[ "${current_cmd}" == "${UNIT_NAME}" ]] || [[ "${current_cmd}" == "${INTEGRATION_NAME}" ]]; then
  script_args=("${BASE_DIR}/tests/runner.py"
    "--test-dir"
    "${BASE_DIR}/tests/${current_cmd}_tests"
    "--coverage-file"
    "${BASE_DIR}/.coveragerc_${current_cmd}")
fi

# determine if env var is set containing test filepaths
if [[ -n "${CURATION_TESTS_FILEPATH}" ]]; then
  script_args+=(
    "--test-paths-filepath"
    "${CURATION_TESTS_FILEPATH}")
fi

python "${script_args[@]}"
