#!/bin/bash -e

# print executed commands
set -x

BASE_DIR="$(git rev-parse --show-toplevel)"
. "${BASE_DIR}/data_steward/tools/set_path.sh"

subset="all"


function usage() {
  echo "Usage: run_test.sh " \
      "[-s unit|integration]" \
      "[-r <file name match glob, e.g. 'extraction_*'>]" >& 2
  exit 1
}

while true; do
  case "$1" in
    -s)
      subset=$2
      shift 2
      ;;
    -r)
      substring=$2
      shift 2
      ;;
    --)
      shift
      break
      ;;
    *) break ;;
  esac
done

if [[ ${substring} ]];
then
   echo Executing tests that match glob ${substring}
fi

if [[ "$subset" == "unit" ]]
then
  path="tests/unit_tests/"
  coverage_arg="--coverage-file .coveragerc_unit"
elif [[ "$subset" == "integration" ]]
then
  path="tests/integration_tests/"
  coverage_arg="--coverage-file .coveragerc_integration"
else
  echo "Please specify unit or integration tests"
  exit 1
fi

if [[ -n "${CURATION_TESTS_FILEPATH}" && -s "${CURATION_TESTS_FILEPATH}" ]]; then
  test_arg="--test-paths-filepath ${CURATION_TESTS_FILEPATH}"
else
  test_arg=""
fi

if [[ -z ${substring} ]]
then
  cmd="tests/runner.py --test-dir ${path}  ${coverage_arg} ${test_arg}"
else
  cmd="tests/runner.py --test-dir ${path} --test-pattern $substring ${coverage_arg} ${test_arg}"
fi

(cd "${BASE_DIR}"; PYTHONPATH=./:./data_steward:${PYTHONPATH} python ${cmd})

# stop printing executed commands
set +x
