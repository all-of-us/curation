#!/bin/bash -e

# print executed commands
set -x

BASE_DIR="$(git rev-parse --show-toplevel)"
. ${BASE_DIR}/data_steward/tools/set_path.sh

subset="all"


function usage() {
  echo "Usage: run_test.sh " \
      "[-s all|unit|integration]" \
      "[-r <file name match glob, e.g. 'extraction_*'>]" >& 2
  exit 1
}

while getopts "s:h:r:" opt; do
  case $opt in
    s)
      subset=$OPTARG
      ;;
    r)
      substring=$OPTARG
      ;;
    \?)
      echo "Invalid option: -$OPTARG" >&2
      exit 1
      ;;
    :)
      echo "Option -$OPTARG requires an argument." >&2
      exit 1
      ;;
    h|*)
      usage
      ;;
  esac
done

if [[ ${substring} ]];
then
   echo Executing tests that match glob ${substring}
fi

if [[ "$subset" == "unit" ]]
then
  path="tests/unit_tests/"
  coverage_file=".coveragerc_unit"
elif [[ "$subset" == "integration" ]]
then
  path="tests/integration_tests/"
  coverage_file=".coveragerc_integration"
else
  path="tests/"
  coverage_file=".coveragerc"
fi

if [[ -z ${substring} ]]
then
  cmd="tests/runner.py --test-path ${path} ${sdk_dir} --coverage-file ${coverage_file}"
else
  cmd="tests/runner.py --test-path ${path} ${sdk_dir} --test-pattern $substring --coverage-file ${coverage_file}"
fi

(cd ${BASE_DIR}; PYTHONPATH=./:./data_steward:${PYTHONPATH} python ${cmd})

# stop printing executed commands
set +x
