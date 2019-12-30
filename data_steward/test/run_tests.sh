#!/bin/bash -e


BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd .. && pwd )"
. ${BASE_DIR}/tools/set_path.sh

subset="all"


function usage() {
  echo "Usage: run_tests.sh " \
      "[-r <file name match glob, e.g. 'extraction_*'>]" >& 2
  exit 1
}

while getopts "s:g:h:r:" opt; do
  case $opt in
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

if [[ -z ${substring} ]]
then
  cmd="test/runner.py --test-path test/unit_test/ "
else
  cmd="test/runner.py --test-path test/unit_test/ --test-pattern $substring"
fi
(cd ${BASE_DIR}; python ${cmd})
