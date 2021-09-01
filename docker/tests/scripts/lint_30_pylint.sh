#!/usr/bin/env bash

set -e

echo "Executing pylint..."

function pylintit {
  set +e
  local res
  res=$(cd "${CIRCLE_WORKING_DIRECTORY}" && pylint -E data_steward tests)
  if [ -z "${res}" ];
  then
    set -e
    return 0
  else
    PYLINT_RESULT="${res}"
    set -e
    return 1
  fi
}

if ! pylintit ;
then
  echo "pylint errors"
  echo ""
  echo "${PYLINT_RESULT}"
  exit 1
else
  exit 0
fi