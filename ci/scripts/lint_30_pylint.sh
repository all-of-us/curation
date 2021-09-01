#!/usr/bin/env bash

set -ex

PYLINT_RESULT=""
function pylintit {
  local res
  res=$(cd "${CIRCLE_WORKING_DIRECTORY}" && pylint -E data_steward tests)
  if [ -z "${res}" ];
  then
    return 0
  else
    PYLINT_RESULT=res
    return 1
  fi
}

if [[ $(pylintit) -ne 0 ]];
then
  echo "pylint errors"
  echo ""
  echo "${PYLINT_RESULT}"
  exit 1
else
  exit 0
fi