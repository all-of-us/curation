#!/usr/bin/env bash

set -ex

#style_fyle="${CIRCLE_WORKING_DIRECTORY}"/.style.yapf

YAPF_RESULT=""
function yapfit {
  local res
  res=$(cd "${CIRCLE_WORKING_DIRECTORY}" && yapf -drp .)
  if [ -z "${res}" ];
  then
    return 0
  else
    YAPF_RESULT=res
    return 1
  fi
}

if [[ $(yapfit) -ne 0 ]] ;
then
  echo "yapf errors found"
  echo
  echo "${YAPF_RESULT}"
  exit 1
else
  exit 0
fi