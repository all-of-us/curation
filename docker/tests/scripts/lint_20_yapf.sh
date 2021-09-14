#!/usr/bin/env bash

set -e

echo "Executing yapf diff..."

function yapfit() {
  set +e
  local res
  res=$(cd "${CIRCLE_WORKING_DIRECTORY}" && yapf -drp .)
  if [ -z "${res}" ]; then
    set -e
    return 0
  else
    YAPF_RESULT="${res}"
    set -e
    return 1
  fi
}

if ! yapfit; then
  echo "yapf errors found"
  echo
  echo "${YAPF_RESULT}"
  exit 1
else
  exit 0
fi
