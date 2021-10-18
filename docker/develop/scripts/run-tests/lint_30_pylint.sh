#!/usr/bin/env bash

set +x

echo "Executing pylint..."

# we want to capture error(s), not bubble them up immediately
set +e
res=$(cd "${CIRCLE_WORKING_DIRECTORY}" && pylint -E data_steward tests)
set -e

if [ -z "${res}" ]; then
  exit 0
else
  echo "pylint errors found"
  echo "${res}"
  exit 1
fi