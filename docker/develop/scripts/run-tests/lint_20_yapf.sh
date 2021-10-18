#!/usr/bin/env bash

set +x

echo "Executing yapf diff..."

# we want to capture error(s), not bubble them up immediately
set +e
res=$(cd "${CIRCLE_WORKING_DIRECTORY}" && yapf -drp .)
set -e

if [ -z "${res}" ]; then
  exit 0
else
  echo "yapf errors found"
  echo "${res}"
  exit 1
fi