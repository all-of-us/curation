#!/usr/bin/env bash

set -e
set +x

yapf_args=(
  "-rip"
  "."
)

echo "Executing \"yapf ${yapf_args[*]}\"..."

cd "${CIRCLE_WORKING_DIRECTORY}" \
  && yapf "${yapf_args[@]}"