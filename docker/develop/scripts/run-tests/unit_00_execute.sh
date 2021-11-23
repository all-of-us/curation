#!/usr/bin/env bash

set -e
set +x

# build test run arg list
run_args=(
  "${CIRCLE_WORKING_DIRECTORY}/tests/runner.py"
  "--test-path"
  "${CIRCLE_WORKING_DIRECTORY}/tests/unit_tests"
  "--coverage-file"
  "${CIRCLE_WORKING_DIRECTORY}/.coveragerc_unit"
)

script_args=("$@")

for v in "${script_args[@]}"; do
  run_args+=("${v}")
done

# execute unit tests
exec python "${run_args[@]}"