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

# determine if env var is set containing test filepaths
if [[ -n "${CURATION_TESTS_FILEPATH}" ]]; then
  echo "Using test filepath ${CURATION_TESTS_FILEPATH}"
  run_args+=(
    "--test-paths-filepath"
    "${CURATION_TESTS_FILEPATH}")
fi

script_args=("$@")

for v in "${script_args[@]}"; do
  run_args+=("${v}")
done

# execute unit tests
exec python "${run_args[@]}"