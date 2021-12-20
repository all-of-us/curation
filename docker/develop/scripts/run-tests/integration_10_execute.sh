#!/usr/bin/env bash

# this script is slightly more complex than the unit test script
# as there isn't a great way to terminate a circle-ci job early

set -e
set +x

source "${CURATION_SCRIPTS_DIR}/funcs.sh"

# these are all the branches that should automatically trigger an integration test run
integration_run_branches=("develop" "master" "main")

# default all to "false"
run_forced=0
in_dev_branch=0
is_pr=0
is_manually_run=0

# test for a truthy state
if [[ -n "${FORCE_RUN_INTEGRATION}" ]] && [[ "${FORCE_RUN_INTEGRATION}" == "1" ]]; then
  echo "Integration test run forced"
  run_forced=1
elif in_ci; then
  # shellcheck disable=SC2076
  if [[ "${integration_run_branches[*]}" =~ "${CIRCLE_BRANCH}" ]]; then
    echo "Branch ${CIRCLE_BRANCH} is one of ${integration_run_branches[*]}"
    in_dev_branch=1
  elif [[ -n "${CIRCLE_PULL_REQUEST}" ]] || [[ -n "${CIRCLE_PULL_REQUESTS}" ]]; then
    echo "Running as part of a pull request:"
    echo "  CIRCLE_PULL_REQUESTS=${CIRCLE_PULL_REQUESTS}"
    echo "  CIRCLE_PULL_REQUEST=${CIRCLE_PULL_REQUEST}"
    is_pr=1
  elif [[ "${GIT_LAST_LOG}" == *"all tests"* ]]; then
    echo "git commit message contains string \"all tests\""
    is_manually_run=1
  fi
fi

# should we run?
if [[ $run_forced -eq 1 ]] || [[ $in_dev_branch -eq 1 ]] || [[ $is_pr -eq 1 ]] || [[ $is_manually_run -eq 1 ]]; then
  echo "Running integration tests..."

  # bootstrap buckets & datasets
  python "${CIRCLE_WORKING_DIRECTORY}/data_steward/ci/test_setup.py"

  # build test run arg list
  run_args=(
    "${CIRCLE_WORKING_DIRECTORY}/tests/runner.py"
    "--test-dir"
    "${CIRCLE_WORKING_DIRECTORY}/tests/integration_tests"
    "--coverage-file"
    "${CIRCLE_WORKING_DIRECTORY}/.coveragerc_integration"
  )

  script_args=("$@")

  # determine if we need to limit to a specific pattern
  if [ "$#" -gt 1 ]; then
    run_args+=("--test-pattern")
    for i in "${script_args[@]:1}"; do
      run_args+=("${i}")
    done
  fi

  # determine if env var is set containing test filepaths
  if [[ -n "${CURATION_TESTS_FILEPATH}" ]]; then
    run_args+=(
      "--test-paths-filepath"
      "${CURATION_TESTS_FILEPATH}")
  fi

  # execute tests
  set +x
  python "${run_args[@]}"
  set -x
else
  echo "Skipping integration tests"
fi
