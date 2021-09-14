#!/usr/bin/env bash

# this script is slightly more complex than the unit test script
# as there isn't a great way to terminate a circle-ci job early

set -e
set +x

function is_forced_run() {
  if [[ -n "${FORCE_RUN_INTEGRATION}" ]] && [[ "${FORCE_RUN_INTEGRATION}" == "1" ]]; then
    return 0
  else
    return 1
  fi
}

function is_dev_branch() {
  if ! in_ci; then
    return 1
  fi

  local branch
  branch="${CIRCLE_BRANCH}"
  if [[ "${branch}" == "develop" ]] || [[ "${branch}" == "master" ]] ||
    [[ "${branch}" == "main" ]]; then
    return 0
  else
    return 1
  fi

}

function is_circle_pr() {
  if ! in_ci; then
    return 1
  fi

  if [[ -n "${CIRCLE_PULL_REQUEST}" ]] || [[ -n "${CIRCLE_PULL_REQUESTS}" ]]; then
    return 0
  else
    return 1
  fi
}

function is_manually_run() {
  if [[ "${GIT_LAST_LOG}" == *"all tests"* ]]; then
    return 0
  else
    return 1
  fi
}

is_forced_run
forced=$?
is_dev_branch
dev_branch=$?
is_circle_pr
circle_pr=$?
is_manually_run
manual_run=$?

if [[ $forced -eq 0 ]]; then
  echo "FORCE_RUN_INTEGRATION is true"
  should_run=0
elif [[ $dev_branch -eq 0 ]]; then
  echo "We're in a dev branch"
  should_run=0
elif [[ $circle_pr -eq 0 ]]; then
  echo "We're in a PR in CircleCI"
  should_run=0
elif [[ $manual_run -eq 0 ]]; then
  echo "Last git commit message contains phrase\"all tests\""
  should_run=0
else
  should_run=1
fi

if [[ $should_run -eq 0 ]]; then
  echo "Running integration tests..."

  # todo: this inline PYTHONPATH modification is silly.
  cd "${CIRCLE_WORKING_DIRECTORY}"/data_steward &&
    PYTHONPATH=./:$PYTHONPATH python ./ci/test_setup.py

  cd "${CIRCLE_WORKING_DIRECTORY}" &&
    ./tests/run_tests.sh -s integration
else
  echo "Skipping integration tests"
fi
