#!/usr/bin/env bash

# this script is slightly more complex than the unit test script
# as there isn't a great way to terminate a circle-ci job early

set -e
set +x

function is_forced_run {
  if [[ -n "${FORCE_RUN_INTEGRATION}" ]] && [[ "${FORCE_RUN_INTEGRATION}" == "1" ]];
  then
    echo "FORCE_RUN_INTEGRATION=${FORCE_RUN_INTEGRATION}"
    echo "Integration run forced"
    return 0
  else
    return 1
  fi
}

function is_dev_branch {
  if [[ "${CIRCLE_BRANCH}" == "develop" ]] || [[ "${CIRCLE_BRANCH}" == "master" ]] \
    || [[ "${CIRCLE_BRANCH}" == "main" ]];
  then
    echo "CIRCLE_BRANCH=${CIRCLE_BRANCH}"
    echo "We're in branch master or develop"
    return 0
  else
    return 1
  fi
}

function is_circle_pr {
  if [[ -n "${CIRCLE_PULL_REQUEST}" ]] || [[ -n "${CIRCLE_PULL_REQUESTS}" ]];
  then
    echo "CIRCLE_PULL_REQUEST=${CIRCLE_PULL_REQUEST}"
    echo "CIRCLE_PULL_REQUESTS=${CIRCLE_PULL_REQUESTS}"
    echo "We are in a Circle-CI pull request"
    return 0
  else
    return 1
  fi
}

function is_manually_run {
  if [[ "${GIT_LAST_LOG}" == *"all tests"* ]];
  then
    echo "GIT_LAST_LOG=${GIT_LAST_LOG}"
    echo "GIT_LAST_LOG contains \"all tests\""
    return 0
  else
    return 1
  fi
}

function should_run {
  if is_forced_run -eq 0 || is_dev_branch -eq 0  ||  is_circle_pr -eq 0  \
    || is_manually_run -eq 0 ;
  then
    return 0
  else
    return 1
  fi
}

if should_run ;
then
  # todo: this inline PYTHONPATH modification is silly.
  cd "${CIRCLE_WORKING_DIRECTORY}"/data_steward \
    && PYTHONPATH=./:$PYTHONPATH python ./ci/test_setup.py

  cd "${CIRCLE_WORKING_DIRECTORY}" \
    && ./tests/run_tests.sh -s integration
else
  echo "Skipping integration tests"
fi