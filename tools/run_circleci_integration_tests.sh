#!/bin/bash
# Runs integration tests for PRs and master and develop branches only.  Will run
# integration tests for any commit if "all tests" is in the commit message.

text=$(git log -1 --pretty=%B)
# If this gets triggered on develop or master, let it run. Note
# however that on develop, this job is only triggered nightly.
# Always run on PRs and on any repo commits that include "all tests".
if [[ "${CIRCLE_BRANCH}" == "develop" ]] || [[ "${CIRCLE_BRANCH}" == "master" ]] || \
    [[ "$text" == *"all tests"* ]] || [[ -n "${CIRCLE_PULL_REQUEST}" ]] || [[ -n "${CIRCLE_PULL_REQUESTS}" ]];
then
    poetry run ./tests/run_tests.sh -s integration
else
    echo "Skipping integration tests"
fi
