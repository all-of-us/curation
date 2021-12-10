#!/usr/bin/env bash

# don't suppress errors
set -e

# If env var is set containing test filepaths, include it and update paths to mount location
if [[ -n "${CURATION_TESTS_FILEPATH}" ]]; then
  sed -i 's/.*curation/\./g' "${CURATION_TESTS_FILEPATH}"
fi

# store test results in junit format to allow CircleCI Test Summary reporting
#  https://circleci.com/docs/2.0/collect-test-data/
mkdir -p tests/results/coverage/unit/xml \
  tests/results/coverage/unit/html \
  tests/results/junit/unit
