#!/usr/bin/env bash

# don't suppress errors
set -e

# store test results in junit format to allow CircleCI Test Summary reporting
#  https://circleci.com/docs/2.0/collect-test-data/
mkdir -p tests/results/coverage/unit/xml \
  tests/results/coverage/unit/html \
  tests/results/junit/unit
