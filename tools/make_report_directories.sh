#!/bin/bash
# make coverage report direcgtories for unit and integration tests.

mkdir -p tests/results/coverage/$1/xml 
mkdir -p tests/results/coverage/$1/html 
# store test results in junit format to allow CircleCI Test Summary reporting
#  https://circleci.com/docs/2.0/collect-test-data/
mkdir -p tests/results/junit/$1
