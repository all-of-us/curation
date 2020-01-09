#!/bin/bash
# This script is used by the CircleCI config file to combine coverage results.

# Enable command printing
set -x

mkdir -p tests/results/coverage/all/xml

pushd tests/results/coverage

# This should happen on pull requests
if [[ -f integration/.coverage ]] && [[ -f unit/.coverage ]];
then
    echo "Combining integration and unit test results."
    cp integration/.coverage all/.coverage.integration
    cp unit/.coverage all/.coverage.unit
    pushd all
    coverage combine .coverage.integration .coverage.unit
    coverage html -d html --title "Curation Python Test Coverage Report - ALL"
    coverage xml -o xml/coverage.xml
    popd
fi

# This will happen for commits, but not pull requests
if [[ ! -f integration/.coverage ]] && [[ -f unit/.coverage ]];
then
    echo "Providing unit test results only.  Integration tests coverage file not found."
    cp unit/.coverage all/.coverage.unit
    pushd all
    coverage combine .coverage.unit
    coverage html -d html --title "Curation Python Test Coverage Report - UNIT"
    coverage xml -o xml/coverage.xml
    popd
fi

# This would be unusual and probably should not happen unless a failure has occurred
if [[ -f integration/.coverage ]] && [[ ! -f unit/.coverage ]];
then
    echo "Providing integration test results only.  Unit tests coverage file not found."
    cp integration/.coverage all/.coverage.integration
    pushd all
    coverage combine .coverage.integration
    coverage html -d html --title "Curation Python Test Coverage Report - INTEGRATION"
    coverage xml -o xml/coverage.xml
    popd
fi

# Likely a test failure occurred
if [[ ! -f integration/.coverage ]] && [[ ! -f unit/.coverage ]];
then
    echo "No test coverage data available.  Did a test failure occur?"
fi

# End where we started
popd

# Disable command printing
set +x
