#!/bin/bash
# Applies pylint -E to the entire curation repository. 
# Fails if there are any errors.

set +e

echo "-----------------------"
echo "${PYTHONPATH}"
echo "-----------------------"
pushd $(git rev-parse --show-toplevel) > /dev/null
lint_out=$(PYTHONPATH=./data_steward:./tests pylint -E data_steward tests)

echo "-----------------------"
echo "${lint_out}"
echo "-----------------------"

if [ -z "$lint_out" ]; then
  echo "Passed pylint linter!"
  exit 0
else
  echo "Full pylint output:"
  echo "${lint_out}"
  echo ""
	echo "Failed pylint linter.  Fix errors and commit changes."
	exit 1
fi
