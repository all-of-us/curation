#!/bin/bash
# Applies pylint -E to the entire curation repository. 
# Fails if there are any errors.

set +e

echo "-----------------------"
echo $PYTHONPATH
echo "-----------------------"
pushd $(git rev-parse --show-toplevel) > /dev/null
lint_out=$(pylint -E data_steward tests)
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
