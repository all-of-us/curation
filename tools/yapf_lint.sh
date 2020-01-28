#!/bin/sh
# Applies yapf to the entire curation repository. Fails if there are any
# formatting issues.

set +e

pushd $(git rev-parse --show-toplevel) > /dev/null
yapf_out=$(yapf -drp .)
if [ -z "$yapf_out" ]; then
  echo "Passed yapf linter!"
  exit 0
else
  echo "Full lint output:"
  echo "${yapf_out}"
  echo ""
	echo "Failed yapf linter, the following commands will autoformat all python files:"
	echo "yapf -rip ."
	exit 1
fi
