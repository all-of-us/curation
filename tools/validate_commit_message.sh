#!/bin/bash
# Validates that single commit branches have an associated [DC-###] or [EDQ-###] Jira
# ticket tag in the beginning of the commit message, fails they don't

TICKET_REGEX="^\[(DC|EDQ)-[[:digit:]]+\][[:space:]]"
ERROR_MSG="Jira tag is missing or incorrectly formatted in the commit message below.
Please rename so it is formatted as '[DC-###] commit msg' or '[EDQ-###] commit msg'."

revs=$(git rev-list origin/develop...HEAD)

# check if single commit on branch
if [[ $(echo "${revs}" | wc -l ) -eq 1 ]]
  then
  msg=$(git cat-file commit "${revs}" | sed '1,/^$/d')
  if [[ ! $msg =~ $TICKET_REGEX ]];
  then
    echo "${ERROR_MSG}"
    echo "${msg}"
    exit 1
  else
    echo "Success! Commit message contains well formatted Jira tag"
  fi
fi
