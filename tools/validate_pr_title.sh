#!/bin/bash
# Checks PR title in Circle CI to make sure it contains the Jira tag
# The Jira tag could be [DC-###] or [EDC-###]
# Since commits on develop are typically generated from the PR title in the Github UI,
# this check tries to ensure that the Jira tag is formatted correctly in the PR title

TICKET_REGEX="^\[(DC|EDQ)-[[:digit:]]+\][[:space:]]"
ERROR_MSG="Jira tag is missing or incorrectly formatted in the PR title below.
Please rename so it is formatted as '[DC-###] PR title' or '[EDQ-###] PR title'."

if [[ -n "${CIRCLE_PULL_REQUEST}" ]];
  then
    pr_url="${CIRCLE_PULL_REQUEST}"
    pr_title=$(curl -s "${pr_url}" | grep \<title\> | sed 's/[[:space:]]*<title>\([^·]*\).*/\1/')

    if [[ ! $pr_title =~ $TICKET_REGEX ]];
      then
        echo "${ERROR_MSG}"
        echo "${pr_title}"
        exit 1
      else
        echo "Success! PR title contains well formatted Jira tag"
    fi
fi
