#!/bin/bash
# Checks PR title in Circle CI to make sure it contains the Jira tag
# The Jira tag could be [DC-###] or [EDC-###]
# Since commits on develop are typically generated from the PR title in the Github UI,
# this check tries to ensure that the Jira tag is formatted correctly in the PR title
# However, the Jira title also needs to follow the Jira tag for commits on develop
# This script does not check for that yet due to Jira authentication

TICKET_REGEX="^\[(DC|EDQ)-[[:digit:]]+\][[:space:]]"
ERROR_MSG="The PR title below does not start with the Jira ticket tag, please rename."

pr_url="https://api.github.com/repos/${CIRCLE_PROJECT_USERNAME}/${CIRCLE_PROJECT_REPONAME}/pulls/${CIRCLE_PR_NUMBER}"
pr_title=$(curl -s "${pr_url}" | grep \<title\> | sed 's/[[:space:]]*<title>\([^·]*\).*/\1/')

if [[ ! $pr_title =~ $TICKET_REGEX ]];
  then
    echo "${ERROR_MSG}"
    echo "${pr_title}"
    exit 1
fi
