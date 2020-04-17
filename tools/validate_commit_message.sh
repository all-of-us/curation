#!/bin/bash
# Validates that all commits have an associated [DC-###] or [EDQ-###] Jira
# ticket tag in the beginning of the commit message, fails if any don't
# For commits on develop, also ensure that the Jira title follows the Jira tag

TICKET_REGEX="\[(DC|EDQ)-[[:digit:]]+\][[:space:]]"
ERROR_MSG="The commit message below does not start with the Jira ticket tag, please rename."

set -x

CURRENT_REV=$(git rev-parse HEAD)
BRANCH_ROOT_REV=$(diff --old-line-format='' --new-line-format='' <(git rev-list --first-parent "${1:-develop}") <(git rev-list --first-parent "${2:-HEAD}") | head -1)

for rev in $(git rev-list "${CURRENT_REV}"..."${BRANCH_ROOT_REV}");
  do
  msg=$(git cat-file commit "${rev}" | sed '1,/^$/d')
  if [[ ! $msg =~ $TICKET_REGEX ]];
  then
    echo "${ERROR_MSG}"
    echo "${msg}"
    exit 1
  fi
done
