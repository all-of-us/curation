#!/usr/bin/env bash

set -e
set +x

# determine user from environment
uid=$(id -u)
gid=$(id -g)
whoiis=$(whoami)

if [ -z "$1" ];
then
  echo At least one argument must be provided, and \
  it must be the name of a service within docker-compose.yml

  exit 1
fi

echo Running as user "${whoiis}" \("${uid}:${gid}"\)...

if [[ -z "${CI}" ]] || [[ "${CI}" != "true" ]];
then
  VOLUMES="--volume ${GOOGLE_APPLICATION_CREDENTIALS}:/home/curation/project/curation/aou-res-curation-test.json"
fi

# run service
exec docker-compose run \
  -u "${uid}":"${gid}" \
  ${VOLUMES} \
  "$@"