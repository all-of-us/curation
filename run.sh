#!/usr/bin/env bash

# don't suppress errors
set -e

# suppress execution printing
set +x

## FUNCTIONS

function in_ci() {
  if [[ -n "${CI}" ]] && [[ "${CI}" == "true" ]]; then
    return 0
  fi
  return 1
}

function missing_required_env() {
  echo Required environment variable \""${1}"\" is missing or empty
  exit 1
}

if [ -z "$1" ]; then
  echo At least one argument must be provided, and \
    it must be the name of a service within docker-compose.yml

  exit 1
fi

## ENVIRONMENT

# set buildx envvars
export DOCKER_BUILDKIT=1
export COMPOSE_DOCKER_CLI_BUILD=1

## EXECUTION

# determine user from environment
uid=$(id -u)
gid=$(id -g)
whoiis=$(whoami)

echo Running "$1" as user "${whoiis}" \("${uid}:${gid}"\)...

# when run on a developer's machine, we need to do some extra things like:
# 1. ensure base container image is up to date
# 2. ensure they have credentials we can use
# 3. utilize compose v2 syntax (i.e. "docker compose" vs. "docker-compose")
#   3a. this can probably be changed with updates to CI env.
if ! in_ci; then
  echo Running outside CI.

  # when run locally, ensure we have google app creds to provide inside container
  if [[ -z "${GOOGLE_APPLICATION_CREDENTIALS}" ]]; then
    missing_required_env "GOOGLE_APPLICATION_CREDENTIALS"
  fi

  # when run on a developer machine, utilize compose v2
  COMPOSE_EXEC="docker compose"

  # TODO: eventually add double checks:
  # 1. last build date > 1 week
  # 2. data_steward/requirements.txt checksum changes

  echo Ensuring base and tests image are up to date...

  # TODO: only output full build log with "verbose" flag

  # execute base image build
  set +e
  docker compose build \
    --build-arg UID="${uid}" \
    --build-arg GID="${gid}" \
    base

  build_ok=$?
  set -e

  # verify build succeeded before proceeding
  if [ $build_ok -ne 0 ]; then
    echo "Build base step failed"
    exit 1
  fi

  # execute tests image build
  set +e
  docker compose build "${1}"
  build_ok=$?
  set -e

  if [ $build_ok -ne 0 ]; then
    echo "Build tests step failed"
    exit 1
  fi

  echo Image build successful.

  # TODO: docker-compose vs. docker compose have different full-name volume flags
  # docker-compose -> --volume
  # docker compose -> --volumes
  # this is dumb.
  VOLUMES="-v ${GOOGLE_APPLICATION_CREDENTIALS}:/home/curation/project/curation/aou-res-curation-test.json"
  echo Adding credential volume mount: \'"${VOLUMES}"\'
else
  echo Running in CI.
  # when operating in CI, utilize compose v1
  COMPOSE_EXEC="docker-compose"
fi

# run service
exec ${COMPOSE_EXEC} run \
  -u "${uid}":"${gid}" \
  ${VOLUMES} \
  "$@"
