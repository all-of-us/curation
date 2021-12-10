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

## ENVIRONMENT

# set buildx envvars
export DOCKER_BUILDKIT=1
export COMPOSE_DOCKER_CLI_BUILD=1

## EXECUTION

# determine user from environment
uid=$(id -u)
gid=$(id -g)
whoiis=$(whoami)

echo Running as user "${whoiis}" \("${uid}:${gid}"\)...

dkr_run_args=(
  "run"
  "-v"
  "$(pwd)/.git:/home/curation/project/curation/.git:z"
  "-v"
  "$(pwd)/data_steward:/home/curation/project/curation/data_steward:z"
  "-v"
  "$(pwd)/tests:/home/curation/project/curation/tests:z"
  "-v"
  "$(pwd)/tools:/home/curation/project/curation/tools:z"
  "-v"
  "$(pwd)/.circleci:/home/curation/project/curation/.circleci:z"
)

# when run on a developer's machine, we need to do some extra things like:
# 1. ensure base container image is up to date
# 2. ensure they have credentials we can use
# 3. utilize compose v2 syntax (i.e. "docker compose" vs. "docker-compose")
#   3a. this can probably be changed with updates to CI env.
# 4. when in ci, ensure host ${BASH_ENV} file is brought into container and used
if ! in_ci; then
  echo Running outside CI.

  # when run locally, ensure we have google app creds to provide inside container
  if [[ -z "${GOOGLE_APPLICATION_CREDENTIALS}" ]]; then
    missing_required_env "GOOGLE_APPLICATION_CREDENTIALS"
  fi

  # when run on a developer machine, utilize compose v2
  COMPOSE_EXEC="docker compose"

  echo Ensuring image is up to date...

  build_args=(
    "build"
    "--build-arg"
    "UID=${uid}"
    "--build-arg"
    "GID=${gid}"
    "--quiet"
    "develop"
  )

  # execute develop image build
  set +e
  build_res=$(${COMPOSE_EXEC} "${build_args[@]}" 2>&1)
  set -e

  # verify build succeeded before proceeding
  if [[ -n "${build_res}" ]]; then
    echo "Build step failed"
    echo "${build_res}"
    exit 1
  fi

  # add google cred volume to arg list
  dkr_run_args+=("-v")
  dkr_run_args+=("${GOOGLE_APPLICATION_CREDENTIALS}:/home/curation/project/curation/aou-res-curation-test.json")
else
  echo Running in CI.
  # when operating in CI, utilize compose v1
  COMPOSE_EXEC="docker-compose"

  echo "Adding CI \$BASH_ENV to image: ${BASH_ENV}"
  dkr_run_args+=("-v" "${BASH_ENV}:/ci.env:ro")
fi

# define script arg array for use below
script_args=("$@")

# finally, add any / all remaining args provided to this script as args to pass into docker

# If the arg list contains "--", assume this to be the separation point between flags to send to
# docker compose, and the container entrypoint command.
#
# Otherwise, assume entire list is to be sent to container entrypoint
#
# This is necessary as we need to inject the name of the service defined within docker-compose.yaml that we want to
# run in-between the flags intended for `docker compose run` and container entrypoint.
if [[ "${script_args[*]}" =~ ([[:space:]]'--'[[:space:]]) ]]; then
  # this will be flipped to 1 (true) when we reach "--"
  at_command=0
  for v in "${script_args[@]}"
  do
    if [[ "${v}" == "--" ]] && [[ "${at_command}" -eq 0 ]]; then
      at_command=1
      dkr_run_args+=("develop")
    else
      dkr_run_args+=("${v}")
    fi
  done
else
  dkr_run_args+=("develop")
  for v in "${script_args[@]}"
  do
    dkr_run_args+=("${v}")
  done
fi

echo "Run cmd: ${dkr_run_args[*]}"

# run service
exec ${COMPOSE_EXEC} "${dkr_run_args[@]}"
