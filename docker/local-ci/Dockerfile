FROM cimg/base:stable-18.04

# expected to be defined via either docker-compose or as docker build --build-arg
ARG UID
ARG GID
ARG CIRCLE_WORKING_DIRECTORY
ARG CIRCLE_ARTIFACTS
ARG VENV_NAME
ARG GSDK_VERSION
ARG GSDK_CHECKSUM

# define envvars from args to be used by scripts
ENV CIRCLE_WORKING_DIRECTORY="${CIRCLE_WORKING_DIRECTORY}"
ENV CIRCLE_ARTIFACTS="${CIRCLE_ARTIFACTS}"
ENV VENV_NAME="${VENV_NAME}"
ENV GSDK_VERSION="${GSDK_VERSION}"
ENV GSDK_CHECKSUM="${GSDK_CHECKSUM}"

USER root

# get this out of the way here.  it helps reduce successive build times and lets us be a bit
# nicer of an internet neighbor.
RUN apt update \
    && apt upgrade -y

# todo: the user manipulation seen here can probably be replaced with something a bit more elegant

# create group for "us"
RUN addgroup \
    --gid "${GID}" \
    curation

# create user for "us"
RUN adduser \
    --shell /usr/bin/bash \
    --no-create-home \
    --uid "${UID}" \
    --gid "${GID}" \
    --gecos "" \
    --disabled-login \
    curation

# add ourselves to circleci group
RUN usermod \
    -a \
    -G circleci \
    curation

# add circleci to our group
RUN usermod \
    -a \
    -G curation \
    circleci

# set sticky gid and uid on
RUN mkdir -p "${CIRCLE_WORKING_DIRECTORY}" \
    && chown -R circleci "${CIRCLE_WORKING_DIRECTORY}" \
    && chmod -R 775 "${CIRCLE_WORKING_DIRECTORY}"

# go back to local user
USER circleci

# specify working dir to same as circle-ci
WORKDIR "${CIRCLE_WORKING_DIRECTORY}"

# copy in requirements.txt so we can go ahead and bake deps into the image
# this requires a rebuild, but oh well.
COPY --chown=curation:circleci --chmod=755 data_steward/requirements.txt "${CIRCLE_WORKING_DIRECTORY}/data_steward/requirements.txt"

# copy over a few, specific scripts that we'll want to execute as part of building the container itself.
# this is done so that the actual process of executing tests is not as onerous for two main reasons:
#   1. time-heavy package installation is baked into the image
#   2. since these scripts are specifically looked at _before_ their associated RUN actions, the RUN actions will
#       only be triggered if each specific file is modified.  if we copied in the entire script dir now, full-fat
#       rebuild would take place basically every time we edited any file within the ci/scripts dir.
COPY --chown=curation:circleci --chmod=755 ci/scripts/common_00_init_image.sh ci/scripts/
COPY --chown=curation:circleci --chmod=755 ci/scripts/common_10_init_venv.sh ci/scripts/
COPY --chown=curation:circleci --chmod=755 ci/scripts/tests_10_install_gsdk.sh ci/scripts/

# initialize image and python venv
RUN ci/scripts/common_00_init_image.sh
RUN ci/scripts/common_10_init_venv.sh

# go ahead and bake gsdk into image, as it can take quite awhile to download and update
# connections
RUN ci/scripts/tests_10_install_gsdk.sh

# remove data_steward dir as it will be mounted inside the container as a volume later.
RUN sudo rm -rf "${CIRCLE_WORKING_DIRECTORY}/data_steward"

# now copy in all scripts (yes, this will overwrite the ones copied in before...)
COPY --chown=curation:circleci --chmod=755 ci/scripts "${CIRCLE_WORKING_DIRECTORY}"/ci/scripts

# copy in a few files used by linters and coveragers
COPY --chown=curation:circleci --chmod=755 .coveragerc "${CIRCLE_WORKING_DIRECTORY}"/
COPY --chown=curation:circleci --chmod=755 .coveragerc_integration "${CIRCLE_WORKING_DIRECTORY}"/
COPY --chown=curation:circleci --chmod=755 .coveragerc_unit "${CIRCLE_WORKING_DIRECTORY}"/
COPY --chown=curation:circleci --chmod=755 .pylintrc "${CIRCLE_WORKING_DIRECTORY}"/
COPY --chown=curation:circleci --chmod=755 .style.yapf "${CIRCLE_WORKING_DIRECTORY}"/
COPY --chown=curation:circleci --chmod=755 .yapfignore "${CIRCLE_WORKING_DIRECTORY}"/

# finally, copy in entrypoint script
COPY --chmod=755 docker/local-ci/entrypoint.sh /entrypoint.sh

ENTRYPOINT [ "bash", "--login", "/entrypoint.sh" ]