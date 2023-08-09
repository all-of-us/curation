FROM ubuntu:18.04
LABEL org.opencontainers.image.authors="daniel.p.carbone@vumc.org"
LABEL org.opencontainers.image.url="https://github.com/all-of-us/curation"
LABEL org.opencontainers.image.documentation="https://github.com/all-of-us/curation"
LABEL org.opencontainers.image.licenses="MIT"
LABEL org.opencontainers.image.title="All of Us Curation base development image"
LABEL org.opencontainers.image.description="Base development container image used by the All of Us Curationm team"

### DESCRIPTION

# this image's intent is to provide a universal base image upon which all subsequent development
# images can be derived

### CONTAINER BUILD

## OS INIT
# run the base container init first, as it is highly unlikely to change.
# this allows us to modify subsequent steps without initating a full image rebuild

# install deps and link "python" and "python3" execs to 3.7
RUN apt update \
  && apt upgrade -y \
  && apt install -y \
  curl \
  git \
  python3.10-dev \
  python3.10-venv \
  python3-pip \
  python3-wheel \
  wget \
  && update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.10 1 \
  && update-alternatives --install /usr/bin/python python /usr/bin/python3.10 1


## BUILD ARGS

# expected to be defined via either docker-compose or as docker build --build-arg
ARG UID
ARG GID

# this will be the name of our in-container user
ARG CURATION_USER="curation"
ARG CURATION_HOME="/home/${CURATION_USER}"

# gsdk args
ARG GSDK_VERSION
ARG GSDK_CHECKSUM
ARG GSDK_INSTALL_PATH="${CURATION_HOME}/google-cloud-sdk"
ARG GSDK_TAR_FILE="google-cloud-cli-${GSDK_VERSION}-linux-x86_64.tar.gz"
ARG GSDK_DOWNLOAD="https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/${GSDK_TAR_FILE}"

# circleci cli args
ARG CIRCLECI_CLI_VERSION
ARG CIRCLECI_CLI_CHECKSUM
ARG CIRCLECI_CLI_TMP_PATH="/tmp/circleci-cli_${CIRCLECI_CLI_VERSION}_linux_amd64"
ARG CIRCLECI_CLI_TAR_FILE="circleci-cli_${CIRCLECI_CLI_VERSION}_linux_amd64.tar.gz"
ARG CIRCLECI_CLI_DOWNLOAD="https://github.com/CircleCI-Public/circleci-cli/releases/download/v${CIRCLECI_CLI_VERSION}/${CIRCLECI_CLI_TAR_FILE}"

# this is the default location into which all per-purpose container scripts should be placed
ENV CURATION_SCRIPTS_DIR "${CURATION_HOME}"/container-scripts

# this is the default location into which all per-purpose container commands should be placed
ENV CURATION_COMMANDS_DIR "${CURATION_HOME}"/container-commands

# we're using the CIRCLE_ envvars to retain backwards compatbility
ENV CIRCLE_PROJECT_NAME "all-of-us"
ENV CIRCLE_WORKING_DIRECTORY "${CURATION_HOME}/project/curation"
ENV CIRCLE_ARTIFACTS "/tmp/circleci-artifacts"

# python venv envvars
ENV VENV_PATH "${CIRCLE_WORKING_DIRECTORY}/curation_venv"
ENV VENV_ACTIVATE "${VENV_PATH}/bin/activate"

# google envvars
ENV GOOGLE_APPLICATION_CREDENTIALS "${CIRCLE_WORKING_DIRECTORY}/aou-res-curation-test.json"

# GLOBAL BINARIES SETUP

# download circleci cli
RUN wget --quiet "${CIRCLECI_CLI_DOWNLOAD}" \
  && if ! $(echo "${CIRCLECI_CLI_CHECKSUM}" "${CIRCLECI_CLI_TAR_FILE}" | sha256sum --check --status); \
  then echo "CircleCI cli tar integrity check failure, please update build arg with correct checksum and/or version";\
  exit 1;\
  fi;

RUN tar -xvf "${CIRCLECI_CLI_TAR_FILE}" -C "/tmp" \
  && mv "${CIRCLECI_CLI_TMP_PATH}/circleci" "/usr/bin/circleci" \
  && rm -rf "${CIRCLECI_CLI_TMP_PATH}" \
  && /usr/bin/circleci update

# CONTAINER USER SETUP

# create runtime user group
RUN if ! find . | grep -q ":${GID}:" /etc/group; then \
  addgroup --gid "${GID}" "${CURATION_USER}"; \
  fi;

# create runtime user
RUN adduser \
  --shell /usr/bin/bash \
  --uid "${UID}" \
  --gid "${GID}" \
  --gecos "" \
  --disabled-login \
  "${CURATION_USER}"

# WORKING DIRECTORY SETUP

# create our penultimate working directory
RUN mkdir -p "${CIRCLE_WORKING_DIRECTORY}" \
  && chown -R "${CURATION_USER}" "${CIRCLE_WORKING_DIRECTORY}" \
  && chmod -R 775 "${CIRCLE_WORKING_DIRECTORY}"

# EVERYTHING ELSE

# ensure /ci.env file exists
RUN touch /ci.env

# move into new home dir
WORKDIR "${CURATION_HOME}"

# switch to curation user
USER "${CURATION_USER}"

# add check for ci.env file
RUN echo source /ci.env | tee -a "${CURATION_HOME}"/.bashrc "${CURATION_HOME}"/.profile

# add a few envvars to runtime user's bashrc and profile files
RUN echo export CIRCLE_WORKING_DIRECTORY="${CIRCLE_WORKING_DIRECTORY}" | \
  tee -a "${CURATION_HOME}"/.bashrc "${CURATION_HOME}"/.profile \
  && echo export CIRCLE_ARTIFACTS="${CIRCLE_ARTIFACTS}" | \
  tee -a "${CURATION_HOME}"/.bashrc "${CURATION_HOME}"/.profile \
  && echo export GOOGLE_APPLICATION_CREDENTIALS="${GOOGLE_APPLICATION_CREDENTIALS}" | \
  tee -a "${CURATION_HOME}"/.bashrc "${CURATION_HOME}"/.profile \
  && echo export CURATION_SCRIPTS_DIR="${CURATION_SCRIPTS_DIR}" | \
  tee -a "${CURATION_HOME}"/.bashrc "${CURATION_HOME}"/.profile \
  && echo export CURATION_COMMANDS_DIR="${CURATION_COMMANDS_DIR}" | \
  tee -a "${CURATION_HOME}"/.bashrc "${CURATION_HOME}"/.profile

# download gsdk and verify checksum
RUN wget --quiet "${GSDK_DOWNLOAD}" \
  && if ! $(echo "${GSDK_CHECKSUM}" "${GSDK_TAR_FILE}" | sha256sum --check --status); \
  then echo "GSDK tar integrity check failure, please update build arg with correct checksum and/or version.";\
  exit 1; \
  fi;

# install gsdk,
RUN mkdir -p "${GSDK_INSTALL_PATH}" \
  && tar -xzf "${GSDK_TAR_FILE}" -C "${CURATION_HOME}" \
  && rm "${GSDK_TAR_FILE}" \
  && cd "${GSDK_INSTALL_PATH}" \
  && ./install.sh --quiet \
  && echo "source ${GSDK_INSTALL_PATH}/path.bash.inc" | tee --append "${CURATION_HOME}/.bashrc" "${CURATION_HOME}/.profile" \
  && echo "source ${GSDK_INSTALL_PATH}/completion.bash.inc" | tee --append "${CURATION_HOME}/.bashrc" "${CURATION_HOME}/.profile" \
  && cd .. \
  && ./google-cloud-sdk/bin/gcloud components update --quiet

# set container working directory to one created above
WORKDIR "${CIRCLE_WORKING_DIRECTORY}"

# copy in requirements.txt so we can go ahead and bake deps into the image
# this requires a rebuild, but oh well.
COPY --chown=${CURATION_USER} --chmod=755 data_steward/requirements.txt "${CIRCLE_WORKING_DIRECTORY}/data_steward/requirements.txt"

# initialize venv and install python deps
RUN python -m venv "${VENV_PATH}" \
  && echo "source ${VENV_ACTIVATE}" | tee -a "${CURATION_HOME}/.bashrc" "${CURATION_HOME}/.profile" \
  && echo "export PYTHONPATH=:${CIRCLE_WORKING_DIRECTORY}:${CIRCLE_WORKING_DIRECTORY}/data_steward:${CIRCLE_WORKING_DIRECTORY}/tests:\"\${PYTHONPATH}\"" \
  | tee -a "${CURATION_HOME}/.bashrc" "${CURATION_HOME}/.profile" \
  && . "${VENV_ACTIVATE}" \
  && python -m pip install --upgrade pip setuptools wheel \
  && python -m pip install -r data_steward/requirements.txt

# remove data_steward dir as it will be mounted inside the container as a volume later.
RUN rm -rf "${CIRCLE_WORKING_DIRECTORY}/data_steward"

# copy in some seldom-modified files.
# TODO: these should probably be mounted as ro volumes by the run script(s)
COPY --chown=${CURATION_USER} --chmod=755 .coveragerc "${CIRCLE_WORKING_DIRECTORY}"/
COPY --chown=${CURATION_USER} --chmod=755 .coveragerc_integration "${CIRCLE_WORKING_DIRECTORY}"/
COPY --chown=${CURATION_USER} --chmod=755 .coveragerc_unit "${CIRCLE_WORKING_DIRECTORY}"/
COPY --chown=${CURATION_USER} --chmod=755 .pylintrc "${CIRCLE_WORKING_DIRECTORY}"/
COPY --chown=${CURATION_USER} --chmod=755 .style.yapf "${CIRCLE_WORKING_DIRECTORY}"/
COPY --chown=${CURATION_USER} --chmod=755 .yapfignore "${CIRCLE_WORKING_DIRECTORY}"/

# copy in our entrypoint script
COPY --chmod=555 docker/develop/entrypoint.sh /entrypoint.sh

# copy in commands
COPY --chown=${CURATION_USER} --chmod=555 docker/develop/commands "${CURATION_COMMANDS_DIR}"

# copy in scripts to be used inside test container
COPY --chown=${CURATION_USER} --chmod=555 docker/develop/scripts "${CURATION_SCRIPTS_DIR}"

# define our entrypoint
ENTRYPOINT [ "bash", "--login", "/entrypoint.sh" ]