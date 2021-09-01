#!/usr/bin/env bash

GSDK_INSTALL_PATH="${HOME}/google-cloud-sdk"
GSDK_TAR_FILE="google-cloud-sdk-${GSDK_VERSION}-linux-x86_64.tar.gz"
GSDK_DOWNLOAD="https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/${GSDK_TAR_FILE}"

echo "Downloading GSDK:"
echo "  checksum: ${GSDK_CHECKSUM}"
echo "  version: ${GSDK_VERSION}"
echo "  tar: ${GSDK_TAR_FILE}"
echo "  url: ${GSDK_DOWNLOAD}"
echo "  dest: ${GSDK_INSTALL_PATH}"

# don't suppress errors
set -e

# TODO: This is done as we cannot safely rely on there being an upstream repo available should we switch base os versions
# we may eventually switch back to using a repo, but I don't know that it buys us anything, really.
cd "${HOME}" \
  && wget --quiet "${GSDK_DOWNLOAD}"

# verify integrity
echo "Verifying checksum..."
if ! echo "${GSDK_CHECKSUM}" "google-cloud-sdk-${GSDK_VERSION}-linux-x86_64.tar.gz" | sha256sum --check --status;
then
  echo "Checksum validation failed:"
  echo "${GSDK_CHECKSUM}" "google-cloud-sdk-${GSDK_VERSION}-linux-x86_64.tar.gz" | sha256sum --check
  exit 1
fi

# if we get here, assume things are ok.  proceed with installation of gsdk and updating of components
mkdir -p "${GSDK_INSTALL_PATH}" \
  && tar -xzf "${GSDK_TAR_FILE}" -C "${HOME}" \
  && rm "${GSDK_TAR_FILE}" \
  && cd "${GSDK_INSTALL_PATH}" \
  && ./install.sh --quiet \
  && echo "source ${GSDK_INSTALL_PATH}/path.bash.inc" | tee --append "${HOME}/.bashrc" "${HOME}/.profile" \
  && echo "source ${GSDK_INSTALL_PATH}/completion.bash.inc" | tee --append "${HOME}/.bashrc" "${HOME}/.profile" \
  && cd .. \
  && ./google-cloud-sdk/bin/gcloud components update --quiet
