#!/usr/bin/env bash

# initializes ci runtime container.  any / all os-level packages must be handled here.

# don't suppress errors
set -e

echo "Updating packages and installing python3.7..."
sudo apt update \
  && sudo apt upgrade -y \
  && sudo apt install -y \
    python3.7-dev \
    python3.7-venv \
    python3-pip \
    python3-wheel \
  && sudo update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.7 1 \
  && sudo update-alternatives --install /usr/bin/python python /usr/bin/python3.7 1

echo "Creating artifact dir \"${CIRCLE_ARTIFACTS}\"..."
sudo mkdir -p "${CIRCLE_ARTIFACTS}"