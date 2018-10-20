#!/bin/bash
# Output the environment variables of the currently running version of the service
gcloud app versions describe $(app-version-current.sh) --service=default | awk '/envVariables:/{flag=1; next} /handlers:/{flag=0} flag'
