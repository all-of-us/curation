#!/bin/bash
# Output the currently running version of the service
gcloud app versions list --hide-no-traffic | awk 'FNR==2 {print $2}'
