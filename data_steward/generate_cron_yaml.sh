#!/bin/bash

CRON_YAML='cron.yaml'

# If cron file already exists exit to prevent accidental overwrite
if [[ -f "${CRON_YAML}" ]]
then
    echo "The file '${CRON_YAML}' already exists. Please remove or rename it and retry."
    exit 1
fi

# Array with hpo_ids
SITES_ARR=( $(cut -d ',' -f1 ./spec/_data/hpo.csv) )
SITES_ARR=("${SITES_ARR[@]:1}")

echo "cron:" > ${CRON_YAML}

### Validate All HPOs
echo "" >> ${CRON_YAML}
echo "# Validate All HPOs" >> ${CRON_YAML}
echo "- description: validate all hpos" >> ${CRON_YAML}
echo "  url: /data_steward/v1/ValidateAllHpoFiles" >> ${CRON_YAML}
echo "  schedule: every 3 hours" >> ${CRON_YAML}
echo "  timezone: America/New_York" >> ${CRON_YAML}

### EHR Union
echo "" >> ${CRON_YAML}
echo "# EHR Union" >> ${CRON_YAML}
echo "- description: ehr union" >> ${CRON_YAML}
echo "  url: /data_steward/v1/UnionEHR" >> ${CRON_YAML}
echo "  schedule: every 24 hours" >> ${CRON_YAML}
echo "  timezone: America/New_York" >> ${CRON_YAML}

### Archive
# Archive the contents of sites' buckets
echo "" >> ${CRON_YAML}
echo "# Archive" >> ${CRON_YAML}
for i in "${SITES_ARR[@]}"
do
  temp="${i%\"}"
  temp="${temp#\"}"
  echo "- description: copy hpo $temp files" >> ${CRON_YAML}
  echo "  url: /data_steward/v1/CopyFiles/$temp" >> ${CRON_YAML}
  echo "  schedule: every 24 hours" >> ${CRON_YAML}
  echo "  timezone: America/New_York" >> ${CRON_YAML}
done

### Force Run
# Process a site's latest submission, regardless of whether it has already been processed.
# These should only be triggered from the cron page.
# NOTE: This will wipe out a site's data if their bucket is empty.

# In order to display them on the cron page, we set jobs to run annually starting in 364 days.
# For example if today's date is 1/1/2020 then YESTERDAY_SCHEDULE evaluates to "31 of December"
# and it is set to run for the first time on December 31, 2020
YESTERDAY_SCHEDULE=$(date -v-1d '+%e of %B 13:00')
# YESTERDAY_SCHEDULE_LOWER="${YESTERDAY_SCHEDULE,,}" # lowercase
# bash3 friendly lowercase
YESTERDAY_SCHEDULE_LOWER=$(echo ${YESTERDAY_SCHEDULE} | tr '[:upper:]' '[:lower:]')
echo "" >> ${CRON_YAML}
echo "# Force Run" >> ${CRON_YAML}
for i in "${SITES_ARR[@]}"
do
  temp="${i%\"}"
  temp="${temp#\"}"
  echo "- description: validate hpo $temp" >> ${CRON_YAML}
  echo "  url: /data_steward/v1/ValidateHpoFiles/$temp" >> ${CRON_YAML}
  echo "  schedule: ${YESTERDAY_SCHEDULE_LOWER}" >> ${CRON_YAML}
  echo "  timezone: America/New_York" >> ${CRON_YAML}
done

## Participant Matching
echo "" >> ${CRON_YAML}
echo "# Participant Matching" >> ${CRON_YAML}
echo "- description: compare submissions vs ppi" >> ${CRON_YAML}
echo "  url: /data_steward/v1/ParticipantValidation/" >> ${CRON_YAML}
echo "  schedule: every 3 hours" >> ${CRON_YAML}
echo "  timezone: America/New_York" >> ${CRON_YAML}

## Participant Matching Site Files
echo "" >> ${CRON_YAML}
echo "# Participant Matching Site Files" >> ${CRON_YAML}
echo "- description: store results of participant matching in site buckets" >> ${CRON_YAML}
echo "  url: /data_steward/v1/ParticipantValidation/SiteFiles" >> ${CRON_YAML}
echo "  schedule: every 3 hours" >> ${CRON_YAML}
echo "  timezone: America/New_York" >> ${CRON_YAML}

## Participant Matching DRC File
echo "" >> ${CRON_YAML}
echo "# Participant Matching DRC File" >> ${CRON_YAML}
echo "- description: store results of participant matching in DRC bucket" >> ${CRON_YAML}
echo "  url: /data_steward/v1/ParticipantValidation/DRCFile" >> ${CRON_YAML}
echo "  schedule: every 3 hours" >> ${CRON_YAML}
echo "  timezone: America/New_York" >> ${CRON_YAML}

## Retract Participant Data
echo "" >> ${CRON_YAML}
echo "# Data Retraction" >> ${CRON_YAML}
echo "- description: retract data for specified person_ids" >> ${CRON_YAML}
echo "  url: /data_steward/v1/RetractPids" >> ${CRON_YAML}
echo "  schedule: ${YESTERDAY_SCHEDULE_LOWER}" >> ${CRON_YAML}
echo "  timezone: America/New_York" >> ${CRON_YAML}
