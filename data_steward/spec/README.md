# Spec
All endpoints are accessible only to specially provisioned service accounts.

## `GET /data_steward/v1/spec`

Generates the static website containing the data model specifications and file transfer procedures and uploads it to
the DRC bucket.


## Dependencies

These are already in requirements.txt
1. flask==0.10
2. flask-restful==0.3.5
3. Flask-FlatPages

## Running

Runs as a cron job for generating the specification site, which contains.

1. Overview
2. OMOP Common Data Model specification
3. How the upload works.
4. Validation report with minimal feedback.

