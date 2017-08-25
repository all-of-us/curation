# Spec
All endpoints are accessible only to specially provisioned service accounts.

## `GET /data_steward/v1/spec`

Generates the static website containing the data model specifications and file transfer procedures and uploads it to
the DRC bucket.

## For developers

### Dependencies

These are already in requirements.txt in the data_steward folder.
1. flask==0.10
2. flask-restful==0.3.5
3. Flask-FlatPages

### Running

Runs as a cron job for generating the specification site, which contains.

1. Overview
2. OMOP Common Data Model specification
3. How the upload works.
4. Validation report with minimal feedback.

To run the cron job locally, run the following command from the 'data_steward' folder,

``` ./dev_appserver.py ./ ```

and visit localhost:8000/cron

## For content editors

1. The markdown(.md) files contain the main content.
2. Editors can find the content in the pages folder.
3. Any extra files like images should be put in the vendor folder. (Yet to be
   created)


