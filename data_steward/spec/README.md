# Spec
All endpoints are accessible only to specially provisioned service accounts.

## `GET /data_steward/v1/spec`

Generates the static website containing the data model specifications and file transfer procedures and uploads it to
the DRC bucket.


## Dependencies

These are already in requirements.txt
1. flask==0.10
+ flask-restful==0.3.5
+ Flask-FlatPages

## Running

Runs as a cron job.
