Setup

- Initial GCP setup:

  - Enable GCP APIs in the project per https://cloud.google.com/dataflow/docs/quickstarts/quickstart-python
  - Create a service account
  - Grant SA "Dataflow Admin" as well as BigQuery Data Editor (for r/w to BigQuery)
  - Configure GCP networks appropriately:

    - For this demo, I created a new VPC network within the project, per https://cloud.google.com/dataflow/docs/guides/specifying-networks
    - In production, we will need to work with Sysadmins to determine what the correct configuration is
    - The sysadmin team appears to have disabled the normal "default" network creation on new GCP projects,
      which breaks standard Dataflow setup. We will need to specify an existing shared VPC network, or
      figure out the appropriate settings and create a new network for Curation projects

- Install requirements:

```
mkvirtualenv cdr-beam
pip install apache-beam apache-beam[gcp]
```

- Create a key for `dataflow-test@aou-res-curation-test.iam.gserviceaccount.com`:

```
gcloud iam service-accounts keys create /tmp/df-key.json --iam-account=dataflow-test@aou-res-curation-test.iam.gserviceaccount.com
```

- Run the pipeline as the SA:

```
GOOGLE_ACCOUNT_CREDENTIALS=/tmp/df-key.json python pipeline.py
```

