# All of Us Curation

[![Circle CI](https://circleci.com/gh/all-of-us/curation/tree/develop.svg?style=shield)](https://circleci.com/gh/all-of-us/curation)

## Purpose of this document

Describes the All of Us deliverables associated with data ingestion and quality control, intended to support 
alpha release requirements. This document is version controlled; you should read the version that lives in the branch 
or tag you need. The specification document should always be consistent with the implemented curation processes. 

## Directory Overview

*   `data_steward` 
    *   `validation` Source for data curation processes. DRC will execute this package in order to assess whether
        data sets submitted by HPO data stewards satisfy the requirements outlined in the specification document.
        HPO data stewards may refer to this package to validate their data sets __before__ submitting them
        to the DRC.
    *   `tools` Scripts for setup, maintenance, deployment, etc.
        [README](data_steward/tools/README.md) summarizes each tool's purpose.
    *   `test` Unit tests.
        [README](data_steward/test/README.md) has instructions for running tests.

## Developer setup

Please reference [this guide](https://docs.google.com/document/d/16Q_F5ecN1hnKviuPTbXxy914FUGFuTXO1F4zvYbjDmc/edit)
for development setup.

## Authentication Details

All actors calling APIs in production will use [service accounts](https://cloud.google.com/compute/docs/access/service-accounts).
We will use a Google Cloud Project owned by Vanderbilt for testing: `aou-res-curation-test`.
