---
layout: default
---

# Introduction

An essential function of health care provider organizations (HPO) participating in the All of Us Program is to 
locate local electronic health record (EHR) data of participants who have provided consent and to share these data 
with the program following guidance provided by the Data & Research Center (DRC). This document serves as the source
of this guidance. It contains the technical specifications that data sets must adhere to in order for submitting HPOs 
to receive proper credit for their enrollment achievements. It describes the expected format, structure and content of 
EHR data submissions and the standard procedures governing these data sets (e.g. the procedures by which HPOs are 
expected to send data sets to the program).

This document is intended primarily for personnel (who we refer to as _data stewards_) who hold the responsibility 
of submitting EHR data on behalf of HPOs.

 1. Overview
 1. [Data Model](data_model.md)
 1. [File Transfer Procedures](file_transfer_procedures.md)

# Data Submission Steps

1. __Complete HPO data steward registration__ Once you have registered as a data steward for your site, you will receive a `pmi-ops.org` account, the URL for your site's Google Cloud Storage (GCS) bucket and a test data set to be used during alpha.
1. __Upload synthetic data set__ You will upload your data set to your site's GCS bucket.
1. __Review validation report__ Within 6 hours of uploading the data set, a report indicating the status of the upload should appear in your GCS bucket. If the file does not appear or is incorrect, you are expected to report an issue.

# Timeline

_Sites will be expected to upload datasets on a quarterly basis. Associated deadlines will be posted here._ 

[Proceed to Data Model >>](data_model.md)
