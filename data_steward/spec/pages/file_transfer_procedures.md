title: File Transfer Procedures
template: page

## Data Steward Registration

1. For each site you are a data steward for, enter your information in the [Data Steward Contact List](https://docs.google.com/spreadsheets/d/1Slh4teXKBwtD_ZrTFEjUTVH3Jk5ii-tcSJxocZnAqdo/edit?usp=sharing)

1. Request a `pmi-ops.org` account with data steward access by sending an e-mail to `sysadmin@pmi-ops.org` with the following:
 
     * __Subject__ New User Request: Data Steward
     * Attestation of having completed your institutional security training
     * Attestation of having entered your information in the [Data Steward Contact List](https://docs.google.com/spreadsheets/d/1Slh4teXKBwtD_ZrTFEjUTVH3Jk5ii-tcSJxocZnAqdo/edit?usp=sharing)
     * Attached a signed, scanned copy of the [Rules of Behavior for Privileged Use](https://docs.google.com/document/d/1E6bRJ4l7AclEkaFS4Tg2zt9u3WyFMOpu4-omMjhlTRM/edit?usp=sharing). Note that the entire document must be attached, not just the signed page 

1. Within five business days you should receive a response with:
     * Instructions on how to complete registration of your `pmi-ops.org` account
     * The name of your site's assigned bucket on Google Cloud Storage
     * A test data set to use in verifying your account.
     * Information on how to activate your account on our [JIRA site](https://precisionmedicineinitiative.atlassian.net) where you can report and track issues related to this process
  
      Keep this information in a safe place and do not share it with others.

## File Naming Convention

`<Bucket name>_<quarter end date (yyyymmdd)>_<table name>.csv`

An example is:
* AoUSite123_20170930_person.csv
* AoUSite123_20170930_visit_occurrence.csv
* AoUSite123_20170930_condition_occurrence.csv

## Prepare Files to be Uploaded

For this test the same files and file structure will be used as in the [Data Sprint](https://cumc-dbmi.github.io/pmi_sprint_reporter/data_model.html).
 * Person
 * Visit_occurrence
 * Condition_occurrence
 * Procedure_occurence
 * Drug_exposure
 * Measurement
                
## Upload the Test Data Set

The instructions below assume the bucket assigned to you was `test-site-bucket-AoU123`. You should adapt according to the information that was sent to you by `sysadmin@pmi-ops.org`:

1. Open a web browser and navigate to the Google Cloud Storage Browser at `https://console.cloud.google.com/storage/browser/<BUCKET_NAME>` but replace `<BUCKET_NAME>` with the bucket name assigned to your site. For example, if your site was assigned `test-site-bucket-AoU123`, then it would be [https://console.cloud.google.com/storage/browser/test-site-bucket-AoU123](https://console.cloud.google.com/storage/browser/test-site-bucket-AoU123).
1. Enter your `pmi-ops.org` credentials if prompted.
1. Create a folder inside the bucket to load the data into.
Suggested folder naming scheme would be the date of the upload.
1. In your desktop environment, navigate to the local folder where you've downloaded the test data set.
1. Drag and drop the test data set files from your local folder into the browser window where the Google Cloud Storage Browser is open to the folder created in step 3.

## Review the Results - Skip this section for October Testing - 

Within 6 hours of uploading the data set, the report page should be updated with the status of your upload. A more detailed report should appear in your GCS bucket if any issues were detected. 

## Report Issues

If you experience any issues in this process, please report an issue on our [JIRA site](https://precisionmedicineinitiative.atlassian.net).
