title: Uploading data
template: page

# File Transfer Procedures

__Note: You should not upload real participant data or de-identified data during alpha. You will be given a test data set to use instead.__

## Data Steward Registration

1. For each site you are a data steward for, enter your information in the [data steward contact list](https://docs.google.com/spreadsheets/d/1Slh4teXKBwtD_ZrTFEjUTVH3Jk5ii-tcSJxocZnAqdo/edit?usp=sharing)

1. Request a `pmi-ops.org` account with data steward access by sending an e-mail to `sysadmin@pmi-ops.org` with the following:
 
   * Subject: `New User Request: Data Steward`
   * Attestation of having completed your institutional security training
   * Attestation of having entered your information in the [data steward contact list](https://docs.google.com/spreadsheets/d/1Slh4teXKBwtD_ZrTFEjUTVH3Jk5ii-tcSJxocZnAqdo/edit?usp=sharing)
   * Attached a signed, scanned copy of the [Rules of Behavior for Privileged Use](https://docs.google.com/document/d/1E6bRJ4l7AclEkaFS4Tg2zt9u3WyFMOpu4-omMjhlTRM/edit?usp=sharing)
     
     _(Note: the entire document must be attached, not just the signed page)_ 

1. Within five business days you should receive an e-mail from `sysadmin@pmi-ops.org` containing:

   * instructions on how to complete registration of your `pmi-ops.org` account  
   * the name of your site's bucket on Google Cloud Storage
   * a test data set to be used during alpha
 
   Keep this information in a safe place and do not share it with others.

## Upload the test data set

The instructions below make the following assumptions and should be adapted according to the information that was sent to you by `sysadmin@pmi-ops.org`:

 * your account is `john.smith@pmi-ops.org`
 * your site's bucket name is `all-of-us-ehr-uploads-site-123`
    
1. Open a web browser and navigate to the Google Cloud Console `https://console.cloud.google.com/storage/browser/<all-of-us-ehr-uploads-site-123>`
1. When prompted for credentials, use those you have set up for `john.smith@pmi-ops.org`
1. In your desktop environment, navigate to the local folder containing the test data set
1. Drag and drop the files from your local folder into the browser window where the Google Cloud Console is open

## Review the results

Within 6 hours of uploading the data set, a report indicating the status of the upload should appear in your GCS bucket. If the file does not appear or is incorrect, you are expected to report an issue.
