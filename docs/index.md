---
---

# Overview

This Data Sprint is an opportunity for the Data Resource Core (DRC) to assess the technical capabilities and understand the source data of the Healthcare Provider Organization (HPO) sites required to transmit EHR data. Your participation as a HPO will provide key insight towards the integration of this information into the All of Us Research Program. We provide here an overview of the components of the Data Sprint Request:

 1.  Data Sprint Goals - further outlines the objectives of this data sprint
 1.  OMOP Common Data Model - provides an overview of the OMOP common data model used to represent the clinical data from the EHR. This specification adheres to the most recent OMOP version – 5.1.
 1.  De-identification
 1.  Data and Format - details of the data sprint with respect to data tables and fields requested
 1.  File Transfer Procedures
 1.  Timeline - key dates associated with this sprint.

## Data Sprint Goals

 1.  **Become Familiar with the OHDSI Vocabulary**. Based on the previous data sprint, an abridged version of the OMOP vocabulary was created. This data sprint will assess sites capabilities in converting their coded data into OMOP concept IDs.
 1.  **Examine Load Process of Data**. The data sent will be transformed into a common data model to examine the data integrity of the EHR data sent. This will allow the DRC to better facilitate HPOs in transforming their data to the OMOP Common Data Model.

## Timeline

This catch-up data sprint is planned for five weeks.

 * Week 1 (1/5-1/15): Send a selected sample of 100 unique patients and submit the following tables: patient table and visit\_occurrence
 * Week 2 (1/16-1/22): For the same selected sample of 100 patients, send the following tables to the DRC: condition\_occurrence
 * Week 3 (1/23-1/29): For the same selected sample of 100 patients, send the following tables to the DRC: procedure\_occurrence
 * Week 4 (1/30-2/5): For the same selected sample of 100 patients, send the following tables to the DRC: measurement
 * Week 5 (2/6-2/12): For the same selected sample of 100 patients, send the following tables to the DRC: drug\_exposure

## De-Identification

For this sprint, all dates will be de-identified and patient ids will be converted to a random number. All date fields will preserve the year, but set the month and day to Jan 1. For example, if an encounter occurs on March 12, 1985, the date will be de-identified to Jan 1, 1985. Also, all patients **greater than 80 years** of age should be removed from the submission.

## Clinical Case Example

This document will use the following case example to show how a patient’s record would be exported.

A 51-year-old (DOB: 1/2/1965), African American, non-Hispanic, male patient named John Doe visited the ED on 3/1/2106 at 3:12 a.m. with groin and abdominal pain. During the ED visit, it was determined that the patient had a kidney stone. Patient received metoclopramide 10 mg IV and morphine 6 mg IV at the ED. Subsequently, the patient had two outpatient office visits with a urologist on 3/15/2016 and 3/31/2016 with a primary diagnosis of calculus of ureter (ICD9:592.1) and was prescribed Flomax for three months. Following these visits, the patient had a lithotripsy preformed on 4/5/2016 with an ICD9 diagnosis code of 592.1 and procedure code of 98.51.

## Data and Format

Sites are asked to send data for 100 unique patients who had an encounter during the 2015 calendar year. Ideally, patients’ data will have a good mix of inpatient, outpatient and ED visits. Sites are asked to submit one file per table in CSV format with the table name as part of the file name. The file format must adhere to the Comma-Separated Values (CSV) file format (<https://tools.ietf.org/html/rfc4180>).

For data in a given table

Each record should be located on a separate line, delimited by a line break `[CRLF]`

For example:

    aaa,bbb,ccc[CRLF]

For each table, there should be a header line appearing before the data with the same format as normal record lines. This header should contain meaningful names (use the field names from the data request specifics section) corresponding to the fields in the file and should contain the same number of fields as the records in the rest of the file.

For example:

    field_name,field_name,field_name[CRLF]

After the header, each field in a record must be separated by commas

Each field should be enclosed in double quotes and empty fields will be empty quoted.

For example:

    "aaa","bbb","","ddd"[CRLF]

Each line should contain the same number of fields throughout the file. Spaces may be used as needed within the quotes are considered part of a field and should not be ignored. Spaces outside quotes will be ignored.

The last field in the record must not be followed by a comma

A double-quote appearing inside a field must be escaped by preceding it with another double quote

For example:

    "aaa","b""bb","ccc"

## OMOP Common Data Model

The OMOP Common Data Model (CDM) will be used to transfer EHR data to the Data Resource Core (DRC). The data model is an open-source, community standard for observational healthcare data. This document will cover the core tables that will be used to transport data to the DRC. The tables of interest for this export are: person, visit\_occurrence, condition\_occurrence, procedure\_occurrence, drug\_exposure, and measurement. All detailed information on the OMOP common data model can be found here: <http://www.ohdsi.org/web/wiki/doku.php?id=documentation:cdm#common_data_model>.

For the purposes of goal \#1 of this data sprint, the table definitions in this document are now the same as the OMOP specification in all fields for this submission. This document is based on the latest version of the OMOP CDM – version 5.1. The main change to the CDM with the latest version is the addition of the “datetime” columns to all the tables to capture time events. The columns with the name “source” refers to values from source systems. Every “source” field has two columns – source\_value and source\_concept\_id. For example, the LOINC code, 1963-8, for a bicarbonate lab would have “1963-8” for the source\_value column and “3016293” for source\_concept\_id. In cases where the HPO’s source system does not contain a standard terminology and only local codes are used, the source\_concept\_id would be “0” and the source\_value field would contain the local code and the description of the local code separated by a “:”. For example, representing race for a local system would be “A:African American” in the race\_source\_value and the race\_concept\_id would be “0”.

For this data sprint, the OMOP vocabulary will be required required. A version of the vocabulary for the All of Us Research Program can be downloaded [here](https://drive.google.com/file/d/0B1ctb6oEtLWLWFRqYXdWclZkbWM/view?usp=sharing). Once the file is unzipped, the CSV files that contain the vocabulary can be loaded into a database. The load script and table creation files can be found in the [CommonDataModel](https://github.com/OHDSI/CommonDataModel) repo on GitHub. Additionally, for this data sprint, the concept id columns are required to be populated. Examples of how to identify the “concept\_id” fields are in the below tables. It is also recommended to view the tutorials available online for both the CDM ETL (<http://www.ohdsi.org/common-data-model-and-extract-transform-and-load-tutorial/>) and the Vocabulary (<http://www.ohdsi.org/ohdsi-standardized-vocabulary-tutorial-recordings/)>. It is important to note that not all ICD codes map to a condition. It is important to look at the domain of the concept in the OMOP Vocabulary. For example, ICD-9 CM code “V72.19” (Other examination of ears and hearing) is considered a procedure and thus should be placed in the procedure\_occurrence table. Likewise, there are other ICD codes that should be placed in other tables.
