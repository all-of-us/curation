# OMOP CDM

The OMOP Common Data Model (CDM) will be used to transfer EHR data to the Data Resource Core (DRC). The data model is an open-source, community standard for observational healthcare data. This document will cover the core tables that will be used to transport data to the DRC. The tables of interest for this export are: person, visit\_occurrence, condition\_occurrence, procedure\_occurrence, drug\_exposure, and measurement. All detailed information on the OMOP common data model can be found here: <http://www.ohdsi.org/web/wiki/doku.php?id=documentation:cdm#common_data_model>.

For the purposes of goal \#1 of this data sprint, the table definitions in this document are now the same as the OMOP specification in all fields for this submission. This document is based on the latest version of the OMOP CDM – version 5.1. The main change to the CDM with the latest version is the addition of the “datetime” columns to all the tables to capture time events. The columns with the name “source” refers to values from source systems. Every “source” field has two columns – source\_value and source\_concept\_id. For example, the LOINC code, 1963-8, for a bicarbonate lab would have “1963-8” for the source\_value column and “3016293” for source\_concept\_id. In cases where the HPO’s source system does not contain a standard terminology and only local codes are used, the source\_concept\_id would be “0” and the source\_value field would contain the local code and the description of the local code separated by a “:”. For example, representing race for a local system would be “A:African American” in the race\_source\_value and the race\_concept\_id would be “0”.

For this data sprint, the OMOP vocabulary will be required. A version of the vocabulary for the All of Us Research Program can be downloaded [here](https://drive.google.com/file/d/0B1ctb6oEtLWLWFRqYXdWclZkbWM/view?usp=sharing). Once the file is unzipped, the CSV files that contain the vocabulary can be loaded into a database. The load script and table creation files can be found in the [CommonDataModel](https://github.com/OHDSI/CommonDataModel) repo on GitHub. Additionally, for this data sprint, the concept id columns are required to be populated. Examples of how to identify the “concept\_id” fields are in the below tables. It is also recommended to view the tutorials available online for both the CDM ETL (<http://www.ohdsi.org/common-data-model-and-extract-transform-and-load-tutorial/>) and the Vocabulary (<http://www.ohdsi.org/ohdsi-standardized-vocabulary-tutorial-recordings/)>. It is important to note that not all ICD codes map to a condition. It is important to look at the domain of the concept in the OMOP Vocabulary. For example, ICD-9 CM code “V72.19” (Other examination of ears and hearing) is considered a procedure and thus should be placed in the procedure\_occurrence table. Likewise, there are other ICD codes that should be placed in other tables.

# person

| Field                          | Required For Export     | Type     | Description |
| ------------------------------ | ----------------------- | -------- | ----------- |
| person\_id                     | Yes                     | integer  | A unique identifier for each person. |
| gender\_concept\_id            | Yes                     | integer  | Refer to [gender_concept_id.csv](https://github.com/cumc-dbmi/pmi_sprint_reporter/blob/master/resources/valid_concepts/person/gender_concept_id.csv) for the list of allowed concept_ids. |
| year\_of\_birth                | Yes                     | integer  | The year of birth of the person. For data sources with date of birth, the year is extracted. For data sources where the year of birth is not available, the approximate year of birth is derived based on any age group categorization available. |
| month\_of\_birth               | Yes                     | integer  | The month of birth of the person. For data sources that provide the precise date of birth, the month is extracted and stored in this field. |
| day\_of\_birth                 | Yes                     | integer  | The day of the month of birth of the person. For data sources that provide the precise date of birth, the day is extracted and stored in this field. |
| datetime\_of\_birth            | Yes                     | datetime | The date and time of birth. See [datetime](index.html#datetime). |
| race\_concept\_id              | Yes                     | integer  | Refer to [race_concept_id.csv](https://github.com/cumc-dbmi/pmi_sprint_reporter/blob/master/resources/valid_concepts/person/race_concept_id.csv) for the allowed concept_ids. |
| ethnicity\_concept\_id         | Yes                     | integer  | Refer to [ethnicity_concept_id.csv](https://github.com/cumc-dbmi/pmi_sprint_reporter/blob/master/resources/valid_concepts/person/ethnicity_concept_id.csv) for the allowed concept_ids. |
| location\_id                   | No                      | integer  | Leave blank |
| provider\_id                   | No                      | integer  | Leave blank |
| care\_site\_id                 | No                      | integer  | Leave blank |
| person\_source\_value          | No                      | varchar  | Leave blank |
| gender\_source\_value          | Yes                     | varchar  | The source code for the gender of the person as it appears in the source data. The original value is stored here for reference. Separate the code and value with `:` as in `M:Male`. |
| gender\_source\_concept\_id    | No                      | Integer  | Leave blank |
| race\_source\_value            | Yes                     | varchar  | The source code and value for the race of the person as it appears in the source data. Separate the code and value with a `:` as in `AA:African American`. |
| race\_source\_concept\_id      | No                      | integer  | Leave blank |
| ethnicity\_source\_value       | Yes                     | varchar  | The source code and value for the ethnicity of the person as it appears in the source data. Separate the code and value with `:` as in `H:Hispanic`. |
| ethnicity\_source\_concept\_id | No                      | integer  | Leave blank |

<http://www.ohdsi.org/web/wiki/doku.php?id=documentation:cdm:person>

For example, an African American, non-Hispanic, male patient named John Doe born on 1/2/1965 would have a row in the file as seen below. In this example, the source system represents African American as “A” and non-Hispanic as “NH”.

    "1", "8507", "1965", "1", "1", "1965-01-02T00:00:00-05:00", "38003599", "38003564","","","","", "M:Male","", "A:African American","", "NH:Non-Hispanic",""


# visit\_occurrence

| Field                      | Required For Export     | Type     | Description |
| -------------------------- | ----------------------- | -------- | ----------- |
| visit\_occurrence\_id      | Yes                     | integer  | A unique identifier for each Person's visit or encounter at a healthcare provider. |
| person\_id                 | Yes                     | integer  | A foreign key identifier to the Person for whom the visit is recorded. The demographic details of that Person are stored in the PERSON table. |
| visit\_concept\_id         | Yes                     | integer  | Refer to [visit_concept_id.csv](https://github.com/cumc-dbmi/pmi_sprint_reporter/blob/master/resources/valid_concepts/visit_occurrence/visit_concept_id.csv) for the allowed concept_ids. |
| visit\_start\_date         | Yes                     | date     | The start date of the visit in this format YYYY-MM-DD |
| visit\_start\_datetime     | Yes                     | datetime | The end date and time of the visit. *See [datetime](index.html#datetime).* |
| visit\_end\_date           | Yes                     | date     | The end date of the visit. If this is a one-day visit the end date should match the start date - YYYY-MM-DD |
| visit\_end\_datetime       | No                      | datetime | The end date and time of the visit. *See [datetime](index.html#datetime).* For outpatient and ED visits when the end datetime may not be known, leave this field empty since the visit\_end\_date field will be populated. |
| visit\_type\_concept\_id   | Yes                     | integer  | Refer to [visit_type_concept_id.csv](https://github.com/cumc-dbmi/pmi_sprint_reporter/blob/master/resources/valid_concepts/visit_occurrence/visit_type_concept_id.csv) for the allowed concept_ids. |
| provider\_id               | No                      | integer  | Set to 0 for this data sprint. |
| care\_site\_id             | No                      | integer  | Set to 0 for this data sprint. |
| visit\_source\_value       | Yes                     | varchar  | The source code for the visit as it appears in the source data. |
| visit\_source\_concept\_id | No                      | integer  | Set to 0 for this data sprint. |

<http://www.ohdsi.org/web/wiki/doku.php?id=documentation:cdm:visit_occurrence>

For the above case, there was one ED visit in New York City (3/1/2106 at 3:12 a.m. and discharged at 8:32 a.m.), followed by two outpatient clinic visits (3/15/2016 at 1:30 p.m. and 3/31/2016 at 4:45 p.m.), and finally one ambulatory surgery visit (4/5/2016 at 10:00 a.m.). The source system had the following codes: "E" – emergency, "I" – inpatient, "O" – outpatient, and "A" – outpatient ambulatory surgery

ED visit

    "1","1","9203","2016-01-01","2016-01-01T03:12:00-05:00","2016-01-01","2016-01-01T08:32:00-05:00","44818518","0","0","E:Emergency","0"

Clinic visit
 
    "2","1","9202","2016-01-01","2016-01-01T13:30:00-05:00","2016-01-01","","44818518","0","0","O:Outpatient","0"

Clinic visit 

    "3","1","9202","2016-01-01","2016-01-01T16:45:00-05:00","2016-01-01","","44818518","0","0","O:Outpatient","0"

Amb surg visit 

    "4","1","9202","2016-01-01","2016-01-01T10:00:00-05:00","2016-01-01","","44818518","0","0","A:Ambulatory Surgery","0"


# condition\_occurrence

| Field                      | Required For Export     | Type     | Description |
| -------------------------- | ----------------------- | -------- | ----------- |
| condition\_occurrence\_id      | Yes                     | integer     | A unique identifier for each Condition Occurrence event. |
| person\_id                     | Yes                     | integer     | A foreign key identifier to the Person who is experiencing the condition. The demographic details of that Person are stored in the PERSON table. |
| condition\_concept\_id         | Yes                     | integer     | A standard, valid, OMOP concept ID for the associated domain. The SNOMED terminology is considered standard for the condition domain. It is often derived from the `condition_source_concept_id`. If your source data does not use SNOMED (i.e. in `condition_source_concept_id`) then they need to be translated. The query in [condition_concept_id.sql](https://github.com/cumc-dbmi/pmi_sprint_reporter/blob/master/resources/valid_concepts/condition_occurrence/condition_concept_id.sql) demonstrates how you may go about this. |
| condition\_start\_date         | Yes                     | date        | The date when the instance of the Condition is recorded. |
| condition\_start\_datetime     | Yes                     | datetime    | The date and time of the start of the Condition. *See [datetime](index.html#datetime)* |
| condition\_end\_date           | Yes                     | date        | The date when the instance of the Condition is considered to have ended. |
| condition\_end\_datetime       | Yes                     | datetime    | The date and time when the instance of the Condition is considered to have ended. *See [datetime](index.html#datetime)* |
| condition\_type\_concept\_id   | Yes                     | integer     | Refer to [condition_type_concept_id.csv](https://github.com/cumc-dbmi/pmi_sprint_reporter/blob/master/resources/valid_concepts/condition_occurrence/condition_type_concept_id.csv) for the list of allowed concept_ids. |
| stop\_reason                   | No                      | varchar(20) | The reason that the condition was no longer present, as indicated in the source data. |
| provider\_id                   | No                      | integer     | Set to 0 for this data sprint. |
| visit\_occurrence\_id          | Yes                     | integer     | A foreign key to the visit in the VISIT table during which the Condition was determined (diagnosed). |
| condition\_source\_value       | Yes                     | varchar     | The source code for the condition as it appears in the source data. **NOTE**: This may be the ICD9, ICD10, or SNOMED code associated with a visit. |
| condition\_source\_concept\_id | Yes                     | integer     | The OMOP concept ID that corresponds to the diagnosis code, as indicated in the source data. The query in [condition_source_concept_id.sql](https://github.com/cumc-dbmi/pmi_sprint_reporter/blob/master/resources/valid_concepts/condition_occurrence/condition_source_concept_id.sql) provides an example of how to retrieve the concept id for the ICD9 code `592.0` (Calculus of kidney). |

<http://www.ohdsi.org/web/wiki/doku.php?id=documentation:cdm:condition_occurrence>

## Notes
 * Conditions that are reported should be discharge diagnosis codes or problem list entries.
 * Not all ICD codes belong in the condition\_occurrence table. For example, ICD-9 CM code `V72.19` (Other examination of ears and hearing) is considered a procedure and thus should be placed in the procedure\_occurrence table.

The following were the diagnosis codes for each of the above visits:

| | |
|--------------------|----------------------------------------------------------------------------------------|
| ED                 | Primary: Calculus of kidney (ICD9:592.0) <br> Secondary: Abdominal pain, right low quandrant (ICD9:789.03). Renal Colic (ICD9:788.0) |
| Both clinic visits | Calculus of ureter (ICD9:592.1)                                                        |
| Amb Surg           | Calculus of ureter (ICD9:592.1)                                                        |


The following were the condition occurrence export:

ED visit
 
    "1","1","201620","2016-01-01", "2016-01-01T03:12:00-05:00", "2016-01-01", "2016-01-01T03:12:00-05:00", "44786627","","0","1","592.0","44826732"
    "2","1","193322","2016-01-01", "2016-01-01T03:12:00-05:00", "2016-01-01", "2016-01-01T03:12:00-05:00","44786629","","0","1","789.03","44836296"
    "3","1","201690","2016-01-01", "2016-01-01T03:12:00-05:00", "2016-01-01", "2016-01-01T03:12:00-05:00","44786629","","0","1","788.0","44831593"

Clinic visit 

    "4","1","201916","2016-01-01","2016-01-01T16:45:00-05:00","2016-01-01","","44786627","","0","2","592.1","44825543"

Clinic visit 

    "5","1","201916","2016-01-01","2016-01-01T16:45:00-05:00","2016-01-01","","44786627","","0","3","592.1","44825543"

Amb surg visit

    "6","1","201916","2016-01-01","2016-01-01T10:00:00-05:00","2016-01-01","","44786627","","0","4","592.1","44825543"


# procedure\_occurrence

| Field                      | Required For Export     | Type     | Description |
| -------------------------- | ----------------------- | -------- | ----------- |
| procedure\_occurrence\_id      | Yes                     | integer  | A system-generated unique identifier for each Procedure Occurrence.                                                                                                                                                                                                                                                                       |
| person\_id                     | Yes                     | integer  | A foreign key identifier to the Person who is subjected to the Procedure. The demographic details of that Person are stored in the PERSON table.                                                                                                                                                                                          |
| procedure\_concept\_id         | Yes                     | integer  | A standard, valid, OMOP concept ID for the associated domain. The CPT-4 terminology is considered the standard for the procedure domain. It is often derived from the `procedure_source_concept_id`. If your source data is non-standard, then they need to be translated. The query in [procedure_concept_id.sql](https://github.com/cumc-dbmi/pmi_sprint_reporter/blob/master/resources/valid_concepts/procedure_occurrence/procedure_concept_id.sql) demonstrates how you may go about this. |
| procedure\_date                | Yes                     | date     | The date on which the Procedure was performed.                                                                                                                                                                                                                                                                                            |
| procedure\_datetime            | Yes                     | datetime | The date and time on which the Procedure was performed. *See [datetime](index.html#datetime).* |
| procedure\_type\_concept\_id   | No                      | integer  | The type of source data from which the procedure record is derived. Refer to [procedure_type_concept_id.csv](https://github.com/cumc-dbmi/pmi_sprint_reporter/blob/master/resources/valid_concepts/procedure_occurrence/procedure_type_concept_id.csv) for the list of allowed concept_ids. |
| modifier\_concept\_id          | No                      | integer  | Set to 0 for this data sprint. |
| quantity                       | No                      | integer  | The quantity of procedures ordered or administered. |
| provider\_id                   | No                      | integer  | Set to 0 for this data sprint. |
| visit\_occurrence\_id          | Yes                     | integer  | A foreign key to the visit in the visit table during which the Procedure was carried out. |
| procedure\_source\_value       | Yes                     | varchar  | The code for the Procedure as it appears in the source data. |
| procedure\_source\_concept\_id | Yes                     | integer  | The OMOP concept ID that corresponds to the procedure code, as indicated in the source data. The query in [procedure_source_concept_id.sql](https://github.com/cumc-dbmi/pmi_sprint_reporter/blob/master/resources/valid_concepts/procedure_occurrence/procedure_source_concept_id.sql) provides an example of how to retrieve the concept id for the ICD9 procedure code `98.51` (extracorporeal shockwave lithotripsy). |
| qualifier\_source\_value       | No                      | varchar  | The source code for the qualifier as it appears in the source data. |

<http://www.ohdsi.org/web/wiki/doku.php?id=documentation:cdm:procedure_occurrence>

## Notes
 * Procedure source codes are typically ICD9Proc, CPT-4, HCPCS or OPCS-4 codes.

The procedure code for the ambulatory surgery was extracorporeal shockwave lithotripsy (98.51). The following row would be exported for the procedure occurrence table:

Amb surg visit 

    "1","1","2008219","2016-01-01","2016-01-01T10:00:00-05:00","44786630","0","","0","4","98.51","2008219",""


# drug_exposure


| Field                      | Required For Export     | Type     | Description |
| -------------------------- | ----------------------- | -------- | ----------- |
| drug\_exposure\_id              | Yes                     | integer     | A system-generated unique identifier for each Drug utilization event. |
| person\_id                      | Yes                     | integer     | A foreign key identifier to the person who is subjected to the Drug. The demographic details of that person are stored in the person table. |
| drug\_concept\_id       | No                      | integer     | A standard, valid OMOP concept ID for the associated domain. It is typically derived from `procedure_source_concept_id`.  The RxNorm terminology is considered standard for the drug domain. If your source data does not use RxNorm (i.e. in `drug_source_concept_id`) then they need to be translated. The query in [drug_concept_id.sql](https://github.com/cumc-dbmi/pmi_sprint_reporter/blob/master/resources/valid_concepts/drug_exposure/drug_concept_id.sql) demonstrates how you may go about this. |
| drug\_exposure\_start\_date     | Yes                     | date        | The start date for the current instance of Drug utilization. Valid entries include a start date of a prescription, the date a prescription was filled, or the date on which a Drug administration procedure was recorded. |
| drug\_exposure\_start\_datetime | Yes                     | datetime    | The start date and time for the current instance of Drug utilization. Valid entries include a start date of a prescription, the date a prescription was filled, or the date on which a Drug administration procedure was recorded. *See [datetime](index.html#datetime).* |
| drug\_exposure\_end\_date       | No                      | date        | The end date for the current instance of Drug utilization. It is not available from all sources.                                                                                                                                                                                                                                          |
| drug\_exposure\_end\_datetime   | No                      | datetime    | The end date and time for the current instance of Drug utilization. It is not available from all sources. *See [datetime](index.html#datetime).* |
| drug\_type\_concept\_id         | No                      | integer     | Use this [table](https://github.com/cumc-dbmi/pmi_sprint_reporter/blob/master/resources/valid_concepts/drug_exposure/drug_exposure_type_concept_id.csv) of allowable concept IDs to fill in the appropriate value for the type of drug.                                                                                                                                                                                                                                           
| stop\_reason                    | No                      | varchar(20) | The reason the Drug was stopped. Reasons include regimen completed, changed, removed, etc.                                                                                                                                                                                                                                                |
| refills                         | No                      | integer     | The number of refills after the initial prescription. The initial prescription is not counted, values start with 0.                                                                                                                                                                                                                       |
| quantity                        | No                      | float       | The quantity of drug as recorded in the original prescription or dispensing record.                                                                                                                                                                                                                                                       |
| days\_supply                    | No                      | integer     | The number of days of supply of the medication as recorded in the original prescription or dispensing record.                                                                                                                                                                                                                             |
| sig                             | No                      | clob        | The directions (“signetur”) on the Drug prescription as recorded in the original prescription (and printed on the container) or dispensing record.                                                                                                                                                                                        |
| route\_concept\_id              | No                      | integer     | Use this [table](https://github.com/cumc-dbmi/pmi_sprint_reporter/blob/master/resources/valid_concepts/drug_exposure/route_concept_id.csv) of allowable concept IDs to fill in the appropriate value for code values for different route methods for administering medications.                                                                                                                                                                                       
| effective\_drug\_dose           | No                      | float       | Numerical value of Drug dose for this Drug Exposure record.                                                                                                                                                                                                                                                                               |
| dose\_unit\_concept\_ id        | No                      | integer     | This is the foreign key to the Standardized Vocabulary for units. The standard terminology for units is Unified Code for Units of Measure (UCUM). The query in [dose_unit_concept_id.sql](https://github.com/cumc-dbmi/pmi_sprint_reporter/blob/master/resources/valid_concepts/drug_exposure/dose_unit_concept_id.sql) retrieves all UCUM codes. |
| lot\_number                     | No                      | varchar     | An identifier assigned to a particular quantity or lot of Drug product from the manufacturer.                                                                                                                                                                                                                                             |
| provider\_id                    | No                      | integer     | Set to 0 for this data sprint.                                                                                                                                                                                                                                                                                                            |
| visit\_occurrence\_id           | No                      | integer     | A foreign key to the visit in the visit table during which the Drug Exposure was initiated.                                                                                                                                                                                                                                               |
| drug\_source\_value             | Yes                     | varchar     | The source code for the Drug as it appears in the source data. Acceptable vocabularies are RxNorm, NDC, and CVX. If none of these are available, then please provide the descriptive name of the drug.                                                                                                                           |
| drug\_source\_concept\_id       | No                      | integer     | This field contains the OMOP concept ID that corresponds to the drug code in the source system. The example query in [drug_source_concept_id.sql](https://github.com/cumc-dbmi/pmi_sprint_reporter/blob/master/resources/valid_concepts/drug_exposure/drug_source_concept_id.sql) retrieves the concept id associated with RxNorm code `311670` (Metoclopramide). |
| route\_source\_value            | Yes                     | varchar     | The information about the route of administration as detailed in the source.                                                                                                                                                                                                                                                              |
| dose\_unit\_source\_value       | Yes                     | varchar     | The information about the dose unit as detailed in the source.                                                                                                                                                                                                                                                                            |

<http://www.ohdsi.org/web/wiki/doku.php?id=documentation:cdm:drug_exposure>

In this clinical case, John Doe received metoclopramide 10 mg IV and morphine 6 mg IV in the ED, and then he was prescribed Flomax (tamsulosin) for three months. For these three medications, the data would be exported like this:

ED visit
    
    "1","1","19078921","2016-01-01","2016-01-01T03:12:00-05:00","2016-01-01","2016-01-01T03:12:00-05:00","38000180","","","","","","4112421","10", "8861","","0","1","311670","0","IV","mg/mL"


ED visit 

    "2","1","35605644","2016-01-01","2016-01-01T03:12:00-05:00","2016-01-01","2016-01-01T03:12:00-05:00","38000180","","","","","","4112421","10", "8861","","0","1","1731517","0","IV","mg/mL"


Clinic visit
 
    "3","1","40166540","2016-01-01","2016-01-01T03:12:00-05:00","2016-01-01","2016-01-01T03:12:00-05:00","38000177","","3","30","30","","4128794","", "0","","0","2","863669","0","Oral","mg"


# measurement


| Field                      | Required For Export     | Type     | Description |
| -------------------------- | ----------------------- | -------- | ----------- |
| measurement\_id                  | Yes                     | integer  | A unique identifier for each Measurement. |
| person\_id                       | Yes                     | integer  | A foreign key identifier to the Person about whom the measurement was recorded. The demographic details of that Person are stored in the PERSON table. |
| measurement\_concept\_id         | Yes                     | integer  | A standard, valid OMOP concept ID. It is typically derived from `measurement_source_concept_id`. The LOINC terminology is the standard for the measurement domain. If your source data does not use LOINC (i.e. in `measurement_source_concept_id`) then they need to be translated. The query in [measurement_concept_id.sql](https://github.com/cumc-dbmi/pmi_sprint_reporter/blob/master/resources/valid_concepts/measurement/measurement_concept_id.sql) demonstrates how you may go about this. |
| measurement\_date                | Yes                     | date     | The date of the Measurement. |
| measurement\_datetime            | Yes                     | datetime | The date and time of the Measurement. *See [datetime](index.html#datetime).* |
| measurement\_type\_concept\_id   | Yes                     | integer  | A foreign key to the predefined Concept in the Standardized Vocabularies reflecting the provenance from where the Measurement record was recorded. Use this [table](https://github.com/cumc-dbmi/pmi_sprint_reporter/blob/master/resources/valid_concepts/measurement/measurement_type_concept_id.csv) of allowable concept IDs to fill in the appropriate value for this field. |
| operator\_concept\_id            | No                      | integer  | Set to 0 for this data sprint. |
| value\_as\_number                | No                      | float    | A Measurement result where the result is expressed as a numeric value. |
| value\_as\_concept\_id           | No                      | integer  | Set to 0 for this data sprint. This sprint will only deal with numerical measurments. |
| unit\_concept\_id                | No                      | integer  | This is the foreign key to the Standardized Vocabulary for units. The standard terminology for units is Unified Code for Units of Measure (UCUM). The query in [unit_concept_id.sql](https://github.com/cumc-dbmi/pmi_sprint_reporter/blob/master/resources/valid_concepts/measurement/unit_concept_id.sql) retrieves all UCUM codes. |
| range\_low                       | No                      | float    | The lower limit of the normal range of the Measurement result. The lower range is assumed to be of the same unit of measure as the Measurement value. |
| range\_high                      | No                      | float    | The upper limit of the normal range of the Measurement. The upper range is assumed to be of the same unit of measure as the Measurement value. |
| provider\_id                     | No                      | integer  | Set to 0 for this data sprint. |
| visit\_occurrence\_id            | No                      | integer  | A foreign key to the Visit in the VISIT\_OCCURRENCE table during which the Measurement was recorded. |
| measurement\_source\_value       | Yes                     | varchar  | The Measurement name as it appears in the source data. Acceptable vocabularies are LOINC and SNOMED. |
| measurement\_source\_concept\_id | Yes                     | integer  | This field contains the OMOP concept ID that corresponds to the procedure code in the source system. The example query in [measurement_source_concept_id.sql](https://github.com/cumc-dbmi/pmi_sprint_reporter/blob/master/resources/valid_concepts/measurement/measurement_source_concept_id.sql) retrieves the concept id associated with LOINC code `2951-2` (serum sodium lab). |
| unit\_source\_value              | Yes                     | varchar  | The source code for the unit as it appears in the source data. |
| value\_source\_value             | Yes                     | varchar  | The source value associated with the content of the value\_as\_number or value\_as\_concept\_id as stored in the source data. |

<http://www.ohdsi.org/web/wiki/doku.php?id=documentation:cdm:measurement>

During John Doe’s ED visit, a basic metabolic panel was run with the following lab result values at 4:25 am:

| Na  | K   | Cl  | HCO3 | BUN | Creat | Gluc | Ca  |
|:-----:|:-----:|:-----:|:------:|:-----:|:-------:|:------:|:-----:|
| 137 | 4   | 99  | 23   | 21  | 1     | 123  | 9.4 |

The following is the export of the labs done during the ED visit:

ED visit (Na) 

    "1","1","3019550","2016-01-01", "2016-01-01T04:25:00-05:00","44818702","0","137","0","8753","","","0","1","2951-2","3019550","mM/l","137"

ED visit (K) 

    "2","1","3023103","2016-01-01","2016-01-01T04:25:00-05:00","44818702","0","4","0","8753","","","0","1","2823-3","3023103","mM/l","4"

ED visit (Cl) 

    "3","1","3014576","2016-01-01","2016-01-01T04:25:00-05:00","44818702","0","99","0","8753","","","0","1","2075-0","3014576","mM/l","99"

ED visit (HC03) 

    "4","1","3016293","2016-01-01","2016-01-01T04:25:00-05:00","44818702","0","23","0","8753","","","0","1","1963-8","3016293","mM/l","23"

ED visit (BUN) 

    "5","1","3013682","2016-01-01","2016-01-01T04:25:00-05:00","44818702","0","21","0","8840","","","0","1","3094-0","3013682","mg/dl","21"

ED visit (Creat)
 
    "6","1","3016723","2016-01-01","2016-01-01T04:25:00-05:00","44818702","0","1","0","8840","","","0","1","2160-0","3016723","mg/dl","1"

ED visit (Gluc) 

    "7","1","3004501","2016-01-01","2016-01-01T04:25:00-05:00","44818702","0","123","0","8840","","","0","1","2345-7","3004501","mg/dl","123"

ED visit (Ca) 

    "8","1","3006906","2016-01-01","2016-01-01T04:25:00-05:00","44818702","0","9.4","0","8840","","","0","1","17861-6","3006906","mg/dl","9.4"

[Proceed to File Transfer Procedures >>](file_transfer_procedures.md)
