# Condition\_Occurrence Table

Conditions that are reported should be discharge diagnosis codes or problem list entries.

|                                |                         |             |                                                                                                                                                                                                                                                                                                                                           |
|--------------------------------|-------------------------|-------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **Field**                      | **Required For Export** | **Type**    | **Description**                                                                                                                                                                                                                                                                                                                           |
| condition\_occurrence\_id      | Yes                     | integer     | A unique identifier for each Condition Occurrence event.                                                                                                                                                                                                                                                                                  |
| person\_id                     | Yes                     | integer     | A foreign key identifier to the Person who is experiencing the condition. The demographic details of that Person are stored in the PERSON table.                                                                                                                                                                                          |
| condition\_concept\_id         | Yes                     | integer     | This field contains a standard, valid OMOP concept ID. It is derived from the condition\_source\_concept\_id. The SNOMED terminology is considered standard for the condition domain (table).                                                                                                                                             

                                                                          If your source data is not in SNOMED, then it needs to be converted to a standard SNOMED concept. The below query finds the standard concept that the CONDITION\_SOURCE\_CONCEPT\_ID maps to.                                                                                                                                              

                                                                          select c2.concept\_id                                                                                                                                                                                                                                                                                                                      

                                                                          from concept c1                                                                                                                                                                                                                                                                                                                            

                                                                          join  concept\_relationship cr ON c1.concept\_id = cr.concept\_id\_1                                                                                                                                                                                                                                                                       
                                                                          and cr.relationship\_id = 'Maps to'                                                                                                                                                                                                                                                                                                        

                                                                          join concept c2 ON c2.concept\_id = cr.concept\_id\_2                                                                                                                                                                                                                                                                                      

                                                                          where c1.concept\_id = &lt;CONDITION\_SOURCE\_CONCEPT\_ID&gt;                                                                                                                                                                                                                                                                              

                                                                          and c2.standard\_concept = 'S'                                                                                                                                                                                                                                                                                                             

                                                                          and c2.invalid\_reason is null                                                                                                                                                                                                                                                                                                             

                                                                          and c2.domain\_id='Condition'                                                                                                                                                                                                                                                                                                              

                                                                          NOTE: Not all ICD codes belong in the condition\_occurrence table. For example, ICD-9 CM code “V72.19” (Other examination of ears and hearing) is considered a procedure and thus should be placed in the procedure\_occurrence table.                                                                                                     |
| condition\_start\_date         | Yes                     | date        | The date when the instance of the Condition is recorded.                                                                                                                                                                                                                                                                                  |
| condition\_start\_datetime     | Yes                     | datetime    | The datetime of the start of the Condition. The datetime (or timestamp) will be represented as a string for the export file. The format will be in ISO 8601 where time is represented in UTC with time offset in the extended format - \[hh\]:\[mm\]:\[ss\] -                                                                             

                                                                          (<https://en.wikipedia.org/wiki/ISO_8601#Combined_date_and_time_representations)>. NOTE: for datetimes where the time is not known assume midnight (00:00 time). For example in Eastern time, UTC would be represented as 00:00:00-05:00. Please refer to all UTC offsets here: <https://en.wikipedia.org/wiki/List_of_UTC_time_offsets>.  |
| condition\_end\_date           | Yes                     | date        | The date when the instance of the Condition is considered to have ended.                                                                                                                                                                                                                                                                  |
| condition\_end\_datetime       | Yes                     | datetime    | The datetime when the instance of the Condtion is considered to have ended.                                                                                                                                                                                                                                                               

                                                                          The datetime (or timestamp) will be represented as a string for the export file. The format will be in ISO 8601 where time is represented in UTC with time offset in the extended format - \[hh\]:\[mm\]:\[ss\] -                                                                                                                          

                                                                          (<https://en.wikipedia.org/wiki/ISO_8601#Combined_date_and_time_representations)>. NOTE: for datetimes where the time is not known assume midnight (00:00 time). For example in Eastern time, UTC would be represented as 00:00:00-05:00. Please refer to all UTC offsets here: <https://en.wikipedia.org/wiki/List_of_UTC_time_offsets>.  

                                                                          For outpatient and ED visits when the end datetime may not be known, leave this field empty since the condition\_end\_date field will be populated.                                                                                                                                                                                        |
| condition\_type\_concept\_id   | Yes                     | integer     | Using this table of allowable concept IDs, fill in the appropriate value for this field:                                                                                                                                                                                                                                                  

                                                                          | Concept ID | Description                                    |                                                                                                                                                                                                                                                                            
                                                                          |------------|------------------------------------------------|                                                                                                                                                                                                                                                                            
                                                                          | 44786627   | Primary Condition                              |                                                                                                                                                                                                                                                                            
                                                                          | 44786629   | Secondary Condition                            |                                                                                                                                                                                                                                                                            
                                                                          | 38000245   | EHR problem list entry                         |                                                                                                                                                                                                                                                                            
                                                                          | 42894222   | EHR Chief Complaint                            |                                                                                                                                                                                                                                                                            
                                                                          | 0          | Data field is not present in the source system |                                                                                                                                                                                                                                                                            |
| stop\_reason                   | No                      | varchar(20) | The reason that the condition was no longer present, as indicated in the source data.                                                                                                                                                                                                                                                     |
| provider\_id                   | No                      | integer     | Set to 0 for this data sprint.                                                                                                                                                                                                                                                                                                            |
| visit\_occurrence\_id          | Yes                     | integer     | A foreign key to the visit in the VISIT table during which the Condition was determined (diagnosed).                                                                                                                                                                                                                                      |
| condition\_source\_value       | Yes                     | varchar     | The source code for the condition as it appears in the source data.                                                                                                                                                                                                                                                                       

                                                                          **NOTE**: This would be a ICD9, ICD10, or SNOMED code for a visit.                                                                                                                                                                                                                                                                         |
| condition\_source\_concept\_id | Yes                     | integer     | This field contains the OMOP concept ID that corresponds to the diagnosis code in the source system. Below is a query that would retrieve the concept id for the ICD9 code for calculus of kidney (592.0)                                                                                                                                 

                                                                          select concept\_id                                                                                                                                                                                                                                                                                                                         

                                                                          from concept as c                                                                                                                                                                                                                                                                                                                          

                                                                          where c.concept\_code = '592.0' and c.vocabulary\_id in ('ICD9CM', 'ICD10CM') and c.invalid\_reason IS NULL and c.domain\_id='Condition'                                                                                                                                                                                                   |

<http://www.ohdsi.org/web/wiki/doku.php?id=documentation:cdm:condition_occurrence>

The following were the diagnosis codes for each of the above visits:


| ED                 | Primary: Calculus of kidney (ICD9:592.0)                                               
                       Secondary: Abdominal pain, right low quandrant (ICD9:789.03). Renal Colic (ICD9:788.0) |
|--------------------|----------------------------------------------------------------------------------------|
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
