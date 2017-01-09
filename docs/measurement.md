# Measurement Table

|                                  |                         |          |                                                                                                                                                                                                                                |
|----------------------------------|-------------------------|----------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **Field**                        | **Required For Export** | **Type** | **Description**                                                                                                                                                                                                                |
| measurement\_id                  | Yes                     | integer  | A unique identifier for each Measurement.                                                                                                                                                                                      |
| person\_id                       | Yes                     | integer  | A foreign key identifier to the Person about whom the measurement was recorded. The demographic details of that Person are stored in the PERSON table.                                                                         |
| measurement\_concept\_id         | Yes                     | integer  | A foreign key to the standard measurement concept identifier in the Standardized Vocabularies.                                                                                                                                 

                                                                         This field contains a standard, valid OMOP concept ID. It is derived from the procedure\_source\_concept\_id. The LOINC terminology tends to be the standard for the measurement domain (table).                                

                                                                         If your source data is a standard terminology, then it needs to be converted to a standard concept. The below query finds the standard concept that the PROCEDURE\_SOURCE\_CONCEPT\_ID maps to.                                 

                                                                         select c2.concept\_id                                                                                                                                                                                                           

                                                                         from concept c1                                                                                                                                                                                                                 

                                                                         join  concept\_relationship cr ON c1.concept\_id = cr.concept\_id\_1                                                                                                                                                            
                                                                         and cr.relationship\_id = 'Maps to'                                                                                                                                                                                             

                                                                         join concept c2 ON c2.concept\_id = cr.concept\_id\_2                                                                                                                                                                           

                                                                         where c1.concept\_id = &lt;MEASUREMENT\_SOURCE\_CONCEPT\_ID&gt;                                                                                                                                                                 

                                                                         and c2.standard\_concept = 'S'                                                                                                                                                                                                  

                                                                         and c2.invalid\_reason is null                                                                                                                                                                                                  

                                                                         and c2.domain\_id='Measurement'                                                                                                                                                                                                 |
| measurement\_date                | Yes                     | date     | The date of the Measurement.                                                                                                                                                                                                   |
| measurement\_datetime            | Yes                     | datetime | The datetime of the Measurement.                                                                                                                                                                                               |
| measurement\_type\_concept\_id   | Yes                     | integer  | A foreign key to the predefined Concept in the Standardized Vocabularies reflecting the provenance from where the Measurement record was recorded.                                                                             

                                                                         Using this table of allowable concept IDs, fill in the appropriate value for this field:                                                                                                                                        

                                                                         | Concept ID | Description               |                                                                                                                                                                                      
                                                                         |------------|---------------------------|                                                                                                                                                                                      
                                                                         | 5001       | Test ordered through EHR  |                                                                                                                                                                                      
                                                                         | 44818701   | From physical examination |                                                                                                                                                                                      
                                                                         | 44818702   | Lab result                |                                                                                                                                                                                      
                                                                         | 44818703   | Pathology finding         |                                                                                                                                                                                      
                                                                         | 44818704   | Patient reported value    |                                                                                                                                                                                      
                                                                         | 45754907   | Derived value             |                                                                                                                                                                                      
                                                                         | 0          | Unknown                   |                                                                                                                                                                                      |
| operator\_concept\_id            | No                      | integer  | Set to 0 for this data sprint.                                                                                                                                                                                                 |
| value\_as\_number                | No                      | float    | A Measurement result where the result is expressed as a numeric value.                                                                                                                                                         |
| value\_as\_concept\_id           | No                      | integer  | Set to 0 for this data sprint. This sprint will only deal with numerical measurments.                                                                                                                                          |
| unit\_concept\_id                | No                      | integer  | This is the foreign key to the Standardized Vocabulary for units. The standard terminology for units is Unified Code for Units of Measure (UCUM). The below query against the OMOP CONCEPT table will retrieve all UCUM codes.

                                                                         select c.\*                                                                                                                                                                                                                     

                                                                         from concept as c                                                                                                                                                                                                               

                                                                         where c.vocabulary\_id='UCUM' and                                                                                                                                                                                               

                                                                         c.standard\_concept='S' and                                                                                                                                                                                                     

                                                                         c.domain\_id='Unit'                                                                                                                                                                                                             |
| range\_low                       | No                      | float    | The lower limit of the normal range of the Measurement result. The lower range is assumed to be of the same unit of measure as the Measurement value.                                                                          |
| range\_high                      | No                      | float    | The upper limit of the normal range of the Measurement. The upper range is assumed to be of the same unit of measure as the Measurement value.                                                                                 |
| provider\_id                     | No                      | integer  | Set to 0 for this data sprint.                                                                                                                                                                                                 |
| visit\_occurrence\_id            | No                      | integer  | A foreign key to the Visit in the VISIT\_OCCURRENCE table during which the Measurement was recorded.                                                                                                                           |
| measurement\_source\_value       | Yes                     | varchar  | The Measurement name as it appears in the source data.                                                                                                                                                                         

                                                                         Acceptable vocabularies are LOINC and SNOMED                                                                                                                                                                                    |
| measurement\_source\_concept\_id | Yes                     | integer  | This field contains the OMOP concept ID that corresponds to the procedure code in the source system. Below is a query that would retrieve the concept id for the LOINC code for a serum sodium lab (2951-2)                    

                                                                         select concept\_id                                                                                                                                                                                                              

                                                                         from concept as c                                                                                                                                                                                                               

                                                                         where c.concept\_code = '2951-2' and                                                                                                                                                                                            

                                                                         c.invalid\_reason IS NULL and                                                                                                                                                                                                   

                                                                         c.domain\_id='Measurement'                                                                                                                                                                                                      |
| unit\_source\_value              | Yes                     | varchar  | The source code for the unit as it appears in the source data.                                                                                                                                                                 |
| value\_source\_value             | Yes                     | varchar  | The source value associated with the content of the value\_as\_number or value\_as\_concept\_id as stored in the source data.                                                                                                  |

<http://www.ohdsi.org/web/wiki/doku.php?id=documentation:cdm:measurement>

During John Doe’s ED visit, a basic metabolic panel was run with the following lab result values at 4:25 am:

| Na  | K   | Cl  | HCO3 | BUN | Creat | Gluc | Ca  |
|-----|-----|-----|------|-----|-------|------|-----|
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
