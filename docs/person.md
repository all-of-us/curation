# Person Table

| Field                          | Required For Export     | Type     | Description |
| ------------------------------ | ----------------------- | -------- | ----------- |
| person\_id                     | Yes                     | integer  | A unique identifier for each person. |
| gender\_concept\_id            | Yes                     | integer  | Using [this table](https://github.com/cumc-dbmi/pmi_sprint_reporter/blob/master/resources/valid_concepts/person/gender_concept_id.csv) of allowable concept IDs, fill in the appropriate value for this field |
| year\_of\_birth                | Yes                     | integer  | The year of birth of the person. For data sources with date of birth, the year is extracted. For data sources where the year of birth is not available, the approximate year of birth is derived based on any age group categorization available. |
| month\_of\_birth               | Yes                     | integer  | The month of birth of the person. For data sources that provide the precise date of birth, the month is extracted and stored in this field. |
| day\_of\_birth                 | Yes                     | integer  | The day of the month of birth of the person. For data sources that provide the precise date of birth, the day is extracted and stored in this field. |
| datetime\_of\_birth            | Yes                     | datetime | The date and time of birth. The datetime (or timestamp) will be represented as a string for the export file. The format will be in ISO 8601 where time is represented in UTC with time offset in the extended format - [hh]:[mm]:[ss] - 
                                                                          (https://en.wikipedia.org/wiki/ISO_8601#Combined_date_and_time_representations). NOTE: for datetimes where the time is not known assume midnight (00:00 time). For example in Eastern time, UTC would be represented as 00:00:00-05:00. Please refer to all UTC offsets here: https://en.wikipedia.org/wiki/List_of_UTC_time_offsets. |
| race\_concept\_id              | Yes                     | integer  | Using [this table](https://github.com/cumc-dbmi/pmi_sprint_reporter/blob/master/resources/valid_concepts/person/race_concept_id.csv) of allowable concept IDs, fill in the appropriate value for this field |
| ethnicity\_concept\_id         | Yes                     | integer  | Using [this table](https://github.com/cumc-dbmi/pmi_sprint_reporter/blob/master/resources/valid_concepts/person/ethnicity_concept_id.csv) of allowable concept IDs, fill in the appropriate value for this field |
| location\_id                   | No                      | integer  | Leave blank |
| provider\_id                   | No                      | integer  | Leave blank |
| care\_site\_id                 | No                      | integer  | Leave blank |
| person\_source\_value          | No                      | varchar  | Leave blank |
| gender\_source\_value          | Yes                     | varchar  | The source code for the gender of the person as it appears in the source data. The original value is stored here for reference. Separate the code and value with a “:”. For example, “M:Male” |
| gender\_source\_concept\_id    | No                      | Integer  | Leave blank |
| race\_source\_value            | Yes                     | varchar  | The source code and value for the race of the person as it appears in the source data. Separate the code and value with a “:”. For example, “AA:African American” |
| race\_source\_concept\_id      | No                      | Integer  | Leave blank |
| ethnicity\_source\_value       | Yes                     | varchar  | The source code and value for the ethnicity of the person as it appears in the source data. Separate the code and value with a “:”. For example, “H:Hispanic” |
| ethnicity\_source\_concept\_id | No                      | integer  | Leave blank |

<http://www.ohdsi.org/web/wiki/doku.php?id=documentation:cdm:person>

For example, an African American, non-Hispanic, male patient named John Doe born on 1/2/1965 would have a row in the file as seen below. In this example, the source system represents African American as “A” and non-Hispanic as “NH”.

    "1", "8507", "1965", "1", "1", "1965-01-02T00:00:00-05:00", "38003599", "38003564","","","","", "M:Male","", "A:African American","", "NH:Non-Hispanic",""
