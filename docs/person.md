# Person Table

| Field                          | Required For Export     | Type     | Description |
| ------------------------------ | ----------------------- | -------- | ----------- |
| person\_id                     | Yes                     | integer  | A unique identifier for each person. |
| gender\_concept\_id            | Yes                     | integer  | Using this table of allowable concept IDs, fill in the appropriate value for this field |
| ethnicity\_concept\_id         | Yes                     | integer  | Using this table of allowable concept IDs, fill in the appropriate value for this field |
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
