# Quality

Quality measures

## Completeness

Determines to what degree fields in an OMOP dataset are populated.

| | |
| --- | --- |
| table_name | name of the OMOP table |
| column_name | name of the field in the table |
| table_row_count | total number of rows in the table |
| null_count | number of rows where no value was provided for the field |
| concept_zero_count | when a concept id is expected, the number of rows where `0` was provided |
| percent_populated | percent of rows where field is populated `1 - (null_count + concept_zero_count)/(table_row_count)` |
