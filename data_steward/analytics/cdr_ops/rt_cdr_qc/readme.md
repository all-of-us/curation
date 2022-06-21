# Registered Tier Quality Check
  
## How to run the quality checks
#### Quality checks for DEID: [DC-1517](https://precisionmedicineinitiative.atlassian.net/browse/DC-1517)

```python report_runner.py "cdr_deid_qa_report1_generalization_rule.py" -p project_id "my_project_id" -p com_cdr "my_com_cdr" -p deid_cdr "my_deid"  -p pipeline "my_pipeline" ```

```python report_runner.py "cdr_deid_qa_report2_row_suppression.py" -p project_id "my_project_id" -p deid_cdr "my_deid" -p com_cdr "my_com_cdr" ```

```python report_runner.py "cdr_deid_qa_report3_col_suppression.py" -p project_id "my_project_id" -p deid_cdr "my_deid" ```

```python report_runner.py "cdr_deid_qa_report4_dateshift.py" -p project_id "my_project_id" -p com_cdr "my_com_cdr" -p deid_cdr "my_deid" -p pipeline "my_pipeline"  ```

```python report_runner.py "cdr_deid_qa_report5_row_suppression_icd.py" -p project_id "my_project_id" -p deid_cdr "my_deid"  ```

```python report_runner.py "cdr_deid_qa_report6_fitdata.py" -p project_id "my_project_id" -p pipeline "my_pipeline" -p non_deid_fitbit "my_non_deid_fitbit" -p deid_cdr_fitbit "my_deid_cdr_fitbit" -p com_cdr "my_com_cdr" -p deid_cdr "my_deid_cdr" -p truncation_date "2019-11-26" -p maximum_age "89"  ```

```python report_runner.py "cdr_deid_qa_report7_cope_survey.py" -p project_id "my_project_id" -p deid_cdr "my_deid" -p com_cdr "my_com_cdr" -p deid_sandbox "my_deid_sandbox"   ```

```python report_runner.py "cdr_deid_qa_report8_household_state_genera.py" -p project_id "my_project_id" -p deid_cdr "my_deid" -p com_cdr "my_com_cdr"  ```

```python report_runner.py "cdr_deid_qa_report10_extra.py" -p project_id "my_project_id" -p deid_cdr "my_deid"  -p com_cdr "my_com_cdr" -p ct_deid "my_ct_deid" -p ct_deid_sand "my_ct_deid_sandbox" -p deid_sand "my_deid_sandbox" -p pipeline "my_pipeline" ```
  
#### Quality checks for DEID - COVID drug concept NO suppression: [DC-2119](https://precisionmedicineinitiative.atlassian.net/browse/DC-2119)
```python report_runner.py "cdr_deid_qa_report11_covid_concept_no_suppression.py" -p project_id "my_project_id" -p post_deid_dataset "my_deid" ```
  
#### Quality checks for DEID_BASE: [DC-1690](https://precisionmedicineinitiative.atlassian.net/browse/DC-1690) [DC-1404](https://precisionmedicineinitiative.atlassian.net/browse/DC-1404)

```python report_runner.py "cdr_deid_base_qa_report1.py" -p project_id "my_project_id" -p com_cdr "my_com_cdr"  -p deid_base_cdr "my_deid_base" -p pipeline "my_pipeline"  ```
  
#### Quality checks for DEID_CLEAN: [DC-1691](https://precisionmedicineinitiative.atlassian.net/browse/DC-1691)

```python report_runner.py "cdr_deid_clean_qa_report1.py" -p project_id "my_project_id" -p deid_clean "my_deid_clean" ```
