# how to run these notebooks:

# Total have 9 notebooks for deid and the parameters are as follows: [DC-1517]

python report_runner.py "cdr_deid_qa_report1_generalization_rule.py" -p project_id "aou-res-curation-prod" -p com_cdr "2021q2r1_combined" -p deid_cdr "R2021q2r1_deid"  -p pipeline "pipeline_tables" 

python report_runner.py "cdr_deid_qa_report2_row_suppression.py" -p project_id "aou-res-curation-prod" -p deid_cdr "R2021q2r1_deid" -p com_cdr "2021q2r1_combined" 

python report_runner.py "cdr_deid_qa_report3_col_suppression.py" -p project_id "aou-res-curation-prod" -p deid_cdr "R2021q2r1_deid" 

python report_runner.py "cdr_deid_qa_report4_dateshift.py" -p project_id "aou-res-curation-prod" -p com_cdr "2021q2r1_combined" -p deid_cdr "R2021q2r1_deid" -p pipeline "pipeline_tables"  

python report_runner.py "cdr_deid_qa_report5_row_suppression_icd.py" -p project_id "aou-res-curation-prod" -p deid_cdr "R2021q2r1_deid" --output_path test51_deid_row_icd.html

python report_runner.py "cdr_deid_qa_report6_fitdata.py" -p project_id "aou-res-curation-prod" -p pipeline "pipeline_tables" -p non_deid "R2019q4r3_deid_io_fitbit" -p deid_cdr "R2020q4r1_fitbit_deid" -p com_cdr "2020q4r1_combined_release" 

 

python report_runner.py "cdr_deid_qa_report7_cope_survey.py" -p project_id "aou-res-curation-prod" -p deid_cdr "R2021q2r1_deid" -p com_cdr "2021q2r1_combined"  

python report_runner.py "cdr_deid_qa_report8_household_state_genera.py" -p project_id "aou-res-curation-prod" -p deid_cdr "R2021q2r1_deid" -p com_cdr "2021q2r1_combined" 

python report_runner.py "cdr_deid_qa_report10_extra.py" -p project_id "aou-res-curation-prod" -p deid_cdr "R2021q2r1_deid"  -p com_cdr "2021q2r1_combined" -p ct_deid "C2021q2r1_deid" -p ct_deid_sand "C2021q2r1_deid_sandbox" -p deid_sand "R2021q2r1_deid_sandbox" -p pipeline "pipeline_tables"


# have one notebook for deid_base. [DC-1690] [DC-1404]

python report_runner.py "cdr_deid_base_qa_report1.py" -p project_id "aou-res-curation-prod" -p com_cdr "2021q2r1_combined"  -p deid_base_cdr "R2021q2r1_deid_base" -p pipeline "pipeline_tables" 

# have one notebook for deid_clean. [Dc-1691]

python report_runner.py "cdr_deid_clean_qa_report1.py" -p project_id "aou-res-curation-prod" -p deid_clean "R2021q2r1_deid_clean" 

