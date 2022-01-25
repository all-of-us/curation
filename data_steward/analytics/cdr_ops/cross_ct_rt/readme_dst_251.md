
# how to run these notebooks:

python report_runner.py "dst_251_addtional_QCs_for_the_Controlled_Tier_part1.py" -p project_id "project_id" -p rt_dataset "rt_dataset_combined" -p ct_dataset "ct_dataset_clean"   -p cut_off_date "2021-04-01" -p earliest_ehr_date "1980-01-01"

python report_runner.py "dst_251_addtional_QCs_for_the_Controlled_Tier_part2_pdr.py" -p project_id "project_id" -p rt_dataset "rt_dataset_combined" -p ct_dataset "ct_dataset_clean" -p project_id2 "project_id_pdr" -p pdr_dataset "pdr_dataset" -p cut_off_date "2021-04-01" 
 








