# Controlled Tier Quality Check
There are 2 notebooks for the controlled tier QC process.

1. check_controlled_tier.py
This notebook is the main notebook for the controlled tier QC. It covers the quality checks from the following tickets:
[DC-1370](https://precisionmedicineinitiative.atlassian.net/browse/DC-1370), [DC-1377](https://precisionmedicineinitiative.atlassian.net/browse/DC-1377), [DC-1346](https://precisionmedicineinitiative.atlassian.net/browse/DC-1346), [DC-1348](https://precisionmedicineinitiative.atlassian.net/browse/DC-1348), [DC-1355](https://precisionmedicineinitiative.atlassian.net/browse/DC-1355), [DC-1357](https://precisionmedicineinitiative.atlassian.net/browse/DC-1357), [DC-1359](https://precisionmedicineinitiative.atlassian.net/browse/DC-1359), [DC-1362](https://precisionmedicineinitiative.atlassian.net/browse/DC-1362), [DC-1364](https://precisionmedicineinitiative.atlassian.net/browse/DC-1364), [DC-1366](https://precisionmedicineinitiative.atlassian.net/browse/DC-1366), 
[DC-1368](https://precisionmedicineinitiative.atlassian.net/browse/DC-1368), [DC-1373](https://precisionmedicineinitiative.atlassian.net/browse/DC-1373), [DC-1382](https://precisionmedicineinitiative.atlassian.net/browse/DC-1382), [DC-1388](https://precisionmedicineinitiative.atlassian.net/browse/DC-1388), [DC-1496](https://precisionmedicineinitiative.atlassian.net/browse/DC-1496), [DC-1527](https://precisionmedicineinitiative.atlassian.net/browse/DC-1527), [DC-1535](https://precisionmedicineinitiative.atlassian.net/browse/DC-1535), [DC-2112](https://precisionmedicineinitiative.atlassian.net/browse/DC-2112)

2. check_controlled_tier_covid_concept_no_suppression.py
This notebook is for [DC-2119](https://precisionmedicineinitiative.atlassian.net/browse/DC-2119). DC-2119 is not included in `check_controlled_tier.py` because of the following reasons:
- DC-2119 applies to both the controlled and the registered tier. We are using the same logic for both for efficiency. The script for the registered tier is `data_steward/analytics/rt_cdr_qc/cdr_deid_qa_report11_covid_concept_no_suppression.py`
- DC-2119 is created explicitly for the May 2022 CDR. This script might be able to retire after the release is completed.
- DC-2119 checks for the COVID concept to be NOT suppressed. `check_controlled_tier.py` is optimized for checking suppression, and adding checking NO suppression to it would make the script unnecessarily complex.
  
  
## How to run the quality checks
1. check_controlled_tier.py
```
PYTHONPATH=./:$PYTHONPATH python analytics/cdr_ops/report_runner.py \
"analytics/cdr_ops/controlled_tier_qc/check_controlled_tier.py" \
--output_path "path_of_the_output" \
-p project_id "your_project" \
-p post_deid_dataset "your_post_deid_dataset" \
-p pre_deid_dataset "your_pre_deid_dataset" \
-p mapping_dataset "your_mapping_dataset"
```
  

2. check_controlled_tier_covid_concept_no_suppression.py
```
PYTHONPATH=./:$PYTHONPATH python analytics/cdr_ops/report_runner.py \
"analytics/cdr_ops/controlled_tier_qc/check_controlled_tier_covid_concept_no_suppression.py" \
-p project_id "your_project" \
-p post_deid_dataset "your_post_deid_dataset"
```
