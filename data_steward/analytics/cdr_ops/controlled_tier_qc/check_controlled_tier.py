# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.7.1
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# %load_ext autoreload
# %autoreload 2

# + tags=["parameters"]
project_id: str = ""  # identifies the project where datasets are located
post_deid_dataset: str = ""  # the CT deid dataset
pre_deid_dataset: str = ""  # the combined dataset
mapping_dataset: str = ""  # the sandbox dataset where mappings are

# +
import pandas as pd
from code.controlled_tier_qc import run_qc, display_check_summary_by_rule, display_check_detail_of_rule

pd.set_option('display.max_colwidth', -1)
pd.set_option('display.width', None)
# -

# to_include = ['DC-1370', 'DC-1377', 'DC-1346', 'DC-1348', 'DC-1355', 'DC-1357', 'DC-1359', 
#             'DC-1362', 'DC-1364', 'DC-1366', 'DC-1368', 'DC-1373',
#             'DC-1380', 'DC-1382', 'DC-1388', 'DC-1496']
to_include = []
checks = run_qc(project_id, post_deid_dataset, pre_deid_dataset, mapping_dataset, rule_code=to_include)

# For more information on each rule, click on the title. That will take you to the JIRA ticket that has more details of what the rule is supposed to check.

# # Summary

display_check_summary_by_rule(checks)

# # [DC-1370: String type field suppression](https://precisionmedicineinitiative.atlassian.net/browse/DC-1370)

display_check_detail_of_rule(checks, 'DC-1370')

# # [DC-1377: All Zip Code Values are generalized](https://precisionmedicineinitiative.atlassian.net/browse/DC-1377)

display_check_detail_of_rule(checks, 'DC-1377')

# # [DC-1346: PID to RID worked](https://precisionmedicineinitiative.atlassian.net/browse/DC-1346)

display_check_detail_of_rule(checks, 'DC-1346')

# # [DC-1348: Questionnaire_response_id Mapped](https://precisionmedicineinitiative.atlassian.net/browse/DC-1348)

display_check_detail_of_rule(checks, 'DC-1348')

# # [DC-1355: Site id mappings ran on the controlled tier](https://precisionmedicineinitiative.atlassian.net/browse/DC-1355)

display_check_detail_of_rule(checks, 'DC-1355')

# # [DC-1357: Person table does not have month or day of birth](https://precisionmedicineinitiative.atlassian.net/browse/DC-1357)

display_check_detail_of_rule(checks, 'DC-1357')

# # [DC-1359: Observation does not have birth information](https://precisionmedicineinitiative.atlassian.net/browse/DC-1359)

display_check_detail_of_rule(checks, 'DC-1359')

# # [DC-1362: Table suppression](https://precisionmedicineinitiative.atlassian.net/browse/DC-1362)

display_check_detail_of_rule(checks, 'DC-1362')

# # [DC-1364: Explicit identifier record suppression](https://precisionmedicineinitiative.atlassian.net/browse/DC-1364)

display_check_detail_of_rule(checks, 'DC-1364')

# # [DC-1366: Race/Ethnicity record suppression](https://precisionmedicineinitiative.atlassian.net/browse/DC-1366)

display_check_detail_of_rule(checks, 'DC-1366')

# # [DC-1368: Motor Vehicle Accident record suppression](https://precisionmedicineinitiative.atlassian.net/browse/DC-1368)

display_check_detail_of_rule(checks, 'DC-1368')

# # [DC-1373: Identifying field suppression works](https://precisionmedicineinitiative.atlassian.net/browse/DC-1373)

display_check_detail_of_rule(checks, 'DC-1373')

# # [DC-1380: Generalized Zip Code Values are Aggregated](https://precisionmedicineinitiative.atlassian.net/browse/DC-1380)

display_check_detail_of_rule(checks, 'DC-1380')

# # [DC-1382: Record Suppression of some cancer condition](https://precisionmedicineinitiative.atlassian.net/browse/DC-1382)

display_check_detail_of_rule(checks, 'DC-1382')

# # [DC-1388: Free Text survey response are suppressed](https://precisionmedicineinitiative.atlassian.net/browse/DC-1388)

display_check_detail_of_rule(checks, 'DC-1388')

# # [DC-1527: Suppression of organ transplant rows](https://precisionmedicineinitiative.atlassian.net/browse/DC-1527)

display_check_detail_of_rule(checks, 'DC-1527')

# # [DC-1535: Suppression of geolocation records](https://precisionmedicineinitiative.atlassian.net/browse/DC-1535)

display_check_detail_of_rule(checks, 'DC-1535')


