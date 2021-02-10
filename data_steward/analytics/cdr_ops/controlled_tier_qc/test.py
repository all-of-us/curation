# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.7.1
#   kernelspec:
#     display_name: 'Python 3.7.3 64-bit (''base'': conda)'
#     language: python
#     name: python37364bitbaseconda5b767f67375d47e0bc47dd4f7a1d12d0
# ---

# %load_ext autoreload
# %autoreload 2

from jinja2 import Template
import pandas as pd

project_id = "aou-res-curation-output-prod"
dataset = "R2020Q4R2"

# # Helpers

# ## Load check file

from code.config import (CHECK_LIST_CSV_FILE, FIELD_CSV_FILE, CONCEPT_CSV_FILE, TABLE_CSV_FILE, MAPPING_CSV_FILE)
from utils.helpers import load_check_file

load_check_file(CHECK_LIST_CSV_FILE)

load_check_file(TABLE_CSV_FILE)

load_check_file(CONCEPT_CSV_FILE, 'DC-1359')

load_check_file(FIELD_CSV_FILE, ['DC-1357','DC-1370'])

# ## Run check by row

from utils.helpers import run_check_by_row
from code.check_field_suppression import check_field_suppression

string_checks = load_check_file(FIELD_CSV_FILE, ['DC-1370'])
string_checks.head()

check_field_suppression(project_id, dataset, None, ['DC-1373'])

check_field_suppression(project_id, dataset, None, ['DC-1370'])

# +
from code.check_table_suppression import check_table_suppression

check_table_suppression(project_id, dataset, None, 'DC-1362')

# +
from code.check_concept_suppression import check_concept_suppression

check_concept_suppression(project_id, dataset, None, 'DC-1359')
# -

check_concept_suppression(project_id, dataset, None, 'DC-1366')



# +
from code.check_field_suppression import check_cancer_concept_suppression

check_cancer_concept_suppression(project_id, dataset, 'DC-1382', None)
# -














