# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.4'
#       jupytext_version: 1.2.3
#   kernelspec:
#     display_name: Python 2
#     language: python
#     name: python2
# ---

# ### Notebook is intended to test the efficacy of DC-389:
#
# ##### DC-389:
# Checks the concept table in the CDR to ensure that each of the concept IDs:
# - matches a concept_ID in the concept table
# - all of the standard_concept fields are of type 'S'
# - domain_id of the concept matches the domain_id
#
# Concept IDs should be replaced if it does not meet all of the three criteria are not met.

import json
import pandas as pd
import matplotlib.pyplot as plt

# +
from notebooks import bq, render
from notebooks.parameters import SANDBOX, CONCEPT_DATASET_ID

pd.set_option('display.max_colwidth', -1)

# +
# Fully qualified tables
TABLE_BEFORE_CONVERSION = '' # e.g. deid.measurement
TABLE_AFTER_CONVERSION = ''  # e.g. deid_clean.measurement

print("""TABLE_BEFORE_CONVERSION = {TABLE_BEFORE_CONVERSION}
TABLE_AFTER_CONVERSION = {TABLE_AFTER_CONVERSION}""").format(
        TABLE_BEFORE_CONVERSION=TABLE_BEFORE_CONVERSION, 
    TABLE_AFTER_CONVERSION=TABLE_AFTER_CONVERSION
)
# -


