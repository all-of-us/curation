# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.3.0
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# NOTES:
# 1. matplotlib MUST be in 3.1.0; 3.1.1 ruins the heatmap

# # Across-Site Statistics for Person ID Failure Rates
#
# - Cases where the person_id does not exist in the person table or is null
#
# ### NOTE: Aggregate info is weighted by the contribution of each site

import pandas as pd
import xlrd
import matplotlib.pyplot as plt
import seaborn as sns
from math import pi
# %matplotlib inline

# +
sheets = []

fn1 = 'person_id_failure_rate_table_sheets_analytics_report.xlsx'
file_names = [fn1]

s1 = 'Observation'
s2 = 'Measurement'
s3 = 'Visit Occurrence'
s4 = 'Procedure Occurrence'
s5 = 'Drug Exposure'
s6 = 'Condition Occurrence'

sheet_names = [s1, s2, s3, s4, s5, s6]

# +
table_sheets = []

for file in file_names:
    for sheet in sheet_names:
        s = pd.read_excel(file, sheet, index_col=0)
        table_sheets.append(s)

date_cols = table_sheets[0].columns
date_cols = (list(date_cols))

hpo_id_cols = table_sheets[0].index
hpo_id_cols = (list(hpo_id_cols))
# -

# ### Converting the numbers as needed and putting into a dictionary

# +
new_table_sheets = {}

for name, sheet in zip(sheet_names, table_sheets):
    sheet_cols = sheet.columns
    sheet_cols = sheet_cols[0:]
    new_df = pd.DataFrame(columns=sheet_cols)

    for col in sheet_cols:
        old_col = sheet[col]
        new_col = pd.to_numeric(old_col, errors='coerce')
        new_df[col] = new_col

    new_table_sheets[name] = new_df
# -

# ### Fixing typos

# +
fig, ax = plt.subplots(figsize=(18, 12))
sns.heatmap(new_table_sheets['Condition Occurrence'], annot=True, annot_kws={"size": 10},
            fmt='g', linewidths=.5, ax=ax, yticklabels=hpo_id_cols,
            xticklabels=date_cols, cmap="YlGnBu", vmin=0, vmax=100)
ax.set_ylim(len(hpo_id_cols)-0.1, 0)

ax.set_title("Condition Table Person ID Failure Rate", size=14)
# plt.savefig("condition_table_person_id_failure_rate.jpg")

# +
fig, ax = plt.subplots(figsize=(18, 12))
sns.heatmap(new_table_sheets['Drug Exposure'], annot=True, annot_kws={"size": 10},
            fmt='g', linewidths=.5, ax=ax, yticklabels=hpo_id_cols,
            xticklabels=date_cols, cmap="YlGnBu", vmin=0, vmax=100)
ax.set_ylim(len(hpo_id_cols)-0.1, 0)

ax.set_title("Drug Table Person ID Failure Rate", size=14)
# plt.savefig("drug_table_person_id_failure_rate.jpg")

# +
fig, ax = plt.subplots(figsize=(18, 12))
sns.heatmap(new_table_sheets['Measurement'], annot=True, annot_kws={"size": 10},
            fmt='g', linewidths=.5, ax=ax, yticklabels=hpo_id_cols,
            xticklabels=date_cols, cmap="YlGnBu", vmin=0, vmax=100)
ax.set_ylim(len(hpo_id_cols)-0.1, 0)

ax.set_title("Measurement Table Person ID Failure Rate", size=14)
# plt.savefig("measurement_table_person_id_failure_rate.jpg")

# +
fig, ax = plt.subplots(figsize=(18, 12))
sns.heatmap(new_table_sheets['Observation'], annot=True, annot_kws={"size": 10},
            fmt='g', linewidths=.5, ax=ax, yticklabels=hpo_id_cols,
            xticklabels=date_cols, cmap="YlGnBu", vmin=0, vmax=100)
ax.set_ylim(len(hpo_id_cols)-0.1, 0)

ax.set_title("Observation Table Person ID Failure Rate", size=14)
# plt.savefig("observation_table_person_id_failure_rate.jpg")

# +
fig, ax = plt.subplots(figsize=(18, 12))
sns.heatmap(new_table_sheets['Procedure Occurrence'], annot=True, annot_kws={"size": 10},
            fmt='g', linewidths=.5, ax=ax, yticklabels=hpo_id_cols,
            xticklabels=date_cols, cmap="YlGnBu", vmin=0, vmax=100)
ax.set_ylim(len(hpo_id_cols)-0.1, 0)

ax.set_title("Procedure Table Person ID Failure Rate", size=14)
# plt.savefig("procedure_table_person_id_failure_rate.jpg")

# +
fig, ax = plt.subplots(figsize=(18, 12))
sns.heatmap(new_table_sheets['Visit Occurrence'], annot=True, annot_kws={"size": 10},
            fmt='g', linewidths=.5, ax=ax, yticklabels=hpo_id_cols,
            xticklabels=date_cols, cmap="YlGnBu", vmin=0, vmax=100)
ax.set_ylim(len(hpo_id_cols)-0.1, 0)

ax.set_title("Visit Table Person ID Failure Rate", size=14)
# plt.savefig("visit_table_person_id_failure_rate.jpg")
# -

# # Now let's look at the metrics for particular sites with respect to person ID failure rate. This will allow us to send the appropriate information.

fn1_hpo_sheets = 'person_id_failure_rate_hpo_sheets_analytics_report.xlsx'
file_names_hpo_sheets = [fn1_hpo_sheets]

x1 = pd.ExcelFile(fn1_hpo_sheets)
site_name_list = x1.sheet_names

# +
num_hpo_sheets = len(site_name_list)

print(f"There are {num_hpo_sheets} HPO sheets.")
# -

# name_of_interest = 'aggregate_info'
#
# if name_of_interest not in site_name_list:
#     raise ValueError("Name not found in the list of HPO site names.")    
#
# for idx, site in enumerate(site_name_list):
#     if site == name_of_interest:
#         idx_of_interest = idx

# +
hpo_sheets = []

for file in file_names_hpo_sheets:
    for sheet in site_name_list:
        s = pd.read_excel(file, sheet, index_col=0)
        hpo_sheets.append(s)
        
table_id_cols = list(hpo_sheets[0].index)

date_cols = table_sheets[0].columns
date_cols = (list(date_cols))

# +
new_hpo_sheets = []

for sheet in hpo_sheets:
    sheet_cols = sheet.columns
    new_df = pd.DataFrame(columns=sheet_cols)

    for col in sheet_cols:
        old_col = sheet[col]
        new_col = pd.to_numeric(old_col, errors='coerce')
        new_df[col] = new_col

    new_hpo_sheets.append(new_df)
# -

# ### Showing for one particular site

for i in range(len(site_name_list)):
    name_of_interest = site_name_list[i]
    idx_of_interest = i
    fig, ax = plt.subplots(figsize=(9, 6))
    data = new_hpo_sheets[idx_of_interest]
    mask = data.isnull()
    g = sns.heatmap(new_hpo_sheets[idx_of_interest], annot=True, annot_kws={"size": 14},
                fmt='g', linewidths=.5, ax=ax, yticklabels=table_id_cols,
                xticklabels=date_cols, cmap="YlOrBr", vmin=0, vmax=100, mask=mask)
    g.set_facecolor("lightgrey")

    ax.set_title(f"Person ID Failure Rates for {name_of_interest}", size=14)
    ax.set_ylim(len(table_id_cols)-0.1, 0)
    ax.set_xticklabels(ax.get_xticklabels(), rotation=45)

    plt.tight_layout()
    img_name = name_of_interest + "_person_id_failure_rate.png"

    plt.savefig(img_name)
