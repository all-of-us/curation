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

# # Ingredient usage across different sites

import pandas as pd
import xlrd
import matplotlib.pyplot as plt
import seaborn as sns
from math import pi
# %matplotlib inline

# +
sheets = []

fn1 = 'drug_success_table_sheets_analytics_report.xlsx'
file_names = [fn1]

s1 = 'ACE Inhibitors'
s2 = 'Pain NSAIDS'
s3 = 'MSK NSAIDS'
s4 = 'Statins'
s5 = 'Antibiotics'
s6 = 'Opioids'
s7 = 'Oral Hypoglycemics'
s8 = 'Vaccine'
s9 = 'Calcium Channel Blockers'
s10 = 'Diuretics'
s11 = 'All Drugs' 

sheet_names = [s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11]

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

# +
fig, ax = plt.subplots(figsize=(18, 12))

sns.heatmap(new_table_sheets['All Drugs'], annot=True, annot_kws={"size": 10},
            fmt='g', linewidths=.5, ax=ax, yticklabels=hpo_id_cols,
            xticklabels=date_cols, cmap="RdYlGn", vmin=0, vmax=100)
ax.set_ylim(len(hpo_id_cols)-0.1, 0)

ax.set_title("All Drugs Ingredient Usage", size=14)
plt.savefig("all_drugs_ingredient_table.png")
# -

# # Now let's look at the metrics for particular sites with respect to drug ingredient population. This will allow us to send them appropriate information.

fn1_hpo_sheets = 'drug_success_hpo_sheets_analytics_report.xlsx'
file_names_hpo_sheets = [fn1_hpo_sheets]

x1 = pd.ExcelFile(fn1_hpo_sheets)
site_name_list = x1.sheet_names

# +
num_hpo_sheets = len(site_name_list)

print(f"There are {num_hpo_sheets} HPO sheets.")

# +
name_of_interest = 'aggregate_info'

if name_of_interest not in site_name_list:
    raise ValueError("Name not found in the list of HPO site names.")    

for idx, site in enumerate(site_name_list):
    if site == name_of_interest:
        idx_of_interest = idx

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
#start_idx = 0 -- not sure why this is needed?

for sheet in hpo_sheets:
    sheet_cols = sheet.columns
    #sheet_cols = sheet_cols[start_idx:]
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
    g = sns.heatmap(data, annot=True, annot_kws={"size": 14},
            fmt='g', linewidths=.5, ax=ax, yticklabels=table_id_cols,
            xticklabels=date_cols, cmap="RdYlGn", vmin=0, vmax=100, mask=mask)
    g.set_facecolor("lightgrey")
    
    ax.set_title(f"Drug Integration Rates for {name_of_interest}", size=14)
    ax.set_ylim(len(table_id_cols)-0.1, 0)
    ax.set_xticklabels(ax.get_xticklabels(), rotation=45)

    plt.tight_layout()
    img_name = name_of_interest + "_drug_integration_rates.png"

    plt.savefig(img_name)
