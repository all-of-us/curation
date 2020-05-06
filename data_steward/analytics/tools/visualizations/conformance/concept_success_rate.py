# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.4.2
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# NOTES:
# 1. matplotlib MUST be in 3.1.0; 3.1.1 ruins the heatmap

# # Across-Site Statistics for Concept Success Rates
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

fn1 = 'concept_table_sheets_analytics_report.xlsx'
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
        s = pd.read_excel(file, sheet)
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
            xticklabels=date_cols, cmap="RdYlGn", vmin=0, vmax=100)

ax.set_title("Condition Table Concept Success Rate", size=14)
# plt.savefig("condition_table_concept_success_rate.jpg")

# +
fig, ax = plt.subplots(figsize=(18, 12))
sns.heatmap(new_table_sheets['Drug Exposure'], annot=True, annot_kws={"size": 10},
            fmt='g', linewidths=.5, ax=ax, yticklabels=hpo_id_cols,
            xticklabels=date_cols, cmap="RdYlGn", vmin=0, vmax=100)

ax.set_title("Drug Table Concept Success Rate", size=14)
# plt.savefig("drug_table_concept_success_rate.jpg")

# +
fig, ax = plt.subplots(figsize=(18, 12))
sns.heatmap(new_table_sheets['Measurement'], annot=True, annot_kws={"size": 10},
            fmt='g', linewidths=.5, ax=ax, yticklabels=hpo_id_cols,
            xticklabels=date_cols, cmap="RdYlGn", vmin=0, vmax=100)

ax.set_title("Measurement Table Concept Success Rate", size=14)
# plt.savefig("measurement_table_concept_success_rate.jpg")

# +
fig, ax = plt.subplots(figsize=(18, 12))
sns.heatmap(new_table_sheets['Observation'], annot=True, annot_kws={"size": 10},
            fmt='g', linewidths=.5, ax=ax, yticklabels=hpo_id_cols,
            xticklabels=date_cols, cmap="RdYlGn", vmin=0, vmax=100)

ax.set_title("Observation Table Concept Success Rate", size=14)
# plt.savefig("observation_table_concept_success_rate.jpg")

# +
fig, ax = plt.subplots(figsize=(18, 12))
sns.heatmap(new_table_sheets['Procedure Occurrence'], annot=True, annot_kws={"size": 10},
            fmt='g', linewidths=.5, ax=ax, yticklabels=hpo_id_cols,
            xticklabels=date_cols, cmap="RdYlGn", vmin=0, vmax=100)

ax.set_title("Procedure Table Concept Success Rate", size=14)
# plt.savefig("procedure_table_concept_success_rate.jpg")

# +
fig, ax = plt.subplots(figsize=(18, 12))
sns.heatmap(new_table_sheets['Visit Occurrence'], annot=True, annot_kws={"size": 10},
            fmt='g', linewidths=.5, ax=ax, yticklabels=hpo_id_cols,
            xticklabels=date_cols, cmap="RdYlGn", vmin=0, vmax=100)


ax.set_title("Visit Table Concept Success Rate", size=14)
# plt.savefig("visit_table_concept_success_rate.jpg")
# -

# # Now let's look at the metrics for particular sites with respect to concept success rate. This will allow us to send them the appropriate information.

fn1_hpo_sheets = 'concept_hpo_sheets_analytics_report.xlsx'
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
        s = pd.read_excel(file, sheet)
        hpo_sheets.append(s)
        

table_id_cols = list(hpo_sheets[0].index)

date_cols = table_sheets[0].columns
date_cols = (list(date_cols))

# +
new_hpo_sheets = []
start_idx = 0

for sheet in hpo_sheets:
    sheet_cols = sheet.columns
    sheet_cols = sheet_cols[start_idx:]
    new_df = pd.DataFrame(columns=sheet_cols)

    for col in sheet_cols:
        old_col = sheet[col]
        new_col = pd.to_numeric(old_col, errors='coerce')
        new_df[col] = new_col

    new_hpo_sheets.append(new_df)
# -

# ### Showing for one particular site

# +
fig, ax = plt.subplots(figsize=(9, 6))
sns.heatmap(new_hpo_sheets[idx_of_interest], annot=True, annot_kws={"size": 14},
            fmt='g', linewidths=.5, ax=ax, yticklabels=table_id_cols,
            xticklabels=date_cols, cmap="RdYlGn", vmin=0, vmax=100)

ax.set_title(f"Concept Success Rates for {name_of_interest}", size=14)

plt.tight_layout()
img_name = name_of_interest + "_concept_success_rate.png"

plt.savefig(img_name)
# -

dates = new_hpo_sheets[0].columns.tolist()

# ## NOTE: This is more experimental than anything else; Could be improved upon in the future. This particular graphic is also only quasi-informative.

# +
num_tables = len(new_hpo_sheets[0]) - 1  # do not include aggregate

angles = [(angle / num_tables) * (2 * pi) for angle in range(num_tables)]
angles += angles[:1] # back to the start


# +
max_val = 0

site = new_hpo_sheets[idx_of_interest]
site_name = site_name_list[idx_of_interest]

for date in dates:
    date_vals = site[date].values
    date_vals = date_vals.flatten().tolist()[:-1]  # cut off aggregate
    
    for value in date_vals:
        if value > max_val:
            max_val = value

y_ticks = [max_val / 5, max_val * 2 / 5, max_val * 3 / 5, max_val * 4 / 5, max_val]
y_ticks_str = []

for val in y_ticks:
    string = str(val)
    y_ticks_str.append(string)

# +
fig, ax = plt.subplots(figsize=(9, 6))
ax = plt.subplot(111, polar = True)  # initialize

ax.set_theta_offset(pi / 2)  # flip to top
ax.set_theta_direction(-1)

plt.xticks(angles[:-1],table_id_cols)

# Draw ylabels
ax.set_rlabel_position(0)
plt.yticks(y_ticks, y_ticks_str, color="grey", size=8)
plt.ylim(0, max_val)


for date_idx in range(len(dates)):
    date_vals = site[dates[date_idx]].values
    date = date_vals.flatten().tolist()[:-1]  # cut off aggregate
    date += date[:1]  # round out the graph
    
    ax.plot(angles, date, linewidth=1, linestyle='solid', label=dates[date_idx])
    ax.fill(angles, date, alpha=0.1)

plt.title(f"Concept Success Rate: {name_of_interest}", size=15, y = 1.1)
plt.legend(loc='upper right', bbox_to_anchor=(0.1, 0.1))
# -

dates = new_hpo_sheets[idx_of_interest].columns

# ## Want a line chart over time.

times=new_hpo_sheets[idx_of_interest].columns.tolist()

# +
success_rates = {}

for table_num, table_type in enumerate(table_id_cols):
    table_metrics_over_time = new_hpo_sheets[idx_of_interest].iloc[table_num]
    success_rates[table_type] = table_metrics_over_time.values.tolist()

date_idxs = []
for x in range(len(dates)):
    date_idxs.append(x)

# +
for table, values_over_time in success_rates.items():
    sample_list = [x for x in success_rates[table] if str(x) != 'nan']
    if len(sample_list) > 1:
        plt.plot(date_idxs, success_rates[table], '--', label=table)
    
for table, values_over_time in success_rates.items():
    non_nan_idx = 0
    new_lst = []
    
    for idx, x in enumerate(success_rates[table]):
        if str(x) != 'nan':
            new_lst.append(x)
            non_nan_idx = idx
    
    if len(new_lst) == 1:
        plt.plot(date_idxs[non_nan_idx], new_lst, 'o', label=table)

plt.legend(loc="upper left", bbox_to_anchor=(1,1))
plt.title(f"{name_of_interest} Concept Success Rates Over Time")
plt.ylabel("Concept Success Rate (%)")
plt.xlabel("")
plt.xticks(date_idxs, times, rotation = 'vertical')

handles, labels = ax.get_legend_handles_labels()
lgd = ax.legend(handles, labels, loc='upper center', bbox_to_anchor=(0.5,-0.1))

img_name = name_of_interest + "_concept_success_rate_line_graph.jpg"
# plt.savefig(img_name, bbox_extraartist=(lgd,), bbox_inches='tight')
# -

