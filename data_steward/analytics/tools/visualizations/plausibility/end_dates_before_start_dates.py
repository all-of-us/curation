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

# # Across-Site Statistics for End Dates Preceding Start Dates

import pandas as pd
import xlrd
import matplotlib.pyplot as plt
import seaborn as sns
from math import pi

# +
sheets = []

fn1 = 'end_before_begin_table_sheets_analytics_report.xlsx'
file_names = [fn1]

s1 = 'Visit Occurrence'
s2 = 'Drug Exposure'
s3 = 'Condition Occurrence'

sheet_names = [s1, s2, s3]

# +
table_sheets = []

for file in file_names:
    for sheet in sheet_names:
        s = pd.read_excel(file, sheet)
        table_sheets.append(s)

hpo_id_cols = table_sheets[0]
hpo_id_cols  = list(hpo_id_cols[hpo_id_cols.columns[0]])  # reset to first column

date_cols = table_sheets[0].columns[1:]
# -

# ### Converting the numbers as needed and putting into a dictionary

# +
new_table_sheets = {}

for name, sheet in zip(sheet_names, table_sheets):
    sheet_cols = sheet.columns
    sheet_cols = sheet_cols[1:]
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
            xticklabels=date_cols, cmap="YlGnBu")

ax.set_title("Condition Table End Dates Preceding Start Dates", size=14)
# plt.savefig("condition_table_end_before_begin.jpg")

# +
fig, ax = plt.subplots(figsize=(18, 12))
sns.heatmap(new_table_sheets['Drug Exposure'], annot=True, annot_kws={"size": 10},
            fmt='g', linewidths=.5, ax=ax, yticklabels=hpo_id_cols,
            xticklabels=date_cols, cmap="YlGnBu")

ax.set_title("Drug Table End Dates Preceding Start Dates", size=14)
# plt.savefig("drug_table_end_before_begin.jpg")

# +
fig, ax = plt.subplots(figsize=(18, 12))
sns.heatmap(new_table_sheets['Visit Occurrence'], annot=True, annot_kws={"size": 10},
            fmt='g', linewidths=.5, ax=ax, yticklabels=hpo_id_cols,
            xticklabels=date_cols, cmap="YlGnBu")


ax.set_title("Visit Table End Dates Preceding Start Dates", size=14)
# plt.savefig("visit_table_end_before_begin.jpg")
# -

# ## Creating a box-and-whisker plot for the different table types across all sites
#
# #### NOTE: This doesn't work super well. Not saving as an image. Might be helpful down the line so keeping it for now.

# sns.set(style = "ticks")
# f, ax = plt.subplots(figsize=(18, 12))
#
# date = 'september_16_2019'
#
# date_info = {}
#
# for table_type in sheet_names:
#     date_info[table_type] = new_table_sheets[table_type][date].tolist()
#
# july_15_df = pd.DataFrame.from_dict(date_info)
#
# sns.boxplot(data=july_15_df, 
#             whis = "range", palette="vlag")
#
# sns.swarmplot(data=july_15_df,
#               size = 5, color=".3", linewidth=0)
#
# plt.ylabel(ylabel="Success Rate", size=16)
# plt.xlabel(xlabel="\nTable Type", size=16)
# plt.title("End Dates Preceding Start Dates for {}".format(date), size = 18)
# sns.despine(trim=True, left=True)

# # Now let's look at the metrics for particular sites with respect to date/datetime disparity; this will allow us to send them the same information

# +
site_name_list = ['aouw_mcri', 'aouw_mcw', 'aouw_uwh', 'chci', 'chs', 'cpmc_ceders', 
                  'cpmc_ucd', 'cpmc_uci', 'cpmc_ucsd', 'cpmc_ucsf', 'cpmc_usc', 'ecchc',
                  'hrhc', 'ipmc_northshore', 'ipmc_nu', 'ipmc_rush', 'ipmc_uchicago',
                  'ipmc_uic', 'jhchc', 'nec_bmc', 'nec_phs', 'nyc_cornell', 'nyc_cu',
                  'nyc_hh', 'pitt', 'pitt_temple', 'saou_lsu', 'saou_tul', 'saou_uab',
                  'saou_uab_hunt', 'saou_uab_selma', 'saou_umc',
                  'saou_ummc', 'seec_emory', 'seec_miami', 'seec_morehouse',
                  'seec_ufl', 'syhc', 'tach_hfhs', 'trans_am_baylor',
                  'trans_am_essentia', 'trans_am_meyers', 'trans_am_spectrum', 'uamc_banner',
                  'va', 'aggregate_info']

print(len(site_name_list))

# +
name_of_interest = 'aggregate_info'

if name_of_interest not in site_name_list:
    raise ValueError("Name not found in the list of HPO site names.")    

for idx, site in enumerate(site_name_list):
    if site == name_of_interest:
        idx_of_interest = idx

# +
fn1_hpo_sheets = 'end_before_begin_hpo_sheets_analytics_report.xlsx'
file_names_hpo_sheets = [fn1_hpo_sheets]

s1, s2 = site_name_list[0], site_name_list[1]
s3, s4 = site_name_list[2], site_name_list[3]
s5, s6 = site_name_list[4], site_name_list[5]
s7, s8 = site_name_list[6], site_name_list[7]
s9, s10 = site_name_list[8], site_name_list[9]
s11, s12 = site_name_list[10], site_name_list[11]
s13, s14 = site_name_list[12], site_name_list[13]
s15, s16 = site_name_list[14], site_name_list[15]
s17, s18 = site_name_list[16], site_name_list[17]
s19, s20 = site_name_list[18], site_name_list[19]
s21, s22 = site_name_list[20], site_name_list[21]
s23, s24 = site_name_list[22], site_name_list[23]
s25, s26 = site_name_list[24], site_name_list[25]
s27, s28 = site_name_list[26], site_name_list[27]
s29, s30 = site_name_list[28], site_name_list[29]
s31, s32 = site_name_list[30], site_name_list[31]
s33, s34 = site_name_list[32], site_name_list[33]
s35, s36 = site_name_list[34], site_name_list[35]
s37, s38 = site_name_list[36], site_name_list[37]
s39, s40 = site_name_list[38], site_name_list[39]
s41, s42 = site_name_list[40], site_name_list[41]
s43, s44 = site_name_list[42], site_name_list[43]
s45, s46 = site_name_list[44], site_name_list[45] 

hpo_sheet_names = [
    s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12, s13, s14, 
    s15, s16, s17, s18, s19, s20, s21, s22, s23, s24, s25, s26,
    s27, s28, s29, s30, s31, s32, s33, s34, s35, s36, s37, s38,
    s39, s40, s41, s42, s43, s44, s45, s46]
# +
hpo_sheets = []

for file in file_names_hpo_sheets:
    for sheet in hpo_sheet_names:
        s = pd.read_excel(file, sheet)
        hpo_sheets.append(s)
        

table_id_cols = hpo_sheets[0]
table_id_cols  = list(table_id_cols[table_id_cols.columns[0]])  # reset to first column

date_cols = hpo_sheets[0].columns[1:]

for idx, table_id in enumerate(table_id_cols):
    under_encountered = False
    start_idx, end_idx = 0, 0
    
    for c_idx, character in enumerate(table_id):
        if character == '_' and not under_encountered:
            start_idx = c_idx
            under_encountered = True
        elif character == '_' and under_encountered:
            end_idx = c_idx
    
    in_between_str = table_id[start_idx:end_idx + 1]
    
    if in_between_str == '_succes_':
        new_string = table_id[0:start_idx] + '_success' + table_id[end_idx:]
        table_id_cols[idx] = new_string

# +
new_hpo_sheets = []
start_idx = 1  # first does not have data

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
            xticklabels=date_cols, cmap="YlGnBu")

ax.set_title("End Dates Preceding Start Dates for {}".format(name_of_interest), size=14)

plt.tight_layout()
img_name = name_of_interest + "_end_before_begin.png"

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

plt.title("End Dates Preceding Start Dates: {}".format(name_of_interest), size=15, y = 1.1)
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
plt.title("{} End Dates Preceding Start Dates".format(name_of_interest))
plt.ylabel("End Dates Preceding Start Dates (%)")
plt.xlabel("")
plt.xticks(date_idxs, times, rotation = 'vertical')

handles, labels = ax.get_legend_handles_labels()
lgd = ax.legend(handles, labels, loc='upper center', bbox_to_anchor=(0.5,-0.1))

img_name = name_of_interest + "_end_before_begin_line_graph.jpg"
# plt.savefig(img_name, bbox_extraartist=(lgd,), bbox_inches='tight')
# -

