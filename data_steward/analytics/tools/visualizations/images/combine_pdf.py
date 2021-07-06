# ---
# jupyter:
#   jupytext:
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

from google.cloud import bigquery
# %reload_ext google.cloud.bigquery
client = bigquery.Client()
# %load_ext google.cloud.bigquery

# +
from notebooks import parameters
DATASET = parameters.LATEST_DATASET
LOOKUP_TABLES = parameters.LOOKUP_TABLES

print(f"Dataset to use: {DATASET}")
print(f"Lookup tables: {LOOKUP_TABLES}")
# -

from fpdf import FPDF
from PIL import Image
import pandas as pd
import matplotlib.pyplot as plt
import os
import glob
from matplotlib.backends.backend_pdf import PdfPages

cwd = os.getcwd()
cwd = str(cwd)
print("Current working directory is: {cwd}".format(cwd=cwd))

hpo_id_query = f"""
SELECT REPLACE(table_id, '_person', '') AS src_hpo_id
FROM
`{DATASET}.__TABLES__`
WHERE table_id LIKE '%person' 
AND table_id 
NOT LIKE '%unioned_ehr_%' 
AND table_id NOT LIKE '\\\_%'
"""

site_df = pd.io.gbq.read_gbq(hpo_id_query, dialect='standard')

site_list = site_df.values.tolist()

temp_condition = pd.io.gbq.read_gbq('''
    SELECT src_hpo_id, COUNT(*) AS condition
    FROM `{DATASET}.unioned_ehr_condition_occurrence` AS t1
    INNER JOIN
            (SELECT DISTINCT * 
            FROM `{DATASET}._mapping_condition_occurrence`)  AS t2
        ON
            t1.condition_occurrence_id=t2.condition_occurrence_id
    GROUP BY 1
    '''.format(DATASET=DATASET), dialect='standard')
temp_condition

temp_procedure = pd.io.gbq.read_gbq('''
    SELECT src_hpo_id, COUNT(*) AS procedure
    FROM `{DATASET}.unioned_ehr_procedure_occurrence` AS t1
    INNER JOIN
            (SELECT DISTINCT * 
            FROM `{DATASET}._mapping_procedure_occurrence`)  AS t2
        ON
            t1.procedure_occurrence_id=t2.procedure_occurrence_id
    GROUP BY 1
    '''.format(DATASET=DATASET), dialect='standard')
temp_procedure

temp_drug = pd.io.gbq.read_gbq('''
    SELECT src_hpo_id, COUNT(*) AS drug_exposure
    FROM `{DATASET}.unioned_ehr_drug_exposure` AS t1
    INNER JOIN
            (SELECT DISTINCT * 
            FROM `{DATASET}._mapping_drug_exposure`)  AS t2
        ON
            t1.drug_exposure_id=t2.drug_exposure_id
    GROUP BY 1
    '''.format(DATASET=DATASET), dialect='standard')
temp_drug

temp_measurement = pd.io.gbq.read_gbq('''
    SELECT src_hpo_id, COUNT(*) AS measurement
    FROM `{DATASET}.unioned_ehr_measurement` AS t1
    INNER JOIN
            (SELECT DISTINCT * 
            FROM `{DATASET}._mapping_measurement`)  AS t2
        ON
            t1.measurement_id=t2.measurement_id
    GROUP BY 1
    '''.format(DATASET=DATASET), dialect='standard')
temp_measurement

temp_observation = pd.io.gbq.read_gbq('''
    SELECT src_hpo_id, COUNT(*) AS observation
    FROM `{DATASET}.unioned_ehr_observation` AS t1
    INNER JOIN
            (SELECT DISTINCT * 
            FROM `{DATASET}._mapping_observation`)  AS t2
        ON
            t1.observation_id=t2.observation_id
    GROUP BY 1
    '''.format(DATASET=DATASET), dialect='standard')
temp_observation

temp_visit = pd.io.gbq.read_gbq('''
    SELECT src_hpo_id, COUNT(*) AS visit
    FROM `{DATASET}.unioned_ehr_visit_occurrence` AS t1
    INNER JOIN
            (SELECT DISTINCT * 
            FROM `{DATASET}._mapping_visit_occurrence`)  AS t2
        ON
            t1.visit_occurrence_id=t2.visit_occurrence_id
    GROUP BY 1
    '''.format(DATASET=DATASET), dialect='standard')
temp_visit

temp_person = pd.io.gbq.read_gbq('''
    SELECT src_hpo_id, COUNT(*) AS person
    FROM `{DATASET}.unioned_ehr_person` AS t1
    INNER JOIN
            (SELECT DISTINCT * 
            FROM `{DATASET}._mapping_person`)  AS t2
        ON
            t1.person_id=t2.src_person_id
    GROUP BY 1
    '''.format(DATASET=DATASET), dialect='standard')
temp_person

summary_df = temp_person.merge(temp_condition, how = "left", on = "src_hpo_id")
summary_df = summary_df.merge(temp_drug, how = "left", on = "src_hpo_id")
summary_df = summary_df.merge(temp_measurement, how = "left", on = "src_hpo_id")
summary_df = summary_df.merge(temp_observation, how = "left", on = "src_hpo_id")
summary_df = summary_df.merge(temp_procedure, how = "left", on = "src_hpo_id")
summary_df = summary_df.merge(temp_visit, how = "left", on = "src_hpo_id")
summary_df

for site in site_list:
    df = summary_df.loc[summary_df['src_hpo_id'] == site[0]]
    try:
        fig, ax =plt.subplots(figsize=(18,12))
        ax.axis('tight')
        ax.axis('off')
        the_table = ax.table(cellText=df.values, colLabels=df.columns, loc='center', colWidths=[.15]*9)
        the_table.auto_set_font_size(False)
        the_table.set_fontsize(20)
        the_table.scale(1, 4)

        ax.set_title("Summary - Table row count", size=40)
        plt.savefig(site[0] + "_summary.png")
    except IndexError:
        continue

for site in site_list:
    try:
        if site[0] != 'pitt' and site[0] != 'saou_uab':
            imagelist = list(set(glob.glob(site[0] + "*.png")) -
                             set(glob.glob(site[0] + "*_summary.png")) -
                            set(glob.glob(site[0] + "*_measurement_integration*.png")) - 
                            set(glob.glob(site[0] + "*_drug_integration*.png")))
        else:
            if site[0] == 'pitt':
                imagelist = list(set(glob.glob(site[0] + "*.png")) -
                             set(glob.glob(site[0] + "*_summary.png")) -
                            set(glob.glob("pitt_temple*.png")) -
                            set(glob.glob(site[0] + "*_measurement_integration*.png")) - 
                            set(glob.glob(site[0] + "*_drug_integration*.png")))
            elif site[0] == 'saou_uab':
                imagelist = list(set(glob.glob(site[0] + "*.png")) -
                             set(glob.glob(site[0] + "*_summary.png")) - 
                            set(glob.glob("saou_uab_selma*.png")) - set(glob.glob("saou_uab_hunt*.png")) -
                            set(glob.glob(site[0] + "*_measurement_integration*.png")) - 
                            set(glob.glob(site[0] + "*_drug_integration*.png")))
        pdf = FPDF(unit="in")

        cover_page = glob.glob(site[0] + "_summary.png")[0]
        pdf.add_page()
        pdf.image(cover_page, h=7, w=8)
        for image in imagelist:
            pdf.add_page()
            pdf.image(image, h=7, w=8)
        pdf.output(site[0]+".pdf", "F")
    except IndexError:
        continue

# +
# for aggregate images
os.chdir("./aggregate_images")
os.getcwd()

imagelist = glob.glob("*.png")
pdf = FPDF(unit="in")
for image in imagelist:
    pdf.add_page()
    pdf.image(image, h=7, w=8)
pdf.output("aggregate.pdf", "F")

# +
#pd.DataFrame(imagelist)
