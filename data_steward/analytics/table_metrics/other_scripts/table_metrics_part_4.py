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

from google.cloud import bigquery

client = bigquery.Client()

# %load_ext google.cloud.bigquery

# %reload_ext google.cloud.bigquery

# +
#######################################
print('Setting everything up...')
#######################################

import warnings

warnings.filterwarnings('ignore')
import pandas_gbq
import pandas as pd
import numpy as np
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.lines as mlines
from matplotlib.lines import Line2D

import matplotlib.ticker as ticker
import matplotlib.cm as cm
import matplotlib as mpl

import matplotlib.pyplot as plt
# %matplotlib inline

import os
import sys
from datetime import datetime
from datetime import date
from datetime import time
from datetime import timedelta
import time

DATASET = ''

plt.style.use('ggplot')
pd.options.display.max_rows = 999
pd.options.display.max_columns = 999
pd.options.display.max_colwidth = 999

from IPython.display import HTML as html_print


def cstr(s, color='black'):
    return "<text style=color:{}>{}</text>".format(color, s)


print('done.')
# -

cwd = os.getcwd()
cwd = str(cwd)
print(cwd)

# +
dic = {
    'src_hpo_id': [
        "saou_uab_selma", "saou_uab_hunt", "saou_tul", "pitt_temple",
        "saou_lsu", "trans_am_meyers", "trans_am_essentia", "saou_ummc",
        "seec_miami", "seec_morehouse", "seec_emory", "uamc_banner", "pitt",
        "nyc_cu", "ipmc_uic", "trans_am_spectrum", "tach_hfhs", "nec_bmc",
        "cpmc_uci", "nec_phs", "nyc_cornell", "ipmc_nu", "nyc_hh",
        "ipmc_uchicago", "aouw_mcri", "syhc", "cpmc_ceders", "seec_ufl",
        "saou_uab", "trans_am_baylor", "cpmc_ucsd", "ecchc", "chci", "aouw_uwh",
        "cpmc_usc", "hrhc", "ipmc_northshore", "chs", "cpmc_ucsf", "jhchc",
        "aouw_mcw", "cpmc_ucd", "ipmc_rush", "va", "saou_umc"
    ],
    'HPO': [
        "UAB Selma", "UAB Huntsville", "Tulane University", "Temple University",
        "Louisiana State University",
        "Reliant Medical Group (Meyers Primary Care)",
        "Essentia Health Superior Clinic", "University of Mississippi",
        "SouthEast Enrollment Center Miami",
        "SouthEast Enrollment Center Morehouse",
        "SouthEast Enrollment Center Emory", "Banner Health",
        "University of Pittsburgh", "Columbia University Medical Center",
        "University of Illinois Chicago", "Spectrum Health",
        "Henry Ford Health System", "Boston Medical Center", "UC Irvine",
        "Partners HealthCare", "Weill Cornell Medical Center",
        "Northwestern Memorial Hospital", "Harlem Hospital",
        "University of Chicago", "Marshfield Clinic",
        "San Ysidro Health Center", "Cedars-Sinai", "University of Florida",
        "University of Alabama at Birmingham", "Baylor", "UC San Diego",
        "Eau Claire Cooperative Health Center", "Community Health Center, Inc.",
        "UW Health (University of Wisconsin Madison)",
        "University of Southern California", "HRHCare",
        "NorthShore University Health System", "Cherokee Health Systems",
        "UC San Francisco", "Jackson-Hinds CHC", "Medical College of Wisconsin",
        "UC Davis", "Rush University", 
        "United States Department of Veterans Affairs - Boston",
        "University Medical Center (UA Tuscaloosa)"
    ]
}

site_df = pd.DataFrame(data=dic)
site_df

# +
######################################
print('Getting the data from the database...')
######################################

site_map = pd.io.gbq.read_gbq('''
    select distinct * from (
    SELECT
            DISTINCT(src_hpo_id) as src_hpo_id
    FROM
         `{}._mapping_visit_occurrence`
         
    UNION ALL
    SELECT
            DISTINCT(src_hpo_id) as src_hpo_id
    FROM
         `{}._mapping_condition_occurrence`  
         
    UNION ALL
    SELECT
            DISTINCT(src_hpo_id) as src_hpo_id
    FROM
         `{}._mapping_device_exposure`

    UNION ALL
    SELECT
            DISTINCT(src_hpo_id) as src_hpo_id
    FROM
         `{}._mapping_drug_exposure`      
         
    UNION ALL
    SELECT
            DISTINCT(src_hpo_id) as src_hpo_id
    FROM
         `{}._mapping_measurement`               
         
    UNION ALL
    SELECT
            DISTINCT(src_hpo_id) as src_hpo_id
    FROM
         `{}._mapping_observation`         
                  
         
    UNION ALL
    SELECT
            DISTINCT(src_hpo_id) as src_hpo_id
    FROM
         `{}._mapping_procedure_occurrence`         
    
    UNION ALL
    SELECT
            DISTINCT(src_hpo_id) as src_hpo_id
    FROM
         `{}._mapping_visit_occurrence`   
    ) 
    order by 1
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET,
               DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET,
               DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET,
               DATASET, DATASET, DATASET, DATASET, DATASET),
                              dialect='standard')
print(site_map.shape[0], 'records received.')
# -

site_df = pd.merge(site_map, site_df, how='outer', on='src_hpo_id')

site_df

Lipid = (40782589, 40795800, 40772572)

CBC = (40789356, 40789120, 40789179, 40772748, 40782735, 40789182, 40786033,
       40779159)

CBCwDiff = (40785788, 40785796, 40779195, 40795733, 40795725, 40772531,
            40779190, 40785793, 40779191, 40782561, 40789266)

CMP = (3049187, 3053283, 40775801, 40779224, 40782562, 40782579, 40785850,
       40785861, 40785869, 40789180, 40789190, 40789527, 40791227, 40792413,
       40792440, 40795730, 40795740, 40795754)

Physical_Measurement = (40654163, 40655804, 40654162, 40655805, 40654167,
                        40654164)

measurement_codes = Lipid + CBC + CBCwDiff + CMP + Physical_Measurement

# # Integration of Units for All Measurements:
#

# #### Getting the numbers for all of the unit concept IDs by site

# +
unit_concept_ids_by_site_query = """
CREATE TABLE `{DATASET}.sites_unit_counts`
OPTIONS (
expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 3 MINUTE)
)
AS
SELECT
DISTINCT
mm.src_hpo_id, COUNT(m.measurement_id) as number_total_rows
FROM
`{DATASET}.measurement` m
JOIN
`{DATASET}._mapping_measurement` mm
ON
m.measurement_id = mm.measurement_id 
JOIN
`{DATASET}.concept` c
ON
m.unit_concept_id = c.concept_id 
GROUP BY 1
ORDER BY number_total_rows DESC
""".format(DATASET = DATASET)

unit_concept_ids_by_site = pd.io.gbq.read_gbq(unit_concept_ids_by_site_query, dialect='standard')

# +
unit_concept_ids_by_site_query = """
SELECT
*
FROM
`{DATASET}.sites_unit_counts`
""".format(DATASET = DATASET)

unit_concept_ids_by_site = pd.io.gbq.read_gbq(unit_concept_ids_by_site_query, dialect='standard')
# -

unit_concept_ids_by_site

# #### Below are the "successful" unit concept IDs

# +
successful_unit_concept_ids_by_site_query = """
CREATE TABLE `{DATASET}.sites_successful_unit_counts`
OPTIONS (
expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 3 MINUTE)
)
AS
SELECT
DISTINCT
mm.src_hpo_id, COUNT(m.measurement_id) as number_valid_units
FROM
`{DATASET}.measurement` m
JOIN
`{DATASET}._mapping_measurement` mm
ON
m.measurement_id = mm.measurement_id 
JOIN
`{DATASET}.concept` c
ON
m.unit_concept_id = c.concept_id 
WHERE
c.standard_concept IN ('S')
AND
LOWER(c.domain_id) LIKE '%unit%'
GROUP BY 1
ORDER BY number_valid_units DESC
""".format(DATASET = DATASET)

successful_unit_concept_ids_by_site = pd.io.gbq.read_gbq(successful_unit_concept_ids_by_site_query, dialect='standard')

# +
successful_unit_concept_ids_by_site_query = """
SELECT
*
FROM
`{DATASET}.sites_successful_unit_counts`
""".format(DATASET = DATASET)

successful_unit_concept_ids_by_site = pd.io.gbq.read_gbq(successful_unit_concept_ids_by_site_query, dialect='standard')
# -

successful_unit_concept_ids_by_site

final_all_units_df = pd.merge(site_df, unit_concept_ids_by_site, on = 'src_hpo_id', how = 'left')

final_all_units_df = pd.merge(final_all_units_df, successful_unit_concept_ids_by_site, on = 'src_hpo_id', how = 'left')

final_all_units_df['total_unit_success_rate'] = round(final_all_units_df['number_valid_units'] / final_all_units_df['number_total_rows'] * 100, 2)

final_all_units_df = final_all_units_df.fillna(0)

final_all_units_df = final_all_units_df.sort_values(by='total_unit_success_rate', ascending = False)

final_all_units_df

# # Integration of Units for Selected Measurements
#
# #### making the distinction because - according to the [AoU EHR Operations](https://sites.google.com/view/ehrupload/omop-tables/measurement?authuser=0)  page (as of 03/11/2020) - the unit_concept_id are only required for the 'required labs'

# +
selected_unit_concept_ids_by_site_query = """
CREATE TABLE `{DATASET}.sites_unit_counts_selected_measurements`
OPTIONS (
expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 3 MINUTE)
)
AS
SELECT
DISTINCT
mm.src_hpo_id, COUNT(m.measurement_id) as number_total_units_selected_measurements
FROM
`{DATASET}.measurement` m
JOIN
`{DATASET}._mapping_measurement` mm
ON
m.measurement_id = mm.measurement_id 
JOIN
`{DATASET}.concept_ancestor` ca
ON
ca.descendant_concept_id = m.measurement_concept_id
JOIN
`{DATASET}.concept` c
ON
m.unit_concept_id = c.concept_id
WHERE
ca.ancestor_concept_id IN {selected_measurements}
GROUP BY 1
ORDER BY number_total_units_selected_measurements DESC
""".format(DATASET = DATASET, selected_measurements = measurement_codes)

selected_unit_concept_ids_by_site = pd.io.gbq.read_gbq(selected_unit_concept_ids_by_site_query, dialect='standard')

# +
selected_unit_concept_ids_by_site_query = """
SELECT
*
FROM
`{DATASET}.sites_unit_counts_selected_measurements`
""".format(DATASET = DATASET)

selected_unit_concept_ids_by_site = pd.io.gbq.read_gbq(selected_unit_concept_ids_by_site_query, dialect='standard')
# -

selected_unit_concept_ids_by_site

# #### Below are the 'successful' unit_concept_ids

# +
successful_selected_unit_concept_ids_by_site_query = """
CREATE TABLE `{DATASET}.sites_successful_unit_counts_selected_measurements`
OPTIONS (
expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 3 MINUTE)
)
AS
SELECT
DISTINCT
mm.src_hpo_id, COUNT(m.unit_concept_id) as number_valid_units_selected_measurements
FROM
`{DATASET}.measurement` m
JOIN
`{DATASET}._mapping_measurement` mm
ON
m.measurement_id = mm.measurement_id 
JOIN
`{DATASET}.concept_ancestor` ca
ON
ca.descendant_concept_id = m.measurement_concept_id
JOIN
`{DATASET}.concept` c
ON
m.unit_concept_id = c.concept_id
WHERE
c.standard_concept IN ('S')
AND
LOWER(c.domain_id) LIKE '%unit%'
AND
ca.ancestor_concept_id IN {selected_measurements}
GROUP BY 1
ORDER BY number_valid_units_selected_measurements DESC
""".format(DATASET = DATASET, selected_measurements = measurement_codes)

successful_selected_unit_concept_ids_by_site = pd.io.gbq.read_gbq(successful_selected_unit_concept_ids_by_site_query, dialect='standard')

# +
successful_selected_unit_concept_ids_by_site_query = """
SELECT
*
FROM
`{DATASET}.sites_successful_unit_counts_selected_measurements`
""".format(DATASET = DATASET)

successful_selected_unit_concept_ids_by_site = pd.io.gbq.read_gbq(successful_selected_unit_concept_ids_by_site_query, dialect='standard')
# -

successful_selected_unit_concept_ids_by_site

# +
final_all_units_df = pd.merge(final_all_units_df, selected_unit_concept_ids_by_site, on = 'src_hpo_id', how = 'left')

final_all_units_df = pd.merge(final_all_units_df, successful_selected_unit_concept_ids_by_site, on = 'src_hpo_id', how = 'left')
# -

final_all_units_df

final_all_units_df['total_proportion_of_units_that_are_selected'] = round(final_all_units_df['number_total_rows'] / final_all_units_df['number_total_units_selected_measurements'] * 100, 2)


final_all_units_df['selected_unit_success_rate'] = round(final_all_units_df['number_valid_units_selected_measurements'] / final_all_units_df['number_total_units_selected_measurements'] * 100, 2)


# +
final_all_units_df = final_all_units_df.fillna(0)

final_all_units_df = final_all_units_df.sort_values(by='total_unit_success_rate', ascending = False)

final_all_units_df
# -

final_all_units_df.to_csv("{cwd}\measurement_units.csv".format(cwd = cwd))

# #  Integration of Routes for Select Drugs:
# #### This is required for all drugs according to the [AoU EHR Operations Page](https://sites.google.com/view/ehrupload/omop-tables/drug_exposure?authuser=0) 

# #### Getting the numbers for all of the route concept IDs by site

# +
route_concept_ids_by_site_query = """
CREATE TABLE `{DATASET}.sites_route_counts`
OPTIONS (
expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 3 MINUTE)
)
AS
SELECT
DISTINCT
mde.src_hpo_id, COUNT(de.drug_exposure_id) as number_total_routes
FROM
`{DATASET}.drug_exposure` de
JOIN
`{DATASET}._mapping_drug_exposure` mde
ON
de.drug_exposure_id = mde.drug_exposure_id 
JOIN
`{DATASET}.concept` c
ON
de.route_concept_id = c.concept_id 
GROUP BY 1
ORDER BY number_total_routes DESC
""".format(DATASET = DATASET)

route_concept_ids_by_site = pd.io.gbq.read_gbq(route_concept_ids_by_site_query, dialect='standard')

# +
route_concept_ids_by_site_query = """
SELECT
*
FROM
`{DATASET}.sites_route_counts`
""".format(DATASET = DATASET)

route_concept_ids_by_site = pd.io.gbq.read_gbq(route_concept_ids_by_site_query, dialect='standard')
# -

route_concept_ids_by_site

# #### Below are the "successful" route concept IDs

# +
successful_route_concept_ids_by_site_query = """
CREATE TABLE `{DATASET}.sites_successful_route_counts`
OPTIONS (
expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 3 MINUTE)
)
AS
SELECT
DISTINCT
mde.src_hpo_id, COUNT(de.drug_exposure_id) as number_valid_routes
FROM
`{DATASET}.drug_exposure` de
JOIN
`{DATASET}._mapping_drug_exposure` mde
ON
de.drug_exposure_id = mde.drug_exposure_id 
JOIN
`{DATASET}.concept` c
ON
de.route_concept_id = c.concept_id
WHERE
c.standard_concept IN ('S')
AND
LOWER(c.domain_id) LIKE '%route%'
GROUP BY 1
ORDER BY number_valid_routes DESC
""".format(DATASET = DATASET)

successful_route_concept_ids_by_site = pd.io.gbq.read_gbq(successful_route_concept_ids_by_site_query, dialect='standard')

# +
successful_route_concept_ids_by_site_query = """
SELECT
*
FROM
`{DATASET}.sites_successful_route_counts`
""".format(DATASET = DATASET)

successful_route_concept_ids_by_site = pd.io.gbq.read_gbq(successful_route_concept_ids_by_site_query, dialect='standard')
# -

successful_route_concept_ids_by_site

final_all_routes_df = pd.merge(site_df, route_concept_ids_by_site, on = 'src_hpo_id', how = 'left')

final_all_routes_df = pd.merge(final_all_routes_df, successful_route_concept_ids_by_site, on = 'src_hpo_id', how = 'left')

final_all_routes_df['total_route_success_rate'] = round(final_all_routes_df['number_valid_routes'] / final_all_routes_df['number_total_routes'] * 100, 2)

final_all_routes_df = final_all_routes_df.fillna(0)

final_all_routes_df = final_all_routes_df.sort_values(by='total_route_success_rate', ascending = False)

final_all_routes_df

final_all_routes_df.to_csv("{cwd}\drug_routes.csv".format(cwd = cwd))
