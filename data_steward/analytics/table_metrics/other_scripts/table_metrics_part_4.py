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
        "aouw_mcw", "cpmc_ucd", "ipmc_rush"
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
        "UC Davis", "Rush University"
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
         `{}._mapping_care_site`
         
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
         `{}._mapping_location`         
         
         
    UNION ALL
    SELECT
            DISTINCT(src_hpo_id) as src_hpo_id
    FROM
         `{}._mapping_measurement`         
         
         
    UNION ALL
    SELECT
            DISTINCT(src_hpo_id) as src_hpo_id
    FROM
         `{}._mapping_note`        
         
         
    UNION ALL
    SELECT
            DISTINCT(src_hpo_id) as src_hpo_id
    FROM
         `{}._mapping_observation`         
         
    UNION ALL
    SELECT
            DISTINCT(src_hpo_id) as src_hpo_id
    FROM
         `{}._mapping_person`   
         
    UNION ALL
    SELECT
            DISTINCT(src_hpo_id) as src_hpo_id
    FROM
         `{}._mapping_procedure_occurrence`         
         
         
    UNION ALL
    SELECT
            DISTINCT(src_hpo_id) as src_hpo_id
    FROM
         `{}._mapping_provider`
         
    UNION ALL
    SELECT
            DISTINCT(src_hpo_id) as src_hpo_id
    FROM
         `{}._mapping_specimen`
    
    UNION ALL
    SELECT
            DISTINCT(src_hpo_id) as src_hpo_id
    FROM
         `{}._mapping_visit_occurrence`   
    )     
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

# # Integration of Units for Select Measurements:
#

unit_standard_df = pd.io.gbq.read_gbq('''
    WITH
        data1 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS unit_total_row
            FROM
               `{}.measurement` AS t1
            JOIN -- ensuring you 'navigate up' the hierarchy
                 `{}.concept_ancestor` ca
             ON
                 t1.measurement_concept_id = ca.descendant_concept_id
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_measurement`)  AS t2
            ON
                t1.measurement_id=t2.measurement_id
            WHERE ancestor_concept_id in {}
            GROUP BY
                1
        ),

        data2 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS unit_well_defined_row
            FROM
               `{}.measurement` AS t1
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_measurement`)  AS t2
            ON
                t1.measurement_id=t2.measurement_id
            INNER JOIN
                `{}.concept` as t3
            ON
                t3.concept_id = t1.unit_concept_id
            JOIN -- ensuring you 'navigate up' the hierarchy
                 `{}.concept_ancestor` ca
             ON
                 t1.measurement_concept_id = ca.descendant_concept_id
            WHERE 
                 t3.domain_id="Unit"
                 and
                 t3.standard_concept="S"
                 and
                 t3.concept_id!=0
                 and
                 ca.ancestor_concept_id in {}
            GROUP BY
                1
        )

    SELECT
        data1.src_hpo_id,
        unit_well_defined_row,
        unit_total_row,
        round(100*(unit_well_defined_row/unit_total_row),1) as unit_success_rate
    FROM
        data1
    LEFT OUTER JOIN
        data2
    ON
        data1.src_hpo_id=data2.src_hpo_id
    ORDER BY
        1 DESC
    '''.format(DATASET, DATASET, DATASET, measurement_codes, DATASET, DATASET,
               DATASET, DATASET, measurement_codes, DATASET, DATASET, DATASET,
               DATASET, DATASET, DATASET, DATASET),
                                      dialect='standard')
unit_standard_df.shape

unit_standard_df

# +

unit_standard_df = unit_standard_df.fillna(0)
# -

unit_standard_df = pd.merge(unit_standard_df,
                            site_df,
                            how='outer',
                            on='src_hpo_id')

unit_standard_df = unit_standard_df.fillna("No Data")

unit_standard_df

unit_standard_df.to_csv("data\unit_integration.csv")

# #  Integration of Routes for Select Drugs:
#

diuretics = (974166, 956874, 970250, 1395058, 904542, 942350, 932745, 907013,
             978555, 991382, 1309799)

ccb = (1332418, 1328165, 1318853, 1307863, 1353776, 1318137)

vaccine = (45637323, 529411, 529303, 42800027, 45658522, 45628027, 529218,
           36212685, 40163692, 528323, 528986, 792777, 596876)

oralhypoglycemics = (1503297, 1560171, 1580747, 1559684, 1525215, 1597756,
                     45774751, 40239216, 40166035, 1516766, 1529331)

opioids = (1124957, 1103314, 1201620, 1174888, 1126658, 1110410, 1154029,
           1103640, 1102527)

antibiotics = (1734104, 1836430, 1713332, 1797513, 1705674, 1786621, 1742253,
               997881, 1707164, 1738521, 1759842, 1746940, 902722, 45892419,
               1717327, 1777806, 1836948, 1746114, 1775741)

statins = (1551860, 1545958, 1539403, 1510813, 1592085, 1549686, 40165636)

msknsaids = (1115008, 1177480, 1124300, 1178663, 1136980, 1118084, 1150345,
             1236607, 1395573, 1146810)

painnsaids = (1177480, 1125315, 1112807, 1115008, 45660697, 45787568, 36156482,
              45696636, 45696805)

ace_inhibitors = (1308216, 1341927, 1335471, 1331235, 1334456, 1340128, 1363749)

drugs = diuretics + ccb + vaccine + oralhypoglycemics + opioids + antibiotics + statins + msknsaids + painnsaids + ace_inhibitors

# ## Antibiotics

antibiotics_standard_df = pd.io.gbq.read_gbq('''
    WITH
        data1 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS route_total_row
            FROM
               `{}.drug_exposure` AS t1
             INNER JOIN
                 `{}.concept_ancestor` ca
             ON
                 t1.drug_concept_id = ca.descendant_concept_id
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_drug_exposure`)  AS t2
            ON
                t1.drug_exposure_id=t2.drug_exposure_id
            WHERE ca.ancestor_concept_id in {}
            GROUP BY
                1
        ),

        data2 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS route_well_defined_row
            FROM
               `{}.drug_exposure` AS t1
             INNER JOIN
                 `{}.concept_ancestor` ca
             ON
                 t1.drug_concept_id = ca.descendant_concept_id
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_drug_exposure`)  AS t2
            ON
                t1.drug_exposure_id=t2.drug_exposure_id
            INNER JOIN
                `{}.concept` as t3
            ON
                t3.concept_id = t1.route_concept_id
            WHERE 
                 t3.standard_concept="S"
                 and
                 t3.domain_id="Route"
                 and
                 ca.ancestor_concept_id in {}
            GROUP BY
                1
        )

    SELECT
        data1.src_hpo_id,
        route_well_defined_row,
        route_total_row,
        round(100*(route_well_defined_row/route_total_row),1) as route_success_rate
    FROM
        data1
    LEFT OUTER JOIN
        data2
    ON
        data1.src_hpo_id=data2.src_hpo_id
    ORDER BY
        1 DESC
    '''.format(DATASET, DATASET, DATASET, antibiotics, DATASET, DATASET,
               DATASET, DATASET, antibiotics, DATASET, DATASET, DATASET,
               DATASET, DATASET, DATASET, DATASET),
                                             dialect='standard')
antibiotics_standard_df.shape

antibiotics_standard_df

# +
# antibiotics_standard_df.to_csv("data\\antibiotics_standard_df.csv")
# -

# ## Ccb

ccb_standard_df = pd.io.gbq.read_gbq('''
    WITH
        data1 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS route_total_row
            FROM
               `{}.drug_exposure` AS t1
             INNER JOIN
                 `{}.concept_ancestor` ca
             ON
                 t1.drug_concept_id = ca.descendant_concept_id
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_drug_exposure`)  AS t2
            ON
                t1.drug_exposure_id=t2.drug_exposure_id
            WHERE ca.ancestor_concept_id in {}
            GROUP BY
                1
        ),

        data2 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS route_well_defined_row
            FROM
               `{}.drug_exposure` AS t1
             INNER JOIN
                 `{}.concept_ancestor` ca
             ON
                 t1.drug_concept_id = ca.descendant_concept_id
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_drug_exposure`)  AS t2
            ON
                t1.drug_exposure_id=t2.drug_exposure_id
            INNER JOIN
                `{}.concept` as t3
            ON
                t3.concept_id = t1.route_concept_id
            WHERE 
                 t3.standard_concept="S"
                 and
                 t3.domain_id="Route"
                 and
                 ca.ancestor_concept_id in {}
            GROUP BY
                1
        )

    SELECT
        data1.src_hpo_id,
        route_well_defined_row,
        route_total_row,
        round(100*(route_well_defined_row/route_total_row),1) as route_success_rate
    FROM
        data1
    LEFT OUTER JOIN
        data2
    ON
        data1.src_hpo_id=data2.src_hpo_id
    ORDER BY
        1 DESC
    '''.format(DATASET, DATASET, DATASET, ccb, DATASET, DATASET, DATASET,
               DATASET, ccb, DATASET, DATASET, DATASET, DATASET, DATASET,
               DATASET, DATASET),
                                     dialect='standard')
ccb_standard_df.shape

ccb_standard_df

# +
# cbc_standard_df.to_csv("data\\cbc_standard_df.csv")
# -

# ## Diuretics

diuretics_standard_df = pd.io.gbq.read_gbq('''
    WITH
        data1 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS route_total_row
            FROM
               `{}.drug_exposure` AS t1
             INNER JOIN
                 `{}.concept_ancestor` ca
             ON
                 t1.drug_concept_id = ca.descendant_concept_id
INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_drug_exposure`)  AS t2
            ON
                t1.drug_exposure_id=t2.drug_exposure_id
            WHERE      ca.ancestor_concept_id in {}
            GROUP BY
                1
        ),

        data2 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS route_well_defined_row
            FROM
               `{}.drug_exposure` AS t1
             INNER JOIN
                 `{}.concept_ancestor` ca
             ON
                 t1.drug_concept_id = ca.descendant_concept_id
INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_drug_exposure`)  AS t2
            ON
                t1.drug_exposure_id=t2.drug_exposure_id
            INNER JOIN
                `{}.concept` as t3
            ON
                t3.concept_id = t1.route_concept_id
            WHERE 
                 t3.standard_concept="S"
                 and
                 t3.domain_id="Route"
                 and
                      ca.ancestor_concept_id in {}
            GROUP BY
                1
        )

    SELECT
        data1.src_hpo_id,
        route_well_defined_row,
        route_total_row,
        round(100*(route_well_defined_row/route_total_row),1) as route_success_rate
    FROM
        data1
    LEFT OUTER JOIN
        data2
    ON
        data1.src_hpo_id=data2.src_hpo_id
    ORDER BY
        1 DESC
    '''.format(DATASET, DATASET, DATASET, diuretics, DATASET, DATASET, DATASET,
               DATASET, diuretics, DATASET, DATASET, DATASET, DATASET, DATASET,
               DATASET, DATASET),
                                           dialect='standard')
diuretics_standard_df.shape

diuretics_standard_df

# +
# diuretics_standard_df.to_csv("data\\diuretics_standard_df.csv")
# -

# ## Opioids

opioids_standard_df = pd.io.gbq.read_gbq('''
    WITH
        data1 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS route_total_row
            FROM
               `{}.drug_exposure` AS t1
             INNER JOIN
                 `{}.concept_ancestor` ca
             ON
                 t1.drug_concept_id = ca.descendant_concept_id
INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_drug_exposure`)  AS t2
            ON
                t1.drug_exposure_id=t2.drug_exposure_id
            WHERE      ca.ancestor_concept_id in {}
            GROUP BY
                1
        ),

        data2 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS route_well_defined_row
            FROM
               `{}.drug_exposure` AS t1
             INNER JOIN
                 `{}.concept_ancestor` ca
             ON
                 t1.drug_concept_id = ca.descendant_concept_id
INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_drug_exposure`)  AS t2
            ON
                t1.drug_exposure_id=t2.drug_exposure_id
            INNER JOIN
                `{}.concept` as t3
            ON
                t3.concept_id = t1.route_concept_id
            WHERE 
                 t3.standard_concept="S"
                 and
                 t3.domain_id="Route"
                 and
                      ca.ancestor_concept_id in {}
            GROUP BY
                1
        )

    SELECT
        data1.src_hpo_id,
        route_well_defined_row,
        route_total_row,
        round(100*(route_well_defined_row/route_total_row),1) as route_success_rate
    FROM
        data1
    LEFT OUTER JOIN
        data2
    ON
        data1.src_hpo_id=data2.src_hpo_id
    ORDER BY
        1 DESC
    '''.format(DATASET, DATASET, DATASET, opioids, DATASET, DATASET, DATASET,
               DATASET, opioids, DATASET, DATASET, DATASET, DATASET, DATASET,
               DATASET, DATASET),
                                         dialect='standard')
opioids_standard_df.shape

opioids_standard_df

# +
# opioids_standard_df.to_csv("data\\opioids_standard_df.csv")
# -

# ## Statins

statins_standard_df = pd.io.gbq.read_gbq('''
    WITH
        data1 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS route_total_row
            FROM
               `{}.drug_exposure` AS t1
             INNER JOIN
                 `{}.concept_ancestor` ca
             ON
                 t1.drug_concept_id = ca.descendant_concept_id
INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_drug_exposure`)  AS t2
            ON
                t1.drug_exposure_id=t2.drug_exposure_id
            WHERE      ca.ancestor_concept_id in {}
            GROUP BY
                1
        ),

        data2 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS route_well_defined_row
            FROM
               `{}.drug_exposure` AS t1
             INNER JOIN
                 `{}.concept_ancestor` ca
             ON
                 t1.drug_concept_id = ca.descendant_concept_id
INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_drug_exposure`)  AS t2
            ON
                t1.drug_exposure_id=t2.drug_exposure_id
            INNER JOIN
                `{}.concept` as t3
            ON
                t3.concept_id = t1.route_concept_id
            WHERE 
                 t3.standard_concept="S"
                 and
                 t3.domain_id="Route"
                 and
                      ca.ancestor_concept_id in {}
            GROUP BY
                1
        )

    SELECT
        data1.src_hpo_id,
        route_well_defined_row,
        route_total_row,
        round(100*(route_well_defined_row/route_total_row),1) as route_success_rate
    FROM
        data1
    LEFT OUTER JOIN
        data2
    ON
        data1.src_hpo_id=data2.src_hpo_id
    ORDER BY
        1 DESC
    '''.format(DATASET, DATASET, DATASET, statins, DATASET, DATASET, DATASET,
               DATASET, statins, DATASET, DATASET, DATASET, DATASET, DATASET,
               DATASET, DATASET),
                                         dialect='standard')
statins_standard_df.shape

statins_standard_df

# +
# statins_standard_df.to_csv("data\\statins_standard_df.csv")
# -

# ## msknsaids

msknsaids_standard_df = pd.io.gbq.read_gbq('''
    WITH
        data1 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS route_total_row
            FROM
               `{}.drug_exposure` AS t1
             INNER JOIN
                 `{}.concept_ancestor` ca
             ON
                 t1.drug_concept_id = ca.descendant_concept_id
INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_drug_exposure`)  AS t2
            ON
                t1.drug_exposure_id=t2.drug_exposure_id
            WHERE      ca.ancestor_concept_id in {}
            GROUP BY
                1
        ),

        data2 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS route_well_defined_row
            FROM
               `{}.drug_exposure` AS t1
             INNER JOIN
                 `{}.concept_ancestor` ca
             ON
                 t1.drug_concept_id = ca.descendant_concept_id
INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_drug_exposure`)  AS t2
            ON
                t1.drug_exposure_id=t2.drug_exposure_id
            INNER JOIN
                `{}.concept` as t3
            ON
                t3.concept_id = t1.route_concept_id
            WHERE 
                 t3.standard_concept="S"
                 and
                 t3.domain_id="Route"
                 and
                      ca.ancestor_concept_id in {}
            GROUP BY
                1
        )

    SELECT
        data1.src_hpo_id,
        route_well_defined_row,
        route_total_row,
        round(100*(route_well_defined_row/route_total_row),1) as route_success_rate
    FROM
        data1
    LEFT OUTER JOIN
        data2
    ON
        data1.src_hpo_id=data2.src_hpo_id
    ORDER BY
        1 DESC
    '''.format(DATASET, DATASET, DATASET, msknsaids, DATASET, DATASET, DATASET,
               DATASET, msknsaids, DATASET, DATASET, DATASET, DATASET, DATASET,
               DATASET, DATASET),
                                           dialect='standard')
msknsaids_standard_df.shape

msknsaids_standard_df

# +
# msknsaids_standard_df.to_csv("data\\msknsaids_standard_df.csv")
# -

# ## oralhypoglycemics

oralhypoglycemics_standard_df = pd.io.gbq.read_gbq('''
    WITH
        data1 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS route_total_row
            FROM
               `{}.drug_exposure` AS t1
             INNER JOIN
                 `{}.concept_ancestor` ca
             ON
                 t1.drug_concept_id = ca.descendant_concept_id
INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_drug_exposure`)  AS t2
            ON
                t1.drug_exposure_id=t2.drug_exposure_id
            WHERE      ca.ancestor_concept_id in {}
            GROUP BY
                1
        ),

        data2 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS route_well_defined_row
            FROM
               `{}.drug_exposure` AS t1
             INNER JOIN
                 `{}.concept_ancestor` ca
             ON
                 t1.drug_concept_id = ca.descendant_concept_id
INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_drug_exposure`)  AS t2
            ON
                t1.drug_exposure_id=t2.drug_exposure_id
            INNER JOIN
                `{}.concept` as t3
            ON
                t3.concept_id = t1.route_concept_id
            WHERE 
                 t3.standard_concept="S"
                 and
                 t3.domain_id="Route"
                 and
                ca.ancestor_concept_id in {}
            GROUP BY
                1
        )

    SELECT
        data1.src_hpo_id,
        route_well_defined_row,
        route_total_row,
        round(100*(route_well_defined_row/route_total_row),1) as route_success_rate
    FROM
        data1
    LEFT OUTER JOIN
        data2
    ON
        data1.src_hpo_id=data2.src_hpo_id
    ORDER BY
        1 DESC
    '''.format(DATASET, DATASET, DATASET, oralhypoglycemics, DATASET, DATASET,
               DATASET, DATASET, oralhypoglycemics, DATASET, DATASET, DATASET,
               DATASET, DATASET, DATASET, DATASET),
                                                   dialect='standard')
oralhypoglycemics_standard_df.shape

oralhypoglycemics_standard_df

# +
# oralhypoglycemics_standard_df.to_csv("data\\oralhypoglycemics_standard_df.csv")
# -

# ## painnsaids

painnsaids_standard_df = pd.io.gbq.read_gbq('''
    WITH
        data1 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS route_total_row
            FROM
               `{}.drug_exposure` AS t1
             INNER JOIN
                 `{}.concept_ancestor` ca
             ON
                 t1.drug_concept_id = ca.descendant_concept_id
INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_drug_exposure`)  AS t2
            ON
                t1.drug_exposure_id=t2.drug_exposure_id
            WHERE      ca.ancestor_concept_id in {}
            GROUP BY
                1
        ),

        data2 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS route_well_defined_row
            FROM
               `{}.drug_exposure` AS t1
             INNER JOIN
                 `{}.concept_ancestor` ca
             ON
                 t1.drug_concept_id = ca.descendant_concept_id
INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_drug_exposure`)  AS t2
            ON
                t1.drug_exposure_id=t2.drug_exposure_id
            INNER JOIN
                `{}.concept` as t3
            ON
                t3.concept_id = t1.route_concept_id
            WHERE 
                 t3.standard_concept="S"
                 and
                 t3.domain_id="Route"
                 and
                      ca.ancestor_concept_id in {}
            GROUP BY
                1
        )

    SELECT
        data1.src_hpo_id,
        route_well_defined_row,
        route_total_row,
        round(100*(route_well_defined_row/route_total_row),1) as route_success_rate
    FROM
        data1
    LEFT OUTER JOIN
        data2
    ON
        data1.src_hpo_id=data2.src_hpo_id
    ORDER BY
        1 DESC
    '''.format(DATASET, DATASET, DATASET, painnsaids, DATASET, DATASET, DATASET,
               DATASET, painnsaids, DATASET, DATASET, DATASET, DATASET, DATASET,
               DATASET, DATASET),
                                            dialect='standard')
painnsaids_standard_df.shape

painnsaids_standard_df

# +
# painnsaids_standard_df.to_csv("data\\painnsaids_standard_df.csv")
# -

# ## vaccine

vaccine_standard_df = pd.io.gbq.read_gbq('''
    WITH
        data1 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS route_total_row
            FROM
               `{}.drug_exposure` AS t1
             INNER JOIN
                 `{}.concept_ancestor` ca
             ON
                 t1.drug_concept_id = ca.descendant_concept_id
INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_drug_exposure`)  AS t2
            ON
                t1.drug_exposure_id=t2.drug_exposure_id
            WHERE      ca.ancestor_concept_id in {}
            GROUP BY
                1
        ),

        data2 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS route_well_defined_row
            FROM
               `{}.drug_exposure` AS t1
             INNER JOIN
                 `{}.concept_ancestor` ca
             ON
                 t1.drug_concept_id = ca.descendant_concept_id
INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_drug_exposure`)  AS t2
            ON
                t1.drug_exposure_id=t2.drug_exposure_id
            INNER JOIN
                `{}.concept` as t3
            ON
                t3.concept_id = t1.route_concept_id
            WHERE 
                 t3.standard_concept="S"
                 and
                 t3.domain_id="Route"
                 and
                      ca.ancestor_concept_id in {}
            GROUP BY
                1
        )

    SELECT
        data1.src_hpo_id,
        route_well_defined_row,
        route_total_row,
        round(100*(route_well_defined_row/route_total_row),1) as route_success_rate
    FROM
        data1
    LEFT OUTER JOIN
        data2
    ON
        data1.src_hpo_id=data2.src_hpo_id
    ORDER BY
        1 DESC
    '''.format(DATASET, DATASET, DATASET, vaccine, DATASET, DATASET, DATASET,
               DATASET, vaccine, DATASET, DATASET, DATASET, DATASET, DATASET,
               DATASET, DATASET),
                                         dialect='standard')
vaccine_standard_df.shape

vaccine_standard_df

# +
# vaccine_standard_df.to_csv("data\\vaccine_standard_df.csv")
# -

# ## ace_inhibitors

ace_inhibitors_standard_df = pd.io.gbq.read_gbq('''
    WITH
        data1 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS route_total_row
            FROM
               `{}.drug_exposure` AS t1
             INNER JOIN
                 `{}.concept_ancestor` ca
             ON
                 t1.drug_concept_id = ca.descendant_concept_id
INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_drug_exposure`)  AS t2
            ON
                t1.drug_exposure_id=t2.drug_exposure_id
            WHERE      ca.ancestor_concept_id in {}
            GROUP BY
                1
        ),

        data2 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS route_well_defined_row
            FROM
               `{}.drug_exposure` AS t1
             INNER JOIN
                 `{}.concept_ancestor` ca
             ON
                 t1.drug_concept_id = ca.descendant_concept_id
INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_drug_exposure`)  AS t2
            ON
                t1.drug_exposure_id=t2.drug_exposure_id
            INNER JOIN
                `{}.concept` as t3
            ON
                t3.concept_id = t1.route_concept_id
            WHERE 
                 t3.standard_concept="S"
                 and
                 t3.domain_id="Route"
                 and
     ca.ancestor_concept_id in {}
            GROUP BY
                1
        )

    SELECT
        data1.src_hpo_id,
        route_well_defined_row,
        route_total_row,
        round(100*(route_well_defined_row/route_total_row),1) as route_success_rate
    FROM
        data1
    LEFT OUTER JOIN
        data2
    ON
        data1.src_hpo_id=data2.src_hpo_id
    ORDER BY
        1 DESC
    '''.format(DATASET, DATASET, DATASET, ace_inhibitors, DATASET, DATASET,
               DATASET, DATASET, ace_inhibitors, DATASET, DATASET, DATASET,
               DATASET, DATASET, DATASET, DATASET),
                                                dialect='standard')
ace_inhibitors_standard_df.shape

ace_inhibitors_standard_df

# +
# ace_inhibitors_standard_df.to_csv("data\\ace_inhibitors_standard_df.csv")
# -

# ## Drugs

drugs_standard_df = pd.io.gbq.read_gbq('''
    WITH
        data1 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS route_total_row
            FROM
               `{}.drug_exposure` AS t1
             INNER JOIN
                 `{}.concept_ancestor` ca
             ON
                 t1.drug_concept_id = ca.descendant_concept_id
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_drug_exposure`)  AS t2
            ON
                t1.drug_exposure_id=t2.drug_exposure_id
            WHERE ca.ancestor_concept_id in {}
            GROUP BY
                1
        ),

        data2 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS route_well_defined_row
            FROM
               `{}.drug_exposure` AS t1
             INNER JOIN
                 `{}.concept_ancestor` ca
             ON
                 t1.drug_concept_id = ca.descendant_concept_id
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_drug_exposure`)  AS t2
            ON
                t1.drug_exposure_id=t2.drug_exposure_id
            INNER JOIN
                `{}.concept` as t3
            ON
                t3.concept_id = t1.route_concept_id
            WHERE 
                 t3.standard_concept="S"
                 and
                 t3.domain_id="Route"
                 and
                 ca.ancestor_concept_id in {}
            GROUP BY
                1
        )

    SELECT
        data1.src_hpo_id,
        route_well_defined_row,
        route_total_row,
        round(100*(route_well_defined_row/route_total_row),1) as route_success_rate
    FROM
        data1
    LEFT OUTER JOIN
        data2
    ON
        data1.src_hpo_id=data2.src_hpo_id
    ORDER BY
        1 DESC
    '''.format(DATASET, DATASET, DATASET, drugs, DATASET, DATASET, DATASET,
               DATASET, drugs, DATASET, DATASET, DATASET, DATASET, DATASET,
               DATASET, DATASET),
                                       dialect='standard')
drugs_standard_df.shape

drugs_standard_df

# +
# drugs_standard_df.to_csv("data\\drugs_standard_df.csv")
# -

antibiotics_standard_df = antibiotics_standard_df.fillna(0)
ccb_standard_df = ccb_standard_df.fillna(0)
diuretics_standard_df = diuretics_standard_df.fillna(0)
opioids_standard_df = opioids_standard_df.fillna(0)
statins_standard_df = statins_standard_df.fillna(0)
msknsaids_standard_df = msknsaids_standard_df.fillna(0)
oralhypoglycemics_standard_df = oralhypoglycemics_standard_df.fillna(0)
painnsaids_standard_df = painnsaids_standard_df.fillna(0)
vaccine_standard_df = vaccine_standard_df.fillna(0)
ace_inhibitors_standard_df = ace_inhibitors_standard_df.fillna(0)
drugs_standard_df = drugs_standard_df.fillna(0)

antibiotics_standard_df = antibiotics_standard_df[[
    "src_hpo_id", "route_success_rate"
]]
ccb_standard_df = ccb_standard_df[["src_hpo_id", "route_success_rate"]]
diuretics_standard_df = diuretics_standard_df[[
    "src_hpo_id", "route_success_rate"
]]
opioids_standard_df = opioids_standard_df[["src_hpo_id", "route_success_rate"]]
statins_standard_df = statins_standard_df[["src_hpo_id", "route_success_rate"]]
msknsaids_standard_df = msknsaids_standard_df[[
    "src_hpo_id", "route_success_rate"
]]
oralhypoglycemics_standard_df = oralhypoglycemics_standard_df[[
    "src_hpo_id", "route_success_rate"
]]
painnsaids_standard_df = painnsaids_standard_df[[
    "src_hpo_id", "route_success_rate"
]]
vaccine_standard_df = vaccine_standard_df[["src_hpo_id", "route_success_rate"]]
ace_inhibitors_standard_df = ace_inhibitors_standard_df[[
    "src_hpo_id", "route_success_rate"
]]
drugs_standard_df = drugs_standard_df[["src_hpo_id", "route_success_rate"]]

antibiotics_standard_df = antibiotics_standard_df.rename(
    columns={"route_success_rate": "antibiotics_success_rate"})
ccb_standard_df = ccb_standard_df.rename(
    columns={"route_success_rate": "ccb_success_rate"})
diuretics_standard_df = diuretics_standard_df.rename(
    columns={"route_success_rate": "diuretics_success_rate"})
opioids_standard_df = opioids_standard_df.rename(
    columns={"route_success_rate": "opioids_success_rate"})
statins_standard_df = statins_standard_df.rename(
    columns={"route_success_rate": "statins_success_rate"})
msknsaids_standard_df = msknsaids_standard_df.rename(
    columns={"route_success_rate": "msknsaids_success_rate"})
oralhypoglycemics_standard_df = oralhypoglycemics_standard_df.rename(
    columns={"route_success_rate": "oralhypoglycemics_success_rate"})
painnsaids_standard_df = painnsaids_standard_df.rename(
    columns={"route_success_rate": "painnsaids_success_rate"})
vaccine_standard_df = vaccine_standard_df.rename(
    columns={"route_success_rate": "vaccine_success_rate"})
ace_inhibitors_standard_df = ace_inhibitors_standard_df.rename(
    columns={"route_success_rate": "ace_inhibitors_success_rate"})
drugs_standard_df = drugs_standard_df.rename(
    columns={"route_success_rate": "drugs_overall_success_rate"})
liste = [
    antibiotics_standard_df, ccb_standard_df, diuretics_standard_df,
    opioids_standard_df, statins_standard_df, msknsaids_standard_df,
    oralhypoglycemics_standard_df, painnsaids_standard_df, vaccine_standard_df,
    ace_inhibitors_standard_df
]

for i in liste:
    drugs_standard_df = pd.merge(drugs_standard_df,
                                 i,
                                 how="outer",
                                 on="src_hpo_id")

# +

drugs_standard_df = pd.merge(drugs_standard_df,
                             site_df,
                             how='outer',
                             on='src_hpo_id')
drugs_standard_df = drugs_standard_df.fillna("No Data")
drugs_standard_df
# -

drugs_standard_df.to_csv("data\drug_routes.csv")
