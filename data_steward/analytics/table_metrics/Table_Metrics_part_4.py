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
        "pitt_temple", "saou_lsu", "trans_am_meyers", "trans_am_essentia",
        "saou_ummc", "seec_miami", "seec_morehouse", "seec_emory",
        "uamc_banner", "pitt", "nyc_cu", "ipmc_uic", "trans_am_spectrum",
        "tach_hfhs", "nec_bmc", "cpmc_uci", "nec_phs", "nyc_cornell", "ipmc_nu",
        "nyc_hh", "ipmc_uchicago", "aouw_mcri", "syhc", "cpmc_ceders",
        "seec_ufl", "saou_uab", "trans_am_baylor", "cpmc_ucsd", "ecchc", "chci",
        "aouw_uwh", "cpmc_usc", "hrhc", "ipmc_northshore", "chs", "cpmc_ucsf",
        "jhchc", "aouw_mcw", "cpmc_ucd", "ipmc_rush"
    ],
    'HPO': [
        "Temple University", "Louisiana State University",
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

measurement_codes = (
    3015182, 3019897, 3020416, 3023314, 3000963, 3000905, 3012030, 3009744,
    3023599, 3002736, 3043111, 3024929, 3004809, 3022096, 3025159, 3006504,
    3009542, 3000963, 3002030, 3035941, 3003338, 3024731, 3002179, 3019069,
    3017181, 3018010, 3007461, 3003214, 3021440, 3012764, 3026361, 3027945,
    3008757, 3010813, 3006923, 3024561, 3020509, 3035995, 3013721, 3006906,
    3016723, 3045716, 3016293, 3015632, 3014576, 3023103, 3019550, 3027970,
    3029829, 3030354, 3004501, 3020630, 3024128, 3013682, 3018311, 3053283,
    3049187, 3006906, 3016723, 3045716, 3016293, 3015632, 3014576, 3023103,
    3019550, 3029829, 3030354, 3004501, 3013682, 3018311, 3053283, 3049187,
    3004249, 3012888, 3027598, 3036277, 3019204, 3020891, 3013762, 3027018,
    3024171, 3019900, 3027114, 3049873, 3007070, 3023602, 3034482, 3053286,
    3001308, 3028288, 3028437, 3035009, 3035899, 3038988, 42870529, 3009966,
    3053341, 3002109, 3045001, 3007352, 3009596, 3022487, 40768809, 3011163,
    3016087, 3007943, 3019038, 3013678, 3022038, 3022192, 3025839, 3027997,
    42868692)

# # Integration of Units for Select Measurements:
#

unit_standard_df = pd.io.gbq.read_gbq('''
    WITH
        data1 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS unit_total_row
            FROM
               `{}.unioned_ehr_measurement` AS t1
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_measurement`)  AS t2
            ON
                t1.measurement_id=t2.measurement_id
            WHERE measurement_concept_id in {}
            GROUP BY
                1
        ),

        data2 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS unit_well_defined_row
            FROM
               `{}.unioned_ehr_measurement` AS t1
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
            WHERE 
                 t3.domain_id="Unit"
                 and
                 t3.standard_concept="S"
                 and
                 t3.concept_id!=0
                 and
                 measurement_concept_id in {}
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
    '''.format(DATASET, DATASET, measurement_codes, DATASET, DATASET, DATASET,
               measurement_codes, DATASET, DATASET, DATASET, DATASET, DATASET,
               DATASET, DATASET),
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

unit_standard_df.to_csv("data\q7_Integration_Units_Select_Measurements.csv")

# #  Integration of Routes for Select Drugs:
#

antibiotics = (1734104, 1836430, 1713332, 1797513, 1705674, 1786621, 1742253,
               997881, 1707164, 1738521, 1759842, 1746940, 902722, 45892419,
               1717327, 1777806, 1836948, 1746114, 1775741)

ccb = (1332418, 1328165, 1318853, 1307863, 1353776, 1318137)

diuretics = (974166, 956874, 970250, 1395058, 904542, 942350, 932745, 907013,
             978555, 991382, 1309799)

opioids = (1124957, 1103314, 1201620, 1174888, 1126658, 1110410, 1154029,
           1103640, 1102527)

statins = (1551860, 1545958, 1539403, 1510813, 1592085, 1549686, 40165636)

msknsaids = (1115008, 1177480, 1124300, 1178663, 1136980, 1118084, 1150345,
             1236607, 1395573, 1146810)

oralhypoglycemics = (1503297, 1560171, 1580747, 1559684, 1525215, 1597756,
                     45774751, 40239216, 40166035, 1516766, 1529331)

painnsaids = (1177480, 1125315, 1112807, 1115008, 45660697, 45787568, 36156482,
              45696636, 45696805)

vaccine = (45637323, 529411, 529303, 42800027, 45658522, 45628027, 529218,
           36212685, 40163692, 528323, 528986, 792777, 596876)

ace_inhibitors = (1308216, 1341927, 1335471, 1331235, 1334456, 1340128, 1363749)

# +
drugs = antibiotics + ccb + diuretics + opioids + statins + msknsaids + oralhypoglycemics + painnsaids + vaccine + ace_inhibitors

# -

# ## Antibiotics

antibiotics_standard_df = pd.io.gbq.read_gbq('''
    WITH
        data1 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS route_total_row
            FROM
               `{}.unioned_ehr_drug_exposure` AS t1
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_drug_exposure`)  AS t2
            ON
                t1.drug_exposure_id=t2.drug_exposure_id
            WHERE drug_concept_id in {}
            GROUP BY
                1
        ),

        data2 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS route_well_defined_row
            FROM
               `{}.unioned_ehr_drug_exposure` AS t1
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
                 drug_concept_id in {}
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
    '''.format(DATASET, DATASET, antibiotics, DATASET, DATASET, DATASET,
               antibiotics, DATASET, DATASET, DATASET, DATASET, DATASET,
               DATASET, DATASET),
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
               `{}.unioned_ehr_drug_exposure` AS t1
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_drug_exposure`)  AS t2
            ON
                t1.drug_exposure_id=t2.drug_exposure_id
            WHERE drug_concept_id in {}
            GROUP BY
                1
        ),

        data2 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS route_well_defined_row
            FROM
               `{}.unioned_ehr_drug_exposure` AS t1
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
                 drug_concept_id in {}
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
    '''.format(DATASET, DATASET, ccb, DATASET, DATASET, DATASET, ccb, DATASET,
               DATASET, DATASET, DATASET, DATASET, DATASET, DATASET),
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
               `{}.unioned_ehr_drug_exposure` AS t1
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_drug_exposure`)  AS t2
            ON
                t1.drug_exposure_id=t2.drug_exposure_id
            WHERE drug_concept_id in {}
            GROUP BY
                1
        ),

        data2 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS route_well_defined_row
            FROM
               `{}.unioned_ehr_drug_exposure` AS t1
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
                 drug_concept_id in {}
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
    '''.format(DATASET, DATASET, diuretics, DATASET, DATASET, DATASET,
               diuretics, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET,
               DATASET),
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
               `{}.unioned_ehr_drug_exposure` AS t1
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_drug_exposure`)  AS t2
            ON
                t1.drug_exposure_id=t2.drug_exposure_id
            WHERE drug_concept_id in {}
            GROUP BY
                1
        ),

        data2 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS route_well_defined_row
            FROM
               `{}.unioned_ehr_drug_exposure` AS t1
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
                 drug_concept_id in {}
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
    '''.format(DATASET, DATASET, opioids, DATASET, DATASET, DATASET, opioids,
               DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET),
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
               `{}.unioned_ehr_drug_exposure` AS t1
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_drug_exposure`)  AS t2
            ON
                t1.drug_exposure_id=t2.drug_exposure_id
            WHERE drug_concept_id in {}
            GROUP BY
                1
        ),

        data2 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS route_well_defined_row
            FROM
               `{}.unioned_ehr_drug_exposure` AS t1
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
                 drug_concept_id in {}
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
    '''.format(DATASET, DATASET, statins, DATASET, DATASET, DATASET, statins,
               DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET),
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
               `{}.unioned_ehr_drug_exposure` AS t1
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_drug_exposure`)  AS t2
            ON
                t1.drug_exposure_id=t2.drug_exposure_id
            WHERE drug_concept_id in {}
            GROUP BY
                1
        ),

        data2 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS route_well_defined_row
            FROM
               `{}.unioned_ehr_drug_exposure` AS t1
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
                 drug_concept_id in {}
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
    '''.format(DATASET, DATASET, msknsaids, DATASET, DATASET, DATASET,
               msknsaids, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET,
               DATASET),
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
               `{}.unioned_ehr_drug_exposure` AS t1
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_drug_exposure`)  AS t2
            ON
                t1.drug_exposure_id=t2.drug_exposure_id
            WHERE drug_concept_id in {}
            GROUP BY
                1
        ),

        data2 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS route_well_defined_row
            FROM
               `{}.unioned_ehr_drug_exposure` AS t1
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
                 drug_concept_id in {}
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
    '''.format(DATASET, DATASET, oralhypoglycemics, DATASET, DATASET, DATASET,
               oralhypoglycemics, DATASET, DATASET, DATASET, DATASET, DATASET,
               DATASET, DATASET),
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
               `{}.unioned_ehr_drug_exposure` AS t1
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_drug_exposure`)  AS t2
            ON
                t1.drug_exposure_id=t2.drug_exposure_id
            WHERE drug_concept_id in {}
            GROUP BY
                1
        ),

        data2 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS route_well_defined_row
            FROM
               `{}.unioned_ehr_drug_exposure` AS t1
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
                 drug_concept_id in {}
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
    '''.format(DATASET, DATASET, painnsaids, DATASET, DATASET, DATASET,
               painnsaids, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET,
               DATASET),
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
               `{}.unioned_ehr_drug_exposure` AS t1
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_drug_exposure`)  AS t2
            ON
                t1.drug_exposure_id=t2.drug_exposure_id
            WHERE drug_concept_id in {}
            GROUP BY
                1
        ),

        data2 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS route_well_defined_row
            FROM
               `{}.unioned_ehr_drug_exposure` AS t1
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
                 drug_concept_id in {}
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
    '''.format(DATASET, DATASET, vaccine, DATASET, DATASET, DATASET, vaccine,
               DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET),
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
               `{}.unioned_ehr_drug_exposure` AS t1
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_drug_exposure`)  AS t2
            ON
                t1.drug_exposure_id=t2.drug_exposure_id
            WHERE drug_concept_id in {}
            GROUP BY
                1
        ),

        data2 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS route_well_defined_row
            FROM
               `{}.unioned_ehr_drug_exposure` AS t1
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
                 drug_concept_id in {}
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
    '''.format(DATASET, DATASET, ace_inhibitors, DATASET, DATASET, DATASET,
               ace_inhibitors, DATASET, DATASET, DATASET, DATASET, DATASET,
               DATASET, DATASET),
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
               `{}.unioned_ehr_drug_exposure` AS t1
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_drug_exposure`)  AS t2
            ON
                t1.drug_exposure_id=t2.drug_exposure_id
            WHERE drug_concept_id in {}
            GROUP BY
                1
        ),

        data2 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS route_well_defined_row
            FROM
               `{}.unioned_ehr_drug_exposure` AS t1
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
                 drug_concept_id in {}
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
    '''.format(DATASET, DATASET, drugs, DATASET, DATASET, DATASET, drugs,
               DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET),
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

drugs_standard_df.to_csv("data\\q5_Integration_Routes_Select_Drugs.csv")

# #  Integration of Drug Concept Sets:

# ## antibiotics ingredient

# +
# antibiotics_ingredient = pd.io.gbq.read_gbq('''
#     SELECT
#         DISTINCT c.concept_id AS ingredient_concept_id, c.concept_name AS ingredient,COUNT(*) AS count
#     FROM
#        `{}.unioned_ehr_drug_exposure` AS de
#     JOIN
#       `{}.concept_ancestor` ca
#     ON
#       de.drug_concept_id = ca.descendant_concept_id
#     JOIN
#       `{}.concept` c
#     ON
#       ca.ancestor_concept_id = c.concept_id
#     WHERE
#       c.concept_class_id = 'Ingredient'
#     AND
#         de.drug_concept_id IN {}
#     GROUP BY 1,2
#     ORDER BY count DESC
#     '''.format(DATASET, DATASET,DATASET,antibiotics,DATASET,DATASET,DATASET,drugs,DATASET,DATASET,DATASET,DATASET,DATASET,DATASET,DATASET),
#                                     dialect='standard'
# )
# antibiotics_ingredient.shape

# +
# antibiotics_ingredient
# -

top_antibiotics_ingredient = (1734104, 1836430, 1713332, 1797513, 1705674,
                              1786621, 1742253, 997881, 1707164, 1738521,
                              1759842, 1746940, 902722, 45892419, 1717327,
                              1777806, 1836948, 1746114, 1775741)

top_option_size = len(top_antibiotics_ingredient)

# ## antibiotics ingredient by sites

antibiotics_ingredient_site = pd.io.gbq.read_gbq('''
    SELECT
        mde.src_hpo_id,
        COUNT(DISTINCT c.concept_id) AS count
    FROM
       `{}.unioned_ehr_drug_exposure` AS de
    JOIN
      `{}.concept_ancestor` ca
    ON
      de.drug_concept_id = ca.descendant_concept_id
    JOIN
      `{}.concept` c
    ON
      ca.ancestor_concept_id = c.concept_id
    JOIN
      `{}._mapping_drug_exposure`  AS mde
    ON
        de.drug_exposure_id=mde.drug_exposure_id
    WHERE
       c.concept_id IN {}
    AND
        de.drug_concept_id IN {}
    GROUP BY 1
    ORDER BY count DESC
    '''.format(DATASET, DATASET, DATASET, DATASET, top_antibiotics_ingredient,
               antibiotics, DATASET, DATASET, DATASET, drugs, DATASET, DATASET,
               DATASET, DATASET, DATASET, DATASET, DATASET),
                                                 dialect='standard')
antibiotics_ingredient_site.shape

antibiotics_ingredient_site

antibiotics_ingredient_site['success_rate'] = round(
    100 * antibiotics_ingredient_site['count'] / top_option_size, 1)
antibiotics_ingredient_site

antibiotics_ingredient_site = antibiotics_ingredient_site.rename(
    columns={"success_rate": "antibiotics"})
antibiotics_ingredient_site = antibiotics_ingredient_site[[
    "src_hpo_id", "antibiotics"
]]
antibiotics_ingredient_site

# ## ccb ingredient

# +
# cbc_ingredient = pd.io.gbq.read_gbq('''
#     SELECT
#         DISTINCT c.concept_id AS ingredient_concept_id, c.concept_name AS ingredient,COUNT(*) AS count
#     FROM
#        `{}.unioned_ehr_drug_exposure` AS de
#     JOIN
#       `{}.concept_ancestor` ca
#     ON
#       de.drug_concept_id = ca.descendant_concept_id
#     JOIN
#       `{}.concept` c
#     ON
#       ca.ancestor_concept_id = c.concept_id
#     WHERE
#       c.concept_class_id = 'Ingredient'
#     AND
#         de.drug_concept_id IN {}
#     GROUP BY 1,2
#     ORDER BY count DESC
#     '''.format(DATASET, DATASET,DATASET,cbc,DATASET,DATASET,DATASET,drugs,DATASET,DATASET,DATASET,DATASET,DATASET,DATASET,DATASET),
#                                     dialect='standard'
# )
# cbc_ingredient.shape

# +
# cbc_ingredient

# +
#  top_cbc_ingredient=tuple(cbc_ingredient["ingredient_concept_id"].head(20))
# -

top_ccb_ingredient = (1332418, 1318853, 1307863, 1353776, 1318137, 1328165,
                      1319880, 1326012, 1319880, 1319133, 19015802)

top_option_size = len(top_ccb_ingredient)

# ## ccb ingredient by sites

ccb_ingredient_site = pd.io.gbq.read_gbq('''
    SELECT
        mde.src_hpo_id,
        COUNT(DISTINCT c.concept_id) AS count
    FROM
       `{}.unioned_ehr_drug_exposure` AS de
    JOIN
      `{}.concept_ancestor` ca
    ON
      de.drug_concept_id = ca.descendant_concept_id
    JOIN
      `{}.concept` c
    ON
      ca.ancestor_concept_id = c.concept_id
    JOIN
      `{}._mapping_drug_exposure`  AS mde
    ON
        de.drug_exposure_id=mde.drug_exposure_id
    WHERE
       c.concept_id IN {}
    AND
        de.drug_concept_id IN {}
    GROUP BY 1
    ORDER BY count DESC
    '''.format(DATASET, DATASET, DATASET, DATASET, top_ccb_ingredient, ccb,
               DATASET, DATASET, DATASET, drugs, DATASET, DATASET, DATASET,
               DATASET, DATASET, DATASET, DATASET),
                                         dialect='standard')
ccb_ingredient_site.shape

ccb_ingredient_site

ccb_ingredient_site['success_rate'] = round(
    100 * ccb_ingredient_site['count'] / top_option_size, 1)
ccb_ingredient_site

ccb_ingredient_site = ccb_ingredient_site.rename(
    columns={"success_rate": "ccb"})
ccb_ingredient_site = ccb_ingredient_site[["src_hpo_id", "ccb"]]
ccb_ingredient_site

# ## diuretics ingredient

# +
# diuretics_ingredient = pd.io.gbq.read_gbq('''
#     SELECT
#         DISTINCT c.concept_id AS ingredient_concept_id, c.concept_name AS ingredient,COUNT(*) AS count
#     FROM
#        `{}.unioned_ehr_drug_exposure` AS de
#     JOIN
#       `{}.concept_ancestor` ca
#     ON
#       de.drug_concept_id = ca.descendant_concept_id
#     JOIN
#       `{}.concept` c
#     ON
#       ca.ancestor_concept_id = c.concept_id
#     WHERE
#       c.concept_class_id = 'Ingredient'
#     AND
#         de.drug_concept_id IN {}
#     GROUP BY 1,2
#     ORDER BY count DESC
#     '''.format(DATASET, DATASET,DATASET,diuretics,DATASET,DATASET,DATASET,drugs,DATASET,DATASET,DATASET,DATASET,DATASET,DATASET,DATASET),
#                                     dialect='standard'
# )
# diuretics_ingredient.shape

# +
# diuretics_ingredient

# +
# top_diuretics_ingredient=tuple(diuretics_ingredient["ingredient_concept_id"].head(20))
# -

top_diuretics_ingredient = (974166, 992590, 19010015, 948787, 904639, 19082886,
                            956874, 970250, 1395058, 942350, 932745, 991382,
                            1309799, 904542, 978555, 907013)

top_option_size = len(top_diuretics_ingredient)

# ## diuretics ingredient by sites

diuretics_ingredient_site = pd.io.gbq.read_gbq('''
    SELECT
        mde.src_hpo_id,
        COUNT(DISTINCT c.concept_id) AS count
    FROM
       `{}.unioned_ehr_drug_exposure` AS de
    JOIN
      `{}.concept_ancestor` ca
    ON
      de.drug_concept_id = ca.descendant_concept_id
    JOIN
      `{}.concept` c
    ON
      ca.ancestor_concept_id = c.concept_id
    JOIN
      `{}._mapping_drug_exposure`  AS mde
    ON
        de.drug_exposure_id=mde.drug_exposure_id
    WHERE
       c.concept_id IN {}
    AND
        de.drug_concept_id IN {}
    GROUP BY 1
    ORDER BY count DESC
    '''.format(DATASET, DATASET, DATASET, DATASET, top_diuretics_ingredient,
               diuretics, DATASET, DATASET, DATASET, drugs, DATASET, DATASET,
               DATASET, DATASET, DATASET, DATASET, DATASET),
                                               dialect='standard')
diuretics_ingredient_site.shape

diuretics_ingredient_site

diuretics_ingredient_site['success_rate'] = round(
    100 * diuretics_ingredient_site['count'] / top_option_size, 1)
diuretics_ingredient_site

diuretics_ingredient_site = diuretics_ingredient_site.rename(
    columns={"success_rate": "diuretics"})
diuretics_ingredient_site = diuretics_ingredient_site[[
    "src_hpo_id", "diuretics"
]]
diuretics_ingredient_site

# ## opioids ingredient

# +
# opioids_ingredient = pd.io.gbq.read_gbq('''
#     SELECT
#         DISTINCT c.concept_id AS ingredient_concept_id, c.concept_name AS ingredient,COUNT(*) AS count
#     FROM
#        `{}.unioned_ehr_drug_exposure` AS de
#     JOIN
#       `{}.concept_ancestor` ca
#     ON
#       de.drug_concept_id = ca.descendant_concept_id
#     JOIN
#       `{}.concept` c
#     ON
#       ca.ancestor_concept_id = c.concept_id
#     WHERE
#       c.concept_class_id = 'Ingredient'
#     AND
#         de.drug_concept_id IN {}
#     GROUP BY 1,2
#     ORDER BY count DESC
#     '''.format(DATASET, DATASET,DATASET,opioids,DATASET,DATASET,DATASET,drugs,DATASET,DATASET,DATASET,DATASET,DATASET,DATASET,DATASET),
#                                     dialect='standard'
# )
# opioids_ingredient.shape

# +
# opioids_ingredient

# +
# top_opioids_ingredient=tuple(opioids_ingredient["ingredient_concept_id"].head(20))
# -

top_opioids_ingredient = (1110410, 1124957, 1126658, 1154029, 1103314, 1201620,
                          1174888, 1102527, 1103640)

top_option_size = len(top_opioids_ingredient)

# ## opioids ingredient by sites

opioids_ingredient_site = pd.io.gbq.read_gbq('''
    SELECT
        mde.src_hpo_id,
        COUNT(DISTINCT c.concept_id) AS count
    FROM
       `{}.unioned_ehr_drug_exposure` AS de
    JOIN
      `{}.concept_ancestor` ca
    ON
      de.drug_concept_id = ca.descendant_concept_id
    JOIN
      `{}.concept` c
    ON
      ca.ancestor_concept_id = c.concept_id
    JOIN
      `{}._mapping_drug_exposure`  AS mde
    ON
        de.drug_exposure_id=mde.drug_exposure_id
    WHERE
       c.concept_id IN {}
    AND
        de.drug_concept_id IN {}
    GROUP BY 1
    ORDER BY count DESC
    '''.format(DATASET, DATASET, DATASET, DATASET, top_opioids_ingredient,
               opioids, DATASET, DATASET, DATASET, drugs, DATASET, DATASET,
               DATASET, DATASET, DATASET, DATASET, DATASET),
                                             dialect='standard')
opioids_ingredient_site.shape

opioids_ingredient_site

opioids_ingredient_site['success_rate'] = round(
    100 * opioids_ingredient_site['count'] / top_option_size, 1)
opioids_ingredient_site

opioids_ingredient_site = opioids_ingredient_site.rename(
    columns={"success_rate": "opioids"})
opioids_ingredient_site = opioids_ingredient_site[["src_hpo_id", "opioids"]]
opioids_ingredient_site

# ## statins ingredient

# +
# statins_ingredient = pd.io.gbq.read_gbq('''
#     SELECT
#         DISTINCT c.concept_id AS ingredient_concept_id, c.concept_name AS ingredient,COUNT(*) AS count
#     FROM
#        `{}.unioned_ehr_drug_exposure` AS de
#     JOIN
#       `{}.concept_ancestor` ca
#     ON
#       de.drug_concept_id = ca.descendant_concept_id
#     JOIN
#       `{}.concept` c
#     ON
#       ca.ancestor_concept_id = c.concept_id
#     WHERE
#       c.concept_class_id = 'Ingredient'
#     AND
#         de.drug_concept_id IN {}
#     GROUP BY 1,2
#     ORDER BY count DESC
#     '''.format(DATASET, DATASET,DATASET,statins,DATASET,DATASET,DATASET,drugs,DATASET,DATASET,DATASET,DATASET,DATASET,DATASET,DATASET),
#                                     dialect='standard'
# )
# antibiotics_ingredient.shape

# +
# statins_ingredient

# +
# top_statins_ingredient=tuple(statins_ingredient["ingredient_concept_id"].head(20))
# -

top_statins_ingredient = (1539403, 1592085, 1510813, 1549686, 1545958, 40165636,
                          1551860)

top_option_size = len(top_statins_ingredient)

# ## statins ingredient by sites

statins_ingredient_site = pd.io.gbq.read_gbq('''
    SELECT
        mde.src_hpo_id,
        COUNT(DISTINCT c.concept_id) AS count
    FROM
       `{}.unioned_ehr_drug_exposure` AS de
    JOIN
      `{}.concept_ancestor` ca
    ON
      de.drug_concept_id = ca.descendant_concept_id
    JOIN
      `{}.concept` c
    ON
      ca.ancestor_concept_id = c.concept_id
    JOIN
      `{}._mapping_drug_exposure`  AS mde
    ON
        de.drug_exposure_id=mde.drug_exposure_id
    WHERE
       c.concept_id IN {}
    AND
        de.drug_concept_id IN {}
    GROUP BY 1
    ORDER BY count DESC
    '''.format(DATASET, DATASET, DATASET, DATASET, top_statins_ingredient,
               statins, DATASET, DATASET, DATASET, drugs, DATASET, DATASET,
               DATASET, DATASET, DATASET, DATASET, DATASET),
                                             dialect='standard')
statins_ingredient_site.shape

statins_ingredient_site

statins_ingredient_site['success_rate'] = round(
    100 * statins_ingredient_site['count'] / top_option_size, 1)
statins_ingredient_site

statins_ingredient_site = statins_ingredient_site.rename(
    columns={"success_rate": "statins"})
statins_ingredient_site = statins_ingredient_site[["src_hpo_id", "statins"]]
statins_ingredient_site

# ## msknsaids ingredient

# +
# msknsaids_ingredient = pd.io.gbq.read_gbq('''
#     SELECT
#         DISTINCT c.concept_id AS ingredient_concept_id, c.concept_name AS ingredient,COUNT(*) AS count
#     FROM
#        `{}.unioned_ehr_drug_exposure` AS de
#     JOIN
#       `{}.concept_ancestor` ca
#     ON
#       de.drug_concept_id = ca.descendant_concept_id
#     JOIN
#       `{}.concept` c
#     ON
#       ca.ancestor_concept_id = c.concept_id
#     WHERE
#       c.concept_class_id = 'Ingredient'
#     AND
#         de.drug_concept_id IN {}
#     GROUP BY 1,2
#     ORDER BY count DESC
#     '''.format(DATASET, DATASET,DATASET,msknsaids,DATASET,DATASET,DATASET,drugs,DATASET,DATASET,DATASET,DATASET,DATASET,DATASET,DATASET),
#                                     dialect='standard'
# )
# msknsaids_ingredient.shape

# +
# msknsaids_ingredient

# +
# top_msknsaids_ingredient=tuple(msknsaids_ingredient["ingredient_concept_id"].head(20))
# -

top_msknsaids_ingredient = (1136980, 1115008, 1177480, 1236607, 1150345,
                            1395573, 1124300, 1146810, 1185922, 1103374,
                            1178663, 1118084)

top_option_size = len(top_msknsaids_ingredient)

# ## msknsaids ingredient by sites

# +
msknsaids_ingredient_site = pd.io.gbq.read_gbq('''
    SELECT
        mde.src_hpo_id,
        COUNT(DISTINCT c.concept_id) AS count
    FROM
       `{}.unioned_ehr_drug_exposure` AS de
    JOIN
      `{}.concept_ancestor` ca
    ON
      de.drug_concept_id = ca.descendant_concept_id
    JOIN
      `{}.concept` c
    ON
      ca.ancestor_concept_id = c.concept_id
    JOIN
      `{}._mapping_drug_exposure`  AS mde
    ON
        de.drug_exposure_id=mde.drug_exposure_id
    WHERE
       c.concept_id IN {}
    AND
        de.drug_concept_id IN {}
    GROUP BY 1
    ORDER BY count DESC
    '''.format(DATASET, DATASET, DATASET, DATASET, top_msknsaids_ingredient,
               msknsaids, DATASET, DATASET, DATASET, drugs, DATASET, DATASET,
               DATASET, DATASET, DATASET, DATASET, DATASET),
                                               dialect='standard')
msknsaids_ingredient_site.shape

msknsaids_ingredient_site
# -

msknsaids_ingredient_site['success_rate'] = round(
    100 * msknsaids_ingredient_site['count'] / top_option_size, 1)
msknsaids_ingredient_site

msknsaids_ingredient_site = msknsaids_ingredient_site.rename(
    columns={"success_rate": "msknsaids"})
msknsaids_ingredient_site = msknsaids_ingredient_site[[
    "src_hpo_id", "msknsaids"
]]
msknsaids_ingredient_site

# ## oralhypoglycemics ingredient

# +
# oralhypoglycemics_ingredient = pd.io.gbq.read_gbq('''
#     SELECT
#         DISTINCT c.concept_id AS ingredient_concept_id, c.concept_name AS ingredient,COUNT(*) AS count
#     FROM
#        `{}.unioned_ehr_drug_exposure` AS de
#     JOIN
#       `{}.concept_ancestor` ca
#     ON
#       de.drug_concept_id = ca.descendant_concept_id
#     JOIN
#       `{}.concept` c
#     ON
#       ca.ancestor_concept_id = c.concept_id
#     WHERE
#       c.concept_class_id = 'Ingredient'
#     AND
#         de.drug_concept_id IN {}
#     GROUP BY 1,2
#     ORDER BY count DESC
#     '''.format(DATASET, DATASET,DATASET,oralhypoglycemics,DATASET,DATASET,DATASET,drugs,DATASET,DATASET,DATASET,DATASET,DATASET,DATASET,DATASET),
#                                     dialect='standard'
# )
# oralhypoglycemics_ingredient.shape

# +
# oralhypoglycemics_ingredient

# +
# top_oralhypoglycemics_ingredient=tuple(oralhypoglycemics_ingredient["ingredient_concept_id"].head(20))
# -

top_oralhypoglycemics_ingredient = (1503297, 1525215, 1560171, 1559684, 1580747,
                                    19097821, 45774751, 1515249, 19059796,
                                    1597756, 1516766, 1529331, 1510202,
                                    43009032, 40166035, 19122137, 40239216,
                                    43013884)

top_option_size = len(top_oralhypoglycemics_ingredient)

# ## oralhypoglycemics ingredient by sites

oralhypoglycemics_ingredient_site = pd.io.gbq.read_gbq('''
    SELECT
        mde.src_hpo_id,
        COUNT(DISTINCT c.concept_id) AS count
    FROM
       `{}.unioned_ehr_drug_exposure` AS de
    JOIN
      `{}.concept_ancestor` ca
    ON
      de.drug_concept_id = ca.descendant_concept_id
    JOIN
      `{}.concept` c
    ON
      ca.ancestor_concept_id = c.concept_id
    JOIN
      `{}._mapping_drug_exposure`  AS mde
    ON
        de.drug_exposure_id=mde.drug_exposure_id
    WHERE
       c.concept_id IN {}
    AND
        de.drug_concept_id IN {}
    GROUP BY 1
    ORDER BY count DESC
    '''.format(DATASET, DATASET, DATASET, DATASET,
               top_oralhypoglycemics_ingredient, oralhypoglycemics, DATASET,
               DATASET, DATASET, drugs, DATASET, DATASET, DATASET, DATASET,
               DATASET, DATASET, DATASET),
                                                       dialect='standard')
oralhypoglycemics_ingredient_site.shape

oralhypoglycemics_ingredient_site

oralhypoglycemics_ingredient_site['success_rate'] = round(
    100 * oralhypoglycemics_ingredient_site['count'] / top_option_size, 1)
oralhypoglycemics_ingredient_site

oralhypoglycemics_ingredient_site = oralhypoglycemics_ingredient_site.rename(
    columns={"success_rate": "oralhypoglycemics"})
oralhypoglycemics_ingredient_site = oralhypoglycemics_ingredient_site[[
    "src_hpo_id", "oralhypoglycemics"
]]
oralhypoglycemics_ingredient_site

# ## painnsaids ingredient

# +
# painnsaids_ingredient = pd.io.gbq.read_gbq('''
#     SELECT
#         DISTINCT c.concept_id AS ingredient_concept_id, c.concept_name AS ingredient,COUNT(*) AS count
#     FROM
#        `{}.unioned_ehr_drug_exposure` AS de
#     JOIN
#       `{}.concept_ancestor` ca
#     ON
#       de.drug_concept_id = ca.descendant_concept_id
#     JOIN
#       `{}.concept` c
#     ON
#       ca.ancestor_concept_id = c.concept_id
#     WHERE
#       c.concept_class_id = 'Ingredient'
#     AND
#         de.drug_concept_id IN {}
#     GROUP BY 1,2
#     ORDER BY count DESC
#     '''.format(DATASET, DATASET,DATASET,painnsaids,DATASET,DATASET,DATASET,drugs,DATASET,DATASET,DATASET,DATASET,DATASET,DATASET,DATASET),
#                                     dialect='standard'
# )
# painnsaids_ingredient.shape

# +
# painnsaids_ingredient

# +
# top_painnsaids_ingredient=tuple(painnsaids_ingredient["ingredient_concept_id"].head(20))
# -

top_painnsaids_ingredient = (1125315, 1112807, 1177480, 1185922, 1115008)

top_option_size = len(top_painnsaids_ingredient)

# ## painnsaids ingredient by sites

painnsaids_ingredient_site = pd.io.gbq.read_gbq('''
    SELECT
        mde.src_hpo_id,
        COUNT(DISTINCT c.concept_id) AS count
    FROM
       `{}.unioned_ehr_drug_exposure` AS de
    JOIN
      `{}.concept_ancestor` ca
    ON
      de.drug_concept_id = ca.descendant_concept_id
    JOIN
      `{}.concept` c
    ON
      ca.ancestor_concept_id = c.concept_id
    JOIN
      `{}._mapping_drug_exposure`  AS mde
    ON
        de.drug_exposure_id=mde.drug_exposure_id
    WHERE
       c.concept_id IN {}
    AND
        de.drug_concept_id IN {}
    GROUP BY 1
    ORDER BY count DESC
    '''.format(DATASET, DATASET, DATASET, DATASET, top_painnsaids_ingredient,
               painnsaids, DATASET, DATASET, DATASET, drugs, DATASET, DATASET,
               DATASET, DATASET, DATASET, DATASET, DATASET),
                                                dialect='standard')
painnsaids_ingredient_site.shape

painnsaids_ingredient_site

painnsaids_ingredient_site['success_rate'] = round(
    100 * painnsaids_ingredient_site['count'] / top_option_size, 1)
painnsaids_ingredient_site

painnsaids_ingredient_site = painnsaids_ingredient_site.rename(
    columns={"success_rate": "painnsaids"})
painnsaids_ingredient_site = painnsaids_ingredient_site[[
    "src_hpo_id", "painnsaids"
]]
painnsaids_ingredient_site

# ## vaccine ingredient

# +
# vaccine_ingredient = pd.io.gbq.read_gbq('''
#     SELECT
#         DISTINCT c.concept_id AS ingredient_concept_id, c.concept_name AS ingredient,COUNT(*) AS count
#     FROM
#        `{}.unioned_ehr_drug_exposure` AS de
#     JOIN
#       `{}.concept_ancestor` ca
#     ON
#       de.drug_concept_id = ca.descendant_concept_id
#     JOIN
#       `{}.concept` c
#     ON
#       ca.ancestor_concept_id = c.concept_id
#     WHERE
#       c.concept_class_id = 'Ingredient'
#     AND
#         de.drug_concept_id IN {}
#     GROUP BY 1,2
#     ORDER BY count DESC
#     '''.format(DATASET, DATASET,DATASET,vaccine,DATASET,DATASET,DATASET,drugs,DATASET,DATASET,DATASET,DATASET,DATASET,DATASET,DATASET),
#                                     dialect='standard'
# )
# vaccine_ingredient.shape

# +
# vaccine_ingredient

# +
# top_vaccine_ingredient=tuple(vaccine_ingredient["ingredient_concept_id"].head(20))
# -

top_vaccine_ingredient = (42800027, 529303, 529411, 529218, 596876, 528323,
                          19113026, 529076, 1312375, 1312376, 43012953,
                          43532049, 42903450, 509079, 509081, 45775636,
                          40163692, 523283, 523365, 523367, 529176, 529180,
                          529212, 529214, 792777, 19033193, 19136047, 19045672,
                          46234308)

top_option_size = len(top_vaccine_ingredient)

# ## vaccine ingredient by sites

vaccine_ingredient_site = pd.io.gbq.read_gbq('''
    SELECT
        mde.src_hpo_id,
        COUNT(DISTINCT c.concept_id) AS count
    FROM
       `{}.unioned_ehr_drug_exposure` AS de
    JOIN
      `{}.concept_ancestor` ca
    ON
      de.drug_concept_id = ca.descendant_concept_id
    JOIN
      `{}.concept` c
    ON
      ca.ancestor_concept_id = c.concept_id
    JOIN
      `{}._mapping_drug_exposure`  AS mde
    ON
        de.drug_exposure_id=mde.drug_exposure_id
    WHERE
       c.concept_id IN {}
    AND
        de.drug_concept_id IN {}
    GROUP BY 1
    ORDER BY count DESC
    '''.format(DATASET, DATASET, DATASET, DATASET, top_vaccine_ingredient,
               vaccine, DATASET, DATASET, DATASET, drugs, DATASET, DATASET,
               DATASET, DATASET, DATASET, DATASET, DATASET),
                                             dialect='standard')
vaccine_ingredient_site.shape

vaccine_ingredient_site

vaccine_ingredient_site['success_rate'] = round(
    100 * vaccine_ingredient_site['count'] / top_option_size, 1)
vaccine_ingredient_site

vaccine_ingredient_site = vaccine_ingredient_site.rename(
    columns={"success_rate": "vaccine"})
vaccine_ingredient_site = vaccine_ingredient_site[["src_hpo_id", "vaccine"]]
vaccine_ingredient_site

# ## ace_inhibitors ingredient

# +
# ace_inhibitors_ingredient = pd.io.gbq.read_gbq('''
#     SELECT
#         DISTINCT c.concept_id AS ingredient_concept_id, c.concept_name AS ingredient,COUNT(*) AS count
#     FROM
#        `{}.unioned_ehr_drug_exposure` AS de
#     JOIN
#       `{}.concept_ancestor` ca
#     ON
#       de.drug_concept_id = ca.descendant_concept_id
#     JOIN
#       `{}.concept` c
#     ON
#       ca.ancestor_concept_id = c.concept_id
#     WHERE
#       c.concept_class_id = 'Ingredient'
#     AND
#         de.drug_concept_id IN {}
#     GROUP BY 1,2
#     ORDER BY count DESC
#     '''.format(DATASET, DATASET,DATASET,ace_inhibitors,DATASET,DATASET,DATASET,drugs,DATASET,DATASET,DATASET,DATASET,DATASET,DATASET,DATASET),
#                                     dialect='standard'
# )
# ace_inhibitors_ingredient.shape

# +
# ace_inhibitors_ingredient

# +
# top_ace_inhibitors_ingredient=tuple(ace_inhibitors_ingredient["ingredient_concept_id"].head(20))
# -

top_ace_inhibitors_ingredient = (1308216, 1341927, 1334456, 1340128, 1335471,
                                 1363749, 1310756, 1373225, 1331235, 1342439)

top_option_size = len(top_ace_inhibitors_ingredient)

# ## ace_inhibitors ingredient by sites

ace_inhibitors_ingredient_site = pd.io.gbq.read_gbq('''
    SELECT
        mde.src_hpo_id,
        COUNT(DISTINCT c.concept_id) AS count
    FROM
       `{}.unioned_ehr_drug_exposure` AS de
    JOIN
      `{}.concept_ancestor` ca
    ON
      de.drug_concept_id = ca.descendant_concept_id
    JOIN
      `{}.concept` c
    ON
      ca.ancestor_concept_id = c.concept_id
    JOIN
      `{}._mapping_drug_exposure`  AS mde
    ON
        de.drug_exposure_id=mde.drug_exposure_id
    WHERE
       c.concept_id IN {}
    AND
        de.drug_concept_id IN {}
    GROUP BY 1
    ORDER BY count DESC
    '''.format(DATASET, DATASET, DATASET, DATASET,
               top_ace_inhibitors_ingredient, ace_inhibitors, DATASET, DATASET,
               DATASET, drugs, DATASET, DATASET, DATASET, DATASET, DATASET,
               DATASET, DATASET),
                                                    dialect='standard')
ace_inhibitors_ingredient_site.shape

ace_inhibitors_ingredient_site

ace_inhibitors_ingredient_site['success_rate'] = round(
    100 * ace_inhibitors_ingredient_site['count'] / top_option_size, 1)
ace_inhibitors_ingredient_site

ace_inhibitors_ingredient_site = ace_inhibitors_ingredient_site.rename(
    columns={"success_rate": "ace_inhibitors"})
ace_inhibitors_ingredient_site = ace_inhibitors_ingredient_site[[
    "src_hpo_id", "ace_inhibitors"
]]
ace_inhibitors_ingredient_site

# ## combined succes rates:4. Integration of Drug Concept Sets:

# +

succes_rate = pd.merge(ace_inhibitors_ingredient_site,
                       vaccine_ingredient_site,
                       how='outer',
                       on='src_hpo_id')
succes_rate = pd.merge(succes_rate,
                       painnsaids_ingredient_site,
                       how='outer',
                       on='src_hpo_id')
succes_rate = pd.merge(succes_rate,
                       oralhypoglycemics_ingredient_site,
                       how='outer',
                       on='src_hpo_id')
succes_rate = pd.merge(succes_rate,
                       msknsaids_ingredient_site,
                       how='outer',
                       on='src_hpo_id')
succes_rate = pd.merge(succes_rate,
                       statins_ingredient_site,
                       how='outer',
                       on='src_hpo_id')
succes_rate = pd.merge(succes_rate,
                       opioids_ingredient_site,
                       how='outer',
                       on='src_hpo_id')
succes_rate = pd.merge(succes_rate,
                       diuretics_ingredient_site,
                       how='outer',
                       on='src_hpo_id')
succes_rate = pd.merge(succes_rate,
                       antibiotics_ingredient_site,
                       how='outer',
                       on='src_hpo_id')
succes_rate = pd.merge(succes_rate,
                       ccb_ingredient_site,
                       how='outer',
                       on='src_hpo_id')

succes_rate = pd.merge(succes_rate, site_df, how='outer', on='src_hpo_id')
succes_rate = succes_rate.fillna("No Data")
succes_rate
# -

succes_rate.to_csv("data\\q4_Integration_of_Drug_Concept_Sets.csv")

# # q6. Integration of Measurement Concept Sets:

cbc = (3015182, 3019897, 3020416, 3023314, 3000963, 3000905, 3012030, 3009744,
       3023599, 3002736, 3043111, 3024929)
cbc_size = len(cbc)

cbc_diff = (3004809, 3022096, 3025159, 3006504, 3009542, 3000963, 3002030,
            3035941, 3003338, 3024731, 3002179, 3019069, 3017181, 3018010,
            3007461, 3003214, 3021440, 3012764, 3026361, 3027945, 3008757,
            3010813)
cbc_diff_size = len(cbc_diff)

cmp = (3006923, 3024561, 3020509, 3035995, 3013721, 3006906, 3016723, 3045716,
       3016293, 3015632, 3014576, 3023103, 3019550, 3027970, 3029829, 3030354,
       3004501, 3020630, 3024128, 3013682, 3018311, 3053283, 3049187)
cmp_size = len(cmp)

bmp = (3006906, 3016723, 3045716, 3016293, 3015632, 3014576, 3023103, 3019550,
       3029829, 3030354, 3004501, 3013682, 3018311, 3053283, 3049187)
bmp_size = len(bmp)

phy_mea = (3004249, 3012888, 3027598, 3036277, 3019204, 3020891, 3013762,
           3027018, 3024171)
phy_mea_size = len(phy_mea)

lipid = (3019900, 3027114, 3049873, 3007070, 3023602, 3034482, 3053286, 3001308,
         3028288, 3028437, 3035009, 3035899, 3038988, 42870529, 3009966,
         3053341, 3002109, 3045001, 3007352, 3009596, 3022487, 40768809,
         3011163, 3016087, 3007943, 3019038, 3013678, 3022038, 3022192, 3025839,
         3027997, 42868692)
lipid_size = len(lipid)

# ## CBC by sites

cbc_count = pd.io.gbq.read_gbq('''

            SELECT
                src_hpo_id,
                COUNT(DISTINCT(measurement_concept_id)) AS count
            FROM
               `{}.unioned_ehr_measurement` AS t1
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_measurement`)  AS t2
            ON
                t1.measurement_id=t2.measurement_id
            WHERE measurement_concept_id in {}
            GROUP BY
                1
    '''.format(DATASET, DATASET, cbc),
                               dialect='standard')
cbc_count.shape

cbc_count['cbc'] = round(100 * cbc_count['count'] / cbc_size, 1)
cbc_count = cbc_count[["src_hpo_id", "cbc"]]
cbc_count

# ## cbc_diff by sites

cbc_diff_count = pd.io.gbq.read_gbq('''

            SELECT
                src_hpo_id,
                COUNT(DISTINCT(measurement_concept_id)) AS count
            FROM
               `{}.unioned_ehr_measurement` AS t1
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_measurement`)  AS t2
            ON
                t1.measurement_id=t2.measurement_id
            WHERE measurement_concept_id in {}
            GROUP BY
                1
    '''.format(DATASET, DATASET, cbc_diff),
                                    dialect='standard')
cbc_diff_count.shape

cbc_diff_count['cbc_diff'] = round(
    100 * cbc_diff_count['count'] / cbc_diff_size, 1)
cbc_diff_count = cbc_diff_count[["src_hpo_id", "cbc_diff"]]
cbc_diff_count

# ## cmp by sites

cmp_count = pd.io.gbq.read_gbq('''

            SELECT
                src_hpo_id,
                COUNT(DISTINCT(measurement_concept_id)) AS count
            FROM
               `{}.unioned_ehr_measurement` AS t1
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_measurement`)  AS t2
            ON
                t1.measurement_id=t2.measurement_id
            WHERE measurement_concept_id in {}
            GROUP BY
                1
    '''.format(DATASET, DATASET, cmp),
                               dialect='standard')
cmp_count.shape

cmp_count['cmp'] = round(100 * cmp_count['count'] / cmp_size, 1)
cmp_count = cmp_count[["src_hpo_id", "cmp"]]
cmp_count

# ## bmp by sites

bmp_count = pd.io.gbq.read_gbq('''

            SELECT
                src_hpo_id,
                COUNT(DISTINCT(measurement_concept_id)) AS count
            FROM
               `{}.unioned_ehr_measurement` AS t1
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_measurement`)  AS t2
            ON
                t1.measurement_id=t2.measurement_id
            WHERE measurement_concept_id in {}
            GROUP BY
                1
    '''.format(DATASET, DATASET, bmp),
                               dialect='standard')
bmp_count.shape

bmp_count['bmp'] = round(100 * bmp_count['count'] / bmp_size, 1)
bmp_count = bmp_count[["src_hpo_id", "bmp"]]
bmp_count

# ## phy_mea by sites

phy_mea_count = pd.io.gbq.read_gbq('''

            SELECT
                src_hpo_id,
                COUNT(DISTINCT(measurement_concept_id)) AS count
            FROM
               `{}.unioned_ehr_measurement` AS t1
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_measurement`)  AS t2
            ON
                t1.measurement_id=t2.measurement_id
            WHERE measurement_concept_id in {}
            GROUP BY
                1
    '''.format(DATASET, DATASET, phy_mea),
                                   dialect='standard')
phy_mea_count.shape

phy_mea_count['phy_mea'] = round(100 * phy_mea_count['count'] / phy_mea_size, 1)
phy_mea_count = phy_mea_count[["src_hpo_id", "phy_mea"]]
phy_mea_count

# ## lipid by sites

lipid_count = pd.io.gbq.read_gbq('''

            SELECT
                src_hpo_id,
                COUNT(DISTINCT(measurement_concept_id)) AS count
            FROM
               `{}.unioned_ehr_measurement` AS t1
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_measurement`)  AS t2
            ON
                t1.measurement_id=t2.measurement_id
            WHERE measurement_concept_id in {}
            GROUP BY
                1
    '''.format(DATASET, DATASET, lipid),
                                 dialect='standard')
lipid_count.shape

lipid_count['lipid'] = round(100 * lipid_count['count'] / lipid_size, 1)
lipid_count = lipid_count[["src_hpo_id", "lipid"]]
lipid_count

# ## combined succes rates:6. Integration of Measurement Concept Sets:

# +

succes_rate = pd.merge(lipid_count, phy_mea_count, how='outer', on='src_hpo_id')
succes_rate = pd.merge(succes_rate, bmp_count, how='outer', on='src_hpo_id')
succes_rate = pd.merge(succes_rate, cmp_count, how='outer', on='src_hpo_id')
succes_rate = pd.merge(succes_rate,
                       cbc_diff_count,
                       how='outer',
                       on='src_hpo_id')
succes_rate = pd.merge(succes_rate, cbc_count, how='outer', on='src_hpo_id')
success_rate = pd.merge(succes_rate, site_df, how='outer', on='src_hpo_id')
success_rate = success_rate.fillna("No Data")
success_rate
# -

success_rate.to_csv("data\\q6_Integration_Measurement_Concept_Sets.csv")
