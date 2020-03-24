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

# +
# #!pip install --upgrade google-cloud-bigquery[pandas]
# -

from google.cloud import bigquery

# %reload_ext google.cloud.bigquery

client = bigquery.Client()

# %load_ext google.cloud.bigquery

# + endofcell="--"
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

DATASET = ''

import os
import sys
from datetime import datetime
from datetime import date
from datetime import time
from datetime import timedelta
import time

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

# # +
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

# # +
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
    WHERE src_hpo_id NOT LIKE '%rdr%'
    order by 1
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET,
               DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET,
               DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET,
               DATASET, DATASET, DATASET, DATASET, DATASET),
                              dialect='standard')
print(site_map.shape[0], 'records received.')
# -

site_map

site_df = pd.merge(site_map, site_df, how='outer', on='src_hpo_id')

site_df
# --

# # Below is how we calculate **true** concept_success_rate. This involves concept_id fields that are of Standard Concept = 'S' and of the correct domain.
# #### NOTE: Domain enforcement is not necessary for the Observation table

# # 16.Dataframe (row for each hpo_id) Condition_occurrence table, condition_concept_id field

condition_concept_df = pd.io.gbq.read_gbq('''
    WITH
        data1 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS condition_total_row
            FROM
               `{}.condition_occurrence` AS t1
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_condition_occurrence`)  AS t2
            ON
                t1.condition_occurrence_id=t2.condition_occurrence_id
            GROUP BY
                1
        ),


        data2 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS condition_well_defined_row
            FROM
               `{}.condition_occurrence` AS t1
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_condition_occurrence`)  AS t2
            ON
                t1.condition_occurrence_id=t2.condition_occurrence_id
            INNER JOIN
                `{}.concept` as t3
            ON
                t3.concept_id = t1.condition_concept_id
            WHERE 
                 t3.standard_concept="S" and t3.domain_id="Condition"
            GROUP BY
                1
        ),
        
        data3 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS condition_total_zeros_or_null
            FROM
               `{}.condition_occurrence` AS t1
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_condition_occurrence`)  AS t2
            ON
                t1.condition_occurrence_id=t2.condition_occurrence_id
            WHERE 
                (t1.condition_concept_id=0 or t1.condition_concept_id IS NULL)
            GROUP BY
                1
        ),
        
                data4 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS condition_total_null
            FROM
               `{}.condition_occurrence` AS t1
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_condition_occurrence`)  AS t2
            ON
                t1.condition_occurrence_id=t2.condition_occurrence_id
            WHERE 
                t1.condition_concept_id IS NULL
            GROUP BY
                1
        )


    SELECT
        data1.src_hpo_id,
        condition_well_defined_row,
        condition_total_row,
        condition_total_zeros_or_null,
        condition_total_null,
        round(100*(condition_well_defined_row/condition_total_row),1) as condition_success_rate
    FROM
        data1
    LEFT OUTER JOIN
        data2
    ON
        data1.src_hpo_id=data2.src_hpo_id
    LEFT OUTER JOIN
        data3
    ON
        data1.src_hpo_id=data3.src_hpo_id
    LEFT OUTER JOIN
        data4
    ON
        data1.src_hpo_id=data4.src_hpo_id
    WHERE
        data1.src_hpo_id NOT LIKE '%rdr%r'
    ORDER BY
        4 DESC
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET,
               DATASET, DATASET, DATASET, DATASET, DATASET, DATASET),
                                          dialect='standard')
condition_concept_df.shape

condition_concept_df = condition_concept_df.fillna(0)
condition_concept_df

# # 17.Dataframe (row for each hpo_id) Procedure_occurrence table, procedure_concept_id field

procedure_concept_df = pd.io.gbq.read_gbq('''
    WITH
        data1 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS procedure_total_row
            FROM
               `{}.procedure_occurrence` AS t1
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_procedure_occurrence`)  AS t2
            ON
                t1.procedure_occurrence_id=t2.procedure_occurrence_id
            GROUP BY
                1
        ),

        data2 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS procedure_well_defined_row
            FROM
               `{}.procedure_occurrence` AS t1
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_procedure_occurrence`)  AS t2
            ON
                t1.procedure_occurrence_id=t2.procedure_occurrence_id
            INNER JOIN
                `{}.concept` as t3
            ON
                t3.concept_id = t1.procedure_concept_id
            WHERE 
                 t3.standard_concept="S" and t3.domain_id="Procedure"
            GROUP BY
                1
        ),
        
        data3 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS procedure_total_zero_null
            FROM
               `{}.procedure_occurrence` AS t1
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_procedure_occurrence`)  AS t2
            ON
                t1.procedure_occurrence_id=t2.procedure_occurrence_id
            WHERE 
                 (t1.procedure_concept_id=0 or t1.procedure_concept_id IS NULL)
            GROUP BY
                1
        ),
        
                data4 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS procedure_total_null
            FROM
               `{}.procedure_occurrence` AS t1
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_procedure_occurrence`)  AS t2
            ON
                t1.procedure_occurrence_id=t2.procedure_occurrence_id
            WHERE 
                 t1.procedure_concept_id IS NULL
            GROUP BY
                1
        )
        

    SELECT
        data1.src_hpo_id,
        procedure_well_defined_row,
        procedure_total_zero_null,
        procedure_total_null,
        procedure_total_row,
        round(100*(procedure_well_defined_row/procedure_total_row),1) as procedure_success_rate
    FROM
        data1
    LEFT OUTER JOIN
        data2
    ON
        data1.src_hpo_id=data2.src_hpo_id
    LEFT OUTER JOIN
        data3
    ON
        data1.src_hpo_id=data3.src_hpo_id
    LEFT OUTER JOIN
        data4
    ON
        data1.src_hpo_id=data4.src_hpo_id
    WHERE data1.src_hpo_id NOT LIKE '%rdr%'
    ORDER BY
        1 DESC
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET,
               DATASET, DATASET, DATASET, DATASET, DATASET, DATASET),
                                          dialect='standard')
procedure_concept_df.shape

procedure_concept_df = procedure_concept_df.fillna(0)
procedure_concept_df

# # 18.Dataframe (row for each hpo_id) Drug_exposures table, drug_concept_id field

drug_concept_df = pd.io.gbq.read_gbq('''
    WITH
        data1 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS drug_total_row
            FROM
               `{}.drug_exposure` AS t1
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_drug_exposure`)  AS t2
            ON
                t1.drug_exposure_id=t2.drug_exposure_id
            GROUP BY
                1
        ),

        data2 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS drug_well_defined_row
            FROM
               `{}.drug_exposure` AS t1
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
                t3.concept_id = t1.drug_concept_id
            WHERE 
                 t3.standard_concept="S" and t3.domain_id="Drug"
            GROUP BY
                1
        ),

        data3 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS drug_total_zero_null
            FROM
               `{}.drug_exposure` AS t1
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
                t3.concept_id = t1.drug_concept_id
            WHERE 
                 (t1.drug_concept_id=0 OR t1.drug_concept_id IS NULL)
            GROUP BY
                1
        ),
        
                data4 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS drug_total_null
            FROM
               `{}.drug_exposure` AS t1
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_drug_exposure`)  AS t2
            ON
                t1.drug_exposure_id=t2.drug_exposure_id
            WHERE 
                 t1.drug_concept_id IS NULL
            GROUP BY
                1
        )
        
    SELECT
        data1.src_hpo_id,
        drug_well_defined_row,
        drug_total_zero_null,
        drug_total_null,
        drug_total_row,
        round(100*(drug_well_defined_row/drug_total_row),1) as drug_success_rate
    FROM
        data1
    LEFT OUTER JOIN
        data2
    ON
        data1.src_hpo_id=data2.src_hpo_id
    LEFT OUTER JOIN
        data3
    ON
        data1.src_hpo_id=data3.src_hpo_id
    LEFT OUTER JOIN
        data4
    ON
        data1.src_hpo_id=data4.src_hpo_id
    WHERE 
        data1.src_hpo_id NOT LIKE '%rdr%'
    ORDER BY
        1 DESC
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET,
               DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET,
               DATASET, DATASET),
                                     dialect='standard')
drug_concept_df.shape

# +

drug_concept_df = drug_concept_df.fillna(0)
drug_concept_df
# -

# # 19.Dataframe (row for each hpo_id) Observation table, Observation_concept_id field
#

observation_concept_df = pd.io.gbq.read_gbq('''
    WITH
        data1 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS observation_total_row
            FROM
               `{}.observation` AS t1
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_observation`)  AS t2
            ON
                t1.observation_id=t2.observation_id
            GROUP BY
                1
        ),

        data2 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS observation_well_defined_row
            FROM
               `{}.observation` AS t1
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_observation`)  AS t2
            ON
                t1.observation_id=t2.observation_id
            INNER JOIN
                `{}.concept` as t3
            ON
                t3.concept_id = t1.observation_concept_id
            WHERE 
                 t3.standard_concept="S" 
            GROUP BY
                1
        ),

        data3 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS observation_total_zero_missing
            FROM
               `{}.observation` AS t1
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_observation`)  AS t2
            ON
                t1.observation_id=t2.observation_id
            INNER JOIN
                `{}.concept` as t3
            ON
                t3.concept_id = t1.observation_concept_id
            WHERE 
                 (t1.observation_concept_id=0 OR t1.observation_concept_id IS NULL)
            GROUP BY
                1
        ),
        
            data4 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS observation_total_missing
            FROM
               `{}.observation` AS t1
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_observation`)  AS t2
            ON
                t1.observation_id=t2.observation_id
            WHERE 
              t1.observation_concept_id IS NULL
            GROUP BY
                1
        )

    SELECT
        data1.src_hpo_id,
        observation_total_zero_missing,
        observation_total_missing,
        observation_well_defined_row,
        observation_total_row,
        round(100*(observation_well_defined_row/observation_total_row),1) as observation_success_rate
    FROM
        data1
    LEFT OUTER JOIN
        data2
    ON
        data1.src_hpo_id=data2.src_hpo_id
    LEFT OUTER JOIN
        data3
    ON
        data1.src_hpo_id=data3.src_hpo_id
    LEFT OUTER JOIN
        data4
    ON
        data1.src_hpo_id=data4.src_hpo_id
    WHERE 
        data1.src_hpo_id NOT LIKE '%rdr%'
    ORDER BY
        1 DESC
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET,
               DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET,
               DATASET, DATASET, DATASET),
                                            dialect='standard')
observation_concept_df.shape

observation_concept_df = observation_concept_df.fillna(0)
observation_concept_df

# # 19.Dataframe (row for each hpo_id) measurement table, measurement_concept_id field
#

measurement_concept_df = pd.io.gbq.read_gbq('''
    WITH
        data1 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS measurement_total_row
            FROM
               `{}.measurement` AS t1
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_measurement`)  AS t2
            ON
                t1.measurement_id=t2.measurement_id
            GROUP BY
                1
        ),

        data2 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS measurement_well_defined_row
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
                t3.concept_id = t1.measurement_concept_id
            WHERE 
                 t3.standard_concept="S" and t3.domain_id="Measurement"
            GROUP BY
                1
        ),

        data3 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS measurement_total_zero_missing
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
                t3.concept_id = t1.measurement_concept_id
            WHERE 
                 (t1.measurement_concept_id=0 OR t1.measurement_concept_id IS NULL)
            GROUP BY
                1
        ),
        
            data4 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS measurement_total_missing
            FROM
               `{}.measurement` AS t1
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_measurement`)  AS t2
            ON
                t1.measurement_id=t2.measurement_id
            WHERE 
              t1.measurement_concept_id IS NULL
            GROUP BY
                1
        )

    SELECT
        data1.src_hpo_id,
        measurement_total_zero_missing,
        measurement_total_missing,
        measurement_well_defined_row,
        measurement_total_row,
        round(100*(measurement_well_defined_row/measurement_total_row),1) as  measurement_success_rate
    FROM
        data1
    LEFT OUTER JOIN
        data2
    ON
        data1.src_hpo_id=data2.src_hpo_id
    LEFT OUTER JOIN
        data3
    ON
        data1.src_hpo_id=data3.src_hpo_id
    LEFT OUTER JOIN
        data4
    ON
        data1.src_hpo_id=data4.src_hpo_id
    WHERE 
        data1.src_hpo_id NOT LIKE '%rdr%'
    ORDER BY
        1 DESC
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET,
               DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET,
               DATASET, DATASET, DATASET),
                                            dialect='standard')
measurement_concept_df.shape

measurement_concept_df = measurement_concept_df.fillna(0)
measurement_concept_df

# # 17.Dataframe (row for each hpo_id) visit_occurrence table, visit_concept_id field

visit_concept_df = pd.io.gbq.read_gbq('''
    WITH
        data1 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS visit_total_row
            FROM
               `{}.visit_occurrence` AS t1
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_visit_occurrence`)  AS t2
            ON
                t1.visit_occurrence_id=t2.visit_occurrence_id
            GROUP BY
                1
        ),

        data2 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS visit_well_defined_row
            FROM
               `{}.visit_occurrence` AS t1
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_visit_occurrence`)  AS t2
            ON
                t1.visit_occurrence_id=t2.visit_occurrence_id
            INNER JOIN
                `{}.concept` as t3
            ON
                t3.concept_id = t1.visit_concept_id
            WHERE 
                 t3.standard_concept="S" and t3.domain_id="Visit"
            GROUP BY
                1
        ),
        
        data3 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS visit_total_zero_null
            FROM
               `{}.visit_occurrence` AS t1
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_visit_occurrence`)  AS t2
            ON
                t1.visit_occurrence_id=t2.visit_occurrence_id
            WHERE 
                 (t1.visit_concept_id=0 or t1.visit_concept_id IS NULL)
            GROUP BY
                1
        ),
        
                data4 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS visit_total_null
            FROM
               `{}.visit_occurrence` AS t1
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{}._mapping_visit_occurrence`)  AS t2
            ON
                t1.visit_occurrence_id=t2.visit_occurrence_id
            WHERE 
                 t1.visit_concept_id IS NULL
            GROUP BY
                1
        )
        

    SELECT
        data1.src_hpo_id,
        visit_well_defined_row,
        visit_total_zero_null,
        visit_total_null,
        visit_total_row,
        round(100*(visit_well_defined_row/visit_total_row),1) as visit_success_rate
    FROM
        data1
    LEFT OUTER JOIN
        data2
    ON
        data1.src_hpo_id=data2.src_hpo_id
    LEFT OUTER JOIN
        data3
    ON
        data1.src_hpo_id=data3.src_hpo_id
    LEFT OUTER JOIN
        data4
    ON
        data1.src_hpo_id=data4.src_hpo_id
    WHERE 
        data1.src_hpo_id NOT LIKE '%rdr%'
    ORDER BY
        1 DESC
    '''.format(DATASET, DATASET, DATASET, DATASET, DATASET, DATASET, DATASET,
               DATASET, DATASET, DATASET, DATASET, DATASET, DATASET),
                                      dialect='standard')
visit_concept_df.shape

visit_concept_df = visit_concept_df.fillna(0)
visit_concept_df

# ## Sites combined

# +
datas = [
    drug_concept_df, procedure_concept_df, condition_concept_df,
    measurement_concept_df, visit_concept_df
]

master_df = observation_concept_df

for filename in datas:
    master_df = pd.merge(master_df, filename, on='src_hpo_id', how='outer')
# -

master_df

success_rate = pd.merge(master_df, site_df, how='outer', on='src_hpo_id')
success_rate

success_rate = success_rate.fillna(0)
success_rate

success_rate.to_csv("{cwd}/concept.csv".format(cwd = cwd))
