# -*- coding: utf-8 -*-
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

# ### This notebook is intended to show the percentage of rows with 'invalid' 'person_id' (a type of 'foreign key') in each of the 6 canonical tables.
#
# ### An 'invalid' row would be one where the field value ('person_id') either:
# ###     a. does not exist in the person table
# ###     b. is seemingly associated with another site in the `unioned_ehr_person` table

# ### The notebook will evaluate the following
#
# #### person_id and visit_occurrence_id in the following tables:
#
# - condition occurrence
# - observation
# - drug exposure
# - procedure occurrence
# - measurement
# - visit occurrence

# + tags=["parameters"]
PROJECT_ID = ""
DATASET = ""
LOOKUP_TABLES = ""
RUN_AS = ""
# -

# +
import warnings

warnings.filterwarnings('ignore')
import pandas as pd
import matplotlib.pyplot as plt
import os
from utils import auth
from analytics.cdr_ops.notebook_utils import execute, IMPERSONATION_SCOPES
from gcloud.bq import BigQueryClient

impersonation_creds = auth.get_impersonation_credentials(
    RUN_AS, target_scopes=IMPERSONATION_SCOPES)

client = BigQueryClient(PROJECT_ID, credentials=impersonation_creds)

plt.style.use('ggplot')
pd.options.display.max_rows = 999
pd.options.display.max_columns = 999
pd.options.display.max_colwidth = 999


def cstr(s, color='black'):
    return "<text style=color:{}>{}</text>".format(color, s)


# -

cwd = os.getcwd()
cwd = str(cwd)
print("Current working directory is: {cwd}".format(cwd=cwd))

# ### Get the list of HPO IDs
#
# ### NOTE: This assumes that all of the relevant HPOs have a person table.

hpo_id_query = f"""
SELECT REPLACE(table_id, '_person', '') AS src_hpo_id
FROM
`{DATASET}.__TABLES__`
WHERE table_id LIKE '%person' 
AND table_id 
NOT LIKE '%unioned_ehr_%' 
AND table_id NOT LIKE '\\\_%'
"""

site_df = execute(client, hpo_id_query)

get_full_names = f"""
select * from {LOOKUP_TABLES}.hpo_site_id_mappings
"""

full_names_df = execute(client, get_full_names)

# +
full_names_df.columns = ['org_id', 'src_hpo_id', 'site_name', 'display_order']
columns_to_use = ['src_hpo_id', 'site_name']

full_names_df = full_names_df[columns_to_use]
full_names_df['src_hpo_id'] = full_names_df['src_hpo_id'].str.lower()

# +
cols_to_join = ['src_hpo_id']

site_df = pd.merge(site_df, full_names_df, on=['src_hpo_id'], how='left')
# -

# # person_id evaluations

# ## Condition Occurrence Table

condition_occurrence_query = f"""
SELECT
DISTINCT
total_rows.src_hpo_id,
-- IFNULL(invalid_row_count.number_rows_w_no_valid_person, 0) as rows_w_no_valid_person,
-- total_rows.total_rows,
round(IFNULL(invalid_row_count.number_rows_w_no_valid_person, 0) / total_rows.total_rows * 100, 2) AS condition_occurrence

FROM

  (SELECT
  DISTINCT
  mco.src_hpo_id, COUNT(mco.src_hpo_id) as total_rows
  FROM
  `{DATASET}.unioned_ehr_condition_occurrence` co
  LEFT JOIN
  `{DATASET}._mapping_condition_occurrence` mco
  ON
  co.condition_occurrence_id = mco.condition_occurrence_id
  GROUP BY 1
  ORDER BY total_rows DESC) total_rows

LEFT JOIN

  (SELECT
  DISTINCT
  mco.src_hpo_id, COUNT(mco.src_hpo_id) as number_rows_w_no_valid_person

  -- to enable site tracing
  FROM
  `{DATASET}.unioned_ehr_condition_occurrence` co
  LEFT JOIN
  `{DATASET}._mapping_condition_occurrence` mco
  ON
  co.condition_occurrence_id = mco.condition_occurrence_id

  -- to enable person/src_hpo_id cross-checking
  LEFT JOIN
  `{DATASET}.unioned_ehr_person` p
  ON
  co.person_id = p.person_id
  LEFT JOIN
  `{DATASET}._mapping_person` mp
  ON
  p.person_id = mp.person_id


  -- person ID not existing in person table
  WHERE
  co.person_id NOT IN
    (
    SELECT
    DISTINCT p.person_id
    FROM
    `{DATASET}.unioned_ehr_person` p
    )

  GROUP BY 1
  ORDER BY number_rows_w_no_valid_person DESC) invalid_row_count

ON
total_rows.src_hpo_id = invalid_row_count.src_hpo_id
ORDER BY condition_occurrence DESC
"""

condition_occurrence_df = execute(client, condition_occurrence_query)

condition_occurrence_df

# # Observation

observation_query = f"""
SELECT
DISTINCT
total_rows.src_hpo_id,
-- IFNULL(invalid_row_count.number_rows_w_no_valid_person, 0) as rows_w_no_valid_person,
-- total_rows.total_rows,
round(IFNULL(invalid_row_count.number_rows_w_no_valid_person, 0) / total_rows.total_rows * 100, 2) AS observation

FROM

  (SELECT
  DISTINCT
  mo.src_hpo_id, COUNT(mo.src_hpo_id) as total_rows
  FROM
  `{DATASET}.unioned_ehr_observation` o
  LEFT JOIN
  `{DATASET}._mapping_observation` mo
  ON
  mo.observation_id = o.observation_id
  GROUP BY 1
  ORDER BY total_rows DESC) total_rows

LEFT JOIN

  (SELECT
  DISTINCT
  mo.src_hpo_id, COUNT(mo.src_hpo_id) as number_rows_w_no_valid_person

  -- to enable site tracing
  FROM
  `{DATASET}.unioned_ehr_observation` o
  LEFT JOIN
  `{DATASET}._mapping_observation` mo
  ON
  mo.observation_id = o.observation_id

  -- to enable person/src_hpo_id cross-checking
  LEFT JOIN
  `{DATASET}.unioned_ehr_person` p
  ON
  o.person_id = p.person_id
  LEFT JOIN
  `{DATASET}._mapping_person` mp
  ON
  p.person_id = mp.person_id


  -- person ID not existing in person table
  WHERE
  o.person_id NOT IN
    (
    SELECT
    DISTINCT p.person_id
    FROM
    `{DATASET}.unioned_ehr_person` p
    )


  GROUP BY 1
  ORDER BY number_rows_w_no_valid_person DESC) invalid_row_count

ON
total_rows.src_hpo_id = invalid_row_count.src_hpo_id
ORDER BY observation DESC
"""

observation_df = execute(client, observation_query)

observation_df

# # Drug Exposure Table

drug_exposure_query = f"""
SELECT
DISTINCT
total_rows.src_hpo_id,
-- IFNULL(invalid_row_count.number_rows_w_no_valid_person, 0) as rows_w_no_valid_person,
-- total_rows.total_rows,
round(IFNULL(invalid_row_count.number_rows_w_no_valid_person, 0) / total_rows.total_rows * 100, 2) AS drug_exposure

FROM

  (SELECT
  DISTINCT
  mde.src_hpo_id, COUNT(mde.src_hpo_id) as total_rows
  FROM
  `{DATASET}.unioned_ehr_drug_exposure` de
  LEFT JOIN
  `{DATASET}._mapping_drug_exposure` mde
  ON
  de.drug_exposure_id = mde.drug_exposure_id
  GROUP BY 1
  ORDER BY total_rows DESC) total_rows

LEFT JOIN

  (SELECT
  DISTINCT
  mde.src_hpo_id, COUNT(mde.src_hpo_id) as number_rows_w_no_valid_person

  -- to enable site tracing
  FROM
  `{DATASET}.unioned_ehr_drug_exposure` de
  LEFT JOIN
  `{DATASET}._mapping_drug_exposure` mde
  ON
  de.drug_exposure_id = mde.drug_exposure_id

  -- to enable person/src_hpo_id cross-checking
  LEFT JOIN
  `{DATASET}.unioned_ehr_person` p
  ON
  de.person_id = p.person_id
  LEFT JOIN
  `{DATASET}._mapping_person` mp
  ON
  p.person_id = mp.person_id


  -- person ID not existing in person table
  WHERE
  de.person_id NOT IN
    (
    SELECT
    DISTINCT p.person_id
    FROM
    `{DATASET}.unioned_ehr_person` p
    )


  GROUP BY 1
  ORDER BY number_rows_w_no_valid_person DESC) invalid_row_count

ON
total_rows.src_hpo_id = invalid_row_count.src_hpo_id
ORDER BY drug_exposure DESC
"""

drug_exposure_df = execute(client, drug_exposure_query)

drug_exposure_df

# # Procedure Occurrence

procedure_occurrence_query = f"""
SELECT
DISTINCT
total_rows.src_hpo_id,
-- IFNULL(invalid_row_count.number_rows_w_no_valid_person, 0) as rows_w_no_valid_person,
-- total_rows.total_rows,
round(IFNULL(invalid_row_count.number_rows_w_no_valid_person, 0) / total_rows.total_rows * 100, 2) AS procedure_occurrence

FROM

  (SELECT
  DISTINCT
  mpo.src_hpo_id, COUNT(mpo.src_hpo_id) as total_rows
  FROM
  `{DATASET}.unioned_ehr_procedure_occurrence` po
  LEFT JOIN
  `{DATASET}._mapping_procedure_occurrence` mpo
  ON
  po.procedure_occurrence_id = mpo.procedure_occurrence_id
  GROUP BY 1
  ORDER BY total_rows DESC) total_rows

LEFT JOIN

  (SELECT
  DISTINCT
  mpo.src_hpo_id, COUNT(mpo.src_hpo_id) as number_rows_w_no_valid_person

  -- to enable site tracing
  FROM
  `{DATASET}.unioned_ehr_procedure_occurrence` po
  LEFT JOIN
  `{DATASET}._mapping_procedure_occurrence` mpo
  ON
  po.procedure_occurrence_id = mpo.procedure_occurrence_id

  -- to enable person/src_hpo_id cross-checking
  LEFT JOIN
  `{DATASET}.unioned_ehr_person` p
  ON
  po.person_id = p.person_id
  LEFT JOIN
  `{DATASET}._mapping_person` mp
  ON
  p.person_id = mp.person_id


  -- person ID not existing in person table
  WHERE
  po.person_id NOT IN
    (
    SELECT
    DISTINCT p.person_id
    FROM
    `{DATASET}.unioned_ehr_person` p
    )

  GROUP BY 1
  ORDER BY number_rows_w_no_valid_person DESC) invalid_row_count

ON
total_rows.src_hpo_id = invalid_row_count.src_hpo_id
ORDER BY procedure_occurrence DESC
"""

procedure_occurrence_df = execute(client, procedure_occurrence_query)

procedure_occurrence_df

# # Measurement

measurement_query = f"""
SELECT
DISTINCT
total_rows.src_hpo_id,
-- IFNULL(invalid_row_count.number_rows_w_no_valid_person, 0) as rows_w_no_valid_person,
-- total_rows.total_rows,
round(IFNULL(invalid_row_count.number_rows_w_no_valid_person, 0) / total_rows.total_rows * 100, 2) AS measurement

FROM

  (SELECT
  DISTINCT
  mm.src_hpo_id, COUNT(mm.src_hpo_id) as total_rows
  FROM
  `{DATASET}.unioned_ehr_measurement` m
  LEFT JOIN
  `{DATASET}._mapping_measurement` mm
  ON
  m.measurement_id = mm.measurement_id
  GROUP BY 1
  ORDER BY total_rows DESC) total_rows

LEFT JOIN

  (SELECT
  DISTINCT
  mm.src_hpo_id, COUNT(mm.src_hpo_id) as number_rows_w_no_valid_person

  -- to enable site tracing
  FROM
  `{DATASET}.unioned_ehr_measurement` m
  LEFT JOIN
  `{DATASET}._mapping_measurement` mm
  ON
  m.measurement_id = mm.measurement_id

  -- to enable person/src_hpo_id cross-checking
  LEFT JOIN
  `{DATASET}.unioned_ehr_person` p
  ON
  m.person_id = p.person_id
  LEFT JOIN
  `{DATASET}._mapping_person` mp
  ON
  p.person_id = mp.person_id

  WHERE
  m.person_id NOT IN
    (
    SELECT
    DISTINCT p.person_id
    FROM
    `{DATASET}.unioned_ehr_person` p
    )


  GROUP BY 1
  ORDER BY number_rows_w_no_valid_person DESC) invalid_row_count

ON
total_rows.src_hpo_id = invalid_row_count.src_hpo_id
ORDER BY measurement DESC
"""

measurement_df = execute(client, measurement_query)

measurement_df

# # Visit Occurrence

visit_occurrence_query = f"""
SELECT
DISTINCT
total_rows.src_hpo_id,
-- IFNULL(invalid_row_count.number_rows_w_no_valid_person, 0) as rows_w_no_valid_person,
-- total_rows.total_rows,
round(IFNULL(invalid_row_count.number_rows_w_no_valid_person, 0) / total_rows.total_rows * 100, 2) AS visit_occurrence

FROM

  (SELECT
  DISTINCT
  mvo.src_hpo_id, COUNT(mvo.src_hpo_id) as total_rows
  FROM
  `{DATASET}.unioned_ehr_visit_occurrence` vo
  LEFT JOIN
  `{DATASET}._mapping_visit_occurrence` mvo
  ON
  vo.visit_occurrence_id = mvo.visit_occurrence_id
  GROUP BY 1
  ORDER BY total_rows DESC) total_rows

LEFT JOIN

  (SELECT
  DISTINCT
  mvo.src_hpo_id, COUNT(mvo.src_hpo_id) as number_rows_w_no_valid_person

  -- to enable site tracing
  FROM
  `{DATASET}.unioned_ehr_visit_occurrence` vo
  LEFT JOIN
  `{DATASET}._mapping_visit_occurrence` mvo
  ON
  vo.visit_occurrence_id = mvo.visit_occurrence_id

  -- to enable person/src_hpo_id cross-checking
  LEFT JOIN
  `{DATASET}.unioned_ehr_person` p
  ON
  vo.person_id = p.person_id
  LEFT JOIN
  `{DATASET}._mapping_person` mp
  ON
  p.person_id = mp.person_id


  -- person ID not existing in person table
  WHERE
  vo.person_id NOT IN
    (
    SELECT
    DISTINCT p.person_id
    FROM
    `{DATASET}.unioned_ehr_person` p
    )


  GROUP BY 1
  ORDER BY number_rows_w_no_valid_person DESC) invalid_row_count

ON
total_rows.src_hpo_id = invalid_row_count.src_hpo_id
ORDER BY visit_occurrence DESC
"""

visit_occurrence_df = execute(client, visit_occurrence_query)

visit_occurrence_df

# # Bringing it all together

# +
person_id_foreign_key_df = pd.merge(site_df,
                                    observation_df,
                                    how='outer',
                                    on='src_hpo_id')

person_id_foreign_key_df = pd.merge(person_id_foreign_key_df,
                                    measurement_df,
                                    how='outer',
                                    on='src_hpo_id')

person_id_foreign_key_df = pd.merge(person_id_foreign_key_df,
                                    visit_occurrence_df,
                                    how='outer',
                                    on='src_hpo_id')

person_id_foreign_key_df = pd.merge(person_id_foreign_key_df,
                                    procedure_occurrence_df,
                                    how='outer',
                                    on='src_hpo_id')

person_id_foreign_key_df = pd.merge(person_id_foreign_key_df,
                                    drug_exposure_df,
                                    how='outer',
                                    on='src_hpo_id')

person_id_foreign_key_df = pd.merge(person_id_foreign_key_df,
                                    condition_occurrence_df,
                                    how='outer',
                                    on='src_hpo_id')

# +
person_id_foreign_key_df = person_id_foreign_key_df.fillna(0)

person_id_foreign_key_df
# -

person_id_foreign_key_df.to_csv(f"{cwd}/person_id_failure_rate.csv")
