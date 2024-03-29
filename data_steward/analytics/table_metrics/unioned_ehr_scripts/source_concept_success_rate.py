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

# # Below is used to define the 'concept success rate' of the source_concept_ids
#
# ### NOTE: This is not a useful metric for most sites but has 'fringe' cases of utility. Source concept IDs are NOT expected to be of standard concept.

# ### 20.Dataframe (row for each hpo_id) Condition_occurrence table, condition_source_concept_id field

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

condition_concept_query = '''
    WITH
        data1 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS condition_total_row
            FROM
               `{DATASET}.unioned_ehr_condition_occurrence` AS t1
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{DATASET}._mapping_condition_occurrence`)  AS t2
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
               `{DATASET}.unioned_ehr_condition_occurrence` AS t1
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{DATASET}._mapping_condition_occurrence`)  AS t2
            ON
                t1.condition_occurrence_id=t2.condition_occurrence_id
            INNER JOIN
                `{DATASET}.concept` as t3
            ON
                t3.concept_id = t1.condition_source_concept_id
            WHERE 
                 t3.domain_id="Condition" and t3.standard_concept="S"
            GROUP BY
                1
        ),
        
        
        data3 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS condition_total_zero
            FROM
               `{DATASET}.unioned_ehr_condition_occurrence` AS t1
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{DATASET}._mapping_condition_occurrence`)  AS t2
            ON
                t1.condition_occurrence_id=t2.condition_occurrence_id
            INNER JOIN
                `{DATASET}.concept` as t3
            ON
                t3.concept_id = t1.condition_source_concept_id
            WHERE 
                 (t3.concept_id=0 or t3.concept_id is null)
            GROUP BY
                1
        )

    SELECT
        data1.src_hpo_id,
        condition_well_defined_row,
        condition_total_row,
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
    ORDER BY
        1 DESC
    '''.format(DATASET=DATASET)

condition_concept_df = execute(client, condition_concept_query)

condition_concept_df = condition_concept_df.fillna(0)
condition_concept_df

# # 21.Dataframe (row for each hpo_id) Procedure_occurrence table, procedure_source_concept_id field

procedure_concept_query = '''
    WITH
        data1 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS procedure_total_row
            FROM
               `{DATASET}.unioned_ehr_procedure_occurrence` AS t1
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{DATASET}._mapping_procedure_occurrence`)  AS t2
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
               `{DATASET}.unioned_ehr_procedure_occurrence` AS t1
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{DATASET}._mapping_procedure_occurrence`)  AS t2
            ON
                t1.procedure_occurrence_id=t2.procedure_occurrence_id
            INNER JOIN
                `{DATASET}.concept` as t3
            ON
                t3.concept_id = t1.procedure_source_concept_id
            WHERE 
                 t3.standard_concept="S" and t3.domain_id="Procedure"
            GROUP BY
                1
        )

    SELECT
        data1.src_hpo_id,
        procedure_well_defined_row,
        procedure_total_row,
        round(100*(procedure_well_defined_row/procedure_total_row),1) as procedure_success_rate
    FROM
        data1
    LEFT OUTER JOIN
        data2
    ON
        data1.src_hpo_id=data2.src_hpo_id
    ORDER BY
        1 DESC
    '''.format(DATASET=DATASET)
procedure_concept_df = execute(client, procedure_concept_query)

procedure_concept_df = procedure_concept_df.fillna(0)
procedure_concept_df

# # 22.Dataframe (row for each hpo_id) Drug_exposures table, drug_source_concept_id  field

drug_concept_query = '''
    WITH
        data1 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS drug_total_row
            FROM
               `{DATASET}.unioned_ehr_drug_exposure` AS t1
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{DATASET}._mapping_drug_exposure`)  AS t2
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
               `{DATASET}.unioned_ehr_drug_exposure` AS t1
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{DATASET}._mapping_drug_exposure`)  AS t2
            ON
                t1.drug_exposure_id=t2.drug_exposure_id
            INNER JOIN
                `{DATASET}.concept` as t3
            ON
                t3.concept_id = t1.drug_source_concept_id
            WHERE 
                 t3.standard_concept="S" and t3.domain_id="Drug"
            GROUP BY
                1
        )

    SELECT
        data1.src_hpo_id,
        drug_well_defined_row,
        drug_total_row,
        round(100*(drug_well_defined_row/drug_total_row),1) as drug_success_rate
    FROM
        data1
    LEFT OUTER JOIN
        data2
    ON
        data1.src_hpo_id=data2.src_hpo_id
    ORDER BY
        1 DESC
    '''.format(DATASET=DATASET)
drug_concept_df = execute(client, drug_concept_query)

drug_concept_df = drug_concept_df.fillna(0)
drug_concept_df

# # 23.Dataframe (row for each hpo_id) Observation table, Observation_source_concept_id field
#
#

observation_concept_query = '''
    WITH
        data1 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS observation_total_row
            FROM
               `{DATASET}.unioned_ehr_observation` AS t1
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{DATASET}._mapping_observation`)  AS t2
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
               `{DATASET}.unioned_ehr_observation` AS t1
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{DATASET}._mapping_observation`)  AS t2
            ON
                t1.observation_id=t2.observation_id
            INNER JOIN
                `{DATASET}.concept` as t3
            ON
                t3.concept_id = t1.observation_source_concept_id 
            WHERE 
                 t3.standard_concept="S"
            GROUP BY
                1
        )

    SELECT
        data1.src_hpo_id,
        observation_well_defined_row,
        observation_total_row,
        round(100*(observation_well_defined_row/observation_total_row),1) as observation_success_rate
    FROM
        data1
    LEFT OUTER JOIN
        data2
    ON
        data1.src_hpo_id=data2.src_hpo_id
    ORDER BY
        1 DESC
    '''.format(DATASET=DATASET)
observation_concept_df = execute(client, observation_concept_query)

observation_concept_df = observation_concept_df.fillna(0)
observation_concept_df

# # 21.Dataframe (row for each hpo_id) Measurement table, measurement_source_concept_id field

measurement_concept_query = '''
    WITH
        data1 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS measurement_total_row
            FROM
               `{DATASET}.unioned_ehr_measurement` AS t1
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{DATASET}._mapping_measurement`)  AS t2
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
               `{DATASET}.unioned_ehr_measurement` AS t1
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{DATASET}._mapping_measurement`)  AS t2
            ON
                t1.measurement_id=t2.measurement_id
            INNER JOIN
                `{DATASET}.concept` as t3
            ON
                t3.concept_id = t1.measurement_source_concept_id
            WHERE 
                 t3.standard_concept="S" and t3.domain_id="Measurement"
            GROUP BY
                1
        )

    SELECT
        data1.src_hpo_id,
        measurement_well_defined_row,
        measurement_total_row,
        round(100*(measurement_well_defined_row/measurement_total_row),1) as measurement_success_rate
    FROM
        data1
    LEFT OUTER JOIN
        data2
    ON
        data1.src_hpo_id=data2.src_hpo_id
    ORDER BY
        1 DESC
    '''.format(DATASET=DATASET)
measurement_concept_df = execute(client, measurement_concept_query)

measurement_concept_df = measurement_concept_df.fillna(0)
measurement_concept_df

# # 21.Dataframe (row for each hpo_id) visit_occurrence table, visit_source_concept_id field

visit_concept_query = '''
    WITH
        data1 AS (
            SELECT
                src_hpo_id,
                COUNT(*) AS visit_total_row
            FROM
               `{DATASET}.unioned_ehr_visit_occurrence` AS t1
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{DATASET}._mapping_visit_occurrence`)  AS t2
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
               `{DATASET}.unioned_ehr_visit_occurrence` AS t1
            INNER JOIN
                (SELECT
                    DISTINCT * 
                FROM
                     `{DATASET}._mapping_visit_occurrence`)  AS t2
            ON
                t1.visit_occurrence_id=t2.visit_occurrence_id
            INNER JOIN
                `{DATASET}.concept` as t3
            ON
                t3.concept_id = t1.visit_source_concept_id
            WHERE 
                 t3.standard_concept="S" and t3.domain_id="Visit"
            GROUP BY
                1
        )

    SELECT
        data1.src_hpo_id,
        visit_well_defined_row,
        visit_total_row,
        round(100*(visit_well_defined_row/visit_total_row),1) as visit_success_rate
    FROM
        data1
    LEFT OUTER JOIN
        data2
    ON
        data1.src_hpo_id=data2.src_hpo_id
    ORDER BY
        1 DESC
    '''.format(DATASET=DATASET)
visit_concept_df = execute(client, visit_concept_query)

visit_concept_df = visit_concept_df.fillna(0)
visit_concept_df

datas = [
    procedure_concept_df, drug_concept_df, observation_concept_df,
    measurement_concept_df, visit_concept_df
]

master_df = condition_concept_df

for filename in datas:
    master_df = pd.merge(master_df, filename, on='src_hpo_id', how='outer')

master_df

source = pd.merge(master_df, site_df, how='outer', on='src_hpo_id')
source = source.fillna("No Data")
source.to_csv("{cwd}/source_concept_success_rate.csv".format(cwd=cwd))
