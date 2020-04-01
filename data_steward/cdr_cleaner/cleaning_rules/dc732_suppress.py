"""
 Identify and remove all rows from all tables in the specified deid datasets where at least one concept_id field
 contains one of the concept_ids to suppress.

 Note: Assumes sandbox table containing concepts to suppress was loaded prior

 The following is to help ensure generation of valid queries (account for missing tables, changes to our ext
 table conventions). For supplied datasets, retrieve all OMOP clinical tables (those with a person_id and primary key
 column) and their associated ext table.

 For each target dataset, compose a query which identifies all rows from all tables where at least one concept_id
 field contains one of the concept_ids to suppress.

 In memory, compute the total number of rows, number of participants, number of sources (i.e. EHR sites, PPI/PM).
"""
# +
import pandas as pd
import jinja2

from utils import bq

# -

ISSUE_NUMBER = 'DC-732'
ISSUE_PREFIX = 'dc732_'
ROWS_RESOURCE_NAME = f'{ISSUE_PREFIX}suppress_rows'
SUMMARY_RESOURCE_NAME = f'{ISSUE_PREFIX}suppress_summary'

jinja_env = jinja2.Environment(
    # block tags on their own lines
    # will not cause extra white space
    trim_blocks=True,
    lstrip_blocks=True,
    # syntax highlighting should be better
    # with these comment delimiters
    comment_start_string='--',
    comment_end_string=' --')

TABLES_TO_SUPPRESS_QUERY = jinja_env.from_string(
    """
    -- Columns for all tables in all datasets --
    WITH all_columns AS (
    {% for dataset_id in dataset_ids %}
     SELECT TABLE_SCHEMA,
       TABLE_NAME, 
       COLUMN_NAME,
       IS_SYSTEM_DEFINED
     FROM `{{project_id}}.{{dataset_id}}.INFORMATION_SCHEMA.COLUMNS`
    {% if loop.nextitem is defined %}
     UNION ALL
    {% endif %}
    {% endfor %}
    ),
    
    -- OMOP clinical tables and any tables named <clinical table>_ext --
    tables AS (
    SELECT
      TABLE_SCHEMA as dataset_id,
      REGEXP_EXTRACT(TABLE_NAME, r"^((?:dose_era|person|cohort_attribute|device_exposure|cohort|procedure_cost|location|observation_period|cohort_definition|drug_exposure|visit_occurrence|note|cost|cdm_source|death|measurement|provider|drug_cost|visit_cost|specimen|condition_occurrence|condition_era|device_cost|attribute_definition|drug_era|observation|payer_plan_period|fact_relationship|care_site|procedure_occurrence)(?:_ext)?)$") AS table_name,
      ARRAY_AGG(COLUMN_NAME) columns
    FROM all_columns
    WHERE 
      IS_SYSTEM_DEFINED = 'NO'
      AND table_name IS NOT NULL
    GROUP BY 1, 2)
    
    SELECT 
     t.dataset_id,
     -- Name of the OMOP clinical table --
     t.table_name,
     -- If it exists, name of corresponding ext table otherwise NULL -- 
     ext_table.table_name AS ext_table_name,
     -- Array of column names -- 
     t.columns
    FROM tables t
    
    -- Corresponding ext table, if any --
    LEFT JOIN tables ext_table
      ON  t.dataset_id = ext_table.dataset_id 
      AND CONCAT(t.table_name, '_ext') = ext_table.table_name
    WHERE    
    -- Has a key column --
    CONCAT(t.table_name, '_id') IN UNNEST(t.columns)
    
    -- Has a person_id column --
    AND 'person_id' IN UNNEST(t.columns)
    """)

client = bq.get_client()


def get_tables_to_suppress(project_id, target_dataset_ids):
    """
    Retrieve table info for datasets to be suppressed

    :param project_id: identifies the project containing the target datasets
    :param target_dataset_ids: list of dataset_ids of datasets to suppress
    :return: dataframe with dataset_id, table_name, ext_table_name, columns
    """
    query = TABLES_TO_SUPPRESS_QUERY.render(project_id=project_id, dataset_ids=target_dataset_ids)
    query_job = client.query(query)
    tables_to_wipe_df = query_job.to_dataframe()
    return tables_to_wipe_df


ROWS_TO_SUPPRESS_QUERY = jinja_env.from_string("""
SELECT DISTINCT
 '{{dataset_id}}'                         
                                          AS dataset_id,
 '{{table}}'
                                          AS table,
 t.{{table}}_id
                                          AS row_id,
 t.person_id                              AS person_id,

-- The first disallowed concept id encountered --
 COALESCE(
{% for concept_field in concept_fields %}
  {% if loop.previtem is defined %} ,{% else %}  {% endif %}{{concept_field['alias']}}.concept_id
{% endfor %})
                                          AS disallowed_concept_id,
-- If <clinical table>_ext does not exist for this table, NULL --
-- Casting NULL to string is required to prevent BQ mismatched type error --
{% if ext_table %} e.src_id {% else %} CAST(NULL AS STRING) {% endif %}
                                          AS source
FROM `{{dataset_id}}.{{table}}` t

-- Attempt to find disallowed concepts in ALL concept_id columns --
{% for concept_field in concept_fields %}
  LEFT JOIN `{{sandbox_table_id}}` {{concept_field['alias']}}
  ON t.{{concept_field['name']}} = {{concept_field['alias']}}.concept_id
{% endfor %}

-- If <clinical table>_ext exists, try to get the src_id --
{% if ext_table %}
-- Left join ensures rows with no corresponding ext row are still marked for removal --
  LEFT JOIN `{{dataset_id}}.{{ext_table}}` e
  ON t.{{table}}_id = e.{{table}}_id
{% endif %}
WHERE

-- At least one of the concept_id column is a disallowed concept_id --
{% for concept_field in concept_fields %}
  {% if loop.previtem is defined %} OR {% endif %} {{concept_field['alias']}}.concept_id IS NOT NULL
{% endfor %}""")


def is_concept_id_col(col):
    """
    Determine if column is a concept_id column

    :param col: name of the column
    :return: True if column is a concept_id column, False otherwise
    """
    return col.endswith('concept_id')


def get_rows_to_suppress_queries(project_id, target_dataset_ids, concept_lookup_table):
    """
    Get queries to retrieve info on rows to suppress from target datasets

    :param project_id: identifies the project containing the target datasets
    :param target_dataset_ids: list of dataset_ids of datasets to suppress
    :param concept_lookup_table: identifies the lookup table with concepts to suppress
    :return: list of SQL strings
    """
    tables_to_wipe_df = get_tables_to_suppress(project_id, target_dataset_ids)
    dataset_group = tables_to_wipe_df.groupby('dataset_id')
    dataset_queries = []
    for dataset_id, table_info_df in dataset_group:
        sub_queries = []
        for _, table_info in table_info_df.iterrows():
            cols = table_info['columns']
            concept_fields = [dict(name=col, alias=col.replace('_id', '')) for col in cols if is_concept_id_col(col)]
            if concept_fields:
                # Otherwise there are no rows to remove
                sub_query = ROWS_TO_SUPPRESS_QUERY.render(
                    sandbox_table_id=concept_lookup_table,
                    dataset_id=dataset_id,
                    table=table_info['table_name'],
                    ext_table=table_info['ext_table_name'],
                    concept_fields=concept_fields)
                sub_queries.append(sub_query)
        query = "\nUNION ALL\n".join(sub_queries)
        dataset_queries.append(query)
    return dataset_queries


def get_rows_to_suppress_df(project_id, target_dataset_ids, concept_lookup_table):
    """
    Retrieve key info on rows to suppress from target datasets

    :param project_id: identifies the project containing the target datasets
    :param target_dataset_ids: list of dataset_ids of datasets to suppress
    :param concept_lookup_table: identifies the lookup table with concepts to suppress
    :return: dataframe with dataset_id, disallowed_concept_id, table, n_person_id, n_row_id, n_source, ppi_pm
    """
    dataset_queries = get_rows_to_suppress_queries(project_id, target_dataset_ids, concept_lookup_table)
    return pd.concat(client.query(dataset_query).to_dataframe() for dataset_query in dataset_queries)


def ppi_pm(g):
    """
    True if any rows in the group have source PPI/PM
    """
    return g['source'].isin(['PPI/PM']).any()


def get_suppress_summary_df(rows_to_suppress_df):
    """
    Retrieve summary info (counts) on rows to suppress from target datasets

    :param rows_to_suppress_df: info on rows to suppress from target datasets
    :return: dataframe with dataset_id, table, disallowed_concept_id, person_id, row_id, source
    """
    summary_cols = ['dataset_id', 'table', 'disallowed_concept_id', 'person_id', 'row_id', 'source']
    group_by_concept = rows_to_suppress_df[summary_cols].groupby(['dataset_id', 'table', 'disallowed_concept_id'])
    summary_df = group_by_concept.nunique().drop(columns=['dataset_id', 'table', 'disallowed_concept_id'])
    summary_df = summary_df.add_prefix('n_')
    summary_df['ppi_pm'] = group_by_concept.apply(ppi_pm)
    return summary_df.sort_values(by=['dataset_id', 'table', 'n_person_id'], ascending=False)


def main(project_id, sandbox_dataset_id, concept_lookup_table, target_dataset_ids):
    rows_dest_table = f'{sandbox_dataset_id}.{ROWS_RESOURCE_NAME}'
    suppress_rows_df = get_rows_to_suppress_df(project_id, target_dataset_ids, concept_lookup_table)
    suppress_rows_df.to_csv(f'{ROWS_RESOURCE_NAME}.csv')
    suppress_rows_df.to_gbq(destination_table=rows_dest_table, if_exists='replace')
    suppress_summary_df = get_rows_to_suppress_df(SUPPRESS_ROWS_DF)
    suppress_summary_df.to_csv(f'{SUMMARY_RESOURCE_NAME}.csv')
    summary_dest_table = f'{sandbox_dataset_id}.{SUMMARY_RESOURCE_NAME}'
    suppress_summary_df.reset_index(inplace=True)
    suppress_summary_df.to_gbq(destination_table=summary_dest_table, if_exists='replace')


if __name__ == '__main__':
    PROJECT_ID = ''
    SANDBOX_DATASET_ID = ''
    CONCEPT_LOOKUP_TABLE = f'{SANDBOX_DATASET_ID}.expanded_deid_concepts_20200331'
    SUPPRESS_ROWS_DEST_TABLE = f'{SANDBOX_DATASET_ID}.{ROWS_RESOURCE_NAME}'
    SUMMARY_DEST_TABLE = f'{SANDBOX_DATASET_ID}.{SUMMARY_RESOURCE_NAME}'
    TARGET_DATASETS = [
    ]
    SUPPRESS_ROWS_DF = get_rows_to_suppress_df(PROJECT_ID, TARGET_DATASETS, CONCEPT_LOOKUP_TABLE)
    SUPPRESS_ROWS_DF.to_csv(f'{ROWS_RESOURCE_NAME}.csv')
    SUPPRESS_ROWS_DF.to_gbq(destination_table=SUPPRESS_ROWS_DEST_TABLE, if_exists='replace')
    SUPPRESS_SUMMARY_DF = get_suppress_summary_df(SUPPRESS_ROWS_DF)
    SUPPRESS_SUMMARY_DF.to_csv(f'{SUMMARY_RESOURCE_NAME}.csv')
    SUPPRESS_SUMMARY_DF.reset_index(inplace=True)
    SUPPRESS_SUMMARY_DF.to_gbq(destination_table=SUMMARY_DEST_TABLE, if_exists='replace')
