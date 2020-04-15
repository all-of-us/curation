# # DC-732 Retract Rows with Suppressed Concepts
#
# Identify, log and remove all rows from all tables in specified deid datasets where at least one
# concept_id field references a concept which must be suppressed.
#
# ## Notes
#
#  * Prior to running the table identified by `CONCEPT_LOOKUP_TABLE` must be loaded with concepts to
#    suppress. At minimum this table should have the field `concept_id`.
#  * This can safely be re-run (e.g. if a new concept is added to the lookup).

# +
from google.cloud import bigquery
import jinja2
import pandas as pd

from utils import bq

# -

ISSUE_NUMBER = 'DC-732'
ISSUE_PREFIX = 'dc732_'
ROWS_RESOURCE_NAME = f'{ISSUE_PREFIX}suppress_rows'
SUMMARY_RESOURCE_NAME = f'{ISSUE_PREFIX}suppress_summary'

JINJA_ENV = jinja2.Environment(
    # block tags on their own lines
    # will not cause extra white space
    trim_blocks=True,
    lstrip_blocks=True,
    # syntax highlighting should be better
    # with these comment delimiters
    comment_start_string='--',
    comment_end_string=' --')
CLIENT = bq.get_client()

# # Identify and Log Records
#
# The following is to help ensure generation of valid queries (account for missing tables, changes
# to our ext table conventions). For supplied datasets, retrieve all OMOP clinical tables (those
# with a person_id and primary key column) and their associated ext table.
#
# ## Rows
# For each target dataset, compose a query which identifies all rows from all tables where at least
# one concept_id field contains one of the concept_ids to suppress. Log these results to a csv file
# and to a sandbox table with the below schema:
#
# | Field name            | Type    | Description                                      |
# |-----------------------|---------|--------------------------------------------------|
# | dataset_id            | STRING  | Identifies the dataset                           |
# | table                 | STRING  | Identifies the table                             |
# | row_id                | INTEGER | Value of the key column                          |
# | person_id             | INTEGER | person_id in the record                          |
# | disallowed_concept_id | INTEGER | Identifies the first found concept that should be suppressed |
# | source                | STRING  | The source of the record (EHR site or PPI/PM)    |
#
# ## Summary
# In memory, compute the total number of rows, number of participants, number of sources (i.e. EHR
# sites, PPI/PM). Log these results to a csv file and to a sandbox table with the below schema.
#
# | Field name            | Type    | Description                                      |
# |-----------------------|---------|--------------------------------------------------|
# | dataset_id            | STRING  | Identifies the dataset                           |
# | table                 | STRING  | Identifies the table                             |
# | disallowed_concept_id | INTEGER | Identifies the concept that should be suppressed |
# | n_person_id           | INTEGER | Number of unique person_ids                      |
# | n_row_id              | STRING  | Number of rows                                   |
# | n_source              | INTEGER | Number of unique sources                         |
# | ppi_pm                | INTEGER | 1 if found in PPI/PM records, 0 otherwise        |

TABLES_TO_SUPPRESS_QUERY = JINJA_ENV.from_string("""
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


def get_tables_to_suppress_df(project_id, target_dataset_ids):
    """
    Retrieve table info for datasets to be suppressed

    :param project_id: identifies the project containing the target datasets
    :param target_dataset_ids: list of dataset_ids of datasets to suppress
    :return: dataframe with dataset_id, table_name, ext_table_name, columns
    """
    query = TABLES_TO_SUPPRESS_QUERY.render(project_id=project_id,
                                            dataset_ids=target_dataset_ids)
    query_job = CLIENT.query(query)
    tables_to_wipe_df = query_job.to_dataframe()
    return tables_to_wipe_df


ROWS_TO_SUPPRESS_QUERY = JINJA_ENV.from_string("""
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


def get_rows_to_suppress_queries(tables_to_suppress_df, concept_lookup_table):
    """
    Get queries to retrieve info on rows to suppress from target datasets

    :param tables_to_suppress_df: dataframe with dataset_id, table_name, ext_table_name, columns
    :param concept_lookup_table: identifies the lookup table with concepts to suppress
    :return: list of SQL strings
    """
    dataset_group = tables_to_suppress_df.groupby('dataset_id')
    dataset_queries = []
    for dataset_id, table_info_df in dataset_group:
        sub_queries = []
        for _, table_info in table_info_df.iterrows():
            cols = table_info['columns']
            concept_fields = [
                dict(name=col, alias=col.replace('_id', ''))
                for col in cols
                if is_concept_id_col(col)
            ]
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


def get_rows_to_suppress_df(tables_to_suppress_df, concept_lookup_table):
    """
    Retrieve key info on rows to suppress from target datasets

    :param tables_to_suppress_df: dataframe with dataset_id, table_name, ext_table_name, columns
    :param concept_lookup_table: identifies the lookup table with concepts to suppress
    :return: dataframe with dataset_id, disallowed_concept_id, table, n_person_id, n_row_id,
             n_source, ppi_pm
    """
    dataset_queries = get_rows_to_suppress_queries(tables_to_suppress_df,
                                                   concept_lookup_table)
    return pd.concat(
        CLIENT.query(dataset_query).to_dataframe()
        for dataset_query in dataset_queries)


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
    summary_cols = [
        'dataset_id', 'table', 'disallowed_concept_id', 'person_id', 'row_id',
        'source'
    ]
    group_by_concept = rows_to_suppress_df[summary_cols].groupby(
        ['dataset_id', 'table', 'disallowed_concept_id'])
    summary_df = group_by_concept.nunique().drop(
        columns=['dataset_id', 'table', 'disallowed_concept_id'])
    summary_df = summary_df.add_prefix('n_')
    summary_df['ppi_pm'] = group_by_concept.apply(ppi_pm)
    return summary_df.sort_values(by=['dataset_id', 'table', 'n_person_id'],
                                  ascending=False)


# # Backup Rows
# Backup rows are stored in tables whose structure is identical to domain tables except for the
# addition of fields to distinguish the dataset and table the original row came from. For example,
# `visit_occurrence` rows would be backed up in a table with the below schema.
#
# | Field name                    | Type    |
# |-------------------------------|---------|
# | **dataset_id**                | STRING  |
# | **table**                     | STRING  |
# | visit_occurrence_id           | INTEGER |
# | person_id                     | INTEGER |
# | visit_concept_id              | INTEGER |
# | --- _other columns_  ---      |         |
# | discharge_to_source_value     | STRING  |
# | preceding_visit_occurrence_id | INTEGER |

# +
BACKUP_TABLE_ROWS_QUERY = JINJA_ENV.from_string("""
{% for dataset_id in dataset_ids %}
SELECT 
 '{{dataset_id}}' AS dataset_id, 
 '{{table}}' table, 
 t.* 
FROM {{dataset_id}}.{{table}} t
JOIN {{suppress_rows_table}} r
 ON r.row_id = t.{{table}}_id
WHERE r.dataset_id = '{{dataset_id}}'
AND r.table = '{{table}}'
{% if loop.nextitem is defined %}
 UNION ALL
{% endif %}
{% endfor %}
""")

BACKUP_EXT_TABLE_ROWS_QUERY = JINJA_ENV.from_string("""
{% for dataset_id in dataset_ids %}
SELECT 
 '{{dataset_id}}' AS dataset_id, 
 '{{table}}_ext' table, 
 t.* 
FROM {{dataset_id}}.{{table}}_ext t
JOIN {{suppress_rows_table}} r
 ON r.row_id = t.{{table}}_id
WHERE r.dataset_id = '{{dataset_id}}'
AND r.table = '{{table}}'
{% if loop.nextitem is defined %}
 UNION ALL
{% endif %}
{% endfor %}
""")


def backup_rows_to_suppress(tables_to_suppress_df, suppress_rows_table):
    """
    Create a table for each domain and load rows that will be suppressed across all datasets

    Note: Rows are appended if tables exist
    
    :param tables_to_suppress_df: dataframe with dataset_id, table_name, ext_table_name, columns
    :param suppress_rows_table: identifies the table where suppress rows are loaded
    :return: list of query jobs
    """
    tables_group_df = tables_to_suppress_df.groupby(
        by=['table_name', 'ext_table_name'])
    query_jobs = []
    for (table_name, ext_table_name), table_info in tables_group_df:
        dataset_ids = table_info.dataset_id.unique()
        backup_rows_dest_table = f'{PROJECT_ID}.{SANDBOX_DATASET_ID}.{ISSUE_PREFIX}{table_name}'
        job_config = bigquery.QueryJobConfig(
            destination=backup_rows_dest_table)
        job_config.write_disposition = 'WRITE_APPEND'
        backup_table_rows_query = BACKUP_TABLE_ROWS_QUERY.render(
            dataset_ids=dataset_ids,
            table=table_name,
            suppress_rows_table=suppress_rows_table)
        backup_table_query_job = CLIENT.query(backup_table_rows_query,
                                              job_config=job_config)
        query_jobs.append(backup_table_query_job)

        if ext_table_name:
            backup_ext_dest_table = f'{PROJECT_ID}.{SANDBOX_DATASET_ID}.{ISSUE_PREFIX}{ext_table_name}'
            job_config = bigquery.QueryJobConfig(
                destination=backup_ext_dest_table)
            job_config.write_disposition = 'WRITE_APPEND'
            backup_ext_table_rows_query = BACKUP_EXT_TABLE_ROWS_QUERY.render(
                dataset_ids=dataset_ids,
                table=table_name,
                suppress_rows_table=suppress_rows_table)
            backup_ext_table_query_job = CLIENT.query(
                backup_ext_table_rows_query, job_config=job_config)
            query_jobs.append(backup_ext_table_query_job)
    return query_jobs


# -

# # Execute Deletion
# Use a meta-query to generate the minimal set of DELETE statements needed to delete rows in
# domain and \_ext tables.

# +
GET_DELETE_QUERIES = JINJA_ENV.from_string("""
WITH table_ids AS
(SELECT DISTINCT dataset_id, table 
FROM `{{suppress_rows_table}}`)
SELECT CONCAT('DELETE FROM `', 
 dataset_id, '.', table, '` t', 
 ' WHERE EXISTS', 
 '(SELECT 1 FROM {{suppress_rows_table}} WHERE dataset_id = "', 
 dataset_id, '" AND table = "', table, '" AND t.', table, '_id = row_id)')
AS query_string
FROM table_ids
""")


def get_delete_queries(suppress_rows_table):
    """
    Get list of DELETE statements needed to delete rows in domain and _ext tables
    
    :param suppress_rows_table: identifies the table where suppress rows are loaded
    """
    get_delete_query = GET_DELETE_QUERIES.render(
        suppress_rows_table=suppress_rows_table)
    delete_query_job = CLIENT.query(get_delete_query)
    delete_query_results = list(delete_query_job.result())
    all_delete_queries = []
    for delete_query_result in delete_query_results:
        delete_query = delete_query_result['query_string']
        # Derive the query to delete the associated extension table rows by adding _ext suffix to
        # the domain table in the query string. For reference, this is an example query on
        # condition_occurrence:
        #
        # DELETE FROM `project.dataset.condition_occurrence` t WHERE EXISTS(
        #  SELECT 1 FROM dc732_suppress_rows WHERE
        #      project.sandbox.dataset_id = "dataset"
        #  AND table = "condition_occurrence"
        #  AND t.condition_occurrence_id = row_id)
        delete_ext_query = delete_query.replace('` t WHERE', '_ext` t WHERE')
        all_delete_queries.extend([delete_query, delete_ext_query])
    return all_delete_queries


# -


def run_delete_queries(delete_queries):
    """
    Execute list of queries which delete rows
    
    :param delete_queries: list of DELETE statements
    """
    # Delete rows in batch to prevent concurrent request limit
    delete_query_jobs = []
    for delete_query in delete_queries:
        print(delete_query + '\n')
        job_config = bigquery.QueryJobConfig(
            priority=bigquery.QueryPriority.BATCH)
        delete_query_job = CLIENT.query(delete_query, job_config=job_config)
        delete_query_jobs.append(delete_query_job)
    return delete_query_jobs


def print_jobs(delete_query_jobs):
    # Manually checking if jobs are all done...
    for delete_query_job in delete_query_jobs:
        delete_query_job.reload()
        print(
            "{j.job_id},{j.destination.dataset_id},{j.destination.table_id},{j.num_dml_affected_rows}"
            .format(j=delete_query_job))


def main(project_id, sandbox_dataset_id, concept_lookup_table,
         target_dataset_ids):
    """
    Identify, log and remove rows with suppresed concepts
    Note: Rows are appended to log and backup tables if they already exist
    
    :param project_id: Identifies the project containing all associated datasets
    :param sandbox_dataset_id: Identifies a dataset to store log and backup records
    :param concept_lookup_table: Identifies the table containing the concepts to suppress
    :param target_dataset_ids: List of datasets to retract data from
    
    """
    # get suppress row info
    rows_dest_table = f'{sandbox_dataset_id}.{ROWS_RESOURCE_NAME}'
    tables_to_suppress_df = get_tables_to_suppress_df(project_id,
                                                      target_dataset_ids)
    suppress_rows_df = get_rows_to_suppress_df(tables_to_suppress_df,
                                               concept_lookup_table)
    suppress_rows_df.to_csv(f'{ROWS_RESOURCE_NAME}.csv')
    suppress_rows_df.to_gbq(destination_table=rows_dest_table,
                            if_exists='append')

    # get suppress summary
    summary_dest_table = f'{sandbox_dataset_id}.{SUMMARY_RESOURCE_NAME}'
    suppress_summary_df = get_suppress_summary_df(suppress_rows_df)
    suppress_summary_df.to_csv(f'{SUMMARY_RESOURCE_NAME}.csv')
    suppress_summary_df.reset_index(inplace=True)
    suppress_summary_df.to_gbq(destination_table=summary_dest_table,
                               if_exists='append')

    # backup rows
    backup_rows_to_suppress(tables_to_suppress_df, rows_dest_table)

    # delete rows
    all_delete_queries = get_delete_queries(rows_dest_table)
    delete_query_jobs = run_delete_queries(all_delete_queries)
    print_jobs(delete_query_jobs)


if __name__ == '__main__':
    PROJECT_ID = ''
    SANDBOX_DATASET_ID = ''
    CONCEPT_LOOKUP_TABLE = f'{SANDBOX_DATASET_ID}.expanded_deid_concepts_20200331'
    SUPPRESS_ROWS_DEST_TABLE = f'{SANDBOX_DATASET_ID}.{ROWS_RESOURCE_NAME}'
    SUMMARY_DEST_TABLE = f'{SANDBOX_DATASET_ID}.{SUMMARY_RESOURCE_NAME}'
    TARGET_DATASETS = []
    TABLES_TO_SUPPRESS_DF = get_tables_to_suppress_df(PROJECT_ID,
                                                      TARGET_DATASETS)
    SUPPRESS_ROWS_DF = get_rows_to_suppress_df(TABLES_TO_SUPPRESS_DF,
                                               CONCEPT_LOOKUP_TABLE)
    SUPPRESS_ROWS_DF.to_csv(f'{ROWS_RESOURCE_NAME}.csv')
    SUPPRESS_ROWS_DF.to_gbq(destination_table=SUPPRESS_ROWS_DEST_TABLE,
                            if_exists='append')
    SUPPRESS_SUMMARY_DF = get_suppress_summary_df(SUPPRESS_ROWS_DF)
    SUPPRESS_SUMMARY_DF.to_csv(f'{SUMMARY_RESOURCE_NAME}.csv')
    SUPPRESS_SUMMARY_DF.reset_index(inplace=True)
    SUPPRESS_SUMMARY_DF.to_gbq(destination_table=SUMMARY_DEST_TABLE,
                               if_exists='append')

    backup_rows_to_suppress(TABLES_TO_SUPPRESS_DF, SUPPRESS_ROWS_DEST_TABLE)

    ALL_DELETE_QUERIES = get_delete_queries(SUPPRESS_ROWS_DEST_TABLE)
    DELETE_QUERY_JOBS = run_delete_queries(ALL_DELETE_QUERIES)
    print_jobs(DELETE_QUERY_JOBS)
