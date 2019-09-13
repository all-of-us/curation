# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.3'
#       jupytext_version: 0.8.3
#   kernelspec:
#     display_name: Python 2
#     language: python
#     name: python2
#   language_info:
#     codemirror_mode:
#       name: ipython
#       version: 2
#     file_extension: .py
#     mimetype: text/x-python
#     name: python
#     nbconvert_exporter: python
#     pygments_lexer: ipython2
#     version: 2.7.16
# ---

# +
# %matplotlib inline
import google.datalab.bigquery as bq
import warnings

warnings.filterwarnings('ignore')
# -

dataset=''

# def row_counts(dataset_ids):
# sq = "SELECT '{dataset_id}' dataset_id, table_id, row_count FROM {dataset_id}.__TABLES__"
# sqs = map(lambda d: sq.format(dataset_id=d), dataset_ids)
# iq = "\nUNION ALL\n".join(sqs)
def get_hpo_ids():
    query = "SELECT distinct hpo_id FROM lookup_tables.hpo_site_id_mappings where hpo_id<>''"
    df = bq.Query(query).execute(output_options=bq.QueryOutput.dataframe(use_cache=False)).result()
    return df['hpo_id']


def get_hpo_table_columns(hpo_id):
    '''
    This function returns all the table names with all their column names for a given hpo_id. Also, the table row count
    is retrieved as well.
    :param hpo_id: hpo site id
    :return: dataframe with table name, column name and table row count
    '''
    query = """SELECT table_name, column_name, t.row_count as table_row_count, '{hpo_id}' as hpo_id 
               FROM {dataset}.INFORMATION_SCHEMA.COLUMNS c
               JOIN {dataset}.__TABLES__ t on c.table_name=t.table_id
               WHERE STARTS_WITH(table_id, lower('{hpo_id}'))=true AND
               NOT(table_id like '_mapping%') AND 
                (
                  table_id like '%person' OR
                  table_id like '%visit_occurrence' OR
                  table_id like '%condition_occurrence' OR
                  table_id like '%procedure_occurrence' OR
                  table_id like '%drug_exposure' OR
                  table_id like '%measurement' OR
                  table_id like '%observation' OR
                  table_id like '%device_exposure' OR
                  table_id like '%death' OR
                  table_id like '%provider' OR
                  table_id like '%specimen' OR
                  table_id like '%location' OR
                  table_id like '%care_site' OR
                  table_id like '%note'
                  )""".format(hpo_id=hpo_id, dataset=dataset)
    df = bq.Query(query).execute(output_options=bq.QueryOutput.dataframe(use_cache=False)).result()
    return df


def create_hpo_completeness_query(table_columns, hpo_id):
    query_with_concept_id = """SELECT current_datetime() as report_run_time, x.*, CASE WHEN total_rows=0 THEN 0 ELSE (num_nonnulls_zeros)/(total_rows) END as percent_field_populated 
       FROM (
            SELECT '{table_name}' as table_name, '{column_name}' as column_name,
                   '{hpo_id}' as site_name,
                   {table_row_count} as total_rows, 
                   sum(case when {column_name}=0 then 0 else 1 end) as num_nonnulls_zeros,
                   ({table_row_count} - count({column_name})) as non_populated_rows 
                   FROM {dataset}.{table_name} 
        ) as x 
    """
    query_without_concept_id = """SELECT current_datetime() as report_run_time, x.*, CASE WHEN total_rows=0 THEN 0 ELSE (num_nonnulls_zeros)/(total_rows) END as percent_field_populated 
       FROM (
            SELECT '{table_name}' as table_name, '{column_name}' as column_name,
                   '{hpo_id}' as site_name,
                   {table_row_count} as total_rows, 
                   count({column_name}) as num_nonnulls_zeros, 
                   ({table_row_count} - count({column_name})) as non_populated_rows 
                   FROM {dataset}.{table_name} 
        ) as x 
    """
    # queries = [query_with_concept_id.format(table_name=row['table_name'], column_name=row['column_name'],
    #                         table_row_count=row['table_row_count']) if row['column_name'].endswith('concept_id')
    #                    else query_with_concept_id.format(table_name=row['table_name'], column_name=row['column_name'],
    #                         table_row_count=row['table_row_count']) for i, row in table_columns.iteritems()]
    queries = []
    for i, row in table_columns.iterrows():
        if row['column_name']=='_PARTITIONTIME':
            continue

        if row['column_name'].endswith('concept_id'):
            x = query_with_concept_id.format(table_name=row['table_name'],
                                             column_name=row['column_name'],
                                             hpo_id=hpo_id.lower(),
                                             table_row_count=row['table_row_count'],
                                             dataset=dataset)
        else:
            x = query_without_concept_id.format(table_name=row['table_name'],
                                                column_name=row['column_name'],
                                                hpo_id=hpo_id.lower(),
                                                table_row_count=row['table_row_count'],
                                                dataset=dataset)
        queries.append(x)

    return " union all ".join(queries)


# Output the data completeness for the omop tables
# All the data is written to a file per site.
hpo_ids = get_hpo_ids()
for i, hpo_id in hpo_ids.iteritems():
    table_columns = get_hpo_table_columns(hpo_id)

    query = create_hpo_completeness_query(table_columns, hpo_id)
    if query == "":
        continue
#     print(query)
#     break
    try:
        df = bq.Query(query).execute(output_options=bq.QueryOutput.dataframe(use_cache=False)).result()
        df.to_csv("{hpo_id}_omop_tables_coverage.csv".format(hpo_id=hpo_id), sep=',', encoding='utf-8')
    except:
        print("hpo-->{hpo_id}".format(hpo_id=hpo_id))
        print("query-->{q}".format(q=query))
        print("table_columns-->{table_columns}".format(table_columns=table_columns))
        break
