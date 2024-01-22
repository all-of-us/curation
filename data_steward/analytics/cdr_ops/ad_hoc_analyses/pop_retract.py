# +
from jinja2 import Template

import utils.bq
from notebooks import render
from notebooks.defaults import is_deid_dataset
from notebooks.parameters import SANDBOX, RDR_DATASET_ID, COMBINED_DATASET_ID, DEID_DATASET_ID

RDR = RDR_DATASET_ID
DEID = DEID_DATASET_ID
COMBINED = COMBINED_DATASET_ID

# identifies dataset containing the records to remove (original or backup)
INPUT_DATASET = COMBINED

# identifies dataset to save retracted instance
OUTPUT_DATASET = ''

# table containing the research IDs whose records must be retracted
ID_TABLE = '{SANDBOX}.{COMBINED}_aian_ids'.format(SANDBOX=SANDBOX,
                                                  COMBINED=COMBINED)

IS_INPUT_DATASET_DEID = is_deid_dataset(INPUT_DATASET)

print("""
INPUT_DATASET={INPUT_DATASET}
OUTPUT_DATASET={OUTPUT_DATASET}
ID_TABLE={ID_TABLE}
""".format(INPUT_DATASET=INPUT_DATASET,
           OUTPUT_DATASET=OUTPUT_DATASET,
           ID_TABLE=ID_TABLE))
# -

# # IDs whose records must be retracted

# Determine associated research IDs for RDR participants whose data must be retracted
AIAN_PID_QUERY = """
SELECT DISTINCT
       rdr.person_id    AS person_id,
       deid.research_id AS research_id
FROM `{RDR}.observation` rdr
 JOIN `{COMBINED}.deid_map` deid
  ON rdr.person_id = deid.person_id
WHERE
    rdr.observation_source_concept_id = 1586140
AND rdr.value_source_concept_id       = 1586141
"""
q = AIAN_PID_QUERY.format(RDR=RDR, COMBINED=COMBINED)
aian_pid_df = utils.bq.query(q)
render.dataframe(aian_pid_df)

# Save research IDs to a table in the sandbox
aian_pid_df.to_gbq(destination_table=ID_TABLE, if_exists='fail')

# # Expected row counts after retraction vs. actual row counts

# +
PERSON_TABLE_QUERY = """
SELECT table_name
FROM `{INPUT_DATASET}.INFORMATION_SCHEMA.COLUMNS`
WHERE COLUMN_NAME = 'person_id'
"""


def get_tables_with_person_id(input_dataset):
    """
    Get list of tables that have a person_id column
    """
    person_table_query = PERSON_TABLE_QUERY.format(INPUT_DATASET=input_dataset)
    person_tables_df = utils.bq.query(person_table_query)
    return list(person_tables_df.table_name.get_values())


person_tables = get_tables_with_person_id(INPUT_DATASET)
# -

# Construct query to get row counts and number of rows to be deleted for any table
ROW_COUNTS_QUERY_TPL = """
WITH delete_row_counts AS (
 {% for table in TABLES %}
   (
     SELECT '{{ table }}' AS table_name,
     COUNT(1) AS rows_to_delete,
     (SELECT row_count FROM {{ INPUT_DATASET }}.__TABLES__ WHERE table_id = '{{ table }}') AS total_rows
     FROM `{{ INPUT_DATASET }}.{{ table }}` t
     WHERE EXISTS (
      SELECT 1 FROM `{{ ID_TABLE }}`
      WHERE {{ 'research_id' if IS_INPUT_DATASET_DEID else 'person_id' }} = t.person_id)
   )
   {% if not loop.last %}
   UNION ALL
   {% endif %}
 {% endfor %}
)
SELECT
 d.table_name,
 d.total_rows                                    AS input_row_count,
 d.rows_to_delete                                AS rows_to_delete,
 d.total_rows - d.rows_to_delete                 AS expected_output_row_count,
 t.row_count                                     AS actual_output_row_count,
 t.row_count = (d.total_rows - d.rows_to_delete) AS pass
FROM delete_row_counts d
JOIN {{ OUTPUT_DATASET }}.__TABLES__ t
 ON d.table_name = t.table_id
"""
query = Template(ROW_COUNTS_QUERY_TPL).render(
    OUTPUT_DATASET=OUTPUT_DATASET,
    INPUT_DATASET=INPUT_DATASET,
    ID_TABLE=ID_TABLE,
    TABLES=person_tables,
    IS_INPUT_DATASET_DEID=IS_INPUT_DATASET_DEID)
row_counts_df = utils.bq.query(query)
render.dataframe(row_counts_df)
