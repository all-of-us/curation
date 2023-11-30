BLANK = ''

# Dataset Environment variable names
MATCH_DATASET = 'VALIDATION_RESULTS_DATASET_ID'

# HPO table info
LOOKUP_TABLES_DATASET_ID = 'lookup_tables'
HPO_SITE_ID_MAPPINGS_TABLE_ID = 'hpo_site_id_mappings'
HPO_ID_BUCKET_NAME_TABLE_ID = 'hpo_id_bucket_name'
HPO_ID_CONTACT_LIST_TABLE_ID = 'hpo_id_contact_list'

HPO_ID = 'HPO_ID'
BUCKET_NAME = 'bucket_name'

# Query to select bucket name
SELECT_BUCKET_NAME_QUERY = """
SELECT
  bucket_name
FROM
  `{{project_id}}.{{dataset_id}}.{{table_id}}`
WHERE
  LOWER(hpo_id) = LOWER('{{hpo_id}}')
  AND LOWER(service) = LOWER('{{service}}')
"""

# Validation dataset prefix
VALIDATION_PREFIX = 'validation'
VALIDATION_DATASET_FORMAT = 'validation_{}'
VALIDATION_DATASET_REGEX = 'validation_\d{8}'
VALIDATION_DATE_FORMAT = '%Y%m%d'

# Query to list all table information within a dataset
DATASET_COLUMNS_QUERY = """
SELECT *
FROM `{{project_id}}.{{dataset_id}}.INFORMATION_SCHEMA.COLUMNS`
"""

TABLE_NAME = 'table_name'
COLUMN_NAME = 'column_name'

#Create or Replace Table query
CREATE_OR_REPLACE_TABLE_QUERY = """
CREATE OR REPLACE TABLE `{{project_id}}.{{dataset_id}}.{{table_id}}` (
{% for field in schema -%}
  {{ field.name }} {{ field.field_type }} {% if field.mode.lower() == 'required' -%} NOT NULL {%- endif %}
  {% if field.description %} OPTIONS (description="{{ field.description }}") {%- endif %}
  {% if loop.nextitem %},{% endif -%}
{%- endfor %} )
{% if opts -%}
OPTIONS (
    {% for opt_name, opt_val in opts.items() -%}
    {{opt_name}}=
        {% if opt_val is string %}
        "{{opt_val}}"
        {% elif opt_val is mapping %}
        [
            {% for opt_val_key, opt_val_val in opt_val.items() %}
                ("{{opt_val_key}}", "{{opt_val_val}}"){% if loop.nextitem is defined %},{% endif %}
            {% endfor %}
        ]
        {% endif %}
        {% if loop.nextitem is defined %},{% endif %}
    {%- endfor %} )
{%- endif %}
{% if cluster_by_cols -%}
CLUSTER BY
{% for col in cluster_by_cols -%}
    {{col}}{% if loop.nextitem is defined %},{% endif %}
{%- endfor %}
{%- endif -%}
-- Note clustering/partitioning in conjunction with AS query_expression is --
-- currently unsupported (see https://bit.ly/2VeMs7e) --
{% if query -%} AS {{ query }} {%- endif %}
"""

GET_HPO_CONTENTS_QUERY = """
SELECT *
FROM `{project_id}.{TABLES_DATASET_ID}.{HPO_SITE_TABLE}`
"""
