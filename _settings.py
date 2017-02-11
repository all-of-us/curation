# Configuration settings

# ID of HPO to validate (see resources/hpo.csv)
hpo_id = 'hpo_id'

# location of files to validate, evaluate
csv_dir = 'path/to/csv_files'

# sprint number being validated against
sprint_num = 2

# URL for database where submissions should be stored
# For more examples and requirements see http://docs.sqlalchemy.org/en/latest/core/engines.html
# Note: Connecting to MSSQL from *nix may require additional FreeTDS configuration (see https://goo.gl/qKhusY)
conn_str = 'mssql+pymssql://localhost/pmi_sprint_2'

# --- WebAPI configuration (optional) ---
# Update the following settings if you are running [[webapi.py]]

# URL for the webapi database
webapi_conn_str = 'mssql+pymssql://localhost/webapi'

# JDBC URL for database where submissions should be stored
# This is usually the same database as conn_str
cdm_jdbc_conn_str = 'jdbc:sqlserver://localhost;databaseName=pmi_sprint_2;integratedSecurity=true'

force_multi_schema = False
