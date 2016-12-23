# Configuration settings

# ID of HPO to validate (see resources/hpo.csv)
hpo_id = 'hpo_id'

# location of files to validate, evaluate
csv_dir = 'path/to/csv_files'

# sprint number being validated against
sprint_num = 0

# Submissions and logs stored here
# For more examples and requirements see http://docs.sqlalchemy.org/en/latest/core/engines.html
conn_str = 'mssql+pymssql://localhost/pmi_sprint_1'
