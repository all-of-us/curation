from pathlib import Path

CSV_FOLDER = Path('csv')
SQL_FOLDER = Path('sql')

CHECK_LIST_CSV_FILE = "Controlled_Tier_Check_Description.csv"
CONCEPT_CSV_FILE = "Controlled_Tier_Concept_Level.csv"
FIELD_CSV_FILE = "Controlled_Tier_Field_Level.csv"
TABLE_CSV_FILE = "Controlled_Tier_Table_Level.csv"
MAPPING_CSV_FILE = "Controlled_Tier_Mapping.csv"

COLUMNS_IN_CHECK_RESULT = ['table_name', 'column_name', 'concept_id', 'concept_code', 'rule', 'n_row_violation']
