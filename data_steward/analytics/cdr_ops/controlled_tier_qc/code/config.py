from pathlib import Path

current_path = Path(__file__)
code_path = current_path.parent
controlled_tier_qc_path = code_path.parent

CSV_FOLDER = controlled_tier_qc_path / 'csv'
SQL_FOLDER = controlled_tier_qc_path / 'sql'
CHECK_LIST_CSV_FILE = "Controlled_Tier_Check_Description.csv"
CONCEPT_CSV_FILE = "Controlled_Tier_Concept_Level.csv"
FIELD_CSV_FILE = "Controlled_Tier_Field_Level.csv"
TABLE_CSV_FILE = "Controlled_Tier_Table_Level.csv"
MAPPING_CSV_FILE = "Controlled_Tier_Mapping.csv"

COLUMNS_IN_CHECK_RESULT = [
    'table_name', 'column_name', 'concept_id', 'concept_code', 'rule',
    'n_row_violation'
]
