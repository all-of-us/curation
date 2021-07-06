import pandas as pd
from jinja2 import Template
from code.config import (CSV_FOLDER, COLUMNS_IN_CHECK_RESULT, TABLE_CSV_FILE, 
                        FIELD_CSV_FILE, CONCEPT_CSV_FILE, MAPPING_CSV_FILE, CHECK_LIST_CSV_FILE)

from collections import defaultdict
from IPython.display import display, HTML

def load_check_description(rule_code=None):
    """Extract the csv file containing the descriptions of checks

    Parameters
    ----------
    rule_code: str or list
        contains all the codes to be checked
    
    Returns
    -------
    pd.DataFrame

    """
    check_df = pd.read_csv(CSV_FOLDER/CHECK_LIST_CSV_FILE, dtype='object')
    if rule_code:
        valid_rule_code = extract_valid_codes_to_run(check_df, rule_code)
        if valid_rule_code:
            make_header(f"Running the following checks {str(valid_rule_code)}")  
            check_df = filter_data_by_rule(check_df, valid_rule_code)
        else:
            make_header("Code(s) invalid so running all checks...")
    return check_df

def make_header(message):
    print("#####################################################")
    print(message)
    print("#####################################################\n")
    return True

def is_rule_valid(check_df, code):
    return code in check_df['rule'].values

def extract_valid_codes_to_run(check_df, rule_code):
    # valid_rule_code = []
    if not isinstance(rule_code, list):
        rule_code = [rule_code]
    return [code for code in rule_code if is_rule_valid(check_df, code)]


def filter_data_by_rule(check_df, rule_code):
    """Filter specific check rules by using code
    
    Parameters
    ----------
    check_df: pd.DataFrame
        contains all the checks
    rule_code: str or list
        contains the codes to be checked
    
    Returns
    -------
    pd.DataFrame

    """
    if not isinstance(rule_code, list):
        rule_code = [rule_code]
    return check_df[check_df['rule'].isin(rule_code)]


def load_tables_for_check():
    """Load all the csv files for check
    
    Returns
    -------
    dict
    """
    check_dict = defaultdict()
    list_of_files = [TABLE_CSV_FILE, FIELD_CSV_FILE, CONCEPT_CSV_FILE, MAPPING_CSV_FILE]
    list_of_levels = ['Table', 'Field', 'Concept', 'Mapping']

    for level, filename in zip(list_of_levels, list_of_files):
        check_dict[level]= pd.read_csv(CSV_FOLDER/filename, dtype='object')
    return check_dict


def form_field_param_from_row(row, field):
    return row[field] if field in row and row[field] != None else ''


def get_list_of_common_columns_for_merge(check_df, results_df):
    """Extract common columns from two dataframes
    
    Parameters
    ----------
    check_df  :   pd.DataFrame
    results_df:   pd.DataFrame

    Returns
    -------
    list
    """
    return [col for col in check_df if col in results_df]


def format_cols_to_string(df):
    """Format all columns (except for some) to string
    
    Parameters
    ----------
    df: pd.DataFrame

    Returns
    -------
    pd.DataFrame
    """
    df = df.copy()
    for col in df:
        if col == 'n_row_violation':
            df[col] = df[col].astype(int)
            continue
        if df[col].dtype == 'float':
            df[col] = df[col].astype(pd.Int64Dtype())
        df[col] = df[col].astype(str)
    return df


def run_check_by_row(df, template_query, project_id, post_deid_dataset, pre_deid_dataset=None, mapping_issue_description=None, mapping_dataset=None):
    """Run all checks in a dataframe row by row

    Parameters
    ----------
    df: pd.DataFrame
        contains all the checks to be run
    template_query: str
        query template that changes according to the check
    project_id: str
        Google Bigquery project
    post_deid_dataset: str
        Bigquery dataset name after de-id was run
    pre_deid_dataset: str
        Bigqery dataset name before de-id was run
    mapping_issue_description: str
        Describes what the issue is

    Returns
    -------
    pd.DataFrame
    """
    if df.empty:
        return pd.DataFrame(columns=[col for col in df if col in COLUMNS_IN_CHECK_RESULT])

    check_df = df.copy()
    results = []
    for _, row in check_df.iterrows():
        column_name = form_field_param_from_row(row, 'column_name')
        concept_id = form_field_param_from_row(row, 'concept_id')
        concept_code = form_field_param_from_row(row, 'concept_code')
        data_type = form_field_param_from_row(row, 'data_type')
        primary_key = form_field_param_from_row(row, 'primary_key')
        mapping_table = form_field_param_from_row(row, 'mapping_table')
        new_id = form_field_param_from_row(row, 'new_id')
        query = Template(template_query).render(project_id=project_id, 
                post_deid_dataset=post_deid_dataset, pre_deid_dataset=pre_deid_dataset,
                table_name=row['table_name'],column_name=column_name,
                concept_id=concept_id, concept_code=concept_code, data_type=data_type,
                primary_key=primary_key, new_id=new_id, mapping_dataset=mapping_dataset, mapping_table=mapping_table)
        result_df = pd.read_gbq(query, dialect="standard")
        result_df['query'] = str(query)
        results.append(result_df)

    results_df = (pd.concat(results, sort=True)
                    .pipe(format_cols_to_string))

    for col in results_df:
        if col == 'concept_id' or col == 'concept_code':
            results_df[col] = results_df[col].str.replace('nan', '')
            check_df[col] = check_df[col].fillna('')

    merge_cols = get_list_of_common_columns_for_merge(check_df, results_df)
    result_columns = merge_cols + ['rule', 'n_row_violation', 'query']
    final_result =  (results_df.merge(check_df, on=merge_cols, how='inner')
                            .filter(items=result_columns)
                            .query('n_row_violation > 0')
            )

    if not final_result.empty and mapping_issue_description:
        final_result['mapping_issue'] = mapping_issue_description
    return final_result if not final_result.empty else pd.DataFrame(columns=result_columns)


def highlight(row):
    """Highlight if violations (row counts > 0) are found
    
    Parameters
    ----------
    row: pd.DataFrame
    """

    s = row['n_row_violation']
    if s > 0:
        css = 'background-color: yellow'
    else:
        css = ''
    return [css] * len(row)


def pretty_print(df):
    return display(HTML(df.to_html().replace("\\n","<br>")))
