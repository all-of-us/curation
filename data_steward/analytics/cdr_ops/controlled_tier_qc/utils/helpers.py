from collections import defaultdict

import pandas as pd
from jinja2 import Template
from IPython.display import display, HTML

from analytics.cdr_ops.controlled_tier_qc.code.config import (
    CSV_FOLDER, COLUMNS_IN_CHECK_RESULT, TABLE_CSV_FILE, FIELD_CSV_FILE,
    CONCEPT_CSV_FILE, MAPPING_CSV_FILE, CHECK_LIST_CSV_FILE)
from common import PIPELINE_TABLES, ZIP_CODE_AGGREGATION_MAP


def load_check_description(rule_code=None) -> pd.DataFrame:
    """Extract the csv file containing the descriptions of checks
    :param rule_code: str or list. Contains all the rule codes to be checked.
                      If None, all the rule codes in CHECK_LIST_CSV_FILE are checked.
    :returns: dataframe that has the data from CHECK_LIST_CSV_FILE.
              If rule_code is valid, this data frame is filtered to have
              only the rows that are related to the rule.
    """
    check_df = pd.read_csv(CSV_FOLDER / CHECK_LIST_CSV_FILE, dtype='object')
    if rule_code:
        valid_rule_code = extract_valid_codes_to_run(check_df, rule_code)
        if valid_rule_code:
            make_header(f"Running the following checks {str(valid_rule_code)}")
            check_df = filter_data_by_rule(check_df, valid_rule_code)
        else:
            make_header("Code(s) invalid so running all checks...")
    return check_df


def make_header(message) -> bool:
    print("#####################################################")
    print(message)
    print("#####################################################\n")
    return True


def is_rule_valid(check_df, code) -> bool:
    """Check if the rule is in the CSV file.
    :param check_df: dataframe that has the data from CHECK_LIST_CSV_FILE.
    :param code: str. Rule code.
    :returns: True or False
    """
    return code in check_df['rule'].values


def extract_valid_codes_to_run(check_df, rule_code) -> list:
    """Out of the given rule_code, only return the ones that are defined in the CSV file.
    :param check_df: dataframe that has the data from CHECK_LIST_CSV_FILE.
    :param rule_code: str or list. The rule code(s) to be checked.
    :returns: list of the valid rule codes
    """
    if not isinstance(rule_code, list):
        rule_code = [rule_code]
    return [code for code in rule_code if is_rule_valid(check_df, code)]


def filter_data_by_rule(check_df, rule_code) -> pd.DataFrame:
    """Filter specific check rules by using the rule code
    :param check_df: dataframe that has the data from either
                     CONCEPT_CSV_FILE, FIELD_CSV_FILE, TABLE_CSV_FILE, or MAPPING_CSV_FILE
    :param rule_code: str or list. The rule code(s) to be checked.
    :returns: Filtered dataframe. It has only the rows that are related to the rule.
    """
    if not isinstance(rule_code, list):
        rule_code = [rule_code]
    return check_df[check_df['rule'].isin(rule_code)]


def load_tables_for_check():
    """Load all the csv files for check
    :returns: dict. Its keys and values are the following:
              Key - Table/Field/Concept/Mapping
              Value - dataframe from the corresponding CSV file
    """
    check_dict = defaultdict()
    list_of_files = [
        TABLE_CSV_FILE, FIELD_CSV_FILE, CONCEPT_CSV_FILE, MAPPING_CSV_FILE
    ]
    list_of_levels = ['Table', 'Field', 'Concept', 'Mapping']

    for level, filename in zip(list_of_levels, list_of_files):
        check_dict[level] = pd.read_csv(CSV_FOLDER / filename, dtype='object')
    return check_dict


def form_field_param_from_row(row, field):
    return row[field] if field in row and row[field] != None else ''


def get_list_of_common_columns_for_merge(check_df, results_df):
    """Extract common columns from the two dataframes
    :param check_df: dataframe that has the data from either
                     CONCEPT_CSV_FILE, FIELD_CSV_FILE, TABLE_CSV_FILE, or MAPPING_CSV_FILE
    :param results_df: dataframe that has the result of SQL run
    :returns: list of columns that exist both in check_df and result_df
    """
    return [col for col in check_df if col in results_df]


def format_cols_to_string(df):
    """Format all columns to string except the following:
    1) the column 'n_row_violation'
    2) the columns with data type 'float' are casted to 'int'

    :param df: dataframe that needs this reformatting
    :returns: dataframe with reformatted columns.

    This reformatting is necessary for the result of run_check_by_row()
    to look pretty.
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


def run_check_by_row(df,
                     template_query,
                     project_id,
                     post_deid_dataset,
                     questionnaire_response_dataset,
                     pre_deid_dataset=None,
                     mapping_issue_description=None,
                     mapping_dataset=None):
    """Run all the checks from the QC rules dataframe one by one
    :param check_df: dataframe that has the data from either
                     CONCEPT_CSV_FILE, FIELD_CSV_FILE, TABLE_CSV_FILE, or MAPPING_CSV_FILE
    :param template_query: query template for the QC.
    :param project_id: Project ID of the dataset.
    :param post_deid_dataset: ID of the dataset after DEID.
    :param questionnaire_response_dataset: ID of the dataset containing questionnaire_response deid mapping table
    :param pre_deid_dataset: ID of the dataset before DEID.
    :param mapping_issue_description: Description of the issue.
    :param mapping_dataset: ID of the dataset for mapping.
    :returns: dataframe that has the results of this QC.
    """
    if df.empty:
        # Return check result dataframe empty with specified columns
        return pd.DataFrame(
            columns=[col for col in df if col in COLUMNS_IN_CHECK_RESULT])

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
        query = Template(template_query).render(
            project_id=project_id,
            post_deid_dataset=post_deid_dataset,
            questionnaire_response_dataset=questionnaire_response_dataset,
            pre_deid_dataset=pre_deid_dataset,
            table_name=row['table_name'],
            column_name=column_name,
            concept_id=concept_id,
            concept_code=concept_code,
            data_type=data_type,
            primary_key=primary_key,
            new_id=new_id,
            mapping_dataset=mapping_dataset,
            mapping_table=mapping_table,
            pipeline_dataset=PIPELINE_TABLES,
            zip_table_name=ZIP_CODE_AGGREGATION_MAP)
        result_df = pd.read_gbq(query, dialect="standard")
        result_df['query'] = str(query)
        results.append(result_df)

    results_df = (pd.concat(results, sort=True).pipe(format_cols_to_string))

    for col in results_df:
        if col == 'concept_id' or col == 'concept_code':
            results_df[col] = results_df[col].str.replace('nan', '')
            check_df[col] = check_df[col].fillna('')

    merge_cols = get_list_of_common_columns_for_merge(check_df, results_df)
    result_columns = merge_cols + ['rule', 'n_row_violation', 'query']
    final_result = (results_df.merge(
        check_df, on=merge_cols,
        how='inner').filter(items=result_columns).query('n_row_violation > 0'))

    if not final_result.empty and mapping_issue_description:
        final_result['mapping_issue'] = mapping_issue_description

    return final_result if not final_result.empty else pd.DataFrame(
        columns=result_columns)


def highlight(row):
    """Highlight if violations (row counts > 0) are found

    Parameters
    ----------
    row: pd.DataFrame
    """

    s = row['n_row_violation']
    if s != 0:
        css = 'background-color: yellow'
    else:
        css = ''
    return [css] * len(row)


def pretty_print(df):
    return display(HTML(df.to_html().replace("\\n", "<br>")))
