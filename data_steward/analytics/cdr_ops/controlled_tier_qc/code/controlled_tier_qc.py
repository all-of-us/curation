# for data manipulation
import pandas as pd

# Path
from analytics.cdr_ops.controlled_tier_qc.code.config import (
    CSV_FOLDER, CHECK_LIST_CSV_FILE)

# SQL template
from jinja2 import Template

# functions for QC
from analytics.cdr_ops.controlled_tier_qc.code.check_table_suppression import check_table_suppression
from analytics.cdr_ops.controlled_tier_qc.code.check_field_suppression import (
    check_field_suppression, check_vehicle_accident_suppression,
    check_field_cancer_concept_suppression,
    check_field_freetext_response_suppression,
    check_field_geolocation_records_suppression)
from analytics.cdr_ops.controlled_tier_qc.code.check_concept_suppression import check_concept_suppression
from analytics.cdr_ops.controlled_tier_qc.code.check_mapping import (
    check_mapping, check_site_mapping, check_mapping_zipcode_generalization)

# funtions from utils
from analytics.cdr_ops.controlled_tier_qc.utils.helpers import (
    highlight, load_check_description, load_tables_for_check,
    filter_data_by_rule, pretty_print)

import logging
import sys

# config log
logging.basicConfig(format='%(asctime)s [%(levelname)s] - %(message)s',
                    level=logging.INFO,
                    stream=sys.stdout)
logger = logging.getLogger()


def run_qc(project_id,
           post_deid_dataset,
           questionnaire_response_dataset,
           pre_deid_dataset,
           mapping_dataset=None,
           rule_code=None) -> pd.DataFrame:
    """
    Run quality check under the specified condition.

    :param project_id: Project ID of the dataset.
    :param post_deid_dataset: ID of the dataset after DEID.
    :param pre_deid_dataset: ID of the dataset before DEID.
    :param questionnaire_response_dataset: the dataset where this dataset is created
    :param mapping_dataset: ID of the dataset for mapping.
    :param rule_code: str or list. The rule code(s) to be checked.
                      If None, all the rule codes in CHECK_LIST_CSV_FILE are checked.
    :returns: dataframe that has the results of the quality checks.
    """
    list_checks = load_check_description(rule_code)
    list_checks = list_checks[list_checks['level'].notnull()].copy()

    check_dict = load_tables_for_check()

    checks = []
    for _, row in list_checks.iterrows():
        rule = row['rule']
        logger.info(f"Running {rule} - {row['description']}")

        check_level = row['level']
        check_file = check_dict.get(check_level)
        check_df = filter_data_by_rule(check_file, rule)
        check_function = eval(row['code'])
        df = check_function(check_df, project_id, post_deid_dataset,
                            pre_deid_dataset, questionnaire_response_dataset,
                            mapping_dataset)
        checks.append(df)
    return pd.concat(checks, sort=True).reset_index(drop=True)


def display_check_summary_by_rule(checks_df, to_include):
    """
    Display the summary of all the quality checks.

    :param checks_df: dataframe that has the results of the quality checks.
    :param to_include: str or list. The rule code(s) to be checked.
                       If None, all the rule codes in CHECK_LIST_CSV_FILE are checked.
    :returns: Styler, just for display purposes.
    """
    by_rule = checks_df.groupby('rule')['n_row_violation'].sum().reset_index()
    needed_description_columns = ['rule', 'description']
    check_description = (load_check_description().filter(
        items=needed_description_columns))
    if not by_rule.empty:
        rules_not_run = set(check_description['rule']) - set(to_include)
        nothing_to_report = set(to_include) - set(by_rule['rule'])

        by_rule = by_rule.merge(check_description, how='outer', on='rule')

        by_rule['n_row_violation'] = by_rule['n_row_violation'].fillna(
            0).astype(int)

        by_rule.loc[by_rule['rule'].isin(rules_not_run), 'note'] = 'NOT RUN'
        by_rule.loc[by_rule['rule'].isin(nothing_to_report),
                    'note'] = 'NOTHING TO REPORT'
        by_rule.loc[by_rule['n_row_violation'] != 0,
                    'note'] = 'PROBLEM, INVESTIGATE'
    else:
        by_rule = check_description.copy()
        by_rule['n_row_violation'] = 0

        by_rule['n_row_violation'] = by_rule['n_row_violation'].fillna(
            0).astype(int)

        by_rule['note'] = 'NOT RUN'
        by_rule.loc[by_rule['rule'].isin(to_include),
                    'note'] = 'NOTHING TO REPORT'

        col_order = [col for col in check_description
                    ] + ['n_row_violation', 'note']
        by_rule = by_rule[col_order]
    return by_rule.style.apply(highlight, axis=1)


def display_check_detail_of_rule(checks_df, rule, to_include):
    """
    Display the details of the specified quality check.

    :param checks_df: dataframe that has the results of the quality checks.
    :param rule: The rule that you want to show the detailed result for.
    :param to_include: str or list. The rule code(s) to be checked.
                       If None, all the rule codes in CHECK_LIST_CSV_FILE are checked.
    :returns: pretty printed HTML or string, just for display purposes.
    """
    col_orders = [
        'table_name', 'column_name', 'concept_id', 'concept_code',
        'n_row_violation', 'query'
    ]
    if rule in checks_df['rule'].values:
        to_print_df = checks_df[checks_df['rule'] == rule].dropna(axis=1,
                                                                  how='all')
        columns = [col for col in col_orders if col in to_print_df]
        return pretty_print(to_print_df[columns])
    else:
        if to_include and rule not in to_include:
            return 'Not Run'
        else:
            return 'Nothing to Report'
