"""
Python library is intended to house the functions for use in the
general 'main' file in the library.

These functions are comparatively straighforward
and are better sequestered in a separate script rather than bogging down
the 'heavy lifting' of the code in create_dq_issue_site_dfs.py.
"""

import os
import pandas as pd
import sys
import datetime


def load_files(sheet_name, file_name):
    """
    Function loads the relevant sheets from all of the
    files in the directory.

    'Relevant sheet' is defined by previous user input.
    This function is also designed so it skips over instances where
    the user's input only exists in some of the defined sheets.

    Parameters
    ----------
    sheet (str): represents the sheet from the analysis reports
        whose metrics will be compared over time

    file_name (str): name of the user-specified Excel files in the
        in the current directory. this should be an analytics report
        to be scanned

    Returns
    -------
    sheet (df): dataframe contains info about data quality
        for all of the sites on a particular dimension of data
        quality for a particular date
    """
    cwd = os.getcwd()

    try:  # looking for the sheet
        sheet = pd.read_excel(file_name, sheet_name=sheet_name)

        if sheet.empty:
            print("WARNING: No data found in the {} sheet "
                  "in dataframe {}".format(
                   sheet_name, file_name))

    except Exception as ex:  # sheet not in specified excel file
        if type(ex).__name__ == "FileNotFoundError":
            print("{} not found in the current directory: {}. Please "
                  "ensure that the file names are consistent between "
                  "the Python script and the file name in your current "
                  "directory. ".format(file_name, cwd))
            sys.exit(0)

        else:
            print("WARNING: No {} sheet found in dataframe {}. "
                  "This is a(n) {}.".format(
                    sheet_name, file_name, type(ex).__name__))

            print("The error message is as follows: {err}".format(err=ex))
            print(ex)
            sys.exit(0)

    return sheet


def generate_hpo_id_col(current_file):
    """
    Function is used to get the names of all the HPOs in the
    current file.

    Parameters
    ----------
    current_file (str): user-specified Excel file that should
        reside in the current directory. Files are analytics
        reports to be scanned.

    Returns
    -------
    hpo_id_col (list): list of the strings that should go
        into an HPO ID column. these will later be used
        to instantiate HPO objects.
    """

    # use concept sheet; always has all of the HPO IDs
    df = load_files('concept', current_file)
    hpo_col_name = 'src_hpo_id'
    total_hpo_id_columns = []

    hpo_id_col = df[hpo_col_name].values
    total_hpo_id_columns.append(hpo_id_col)
    hpo_id_col = sorted(hpo_id_col)

    return hpo_id_col


def find_hpo_row(sheet, hpo):
    """
    Finds the row index of a particular HPO site within
    a larger sheet.

    Parameters
    ----------
    sheet (dataframe): dataframe with all of the data quality
        metrics for the sites.

    hpo (string): represents the HPO site whose row in
        the particular sheet needs to be determined

    :returns
    row_num (int): row number where the HPO site of question
        lies within the sheet. returns none if the row is not
        in the sheet in question but exists in other sheets
    """
    hpo_column_name = 'src_hpo_id'
    sheet_hpo_col = sheet[hpo_column_name]

    row_num = 99999

    for idx, site_id in enumerate(sheet_hpo_col):
        if hpo == site_id:
            row_num = idx

    if row_num == 99999:  # was never found; no data available
        return None

    return row_num


def get_err_rate(sheet, row_num, metric, hpo_name, column):
    """
    Function is used to get the 'error rate' - or the number
    that we traditionally report out to the sites. This rate
    will be used to ultimately determine if the data quality
    metric is up to part.

    Parameters
    ----------
    sheet (df): dataframe contains info about data quality
        for all of the sites on a particular dimension of data
        quality for a particular date

    row_num (int): row number where the HPO site of question
        lies within the sheet

    metric (string): the name of the sheet that contains the
        dimension of data quality to be investigated

    hpo_name (string): ID for the HPO to be investigated

    column (string): column to be used to find the relevant
        quantitative metric that captures the data quality
        issue (often includes the metric name and relevant
        table)

    Returns
    -------
    val (float): value that represents the quantitative value
        of the data quality metric being investigated
    """
    if row_num is not None:
        data_info = sheet.iloc[row_num, :]  # series, column labels and values
    else:
        print("{hpo_name} is now in the following sheet: {sheet}".format(
            hpo_name=hpo_name, sheet=metric
        ))
        sys.exit(0)

    # tend to reverse the reported metric for ACHILLES errors
    if metric in ['end_before_begin', 'data_after_death']:
        val = round(100 - data_info[column], 2)
    else:
        val = data_info[column]

    # to ensure that comparisons can be drawn across
    # the threshold
    if val in ["No Data"] or val is None:
        val = 0

    return val


def sort_and_convert_dates(file_names):
    """
    Function is used to sort the file names to order
    them by the date in their names. These string date
    names are then converted into datetime objects so
    they could eventually be assigned to DataQualityMetric
    objects.

    Parameters
    ----------
    file_names (list): list of the files for the main
        script

    Returns
    -------
    ordered_dates_dt (list) list of ordered dates
        that could be assigned to a DataQualityMetric
        object
    """
    ordered_dates_dt = []

    # NOTE: requires files to have full month name and 4-digit year
    for date_str in file_names:
        date_str = date_str[:-5]  # take off the .xlsx
        date = datetime.datetime.strptime(date_str, '%B_%d_%Y')
        ordered_dates_dt.append(date)

    ordered_dates_dt = sorted(ordered_dates_dt)

    return ordered_dates_dt
