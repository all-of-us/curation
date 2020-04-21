"""
Function is used to 'start up' the e-mail generator script.
"""

import pandas as pd
import os
import sys
from messages import fnf_error
from dictionaries_and_lists import \
    target_low_dict, percentage_dict
import datetime


def startup(file_names, metric_choice):
    """
    Function is used to 'startup' the script. This should in essence
    load the appropriate files and allow us
    to determine which HPO objects we will instantiate.

    Parameters
    -----------
    file_names (list): list of the user-specified Excel files that are
        in the current directory. Files are analytics reports to be
        scanned.

    metric_choice (str): the name of the sheet that is going to be
        used to ultimately generate HPO objects

    Returns
    -------
    sheets (list): list of pandas dataframes. each dataframe contains
        info about data quality for all of the sites for a date. each
        index of the list should represent a particular date's metrics.

    hpo_name_col (list): list of the strings that should go
        into an HPO ID column. for use in generating subsequent
        dataframes.

    target_low (bool): determines whether the number displayed should
        be considered a desirable or undesirable characteristic

    percent_bool (bool): determines whether the data will be seen
        as 'percentage complete' or individual instances of a
        particular error
    """
    sheets = load_files(metric_choice, file_names)
    hpo_name_col = generate_hpo_id_col(file_names)

    percent_bool = percentage_dict[metric_choice]
    target_low = target_low_dict[metric_choice]

    return sheets, hpo_name_col, target_low, percent_bool


def load_files(user_choice, file_names):
    """
    Function loads the relevant sheets from all of the
    files in the directory (see 'file_names' list from above).
    'Relevant sheet' is defined by previous user input.
    This function is also designed so it skips over instances where
    the user's input only exists in some of the defined sheets.

    Parameters
    ----------
    user_choice (string): represents the sheet from the analysis reports
        whose metrics will be compared over time

    file_names (list): list of the user-specified Excel files that are
        in the current directory. Files are analytics reports to be
        scanned.

    Returns
    -------
    sheets (list): list of pandas dataframes. each dataframe contains
        info about data quality for all of the sites for a date.

    NOTE: I recognize that the 'skip sheet' protocol is implemented
    twice but - since it was only implemented twice - I do not believe
    it warrants its own separate function.
    """
    num_files_indexed = 0
    cwd = os.getcwd()
    sheets = []

    while num_files_indexed < len(file_names):
        file_name = file_names[num_files_indexed]

        try:  # looking for the sheet
            sheet = pd.read_excel(file_name, sheet_name=user_choice)

            if sheet.empty:

                print("WARNING: No data found in the {user_choice} sheet "
                      "in dataframe {file_name}".format(
                       user_choice=user_choice,
                       file_name=file_name))

                del file_names[num_files_indexed]
                num_files_indexed -= 1  # skip over the date

            else:
                sheets.append(sheet)

        except Exception as ex:  # sheet not in specified excel file

            if type(ex).__name__ == "FileNotFoundError":
                print(fnf_error.format(
                        file=file_names[num_files_indexed], cwd=cwd))
                sys.exit(0)

            else:
                print("WARNING: No {} sheet found in dataframe {}. "
                      "This is a(n) {}.".format(
                        user_choice, file_name, type(ex).__name__))

                print(ex)

                del file_names[num_files_indexed]
                num_files_indexed -= 1  # skip over the date

        num_files_indexed += 1

    return sheets


def generate_hpo_id_col(file_names):
    """
    Function is used to distinguish between HPOs that
    are in all of the data analysis outputs versus HPOs
    that are only in some of the data analysis outputs.
    This function ensures all of the HPOs are logged
    for all of the dates and improves consistency.

    Parameters
    ----------
    file_names (list): list of the user-specified Excel files that are
        in the current directory. Files are analytics reports to be
        scanned.

    Returns
    --------
    hpo_id_col (list): list of the strings that should go
        into an HPO ID column. for use in generating subsequent
        dataframes.

    NOTES
    -----
    This function's efficiency could be improved by simply
    iterating through all of the hpo_col_names and adding HPOs
    to a growing list so long as the HPO is not already in the
    list.
    I chose this approach, however, because the distinction
    between 'selective' and 'intersection' used to have a purpose
    in the script. This distinction can also be helpful in
    future iterations should someone want to leverage it in
    a certain capacity.
    """

    # use concept sheet; always has all of the HPO IDs
    dataframes = load_files(
        user_choice='concept', file_names=file_names)
    hpo_col_name = 'src_hpo_id'
    selective_rows, total_hpo_id_columns = [], []

    for df in dataframes:
        hpo_id_col_sheet = df[hpo_col_name].values
        total_hpo_id_columns.append(hpo_id_col_sheet)

    # find the intersection of all the lists
    in_all_sheets = set(dataframes[0][hpo_col_name].values)
    for df in dataframes[1:]:
        hpo_id_col_sheet = set(df[hpo_col_name].values)
        in_all_sheets.intersection_update(hpo_id_col_sheet)

    # eliminate blank rows
    in_all_sheets = list(in_all_sheets)
    in_all_sheets = [row for row in in_all_sheets if isinstance(row, str)]

    # determining rows in some (but not all) of the dataframes
    for df_num, df in enumerate(dataframes):
        hpo_id_col_sheet = df[hpo_col_name].values
        selective = set(in_all_sheets) ^ set(hpo_id_col_sheet)
        for row in selective:
            if (row not in selective_rows) and isinstance(row, str):
                selective_rows.append(row)

    # standardize; all sheets now have the same rows
    hpo_id_col = in_all_sheets + selective_rows
    hpo_id_col = sorted(hpo_id_col)

    return hpo_id_col


def convert_file_names_to_datetimes(file_names):
    """
    Function is used to convert a list of file names
    to a list of datetime objects.
    This is useful for establishing the 'date' attribute of
    DataQualityMetric objects.

    Parameter
    ---------
    file_names (list): list of strings that indicate the names
        of the files being ingested. each file name should
        follow the convention month_date_year.xlsx
        (year should be 4-digit)

    Return
    ------
    ordered_dates_str (list): list of the strings that
        indicate the names of the files being ingested. these
        are now in sequential order.

    ordered_dates_dt (list): list of datetime objects that
        represent the dates of the files that are being
        ingested
    """

    ordered_dates_dt = []

    # NOTE: requires files to have full month name and 4-digit year
    for date_str in file_names:
        date_str = date_str[:-5]  # get rid of extension
        date = datetime.datetime.strptime(date_str, '%B_%d_%Y')
        ordered_dates_dt.append(date)

    ordered_dates_dt = sorted(ordered_dates_dt)

    # converting back to standard form to index into file
    ordered_dates_str = [
        x.strftime('%B_%d_%Y').lower() + '.xlsx'
        for x in ordered_dates_dt]

    return ordered_dates_str, ordered_dates_dt
