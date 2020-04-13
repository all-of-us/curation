"""
Python file is intended to be used for the 'startup' of the
metrics_over_time script. This includes prompting the user
to specify his/her analysis target and loading applicable files.

Some other smaller functions are also included in this file
to improve the overall readability of the main script.

ASSUMPTIONS
-----------
1. The 'concept' sheet in a dataframe will always have all of
    the HPOs. There should not be an HPO that exists in another
    sheet of the same report that does not exist in the
    'concept' sheet.
"""
import os
import pandas as pd
import sys
import datetime

from dictionaries_and_lists import \
    choice_dict, percentage_dict, \
    target_low_dict, metric_type_to_english_dict


from messages import \
    analysis_type_prompt, output_prompt, fnf_error


def get_user_analysis_choice():
    """
    Function gets the user input to determine what kind of data
    quality metrics s/he wants to investigate.

    Returns
    -------
    analytics_type (str): the data quality metric the user wants to
        investigate

    percent_bool (bool): determines whether the data will be seen
        as 'percentage complete' or individual instances of a
        particular error

    target_low (bool): determines whether the number displayed should
        be considered a desirable or undesirable characteristic
    """

    user_command = input(analysis_type_prompt).lower()

    while user_command not in choice_dict.keys():
        print("\nInvalid choice. Please specify a letter that corresponds "
              "to an appropriate analysis report.\n")
        user_command = input(analysis_type_prompt).lower()

    analytics_type = choice_dict[user_command]
    percent_bool = percentage_dict[analytics_type]
    target_low = target_low_dict[analytics_type]

    return analytics_type, percent_bool, target_low


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


def startup(file_names):
    """
    Function is used to 'startup' the script. This should in essence
    get the user's choice, load the appropriate files, and allow us
    to determine which HPO objects we will instantiate.

    Parameters
    -----------
    file_names (list): list of the user-specified Excel files that are
        in the current directory. Files are analytics reports to be
        scanned.


    Returns
    -------
    metric_choice (string): represents the sheet from the analysis reports
        whose metrics will be compared over time

    metric_is_percent (bool): determines whether the data will be seen
        as 'percentage complete' or individual instances of a
        particular error

    ideal_low (bool): determines whether the number displayed should
        be considered a desirable or undesirable characteristic

    file_names (list): list of the user-specified Excel files that are
        in the current directory. Files are analytics reports to be
        scanned.

    sheets (list): list of pandas dataframes. each dataframe contains
        info about data quality for all of the sites for a date. each
        index of the list should represent a particular date's metrics.

    hpo_name_col (list): list of the strings that should go
        into an HPO ID column. for use in generating subsequent
        dataframes.
    """
    metric_choice, metric_is_percent, ideal_low = get_user_analysis_choice()
    sheets = load_files(metric_choice, file_names)
    hpo_name_col = generate_hpo_id_col(file_names)

    return metric_choice, metric_is_percent, ideal_low, \
        sheets, hpo_name_col


def understand_sheet_output_type(hpo_objects, hpo_names, analytics_type):
    """
    Function used to determine what kind out output formatting
    the user would want for the generated Excel files.

    Parameters
    ----------
    hpo_objects (list): list of HPO objects that have associated
        DataQualityMetric objects

    hpo_names (list): list of the HPO names that are to be
        put into dataframes (either as the titles of the
        dataframe or the rows of a dataframe)

    analytics_type (string): the type of analytics report
        (e.g. duplicates) that the user wants to scan and compile

    Return
    ------
    user_choice (string): determines which variable (either
        distinct site or distinct table) will serve as a
        'anchor' to separate out the sheets
    """
    tables_or_classes = []

    # convert to 'human readable' form
    analytics_type = metric_type_to_english_dict[analytics_type]

    for hpo in hpo_objects:
        relevant_dqm_objects = hpo.use_string_to_get_relevant_objects(
            metric=analytics_type)

        for dqm in relevant_dqm_objects:
            if dqm.table_or_class not in tables_or_classes:
                tables_or_classes.append(dqm.table_or_class)

    num_names, num_tables = (len(hpo_names) + 1), (len(tables_or_classes))

    output_txt = output_prompt.format(num_tables, num_names)

    user_input = input(output_txt).lower()
    output_choice_dict = {
        'a': 'table_sheets', 'b': 'hpo_sheets'}

    while user_input not in output_choice_dict.keys():
        print("\nInvalid choice. Please specify a letter that corresponds "
              "to an appropriate output type.\n")
        user_input = input(output_prompt).lower()

    user_choice = output_choice_dict[user_input]

    return user_choice
