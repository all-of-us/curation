"""
Goals
------
Program should generate a report (Excel file) that shows
how data quality metrics for each HPO site change over time.

Data quality metrics include:
   1. the number of duplicates per table
   2. number of 'start dates' that precede 'end dates'
   3. number of records that are >30 days after a patient's death date
   4. source table success rates
   5. concept table success rates
   6. population of the 'unit' field in the measurement table
       - only for *selected* measurements
   7. population of the 'route' field in the drug exposure table
   8. proportion of expected ingredients observed
   9. proportion of expected measurements observed

ASSUMPTIONS
-----------
1. The user has all of the files s/he wants to analyze in the current
directory

2. The user will know to change the 'report' variables to match the
file names of the .xlsx files in the current working directory.

3. All sheets are saved as month_date_year.xlsx
   - month should be fully spelled (august rather than aug)
   - year should be four digits
   - this name is used to determine the date

4. The sheet names for all of the generated reports are consistent

5. The 'aggregate_info' statistics generated in some reports are
always labeled as 'aggregate_info.' This ensures these rows can
be excluded when generating initial dataframes. These aggregate
statistics can then be generated more effectively down the line
with an appropriate 'weighting'.

6. Assumed certain naming conventions in the sheets
   a. consistency in the column names in the 'source' tab
   b. total/valid rows are logged in the 'source' tab as
      (first word of the table type)_total_row or
      (first word of the table type)_well_defined_row
      ex: drug_total_row

"""

import datetime
import math
import os
import sys
import pandas as pd
import numpy as np


def get_user_analysis_choice():
    """
    Function gets the user input to determine what kind of data
    quality metrics s/he wants to investigate.

    :return:
    analytics_type (str): the data quality metric the user wants to
        investigate

    percent_bool (bool): determines whether the data will be seen
        as 'percentage complete' or individual instances of a
        particular error

    target_low (bool): determines whether the number displayed should
        be considered a desirable or undesirable characteristic
    """

    analysis_type_prompt = \
        "\nWhat kind of analysis over time report would you like " \
        "to generate for each site?\n\n" \
        "A. Duplicates\n" \
        "B. Amount of data following death dates\n" \
        "C. Amount of data with end dates preceding start dates\n" \
        "D. Success rate for concept_id field\n" \
        "F. Population of the 'unit' field in the measurement table (" \
        "only for specified measurements)\n" \
        "G. Population of the 'route' field in the drug exposure table\n" \
        "H. Percentage of expected drug ingredients observed\n" \
        "I. Percentage of expected measurements observed\n\n" \
        "Please specify your choice by typing the corresponding letter."

    user_command = input(analysis_type_prompt).lower()

    choice_dict = {
        'a': 'duplicates',
        'b': 'data_after_death',
        'c': 'end_before_begin',
        'd': 'concept',
        'f': 'measurement_units',
        'g': 'drug_routes',
        'h': 'drug_success',
        'i': 'sites_measurement'}

    while user_command not in choice_dict.keys():
        print("\nInvalid choice. Please specify a letter that corresponds "
              "to an appropriate analysis report.\n")
        user_command = input(analysis_type_prompt).lower()

    # NOTE: This dictionary needs to be expanded in the future
    percentage_dict = {
        'duplicates': False,
        'data_after_death': True,
        'end_before_begin': True,
        'concept': True,
        'measurement_units': True,
        'drug_routes': True,
        'drug_success': True,
        'sites_measurement': True
    }

    # dictionary indicates if the target is to minimize or maximize number
    target_low = {
        'duplicates': True,
        'data_after_death': True,
        'end_before_begin': True,
        'concept': False,
        'measurement_units': False,
        'drug_routes': False,
        'drug_success': False,
        'sites_measurement': False
    }

    analytics_type = choice_dict[user_command]
    percent_bool = percentage_dict[analytics_type]
    target_low = target_low[analytics_type]

    return analytics_type, percent_bool, target_low


def load_files(user_choice, file_names):
    """
    Function loads the relevant sheets from all of the
    files in the directory (see 'file_names' list from above).

    'Relevant sheet' is defined by previous user input.

    This function is also designed so it skips over instances where
    the user's input only exists in some of the defined sheets.

    :parameter
    user_choice (string): represents the sheet from the analysis reports
        whose metrics will be compared over time

    file_names (list): list of the user-specified Excel files that are
        in the current directory. Files are analytics reports to be
        scanned.

    :returns
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
                print("WARNING: No data found in the {} sheet "
                      "in dataframe {}".format(
                       user_choice, file_name))
                del file_names[num_files_indexed]
                num_files_indexed -= 1  # skip over the date
            else:
                sheets.append(sheet)
        except Exception as ex:  # sheet not in specified excel file
            if type(ex).__name__ == "FileNotFoundError":
                print("{} not found in the current directory: {}. Please "
                      "ensure that the file names are consistent between "
                      "the Python script and the file name in your current "
                      "directory. ".format(file_names[num_files_indexed], cwd))
                sys.exit(0)

            else:
                print("WARNING: No {} sheet found in dataframe {}. "
                      "This is a(n) {}.".format(
                        user_choice, file_name, type(ex).__name__))

                del file_names[num_files_indexed]
                num_files_indexed -= 1  # skip over the date

        num_files_indexed += 1

    return sheets


def get_comprehensive_tables(dataframes, analytics_type):
    """
    Function is used to ensure that all of the HPO sites will
        have all of the same table types. This is important
        if a table type is introduced in future iterations of
        the analysis script.

    :param
    dataframes (lst): list of pandas dataframes that are
        representations of the Excel analytics files

    analytics_type (str): the data quality metric the user wants to
        investigate

    :return:
    final_tables (lst): list of the tables that should be represented
        for each HPO at each date. these are extracted from the
        column labels of the Excel analytics files.
    """

    undocumented_cols = ['Unnamed: 0', 'src_hpo_id', 'HPO',
                         'total', 'device_exposure',
                         'number_valid_units', 'number_total_units',
                         'number_sel_meas', 'number_valid_units_sel_meas']

    rate_focused_inputs = ['source_concept_success_rate', 'concept']
    final_tables = []

    for sheet in dataframes:  # for each date
        data_info = sheet.iloc[1, :]  # just the columns
        column_names = data_info.keys()

        # NOTE: older Excel outputs had naming inconsistencies
        # this was a quick fix

        # get all of the columns; ensure the columns are only logged once
        if analytics_type in rate_focused_inputs:
            for col_label, _ in data_info.iteritems():
                if col_label[-5:] != '_rate':
                    undocumented_cols.append(col_label)

        final_tables = [x for x in column_names if x not in
                        undocumented_cols]

    # eliminate duplicates
    final_tables = list(dict.fromkeys(final_tables))
    return final_tables


def get_info(sheet, row_num, percentage, sheet_name,
             mandatory_tables, target_low):
    """
    Function is used to create a dictionary that contains
    the number of flawed records for a particular site.

    :param
    sheet (dataframe): pandas dataframe to traverse. Represents a
        sheet with numbers indicating data quality.

    row_num (int): row (0-index) with all of the information for
        the specified site's data quality

    percentage (boolean): used to determine whether or not the
        number is a simple record count (e.g. duplicates)
        versus the percentage of records (e.g. the success rate
        for each of the tables)

    sheet_name (str): name for the sheet for use in the error
        message

    mandatory_tables (lst): contains the tables that should be
        documented for every table and at every date.

    target_low (bool): determines whether the number displayed
        should be considered a positive or negative metric

    :return:
    err_dictionary (dictionary): key:value pairs represent the
        column name:number that represents the quality of the data
        for a particular HPO

    NOTE: This function includes 0 values if the data is wholly
    complete.
    """
    if row_num is not None:
        data_info = sheet.iloc[row_num, :]  # series, column labels and values
    else:  # HPO in future sheets but not current sheet
        data_info = sheet.iloc[1, :]  # just to get the columns
        column_names = data_info.keys()
        null_list = [None] * len(column_names)
        data_info = pd.Series(null_list, column_names)

    err_dictionary = {}

    for col_label, number in data_info.iteritems():
        if col_label in mandatory_tables:

            # data for table for site does not exist
            if number is None or number == 'No Data':
                err_dictionary[col_label] = float('NaN')

            else:
                try:
                    number = float(number)
                except ValueError:
                    pass
                else:
                    # to detect potential problems in excel file generator
                    if number < 0:
                        raise ValueError(
                            "Negative number detected in sheet {} for column "
                            "{}".format(sheet_name, col_label))
                    # elif percentage and number > 100:  # just in case
                    #     raise ValueError(
                    #         "Percentage value > 100 detected in sheet {} for "
                    #         "column {}".format(sheet_name, col_label))

                    # actual info to be logged if sensible data
                    elif percentage and target_low:  # proportion w/ errors
                        err_dictionary[col_label] = round(100 - number, 2)
                    elif percentage and not target_low:  # effective
                        err_dictionary[col_label] = round(number, 2)
                    elif not percentage and number > -1:
                        err_dictionary[col_label] = int(number)
        else:
            pass  # do nothing; do not want to document the column

    # adding all the tables for the HPO; maintaining consistency across all
    # HPOs for consistency and versatility
    for table in mandatory_tables:
        if table not in err_dictionary.keys():
            err_dictionary[table] = float('NaN')

    return err_dictionary


def find_hpo_row(sheet, hpo):
    """
    Finds the row index of a particular HPO site within
    a larger sheet.

    :param
    sheet (dataframe): dataframe with all of the data quality
        metrics for the sites.

    hpo (string): represents the HPO site whose row in
        the particular sheet needs to be determined

    :return:
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


def iterate_sheets(dataframes, hpo_id_list, percent,
                   analytics_type, target_low, file_names):
    """
    Function iterates through all of the sheets and ultimately
    generates a series of dictionaries that contain all of the
    data quality information for all of the
        a. dates (for the specified sheet)
        b. sites
        c. table types

    :param
    dataframes (list): list of the Pandas dataframes that
        contain data quality info for each of the sites

    hpo_id_list (list): HPO site IDs to iterate through on
        each sheet. organized alphabetically

    percent (boolean): used to determine whether or not the
        number is a simple record count (e.g. duplicates)
        versus the percentage of records (e.g. the success rate
        for each of the tables)

    analytics_type (string): the user's choice for the
        data metric he/she wants to measure

    target_low (bool): determines whether the number displayed
        should be considered a positive or negative metric

    file_names (list): list of the user-specified Excel files that are
        in the current directory. Files are analytics reports to be
        scanned.

    :return:
    dates_and_info: dictionary with three sub-dictionaries.
        the key value pairs are as follows (highest-to-lowest
        level)
        a. dates_and_info
            key is a date
            value is the following dictionary
        b. hpo_errors_for_date
            key is an HPO site
            value is the following dictionary
        c. err_dict_for_hpo
            key is the table type
            value is a number representing the data quality
                for that table type

    mandatory_tables (lst): contains the tables that should be
        documented for every table and at every date.
    """
    dates_and_info = {}  # key:value will be date:dict

    # ensure all of the relevant tables from all of the sheets
    # are logged
    mandatory_tables = get_comprehensive_tables(
        dataframes, analytics_type)

    for number, sheet in enumerate(dataframes):  # for each date
        num_chars_to_chop = 5  # get rid of .xlsx
        sheet_name = file_names[number]

        sheet_name = sheet_name[:-num_chars_to_chop]
        errors_for_date = {}  # key:value will be hpo:dict

        for hpo in hpo_id_list:  # for each HPO
            hpo_row_idx = find_hpo_row(
                sheet, hpo)

            if hpo == 'aggregate counts':  # will calculate later
                pass
            else:
                err_dict_for_hpo = get_info(
                    sheet, hpo_row_idx, percent,
                    sheet_name, mandatory_tables, target_low)

                errors_for_date[hpo] = err_dict_for_hpo

        # error information for all of the dates
        dates_and_info[sheet_name] = errors_for_date

    return dates_and_info, mandatory_tables


def generate_hpo_id_col(file_names):
    """
    Function is used to distinguish between HPOs that
    are in all of the data analysis outputs versus HPOs
    that are only in some of the data analysis outputs.

    This function ensures all of the HPOs are logged
    for all of the dates and improves consistency.

    :param
    file_names (list): list of the user-specified Excel files that are
        in the current directory. Files are analytics reports to be
        scanned.

    :return:
    hpo_id_col (list): list of the strings that should go
        into an HPO ID column. for use in generating subsequent
        dataframes.

    NOTE: This function's efficiency could be improved by simply
     iterating through all of the hpo_col_names and adding HPOs
     to a growing list so long as the HPO is not already in the
     list. I chose this approach, however, because the distinction
     between 'selective' and 'intersection' used to have a purpose
     in the script.
    """

    # use concept sheet; always has all of the HPO IDs
    dataframes = load_files('concept', file_names)
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
    bad_rows = [' Avarage', 'aggregate counts']  # do not include

    hpo_id_col = [x for x in hpo_id_col if x not in bad_rows]
    hpo_id_col = sorted(hpo_id_col)

    return hpo_id_col


def sort_names_and_tables(site_and_date_info, mandatory_tables):
    """
    Function is used to sort the information (dates, tables,
    and HPO site names) in an intuitive manner to ensure
    consistency across sheets and dataframes.

    :param
    site_and_date_info (dict): dictionary with key:value
        of date:additional dictionaries that contain metrics
        for each HPO's data quality by type

    mandatory_tables (lst): contains the tables that should be
        documented for every table and at every date.

    :return:
    ordered_dates_str (list): list of all the dates (from
        most oldest to most recent) in string form

    sorted_names (list): names of all the HPO sites in
        alphabetical order (with the addition of 'aggregate
        info')

    mandatory_tables (lst): contains the tables that should be
        documented for every table and at every date. This
        information is now sorted.
    """
    ordered_dates_dt = []

    # NOTE: requires files to have full month name and 4-digit year
    for date_str in site_and_date_info.keys():
        date = datetime.datetime.strptime(date_str, '%B_%d_%Y')
        ordered_dates_dt.append(date)

    ordered_dates_dt = sorted(ordered_dates_dt)
    # converting back to standard form to index into file
    ordered_dates_str = [x.strftime('%B_%d_%Y').lower() for x
                         in ordered_dates_dt]

    # earlier code ensured all sheets from diff dates
    # have same rows - can just take first
    all_rows = site_and_date_info[ordered_dates_str[0]]
    sorted_names = sorted(all_rows.keys())
    sorted_names.append('aggregate_info')

    mandatory_tables.sort()

    return ordered_dates_str, sorted_names, mandatory_tables


def add_aggregate_info(site_and_date_info, percentage, sorted_names):
    """
    Function is used to add an 'aggregate metric' that
    summarizes all of the data quality issues for a
    particular site on a particular date.

    NOTE: This function DOES NOT take the weighted value
        of all of these metrics. This is merely to attach
        the aggregate statistic.

    NOTE: This is for the DICTIONARY with the date as the
    first set of keys.

    :param
    site_and_date_info (dict): dictionary with key:value
        of date:additional dictionaries that contain metrics
        for each HPO's data quality by type

    percentage (boolean): used to determine whether or not the
        number is a simple record count (e.g. duplicates)
        versus the percentage of records (e.g. the success rate
        for each of the tables)

    sorted_names (lst): list of the names that should have an
        aggregate statistic analyzed (e.g. avoiding 'avarage'
        statistics)

    :return:
    site_and_date_info (dict): same as input parameter but
        now each site and date has an added aggregate
        statistic.
    """
    for date in site_and_date_info.keys():
        date_report = site_and_date_info[date]
        date_metric, num_iterated = 0, 0

        for site in sorted_names:
            table_metrics = date_report[site]
            date_metric, num_iterated = 0, 0

            for table in table_metrics.keys():
                stat = table_metrics[table]
                if not math.isnan(stat):
                    date_metric += stat
                    num_iterated += 1

        # NOTE: 'AGGREGATE INFO' SHOULD NOT BE USED FOR
        # THE PERCENTAGE METRIC. THIS IS BECAUSE THE
        # FIRST 'AGGREGATE INFO' DOES NOT WEIGHT SITES
        # BY THEIR RELATIVE CONTRIBUTIONS (BY # OF ROWS).
        if percentage and num_iterated > 0:
            date_metric = date_metric / num_iterated
        elif percentage and num_iterated == 0:
            date_metric = float('NaN')

        date_report['aggregate_info'] = date_metric

    return site_and_date_info


def get_row_totals(dataframes, contribution_type):
    """
    Function is used to determine the number of a particular
    'row' type for each site at a particular date. For example,
    this function can show the total number of rows for site
    X on date Y.

    The two types of 'row types' available are:
        a. total number of rows
        b. number of 'valid / well defined' rows

    This function is particularly useful when 'weighting' sites
    for use in the aggregate statistic.

    :param
    dataframes (lst): list of pandas dataframes loaded from the Excel
        files generated from the analysis reports

    contribution_type (str): string representing the types of columns to
        look at for the dataframe. either can represent the 'total' row
        metrics or the 'error' metrics for a particular column.

    :return:
    valid_cols (lst): list of the columns that are consistent across all
        of the sheets and relevant to the HPO weighting report needed
    """
    valid_cols = []

    for df in dataframes:
        for column in df:

            # looking at the 'total' number of rows for sites
            if contribution_type == 'total' and len(column) > 9 and \
                    column[-9:] == 'total_row':
                valid_cols.append(column)

            # looking at the 'valid' number of rows for sites
            elif contribution_type == 'valid' and len(column) > 16 and \
                    column[-16:] == 'well_defined_row':
                valid_cols.append(column)

    valid_cols = list(dict.fromkeys(valid_cols))
    valid_cols.sort()

    return valid_cols


def load_total_row_sheet(file_names, sheet_name):
    """
    Function loads the sheets that contain information regarding the
    total number of rows for each site for each table type. This loads
    the corresponding sheets for all of the analytics reports in the
    current directory.

    :param
    file_names (list): list of the user-specified Excel files that are
        in the current directory. Files are analytics reports to be
        scanned.

    sheet_name (str): label for the sheet with the information
        containing the number of rows

    :return:
    dataframes (list): list of Pandas dataframes that contain the
        information regarding the total number of rows

    hpo_id_col (list): list of the strings that should go
        into an HPO ID column. for use in generating subsequent
        dataframes.
    """

    num_files = len(file_names)
    dataframes = []

    for file_num in range(num_files):
        sheet = pd.read_excel(file_names[file_num], sheet_name)
        dataframes.append(sheet)

    hpo_id_col = generate_hpo_id_col(file_names)

    return dataframes, hpo_id_col


def generate_hpo_contribution(file_names, contribution_type):
    """
    Function is used to determine the 'contribution' for
    each of the HPO sites. This is useful in determining
    the 'weight' each site should be given when generating
    aggregate statistics with respect to data quality.

    :param
    file_names (list): list of the user-specified Excel files that are
        in the current directory. Files are analytics reports to be
        scanned.

    contribution_type (str): string representing the types of columns to
        look at for the dataframe. either can represent the 'total' row
        metrics or the 'success' metrics for a particular column.

    :return:
    hpo_contributions_by_date (dictionary): dictionary with the following
    key:value pairings
        a. date:following dictionary
        b. table type: list with the relative contribution of each HPO
            with respect to the number of rows it contributes for the
            particular table type. the sequence of the list follows
            the alphabetical order of all the HPO sites.

    valid_cols (lst): list of the table types to be iterated over

    NOTE: rows_for_table may be better implemented as a dictionary.
     This may be better than the current 'list in alphabetical order'
     method in terms of code legibility. I chose the alphabetical
     listing approach, however, because a list is more
     computationally efficient for iterations. The list was
     designed to contain all (and only) the HPOs that are relevant
     to the particular Excel file being created.
    """
    hpo_contributions_by_date = {}
    row_sheet_name = 'concept'  # should always have 'total rows' info

    dataframes, hpo_id_col = load_total_row_sheet(
        file_names, row_sheet_name)

    valid_cols = get_row_totals(dataframes, contribution_type)

    for number, sheet in enumerate(dataframes):  # for each date
        num_chars_to_chop = 5  # get rid of .xlsx
        date = file_names[number][:-num_chars_to_chop]
        total_per_sheet = {}

        for table_type in valid_cols:  # for each table
            rows_for_table = []

            for hpo in hpo_id_col:  # follows alphabetical order
                hpo_row_idx = find_hpo_row(sheet, hpo)

                if hpo == 'aggregate counts':  # will be added later
                    pass
                elif table_type not in sheet:
                    pass
                elif hpo_row_idx is None:  # HPO not in the sheet
                    rows_for_table.append(float('NaN'))
                else:
                    rows_for_hpo = sheet[table_type][hpo_row_idx]

                    try:  # in case number of rows is logged as a str
                        rows_for_hpo = float(rows_for_hpo)
                    except ValueError:  # already a float
                        pass

                    if isinstance(rows_for_hpo, (int, float)) and \
                            not math.isnan(rows_for_hpo):
                        rows_for_table.append(rows_for_hpo)
                    else:
                        rows_for_table.append(float('NaN'))

            # error information for the table type
            total_per_sheet[table_type] = rows_for_table

        # error information for all of the dates
        hpo_contributions_by_date[date] = total_per_sheet

    return hpo_contributions_by_date, valid_cols


def determine_means_to_calculate_weighted_avg(
        analytics_type, table, new_col_info):
    """
    Function is used to get the 'standard' table name
    for the table to be investigated. The 'standard'
    name should match a column in the 'concept'
    sheet of the analytics report so it can be
    used to get the total number of rows.

    :param
    analytics_type (str): the data quality metric the
        user wants to investigate

    table (str): table whose weighted average for a particular
        date is being determined

    new_col_info (list): shows the proportion of 'well defined' HPO records
        by HPO site. organized alphabetically.


    :return:
    table_tot (str): name that will be used to get the column
        that contains the number of rows per site for the
        table in question
    """
    first_underscore = True
    underscore_idx = 0

    # need to specify the table to use as a reference
    if analytics_type in ['measurement_units']:
        table = 'measurement'
    elif analytics_type in ['drug_routes']:
        table = 'drug_exposure'

    # just a simple average; all sites contribute equally
    # since 'integration' only means having one instance
    elif analytics_type in ['sites_measurement', 'drug_success']:
        return np.nanmean(new_col_info)

    # ASSUMPTION: table naming convention (see #6 in the header)
    for idx, char in enumerate(table):
        if first_underscore and char == '_':
            underscore_idx = idx
            first_underscore = False

    if not first_underscore:  # underscore in the table name
        table = table[0:underscore_idx]

    table_tot = table + "_total_row"

    return table_tot


def generate_weighted_average_table_sheet(
        file_names, date, table, new_col_info,
        analytics_type):
    """
    Function is used to generate a weighted average to indicate the
    'completeness' of a particular table type across all sites.

    This function is employed when the
        a. HPO sites are the ROWS
        b. The table types are the SHEETS

    This is to make the final value of the column (a weighted average).

    :param
    file_names (list): list of the user-specified Excel files that are
        in the current directory. Files are analytics reports to be
        scanned.

    date (str): date for the column that is being investigated

    table (str): table whose weighted average for a particular date is
        being determined

    new_col_info (list): shows the proportion of 'well defined' HPO records
        by HPO site. organized alphabetically.

    analytics_type (str): the data quality metric the user wants to
        investigate

    :return:
    total_quality (float): indicates the overall proportion of well
        defined rows with respect to the total number of rows

    Function returns None object when it cannot calculate a weighted
        average

    NOTES:
    -----
    Since you are using the rounded 'success rate' to calculate the number
    of 'successful' rows, the final aggregate success rate will be
    slightly off from the 'true' value. This form of calculation, however,
    should not greatly disturb the final values and also prevents us from
    needing another function to get the 'successful' rows column from
    the sheet.
    """
    # getting the number of rows per site for the table type
    table_tot = determine_means_to_calculate_weighted_avg(
        analytics_type, table, new_col_info)

    if table_tot is None:
        return None
    elif isinstance(table_tot, float):  # mean was returned; the aggregate info
        return round(table_tot, 2)

    hpo_total_rows_by_date, valid_cols_tot = generate_hpo_contribution(
        file_names, 'total')

    if table_tot in valid_cols_tot:
        site_totals = hpo_total_rows_by_date[date][table_tot]

        total_table_rows_across_all_sites = 0
        total_succ_rows_across_all_sites = 0

        # can only count actual values
        for site_rows, site_succ_rate in zip(site_totals, new_col_info):

            if not math.isnan(site_rows):
                total_table_rows_across_all_sites += site_rows

            if not math.isnan(site_succ_rate) and not math.isnan(site_rows):
                site_succ_rate = site_succ_rate / 100  # logged as a percent
                site_succ_rows = site_succ_rate * site_rows
                total_succ_rows_across_all_sites += site_succ_rows

        if total_table_rows_across_all_sites > 0:
            total_quality = 100 * round(total_succ_rows_across_all_sites /
                                        total_table_rows_across_all_sites, 3)

        else:  # table only started to appear in later sheets
            return float('NaN')

        return total_quality
    else:  # no row count for table; cannot generate weighted average
        return None


def generate_column_for_table_df(
        site_and_date_info, date, sorted_names, table,
        percentage, file_names, analytics_type,
        ordered_dates_str, dataframes):
    """
    Function is used to generate a column for each dataframe
    in the case where
        a. the rows are the HPOs
        b. the columns are the dates
        c. the sheets are the different table types

    :param
    site_and_date_info (dict): dictionary with key:value
        of date:additional dictionaries that contain metrics
        for each HPO's data quality by type

    date (str): date to investigate for populating the column

    sorted_names (lst): list of the hpo site names sorted
        alphabetically

    table (str): table (e.g. drug exposure) whose data quality
        for all of the sites is being investigated

    percentage (boolean): used to determine whether or not the
        number is a simple record count (e.g. duplicates)
        versus the percentage of records (e.g. the success rate
        for each of the tables)

    file_names (list): list of the user-specified Excel files that are
        in the current directory. Files are analytics reports to be
        scanned.

    analytics_type (str): the data quality metric the user wants to
        investigate

    ordered_dates_str (lst): list of the different dates for
        the data analysis outputs. goes from oldest to most
        recent

    dataframes (list): list of pandas dataframes. each dataframe contains
        info about data quality for all of the sites for a date.

    :return:
    new_col_info (lst): list containing the data quality for each
        of the HPOs. Each index represents the data quality for a
        particular HPO. All of the values are with respect to the
        same table.
    """
    new_col_info = []
    hpo_site_info = site_and_date_info[date]

    for site in sorted_names:  # add the rows for the column
        if site != 'aggregate_info':
            hpo_table_info = hpo_site_info[site][table]

            if not math.isnan(hpo_table_info):
                new_col_info.append(hpo_table_info)
            else:
                new_col_info.append(float('NaN'))

    if not percentage:  # total; add a sum of the aforementioned values
        total = 0

        for site_val in new_col_info:
            if not math.isnan(site_val):
                total += site_val

        new_col_info.append(total)  # adding aggregate
    else:

        if analytics_type in ['sites_measurement', 'drug_success']:
            arith_avg = np.nanmean(new_col_info)
            arith_avg = round(arith_avg, 2)
            new_col_info.append(arith_avg)

        elif analytics_type in ['measurement_units']:
            weighted_avg = calculate_unit_aggregate_information(
                dataframes, date, ordered_dates_str, table)

            new_col_info.append(weighted_avg)

        elif analytics_type in ['drug_routes']:
            agg_info = calculate_route_aggregate_information(
                dataframes, date, ordered_dates_str, table)

            new_col_info.append(agg_info)

        else:
            # need to weight sites' relative contributions when
            # calculating an aggregate 'end' value
            weighted_avg = generate_weighted_average_table_sheet(
                file_names, date, table, new_col_info, analytics_type)

            if weighted_avg is not None:  # successful calculation
                new_col_info.append(weighted_avg)
            else:
                new_col_info.append("N/A")

    return new_col_info


def calculate_route_aggregate_information(
        dataframes, date, ordered_dates_str, table):
    """
    Function is used to calculate the 'aggregate' statistic
    for the 'route_concept_id integration sheet.' This necessitates its own
    function because it references columns that are specific to
    the route integration sheet.

    :param
    dataframes (list): list of pandas dataframes. each dataframe contains
        info about data quality for all of the sites for a date.

    date (str): date to investigate for populating the column

    ordered_dates_str (lst): list of the different dates for
        the data analysis outputs. goes from oldest to most
        recent

    :return:
    agg_info (float): represents the aggregate information
        statistic - either a sum of all the drugs/applicable
        drugs - or an 'overall success rate'
    """

    for date_idx, date_from_list in enumerate(ordered_dates_str):
        if date == date_from_list:
            df = dataframes[date_idx]

    try:
        all_routes = df['number_total_routes'].tolist()
        valid_routes = df['number_valid_routes'].tolist()

        all_routes = sum_df_column(all_routes)
        valid_routes = sum_df_column(valid_routes)

        if table == 'number_total_routes':
            agg_info = all_routes

        elif table == 'number_valid_routes':
            agg_info = valid_routes

        elif table == 'total_route_success_rate':
            weighted_avg = round(
                valid_routes / all_routes * 100, 2)

            agg_info = weighted_avg

        else:
            raise ValueError("Unexpected table found in the route sheet.")
    except:
        raise ValueError("Dataframe for the unit sheet not found.")

    return agg_info


def calculate_unit_aggregate_information(
        dataframes, date, ordered_dates_str, table):
    """
    Function is used to calculate the 'aggregate' statistic
    for the 'unit integration sheet.' This necessitates its own
    function because it references columns that are specific to
    the unit integration sheet.

    :param
    dataframes (list): list of pandas dataframes. each dataframe contains
        info about data quality for all of the sites for a date.

    date (str): date to investigate for populating the column

    ordered_dates_str (lst): list of the different dates for
        the data analysis outputs. goes from oldest to most
        recent

    :return:
    weighted_avg (float): represents the weighted 'unit'
        rate for the particular dimension of data quality
        being evaluated
    """

    for date_idx, date_from_list in enumerate(ordered_dates_str):
        if date == date_from_list:
            df = dataframes[date_idx]

    try:
        # FIXME: This is a duplicated code fragment - could be improved
        all_units = df['number_total_units'].tolist()
        valid_selected_meas = df['number_valid_units_sel_meas'].tolist()
        valid_all_meas = df['number_valid_units'].tolist()
        selected_meas = df['number_sel_meas'].tolist()

        all_units = sum_df_column(all_units)
        valid_selected_meas = sum_df_column(valid_selected_meas)
        valid_all_meas = sum_df_column(valid_all_meas)
        selected_meas = sum_df_column(selected_meas)

        if table == 'sel_meas_unit_success_rate':
            selected_success_rate = round(
                valid_selected_meas / selected_meas * 100, 2)

            weighted_avg = selected_success_rate

        elif table == 'total_unit_success_rate':
            total_success_rate = round(
                valid_all_meas / all_units * 100, 2)

            weighted_avg = total_success_rate

        elif table == 'proportion_sel_meas':
            proportion_selected_measurements = round(
                selected_meas / all_units * 100, 2)

            weighted_avg = proportion_selected_measurements

        else:
            raise ValueError("Unexpected table found in the unit sheet.")
    except:
        raise ValueError("Dataframe for the unit sheet not found.")

    return weighted_avg


def generate_table_dfs(
        sorted_names, sorted_tables, ordered_dates_str, site_and_date_info,
        percentage, file_names, analytics_type, dataframes):
    """
    Function generates a unique dataframe containing data quality
    metrics. Each dataframe should match to the metrics for a
    particular table type.

    The ROWS of each dataframe should be each HPO site.

    :param
    sorted_names (lst): list of the hpo site names sorted
        alphabetically

    sorted_tables (lst): list of the different table types
        sorted alphabetically

    ordered_dates_str (lst): list of the different dates for
        the data analysis outputs. goes from oldest to most
        recent

    site_and_date_info (dict): dictionary with key:value
        of date:additional dictionaries that contain metrics
        for each HPO's data quality by type

    percentage (boolean): used to determine whether or not the
        number is a simple record count (e.g. duplicates)
        versus the percentage of records (e.g. the success rate
        for each of the tables)

    file_names (list): list of the user-specified Excel files that are
        in the current directory. Files are analytics reports to be
        scanned.

    analytics_type (str): the data quality metric the user wants to
        investigate

    dataframes (list): list of pandas dataframes. each dataframe contains
        info about data quality for all of the sites for a date.

    :return:
    df_dictionary_by_table (dict): dictionary with key:value
        of table type: pandas df with the format described above
    """
    # create a new pandas df for each of the table types
    total_dfs = []
    num_tables = len(sorted_tables)

    # instantiate the HPO column in each of the sheets
    for _ in range(num_tables):
        new_df = pd.DataFrame({'hpo_ids': sorted_names})
        total_dfs.append(new_df)

    # df for each table
    for new_sheet_num, table in enumerate(sorted_tables):
        df_in_question = total_dfs[new_sheet_num]

        for date in ordered_dates_str:  # generate the columns
            new_col_info = generate_column_for_table_df

            # new col info are the metrics from all of the sites in
            # alphabetical order followed by an 'aggregate' value.
            df_in_question[date] = new_col_info(
                site_and_date_info, date, sorted_names, table,
                percentage, file_names, analytics_type,
                ordered_dates_str, dataframes)

    df_dictionary_by_table = dict(zip(sorted_tables, total_dfs))
    return df_dictionary_by_table


"""
This part of the script deals with making sheets where the
   a. SITES are SHEETS
   b. TABLES are ROWS
"""


def standardize_column_types(sorted_tables, valid_cols_tot):
    """
    Function is used to ensure that the 'valid_cols_tot'
    list ONLY contains relevant table types. This means
    that there are no table types that cannot be found
    in the sheet that contains the metric being investigated.

    :param
    sorted_tables (lst): list of the different table types
        sorted alphabetically. These are the tables that
        are found in the sheet of interest.

    valid_cols_tot (lst): list of the table types that can
        be iterated. This includes columns that exist in
        the 'source' sheet (and thus have a total row
        count) but may not exist in other sheets (and
        thus need to be removed for iteration)

    :return:
    new_valid_cols (lst): list of the table types that
        should be used for the iteration. This only
        includes tables that are relevant to the
        sheet in question.
    """
    sorted_tables_before_udscr = []
    new_valid_cols = []

    tables_with_no_underscore = ['measurement', 'observation', 'total']

    # for comparison down the line
    for idx, table_type in enumerate(sorted_tables):
        under_encountered = False

        if table_type not in tables_with_no_underscore:
            end_idx = 0
        else:  # want a different ending index
            end_idx = len(table_type)

        for c_idx, char in enumerate(table_type):
            if char == '_' and not under_encountered:
                end_idx = c_idx
                under_encountered = True

        table_type = table_type[0:end_idx]
        sorted_tables_before_udscr.append(table_type)

    # ensure we are only looking for columns that appear in the sheet
    for idx, column_type in enumerate(valid_cols_tot):
        under_encountered = False

        if table_type not in tables_with_no_underscore:
            end_idx = 0
        else:
            end_idx = len(table_type) - 1

        for c_idx, char in enumerate(column_type):
            if char == '_' and not under_encountered:
                end_idx = c_idx
                under_encountered = True

        column_type_trunc = column_type[0:end_idx]

        #  ensures that the the table is both
        #       a. in the 'source' sheet (and thus has
        #          'total_row' information available)
        #       b. in the sheet that is being investigated
        if column_type_trunc in sorted_tables_before_udscr:
            new_valid_cols.append(column_type)

    return new_valid_cols


def determine_dq_for_hpo_on_date(
        site_and_date_info, file_names, sorted_names,
        sorted_tables, date, site):
    """
    This function is used to determine the aggregate 'percent'
    metric for a site's data quality on a particular date.

    This is calculated by giving more weight to categories
    that have more logged rows.

    :param
    site_and_date_info (dict): dictionary with key:value
        of date:additional dictionaries that contain metrics
        for each HPO's data quality by type

    file_names (list): list of the user-specified Excel files that are
        in the current directory. Files are analytics reports to be
        scanned.

    sorted_names (lst): list of the hpo site names sorted
        alphabetically

    sorted_tables (lst): list of the different table types
        sorted alphabetically

    date (string): the column (date) for which the aggregate statistic
        is being measured. date of the analysis report.

    site (string): the HPO site whose aggregate statistic is being
        measured

    :return:
    tot_err_rate (float): shows the total proportion of a particular
        metric for a particular site on a particular date. this is
        the 'weighted' metric.
    """
    hpo_total_rows_by_date, valid_cols_tot = generate_hpo_contribution(
        file_names, 'total')

    # tables that have 'total_row' and data quality metrics available
    # should basically be the 'sorted tables' without any totals
    valid_cols_tot = standardize_column_types(sorted_tables, valid_cols_tot)

    incidence_for_site = site_and_date_info[date]  # data quality

    tot_rows_for_date, tot_errors_for_date = 0, 0

    # iterated first b/c rows per site parallels alphabetical site names
    # also take off the last 'total_row' for both of the lists
    for table, table_row_total in zip(sorted_tables[:-1], valid_cols_tot):
        total_rows_per_site = hpo_total_rows_by_date[date][table_row_total]

        for site_name, site_rows in zip(sorted_names, total_rows_per_site):
            # found the site in question
            if site == site_name and not math.isnan(site_rows):
                site_err_rate = incidence_for_site[site][table]
                if not math.isnan(site_rows) and not math.isnan(site_err_rate):
                    tot_rows_for_date += site_rows
                    site_err_rate = site_err_rate / 100

                    tot_table_errs_for_date = site_err_rate * site_rows
                    tot_errors_for_date += tot_table_errs_for_date

    if tot_rows_for_date > 0:  # calculated across all of the tables
        total_err_rate = tot_errors_for_date / tot_rows_for_date * 100
        total_err_rate = round(total_err_rate, 2)
    else:  # new site; no rows for first date(s)
        total_err_rate = "n/a"

    return total_err_rate


def generate_column_for_hpo_df(
        site_and_date_info, date, site, tables_only,
        percentage, file_names, sorted_names,
        sorted_tables, analytics_type):
    """
    Function is used to generate a column for each dataframe
    in the case where
        a. the rows are the table types
        b. the columns are the dates
        c. the sheets are the HPOs (and the aggregate statistic)

    :param
    site_and_date_info (dict): dictionary with key:value
        of date:additional dictionaries that contain metrics
        for each HPO's data quality by type

    date (string): the date used to populate the data in
        the column

    site (string): the site whose dataframe is being
        generated and will eventually output to an
        Excel sheet

    tables_only (lst): list of the different table types
        sorted alphabetically; excludes 'total' table
        type

    percentage (boolean): used to determine whether or not the
        number is a 'flawed' record count (e.g. duplicates)
        versus the percentage of 'acceptable' records

    file_names (list): list of the user-specified Excel files that are
        in the current directory. Files are analytics reports to be
        scanned.

    sorted_names (lst): list of the hpo site names sorted
        alphabetically

    sorted_tables (lst): list of the different table types
        sorted alphabetically

    analytics_type (str): the data quality metric the user wants to
        investigate

    :return
    new_col_info (lst): list containing the data quality for each
        of the table types/classes. Each index represents the data
        quality for a particular table/class. All of the values
        are with respect to the same HPO.
    """
    tot_errs_date = 0
    new_col_info = []
    site_info = site_and_date_info[date][site]

    no_need_for_total = ['sites_measurement', 'drug_success',
                         'measurement_units', 'drug_routes']

    for table in tables_only:  # populating each row in the col
        new_col_info.append(site_info[table])
        tot_errs_date += site_info[table]

    # creating the 'aggregate' statistic for the bottom of the col
    if analytics_type in no_need_for_total:
        pass
    elif not percentage:
        new_col_info.append(tot_errs_date)
    else:  # need to weight the overall DQ by number of rows by table
        weighted_errs = determine_dq_for_hpo_on_date(
            site_and_date_info, file_names, sorted_names,
            sorted_tables, date, site)

        new_col_info.append(weighted_errs)

    return new_col_info


def generate_site_dfs(sorted_names, sorted_tables,
                      ordered_dates_str, site_and_date_info,
                      percentage, file_names, analytics_type,
                      dataframes):
    """
    Function generates a unique dataframe for each site
    containing data quality metrics. The dictionary has
    the following structure:
        a. HPO name:data quality metrics
           data quality metrics are stored in a pandas df

    The rows of each dataframe should be each table.
    The columns of each dataframe are the dates

    :param
    sorted_names (lst): list of the hpo site names sorted
        alphabetically

    sorted_tables (lst): list of the different table types
        sorted alphabetically

    ordered_dates_str (lst): list of the different dates for
        the data analysis outputs. goes from oldest to most
        recent.

    site_and_date_info (dict): dictionary with key:value
        of date:additional dictionaries that contain metrics
        for each HPO's data quality by type

    percentage (boolean): used to determine whether or not the
        number is a 'flawed' record count (e.g. duplicates)
        versus the percentage of 'acceptable' records

    file_names (list): list of the user-specified Excel files that are
        in the current directory. Files are analytics reports to be
        scanned.

    analytics_type (str): the data quality metric the user wants to
        investigate

    dataframes (list): list of pandas dataframes. each dataframe contains
        info about data quality for all of the sites for a date.

    :return:
    df_dictionary_by_site (dict): dictionary with key:value
        of table type: pandas df with the format described above
    """
    total_dfs = []

    no_need_for_total = ['sites_measurement', 'drug_success',
                         'measurement_units', 'drug_routes']

    if analytics_type not in no_need_for_total:
        sorted_tables.append('total')
        # ignoring 'total'
        tables_only = sorted_tables[:-1]
    else:
        # integration/field metric
        # the 'aggregate' statistic is already in place
        tables_only = sorted_tables

    # ignoring 'aggregate_info'
    hpo_names_only = sorted_names[:-1]

    # df generation for each HPO and 'aggregate info'
    for _ in range(len(hpo_names_only)):
        new_df = pd.DataFrame({'table_type': sorted_tables})
        total_dfs.append(new_df)

    for new_sheet_num, site in enumerate(hpo_names_only):
        df_in_question = total_dfs[new_sheet_num]

        for date in ordered_dates_str:  # populating each column
            # includes aggregate statistic
            new_col_info = generate_column_for_hpo_df(
                site_and_date_info, date, site, tables_only,
                percentage, file_names, sorted_names,
                sorted_tables, analytics_type)

            df_in_question[date] = new_col_info  # adding the col

    # make the aggregate sheet
    aggregate_dfs = generate_aggregate_sheets(
        file_names, tables_only, site_and_date_info,
        ordered_dates_str, percentage, analytics_type,
        sorted_names, dataframes)

    total_dfs.extend(aggregate_dfs)

    if len(aggregate_dfs) == 3:  # when hpo_sheets used
        sorted_names.extend(['poorly_defined_rows_total', 'total_rows'])

    df_dictionary_by_site = dict(zip(sorted_names, total_dfs))
    return df_dictionary_by_site


def generate_nonpercent_aggregate_col(sorted_tables, site_and_date_info,
                                      date):
    """
    Function is used to generate a column that shows the SUM
    of the data quality metrics for a particular table across all
    sites. This column is for one date of analytics.

    This function is employed when the TABLES are the ROWS in the
    outputted Excel sheet.

    This ONLY adds aggregate info for values that should NOW be
    weighted. Example would be the number of duplicates.

    :param
    sorted_tables (lst): list of the different table types
        sorted alphabetically

    site_and_date_info (dict): dictionary with key:value
        of date:additional dictionaries that contain metrics
        for each HPO's data quality by type

    date (string): string for the date used to generate the column.
        should be used to index into the larger data dictionary.

    :return:
    aggregate_metrics_col (list): list of values representing the
        total number of aggregate problems for each table. should follow
        the order of "sorted tables". should also represent the column
        for only one date.
    """
    aggregate_metrics_col = []

    info_for_date = site_and_date_info[date]

    #  do not take the 'aggregate info' table
    for table in sorted_tables[:-1]:
        total, sites_and_dates_iterated = 0, 0

        for site in list(info_for_date.keys())[:-1]:
            site_table_info = info_for_date[site][table]

            if not math.isnan(site_table_info):
                total += site_table_info
                sites_and_dates_iterated += 1

        aggregate_metrics_col.append(total)

    total_across_all_tables = sum(aggregate_metrics_col)
    aggregate_metrics_col.append(total_across_all_tables)

    return aggregate_metrics_col


def calculate_row_counts_by_table(
        hpo_total_rows_by_date, hpo_errors_by_date,
        date, table_tot, table_valid, total_rows,
        total_by_table, total_errs, errs_by_table):
    """
    Function is used to calculate the row counts (the number of
    errors, the total number of rows) for a particular table
    across all of the HPO sites for a particular date. This
    function then adds this 'total' metric to a list so it
    can be reflected in a data quality column.

    :param
    hpo_total_rows_by_date (dictionary): dictionary with the following
    key:value pairings
        a. date:following dictionary
        b. table type: list with the total number of rows for each
            HPO. the sequence of the list follows the alphabetical
            order of all the HPO sites.

    hpo_errors_by_date (dictionary): dictionary with the following
    key:value pairings
        a. date:following dictionary
        b. table type: list with the total number of errors for each
            HPO. the sequence of the list follows the alphabetical
            order of all the HPO sites.

    date (string): date used to index into the dictionaries above

    table_tot (string): name of the table whose row counts are being
        investigated (total_rows)

    table_valid (string) name of the table whose row counts are
        being investigated (well_defined_rows)

    total_rows (int): total number of rows across all sites for
        a date; growing

    total_by_table (list): list of the total number of row counts
        across all sites. sequence of the list parallels the
        alphabetical order of the tables.

    total_errs (int): total number of errors across all sites
        for a particular date; growing

    errs_by_table (list): list of the total number of
        poorly defined row counts across all sites.
        sequence of the list parallels the alphabetical
        order of the tables.

    :return:
    errs_by_table (list): list given to the function
        with an additional 'sum' metric added to the end

    total_by_table (list): list given to the function with an
        additional 'sum' metric added to the end

    total_errs (int): number of errors across all sites for
        a particular date; now has the errors for the table
        in question logged

    total_rows (int): number of rows across all sites for
        a particular date; now has the row number for the
        table in question logged
    """
    # these are lists, each idx represents a site
    site_totals = hpo_total_rows_by_date[date][table_tot]
    site_valids = hpo_errors_by_date[date][table_valid]

    site_totals = [x if isinstance(x, float) and
                   not math.isnan(x) else 0
                   for x in site_totals]

    site_valids = [x if isinstance(x, float) and
                   not math.isnan(x) else 0
                   for x in site_valids]

    site_total_for_table = sum(site_totals)
    total_rows += site_total_for_table

    if site_total_for_table > 0:
        total_by_table.append(site_total_for_table)
    else:
        total_by_table.append(float('NaN'))

    # valids are well_defined rows. sum across all sites
    table_errs = (site_total_for_table - sum(site_valids))
    total_errs += table_errs

    if table_errs > 0:
        errs_by_table.append(table_errs)
    else:
        errs_by_table.append(float('NaN'))

    return errs_by_table, total_by_table, total_errs, total_rows


def add_error_total_and_proportion_cols(
        total_errs, total_rows, date,
        errs_by_table, total_by_table,
        aggregate_df_total, aggregate_df_error,
        aggregate_df_proportion):
    """
    Function adds a new column to the growing dataframes. This
    column contains information depending on the dataframe in
    question. The column may contain information about
        a. the total number of rows
        b. the total number of 'poorly defined' rows
        c. the relative contribution of a particular table
        to the number of 'poorly defined' rows

    The column represents a particular date. Each row
    represents a particular table type. The final row
    is an 'aggregate' metric that is a sum of the
    rows immediately above it.

    :param
    total_errs (int): total number of errors across all sites
        for a particular date; across all tables

    total_rows (int): total number of rows across all sites
        for a particular date; across all tables

    date (string): date used to index into the dictionaries above

    errs_by_table (list): list of the total number of
        poorly defined row counts across all sites.
        sequence of the list parallels the alphabetical
        order of the tables. has 'total' metric at the end.

    total_by_table (list): list of the total number of row counts
        across all sites. sequence of the list parallels the
        alphabetical order of the tables. has 'total'
        metric at the end.

    aggregate_df_total (dataframe): dataframe that contains the
        total number of rows across all sites. each column
        is a date. each row is a table type. last row
        is the total number of rows for a particular
        date.

    aggregate_df_error (dataframe): dataframe that contains
        the total number of poorly defined rows across
        all sites. each column is a date. each row is a table
        type. last row is the total number of poorly defined
        rows for a particular date.

    aggregate_df_proportion (dataframe): dataframe that
        shows the 'contribution' of each table to the
        total number of poorly defined rows. for instance,
        if a table has half of all of the 'poorly defined
        rows' for a particular date, it will have a value of
        0.5. each column is a date. each row is a table
        type.

    :return:

    aggregate_df_total (dataframe): same as the df that
        entered except now with an additional column
        to represent the date in question

    aggregate_df_error (dataframe): same as the df that
        entered except now with an additional column
        to represent the date in question

    aggregate_df_proportion (dataframe): same as the df
        that entered except now with an additional
        column to represent the date in question

    """
    # adding to the growing column
    # total number of 'poorly defined' rows for a date
    if total_errs > 0:
        errs_by_table.append(total_errs)
    else:
        errs_by_table.append(float('NaN'))

    # total number of rows for a table for the date
    if total_rows > 0:
        total_by_table.append(total_rows)
    else:
        total_by_table.append(float('NaN'))

    # column for the 'total' rows; column for one date
    aggregate_df_total[date] = total_by_table

    # column for 'error' rows; column for one date
    aggregate_df_error[date] = errs_by_table

    # column for the contribution of each error type
    succ_rate_by_table = []
    for errors, total in zip(errs_by_table, total_by_table):
        error_rate = round(errors / total * 100, 2)
        success_rate = 100 - error_rate
        succ_rate_by_table.append(success_rate)

    aggregate_df_proportion[date] = succ_rate_by_table

    return aggregate_df_error, aggregate_df_total, \
        aggregate_df_proportion


def generate_additional_aggregate_dq_sheets(
        file_names, ordered_dates_str):
    """
    Function is used to generate two 'aggregate data' sheets that
    show the data quality for various tables across the various
    dates. The three sheets generated show:
        a. the total number of 'poorly defined' rows for each of the
           table types

        b. the total number of rows for each of the table types

        c. the relative proportion by which each table contributes to
           the total number of 'poorly defined' rows. for instance, if
           half of the poorly defined rows are from the condition_occurrence
           table, condition_occurrence will have a value of 0.5

    :param
    file_names (list): list of the user-specified Excel files that are
        in the current directory. Files are analytics reports to be
        scanned.

    ordered_dates_str (lst): list of the different dates for
        the data analysis outputs. goes from oldest to most
        recent.

    :return:
    total_dfs (lst): list with the two pandas dataframes described
        above.
    """
    # getting the number of total rows for each HPO
    hpo_total_rows_by_date, valid_cols_tot = generate_hpo_contribution(
        file_names, 'total')

    # getting the number of 'successful' rows for each HPO
    hpo_errors_by_date, valid_cols_val = generate_hpo_contribution(
        file_names, 'valid')

    valid_cols_tot.append('total')

    aggregate_df_proportion = pd.DataFrame({'table_type': valid_cols_tot})
    aggregate_df_error = pd.DataFrame({'table_type': valid_cols_tot})
    aggregate_df_total = pd.DataFrame({'table_type': valid_cols_tot})
    total_dfs = [aggregate_df_proportion, aggregate_df_error,
                 aggregate_df_total]

    for date in ordered_dates_str:
        total_errs, total_rows = 0, 0
        errs_by_table, total_by_table = [], []

        for table_tot, table_valid in zip(valid_cols_tot[:-1], valid_cols_val):
            # adding errors and totals for each table
            errs_by_table, total_by_table, total_errs, total_rows = \
                calculate_row_counts_by_table(
                    hpo_total_rows_by_date, hpo_errors_by_date, date,
                    table_tot, table_valid, total_rows, total_by_table,
                    total_errs, errs_by_table)

        # adding the columns (for particular date) to the dataframe
        aggregate_df_error, aggregate_df_total, \
            aggregate_df_proportion = add_error_total_and_proportion_cols(
                total_errs, total_rows, date, errs_by_table, total_by_table,
                aggregate_df_total, aggregate_df_error,
                aggregate_df_proportion)

    return total_dfs


def determine_table_dq_for_date(
        hpo_total_rows_by_date, date, column_label,
        sorted_names, incidence_for_site, table,
        tot_rows_for_date, tot_errors_for_date,
        error_amounts_for_date):
    """
    Function looks at a particular table for a particular
    date and calculates the 'aggregate' data quality for said
    timepoint.

    :param
    hpo_total_rows_by_date (dict):  dictionary with the following
    key:value pairings
        a. date:following dictionary
        b. table type: list with the total number of errors for each
            HPO. the sequence of the list follows the alphabetical
            order of all the HPO sites.

    date (string): date used to index into the dictionaries above

    column_label (string): table whose data quality is being
        investigated. this string specifically allows one to find
        the 'total row' count.

    sorted_names (lst): list of the hpo site names sorted
        alphabetically

    incidence_for_site (dict): dictionary with the following
    key:value pairings
        a. hpo_id: following dict
        b. table name: metric being investigated

    table (string): table whose data quality is being investigated

    tot_rows_for_date (int): running total for the total number
        of rows across all tables/sites for a particular date.

    tot_errors_for_date (int): running total for the total
        number of errors across all tables/sites for
        a particular date.

    error_amounts_for_date (list): going to serve as the
        column for the date. each row is the data quality
        total for a particular table across all sites.

    :return:
    tot_errors_for_date (int): same as what was given
        to the function as a parameter with the number
        of errors for a particular table added

    tot_rows_for_date (int): same as what was given
        to the function as a parameter with the total
        number of rows for a particular date added

    error_amounts_for_date (list): same as column given
        but now with an additional index to represent the
        data quality for a particular table.
    """
    tot_errors_for_table, tot_rows_for_table = 0, 0
    total_rows_per_site = hpo_total_rows_by_date[date][column_label]

    for site, site_rows in zip(sorted_names[:-1], total_rows_per_site):
        site_err_rate = incidence_for_site[site][table]

        # NOTE: Many sites have a NaN error rate
        if not math.isnan(site_err_rate) and not math.isnan(site_rows):
            site_err_rate = site_err_rate / 100  # convert from percent
            site_err_tot = site_err_rate * site_rows

            tot_rows_for_table += site_rows
            tot_rows_for_date += site_rows

            tot_errors_for_table += site_err_tot
            tot_errors_for_date += site_err_tot

    # now all the error rates for each table type; append to list
    if tot_rows_for_table > 0:
        err_rate_table = tot_errors_for_table / tot_rows_for_table
        err_rate_table = round(err_rate_table * 100, 2)
        error_amounts_for_date.append(err_rate_table)
    else:
        error_amounts_for_date.append(float('NaN'))

    return tot_errors_for_date, \
        tot_rows_for_date, error_amounts_for_date


def determine_weighted_average_of_percent(
        site_and_date_info, ordered_dates_str,
        sorted_tables, file_names, sorted_names):
    """
    This function determines the overall data quality for each
    of the table types for a particular date. This function ensures
    that the relative contribution of each HPO site is factored in
    and thus gives us a better idea of the 'overall health' of a
    table.

    :paramt): dictionary with key:value
        of date:additional dictionaries that contain metrics
        for each HPO's data
    site_and_date_info (dic quality by type

    ordered_dates_str (lst): list of the different dates for
        the data analysis outputs. goes from oldest to most
        recent.

    sorted_tables (lst): list of the different table types
        sorted alphabetically

    file_names (list): list of the user-specified Excel files that are
        in the current directory. Files are analytics reports to be
        scanned

    sorted_names (lst): list of the hpo site names sorted
        alphabetically

    :return:
    aggregate_df_weighted (dataframe): dataframe with weighted
        metrics for the data quality for each table.

        column represents each date rows are the table types
        final row is an 'aggregate' statistic to represent the date as a whole

        put in a list for consistency with how it will be added with respect to
        the other generated dataframes.
    """
    hpo_total_rows_by_date, valid_cols_tot = generate_hpo_contribution(
        file_names, 'total')

    sorted_tables.append('total')  # use relevant tables

    # NOTE: sorted tables should have all tables that ever appear on
    # the relevant sheet
    aggregate_df_weighted = pd.DataFrame({'table_type': sorted_tables})

    total_row_cols = valid_cols_tot
    total_row_cols.sort()

    for date in ordered_dates_str:
        error_amounts_for_date = []
        incidence_for_site = site_and_date_info[date]
        tot_rows_for_date, tot_errors_for_date = 0, 0

        # do not take the 'total' for the sorted_tables
        for table, column_label in zip(sorted_tables[:-1], total_row_cols):
            # adding a row to the growing column; DQ for the table in question
            tot_errors_for_date, tot_rows_for_date, error_amounts_for_date = \
                determine_table_dq_for_date(
                    hpo_total_rows_by_date, date, column_label,
                    sorted_names, incidence_for_site, table,
                    tot_rows_for_date, tot_errors_for_date,
                    error_amounts_for_date)

        # aggregate data quality for a date
        if tot_rows_for_date > 0:
            tot_errors_for_date = tot_errors_for_date / tot_rows_for_date
            tot_errors_for_date = round(tot_errors_for_date * 100, 2)
            error_amounts_for_date.append(tot_errors_for_date)
        else:
            error_amounts_for_date.append(float('NaN'))

        # end product - finalized column
        aggregate_df_weighted[date] = error_amounts_for_date

    return [aggregate_df_weighted]


def sum_df_column(column):
    """
    Function is used to take the column of a dataframe (presumably
    from a analytics report Excel sheet) and returns the total.)
    This is intentionally designed to avoid errors caused by
    non-integer values.

    :param
    column (list): column from a dataframe that should be
        traversed

    :return:
    total (int) value that represents the sum of the entire column
    """
    total = 0

    for site_val in column:
        try:
            site_val = float(site_val)
            if not math.isnan(site_val):
                total += site_val
        except ValueError:
            pass

    return total


def unit_integration_aggregate_sheet(
        ordered_dates_str, dataframes):
    """
    Function is used to generate an aggregate_info dataframe
    that can be used for the 'unit integration' option. The
    'unit integration' option warrants its own aggregate
    calculation because you must use the original dataframe
    (rather than looking at the total number of rows for
    any given table).

    One must use the original dataframe because the
    measurement_units success rate ONLY looks at particular
    rows in the measurement table (namely rows where the
    concept_id is in a list of concept_ids that we are
    looking at as the DRC). This means that the 'total
    rows' in the measurement table generated earlier
    could not help us with this calculation.

    :param
    ordered_dates_str (lst): list of the different dates for
        the data analysis outputs. goes from oldest to most
        recent.

    dataframes (list): list of pandas dataframes. each dataframe contains
        info about data quality for all of the sites for a date.

    :return:
    aggregate_df (dataframe): a dataframe that contains proportion
        of 'successful unit populations for selected measurements'
        for each date investigated.
    """
    aggregate_df = pd.DataFrame(
        index=['selected_units_success_rate', 'total_units_success_rate',
               'proportion_selected_measurements'], columns=ordered_dates_str)

    # populating column-by-column
    for date_idx, date in enumerate(ordered_dates_str):
        df = dataframes[date_idx]

        # FIXME: This is a duplicated code fragment - could be improved
        all_units = df['number_total_units'].tolist()
        valid_selected_meas = df['number_valid_units_sel_meas'].tolist()
        valid_all_meas = df['number_valid_units'].tolist()
        selected_meas = df['number_sel_meas'].tolist()

        all_units = sum_df_column(all_units)
        valid_selected_meas = sum_df_column(valid_selected_meas)
        valid_all_meas = sum_df_column(valid_all_meas)
        selected_meas = sum_df_column(selected_meas)

        selected_success_rate = round(
            valid_selected_meas / selected_meas * 100, 2)
        total_success_rate = round(
            valid_all_meas / all_units * 100, 2)
        proportion_selected_measurements = round(
            selected_meas / all_units * 100, 2)

        aggregate_df[date] = [
            selected_success_rate, total_success_rate,
            proportion_selected_measurements]

    return [aggregate_df]  # list so it can be easily appended


def aggregate_sheet_route_population(
        file_names, ordered_dates_str,
        sorted_names, site_and_date_info):
    """
    This function creates an 'aggregate' dataframe that shows
    what proportion of rows in the drug exposure table (across
    all sites) have a populated 'route' field.

    :param
    file_names (list): list of the user-specified Excel files that are
        in the current directory. Files are analytics reports to be
        scanned

    ordered_dates_str (lst): list of the different dates for
        the data analysis outputs. goes from oldest to most
        recent.

    sorted_names (lst): list of the hpo site names sorted
        alphabetically

    site_and_date_info (dict): dictionary with key:value
        of date:additional dictionaries that contain metrics
        for each HPO's data quality by type

    :return:
    [aggregate_df] (lst containing a single df):
    dataframe has:
        a. columns that are dates
        b. single row

    the aggregate_df is put in a list so it can be easily
    appended to an existing list.

    NOTE: You cannot really create a 'weighted' metric for a specific drug
    class at this point. This metric is impossible to calculate because
    we do not necessarily know how many drugs of a particular class
    (e.g. antibiotics) that each site contributes. One could hypothetically
    give more weights to sites that contribute more drugs as a whole but
    doing so would merely be a 'guestimate.'
    """
    aggregate_df_weighted = pd.DataFrame(
        index=[
            'number_total_routes', 'number_valid_routes',
            'total_route_success_rate'],
        columns=ordered_dates_str)

    hpo_total_rows_by_date, _ = generate_hpo_contribution(
        file_names, 'total')

    for date in ordered_dates_str:
        rows_w_unit_date, rows_total_date = 0, 0
        drug_rows = hpo_total_rows_by_date[date]['drug_total_row']

        # gathering data from all of the sites
        for site_name, total_drug_rows in zip(sorted_names, drug_rows):
            site_successful_rows = site_and_date_info[date][site_name][
                'number_valid_routes']
            drug_rows = site_and_date_info[date][site_name][
                'number_total_routes']

            if not math.isnan(drug_rows) and not math.isnan(site_successful_rows):
                rows_w_unit_date += site_successful_rows
                rows_total_date += drug_rows

        if rows_total_date > 0:
            quality_for_date = round(rows_w_unit_date / rows_total_date * 100, 2)
        else:
            quality_for_date = np.nan

        aggregate_df_weighted[date] = [
            rows_total_date, rows_w_unit_date, quality_for_date]

    # putting in list to make it easy to append
    return [aggregate_df_weighted]


def aggregate_sheet_integration(
        dataframes, sorted_tables, ordered_dates_str):
    """
    This function is used to generate an 'aggregate' sheet
    that should represent the average integration of drug
    ingredients or measurements across all sites.
    The ingredient and measurement concepts are separated
    into different 'categories'.

    :param
    dataframes (list): list of pandas dataframes. each dataframe
        contains info about data quality for all of the sites
         for a date.

    sorted_tables (lst): list of the different table types
        sorted alphabetically

    ordered_dates_str (lst): list of the different dates for
        the data analysis outputs. goes from oldest to most
        recent.

    :return:
    [aggregate_df] (lst containing a single df):
    dataframe has:
        a. columns that are dates
        b. rows that are the different classes of
        drugs/measurements (e.g. ACE Inhibitor, lipids)
        this include an 'all measurements' class that spans
        all of the aforementioned classes

    each point within the dataframe shows the average
    integration across all sites for the particular
    date and drug class.

    the aggregate_df is put in a list so it can be easily
    appended to an existing list.

    NOTE: There is no 'total' row at this point. Cannot create
    a weighted metric that 'weights' integration of all of
    the concepts since we do not know how many drugs may
    belong to each category.
    """
    aggregate_df_weighted = pd.DataFrame(
        index=sorted_tables, columns=ordered_dates_str)

    for date_idx, date in enumerate(ordered_dates_str):
        df = dataframes[date_idx]
        means_for_dates = []
        for drug_type in sorted_tables:
            column = df[drug_type]
            tot, iterated = 0, 0

            # all sites weighted equally - simple mean
            for stat in column:
                try:
                    stat = float(stat)
                    tot += stat
                    iterated += 1
                except ValueError:  # 'No Data' logged
                    pass

            average_for_drug_int = round(tot / iterated, 2)
            means_for_dates.append(average_for_drug_int)

        aggregate_df_weighted[date] = means_for_dates

    return [aggregate_df_weighted]


def generate_aggregate_sheets(file_names, sorted_tables, site_and_date_info,
                              ordered_dates_str, percentage, analytics_type,
                              sorted_names, dataframes):
    """
    This function is called when
        a. The rows of each dataframe should be each table.
        b. The columns of each dataframe are the dates

    This function generates a SEPARATE sheet that can be used to show
    the aggregate metrics for all of the tables (across all sites)
    during a particular date.

    :param
    file_names (list): list of the user-specified Excel files that are
        in the current directory. Files are analytics reports to be
        scanned

    sorted_tables (lst): list of the different table types
        sorted alphabetically

    site_and_date_info (dict): dictionary with key:value
        of date:additional dictionaries that contain metrics
        for each HPO's data quality by type

    ordered_dates_str (lst): list of the different dates for
        the data analysis outputs. goes from oldest to most
        recent.

    percentage (boolean): used to determine whether or not the
        number is a 'flawed' record count (e.g. duplicates)
        versus the percentage of 'acceptable' records

    analytics_type (str): the data quality metric the user wants to
        investigate

    sorted_names (lst): list of the hpo site names sorted
        alphabetically

    dataframes (list): list of pandas dataframes. each dataframe contains
        info about data quality for all of the sites for a date.

    :return:
    total_dfs (lst): list of pandas dataframes that document the
        data quality of the various data types
    """
    if analytics_type in ['source_concept_success_rate', 'concept']:
        # three additional dataframes
        total_dfs = generate_additional_aggregate_dq_sheets(
            file_names, ordered_dates_str)

    elif analytics_type in ['measurement_units']:
        # warrants unique approach since units
        # are only for selected measurements
        total_dfs = unit_integration_aggregate_sheet(
            ordered_dates_str, dataframes)

    elif analytics_type in ['drug_routes']:
        # do not want categories; only one statistic per date
        total_dfs = aggregate_sheet_route_population(
            file_names, ordered_dates_str,
            sorted_names, site_and_date_info)

    elif analytics_type in ['sites_measurement', 'drug_success']:
        # categorical classification; no weighting
        total_dfs = aggregate_sheet_integration(
            dataframes, sorted_tables, ordered_dates_str)

    elif percentage:
        # weighting with respect to established tables
        total_dfs = determine_weighted_average_of_percent(
            site_and_date_info, ordered_dates_str, sorted_tables,
            file_names, sorted_names)

    else:
        # summing the numbers (e.g. duplicates)
        sorted_tables.append('total')
        total_dfs = []
        aggregate_df = pd.DataFrame({'table_type': sorted_tables})

        for date in ordered_dates_str:
            aggregate_col = generate_nonpercent_aggregate_col(
                sorted_tables, site_and_date_info, date)

            aggregate_df[date] = aggregate_col

        total_dfs.append(aggregate_df)

    return total_dfs


def understand_sheet_output_type(sorted_tables, sorted_names):
    """
    Function used to determine what kind out output formatting
    the user would want for the generated Excel files.

    :param
    sorted_tables (lst): list of the different table types
        sorted alphabetically

    sorted_names (lst): list of the hpo site names sorted
        alphabetically

    :return:
    user_choice (string): determines which variable (either
        distinct site or distince talbe) will serve as a
        'anchor' to separate out the sheets
    """
    output_prompt = \
        "\nWould you prefer to generate: \n" \
        "A. {} sheets detailing the data quality for each table. " \
        "The HPO IDs would be displayed as rows. \nor \n" \
        "B. {} sheets detailing the data quality for each HPO site. " \
        "The table type would be displayed as rows. This will " \
        "also include 1-3 table(s) with statistics on the " \
        "aggregate data for each table type on each date.". \
        format(len(sorted_tables), len(sorted_names))

    user_input = input(output_prompt).lower()
    output_choice_dict = {'a': 'table_sheets',
                          'b': 'hpo_sheets'}

    while user_input not in output_choice_dict.keys():
        print("\nInvalid choice. Please specify a letter that corresponds "
              "to an appropriate output type.\n")
        user_input = input(output_prompt).lower()

    user_choice = output_choice_dict[user_input]

    return user_choice


"""
NOW we are actually getting into the 'execution' of the
code. This marks the end of the functions that organize
and quantify the data.
"""
# CDR releases
# ------------
# report1 = 'march_27_2019.xlsx'
# report2 = 'may_10_2019.xlsx'
# report3 = 'october_04_2019.xlsx'

# Weekly reports
# --------------
# report1 = 'july_15_2019.xlsx'
# report2 = 'july_23_2019.xlsx'
# report3 = 'august_05_2019.xlsx'
# report4 = 'august_13_2019.xlsx'
# report5 = 'august_19_2019.xlsx'
# report6 = 'august_26_2019.xlsx'
# report7 = 'september_03_2019.xlsx'
# report8 = 'september_09_2019.xlsx'
# report9 = 'september_16_2019.xlsx'
# report10 = 'september_23_2019.xlsx'
# report11 = 'september_30_2019.xlsx'
# report12 = 'october_07_2019.xlsx'
# report13 = 'october_15_2019.xlsx'
# report14 = 'october_22_2019.xlsx'
# report15 = 'october_28_2019.xlsx'
# report16 = 'november_04_2019.xlsx'
# report17 = 'november_11_2019.xlsx'
# report18 = 'november_18_2019.xlsx'
# report19 = 'november_27_2019.xlsx'
# report20 = 'december_02_2019.xlsx'
# report21 = 'december_09_2019.xlsx'
# report22 = 'december_16_2019.xlsx'
# report23 = 'december_23_2019.xlsx'
# report24 = 'january_21_2020.xlsx'
# report25 = 'january_27_2020.xlsx'
# report26 = 'february_03_2020.xlsx'
# report27 = 'february_10_2020.xlsx'
# report28 = 'february_17_2020.xlsx'
# report29 = 'february_24_2020.xlsx'

# UNIONED EHR COMPARISON
report1 = 'may_10_2019.xlsx'
report2 = 'july_15_2019.xlsx'
report3 = 'october_04_2019.xlsx'
report4 = 'march_16_2020.xlsx'

# DEID versus DEID_clean
# report17 = 'october_05_2019.xlsx'
# report18 = 'october_06_2019.xlsx'

# 2019 retrospective
# report1 = 'january_07_2019.xlsx'
# report2 = 'april_10_2019.xlsx'
# report3 = 'july_18_2019.xlsx'
# report4 = 'october_10_2019.xlsx'
# report5 = 'december_16_2019.xlsx'

report_titles = [report1, report2, report3, report4]

metric_choice, metric_is_percent, ideal_low = get_user_analysis_choice()

data_frames = load_files(metric_choice, report_titles)

# creating a consistent row/column label set
hpo_name_col = generate_hpo_id_col(report_titles)

# creating an organized dictionary with all of the information easily-indexed
date_site_table_info, all_tables = iterate_sheets(
    data_frames, hpo_name_col, metric_is_percent,
    metric_choice, ideal_low, report_titles)

# creating a consistent means to navigate the tables
ordered_dates, ordered_names, ordered_tables = sort_names_and_tables(
    date_site_table_info, all_tables)

# adding new aggregate metrics for each date; sum or average of HPO sites
date_site_table_info = add_aggregate_info(
    date_site_table_info, metric_is_percent, hpo_name_col)

# understanding which variable will distinguish new sheets
output_choice = understand_sheet_output_type(
    ordered_tables, ordered_names)

df_dict = {}  # will be replaced later on; need holder

# creating dataframes with site/table/date metrics
if output_choice == 'table_sheets':
    df_dict = generate_table_dfs(
        ordered_names, ordered_tables, ordered_dates,
        date_site_table_info, metric_is_percent, report_titles,
        metric_choice, data_frames)
elif output_choice == 'hpo_sheets':
    df_dict = generate_site_dfs(
        ordered_names, ordered_tables, ordered_dates,
        date_site_table_info, metric_is_percent, report_titles,
        metric_choice, data_frames)

output_file_name = metric_choice + "_" + output_choice + \
                   "_data_analytics.xlsx"

writer = pd.ExcelWriter(output_file_name, engine='xlsxwriter')

for df_name, dataframe in df_dict.items():
    dataframe.to_excel(writer, sheet_name=df_name)

writer.save()
