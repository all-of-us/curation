"""
Goals
------
Program should generate a report (Excel File) that shows
how data quality metrics for each HPO site change over time.
Data quality metrics include:
    1. the number of duplicates per table
    2. number of 'start dates' that precede 'end dates'
    3. number of records that are >30 days after a patient's death date
    4. source table success rates
    5. concept table success rates

ASSUMPTIONS
-----------
1. The user has all of the files s/he wants to analyze in the current
directory

2. The user will know to change the 'report' variables to match the
file names of the .xlsx files in the current working directory.

3. All sheets are saved as month_date_year.xlsx
    - year should be four digits
    - this name is used to determine the date

4. The sheet names for all of the generated reports are consistent

5. The 'aggregate_info' statistics generated in some reports are
always labeled as 'aggregate_info.' This ensures these rows can
be excluded when generating initial dataframes. These aggregate
statistics can then be generated more effectively down the line
with an appropriate 'weighting'.

6. Assumes that all of the columns from the 'source' tab of the
analytics reports will always have the same names.

FIXME:
-------------------------------------------------------
1. tons of naming inconsistencies; does not allow effective
   parsing through files
"""

import datetime
import math
import os
import sys
import xlsxwriter
import pandas as pd


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
    analysis_type_prompt = "\nWhat kind of analysis over time report " \
                           "would you like to generate for each site?\n\n" \
                           "A. Duplicates\n" \
                           "B. Amount of data following death dates\n" \
                           "C. Amount of data with end dates preceding start dates\n" \
                           "D. Success Rate for Source Tables\n" \
                           "E. Success Rate for Concept Tables\n\n" \
                           "Please specify your choice by typing the corresponding letter."

    user_command = input(analysis_type_prompt).lower()

    choice_dict = {
        'a': 'duplicates',
        'b': 'data_after_death',
        'c': 'end_before_begin',
        'd': 'source',
        'e': 'concept'}

    while user_command not in choice_dict.keys():
        print("\nInvalid choice. Please specify a letter that corresponds "
              "to an appropriate analysis report.\n")
        user_command = input(analysis_type_prompt).lower()

    # NOTE: This dictionary needs to be expanded in the future
    percentage_dict = {
        'duplicates': False,
        'data_after_death': True,
        'end_before_begin': True,
        'source': True,
        'concept': True}

    # dictionary indicates if the target is to minimize or maximize number
    target_low = {
        'duplicates': True,
        'data_after_death': True,
        'end_before_begin': True,
        'source': False,  # table success rates
        'concept': False}

    analytics_type = choice_dict[user_command]
    percent_bool = percentage_dict[analytics_type]
    target_low = target_low[analytics_type]

    return analytics_type, percent_bool, target_low


def load_files(user_choice, file_names):
    """
    Function loads the relevant sheets from all of the
    files in the directory (see 'file_names' list from above).

    'Relevant sheet' is defined by previous user input.

    :param
    user_choice (string): represents the sheet from the analysis reports
        whose metrics will be compared over time
    file_names (list): list of the user-specified Excel files that are
        in the current directory. Files are analytics reports to be
        scanned.

    :returns
    sheets (list): list of pandas dataframes. each dataframe contains
        info about data quality for all of the sites for a date.
    """
    num_files_indexed = 0
    sheets = []

    while num_files_indexed < len(file_names):
        try:
            file_name = file_names[num_files_indexed]

            sheet = pd.read_excel(file_name, sheet_name=user_choice)

            if sheet.empty:
                print("WARNING: No {} sheet found in dataframe {}".format(
                    user_choice, file_name))
                del file_names[num_files_indexed]
                num_files_indexed -= 1  # skip over the date
            else:
                sheets.append(sheet)

            num_files_indexed += 1

        except IOError:
            print("{} not found in the current directory: {}. Please "
                  "ensure that the file names are consistent between "
                  "the Python script and the file name in your current "
                  "directory. ".format(file_names[num_files_indexed], cwd))
            sys.exit(0)

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

    # FIXME: this is a hacky way to bypass columns we are less
    #  interested in. Could be improved. Still want to prevent
    #  hard-coding in tables that 'should' be there.
    undocumented_cols = ['Unnamed: 0', 'src_hpo_id', 'HPO',
                         'total', 'device_exposure']
    rate_focused_inputs = ['source', 'concept']

    # FIXME: Need consistent way to document the rates between
    #  different error reports; right now a 'hacky' fix

    for number, sheet in enumerate(dataframes):  # for each date
        data_info = sheet.iloc[1, :]  # just to get the columns
        column_names = data_info.keys()

        if analytics_type in rate_focused_inputs:
            for col_label, val in data_info.iteritems():
                if col_label[-5:] != '_rate' and \
                        col_label[-7:] != '_rate_y':
                    undocumented_cols.append(col_label)

        final_tables = [x for x in column_names if x not in
                        undocumented_cols]

    # eliminate duplicates
    final_tables = list(dict.fromkeys(final_tables))
    return final_tables


def get_info(sheet, row_num, percentage, sheet_name,
             mandatory_tables):
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
    analytics_type (str): the data quality metric the user wants to
        investigate
    mandatory_tables (lst): contains the tables that should be
        documented for every table and at every date.

    :return:
    err_dictionary (dictionary): key:value pairs represent the
        column name:number that represents the quality of the data
    NOTE: This function was modified from the e-mail generator. This
    function, however, logs ALL of the information in the returned
    error dictionary. This includes 0 values if the data is wholly
    complete.
    """
    if row_num is not None:
        data_info = sheet.iloc[row_num, :]  # series, row labels and values
    else:  # row in future sheets but not current sheet
        data_info = sheet.iloc[1, :]  # just to get the columns
        column_names = data_info.keys()
        null_list = [None] * len(column_names)
        data_info = pd.Series(null_list, column_names)

    err_dictionary = {}

    for col_label, number in data_info.iteritems():
        if col_label in mandatory_tables:
            if number is None or number == 'No Data':  # row does not exist
                err_dictionary[col_label] = float('NaN')
            else:
                try:
                    number = float(number)
                except ValueError:
                    pass
                else:
                    if number < 0:  # just in case
                        raise ValueError("Negative number detected in sheet "
                                         "{} for column {}".format(
                            sheet_name, col_label))
                    elif percentage and number > 100:
                        raise ValueError("Percentage value > 100 detected in "
                                         "sheet {} for column {}".format(
                            sheet_name, col_label))
                    elif percentage and target_low:  # proportion w/ errors
                        err_dictionary[col_label] = round(100 - number, 1)
                    elif percentage and not target_low:  # effective
                        err_dictionary[col_label] = round(number, 1)
                    elif not percentage and number > -1:
                        err_dictionary[col_label] = int(number)
        else:
            pass  # do nothing; do not want to document the column

    # adding all the tables; maintaining consistency for versatility
    for table in mandatory_tables:
        if table not in err_dictionary.keys():
            err_dictionary[table] = float('NaN')

    return err_dictionary


def find_hpo_row(sheet, hpo, sheet_name, selective_rows,
                 analytics_type):
    """
    Finds the row index of a particular HPO site within
    a larger sheet.

    :param
    sheet (dataframe): dataframe with all of the data quality
        metrics for the sites.
    hpo (string): represents the HPO site whose row in
        the particular sheet needs to be determined
    sheet_name (string): name of the file from which the
        particular sheet of user_command type was extracted
    selective_rows (list): list of rows (potentially HPO sites)
        that are in some (but not all) of the sheets used in the
        analysis report
    analytics_type (str): the data quality metric the user wants to
        investigate

    :return:
    row_num (int): row number where the HPO site of question
        lies within the sheet. returns none if the row is not
        in the sheet in question but exists in other sheets
    """
    hpo_column_name = 'src_hpo_id'
    sheet_hpo_col = sheet[hpo_column_name]

    row_num = 9999

    for idx, site_id in enumerate(sheet_hpo_col):

        row_not_in_sheet = ((hpo in selective_rows) and
                            (hpo not in sheet_hpo_col))

        if hpo == site_id:
            row_num = idx
        elif row_not_in_sheet:
            return None

    if row_num == 9999:  # just in case
        raise NameError("{} not found in the {} sheet "
                        "from {}".format(
            hpo, analytics_type, sheet_name))

    return row_num


def iterate_sheets(dataframes, hpo_id_list, selective_rows,
                   percent, analytics_type, file_names):
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
    selective_rows (list): list of rows (potentially HPO
        sites) that are in some (but not all) of the sheets
        used in the analysis report
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

    mandatory_tables = get_comprehensive_tables(
        dataframes, analytics_type)

    for number, sheet in enumerate(dataframes):  # for each date
        num_chars_to_chop = 5  # get rid of .xlsx
        sheet_name = file_names[number]

        sheet_name = sheet_name[:-num_chars_to_chop]
        errors_for_date = {}  # key:value will be hpo:dict

        for hpo in hpo_id_list:  # for each HPO
            hpo_row_idx = find_hpo_row(
                sheet, hpo, sheet_name,
                selective_rows, analytics_type)

            if hpo == 'aggregate counts':  # will be added later
                pass
            else:
                err_dict_for_hpo = get_info(
                    sheet, hpo_row_idx, percent,
                    sheet_name, mandatory_tables)

                errors_for_date[hpo] = err_dict_for_hpo

        # error information for all of the dates
        dates_and_info[sheet_name] = errors_for_date

    return dates_and_info, mandatory_tables


def generate_hpo_id_col(dataframes):
    """
    Function is used to distinguish between row labels that
    are in all of the data analysis outputs versus row labels
    that are only in some of the data analysis outputs.

    :param
    dataframes (list): list of dataframes that were loaded
        from the analytics files in the path
    :return:
    hpo_id_col (list): list of the strings that should go
        into an HPO ID column. for use in generating subsequent
        dataframes.
    selective_rows (list): list of the strings that are in
        the HPO ID columns for some but not all of the sheets.
        useful down the line when detecting an HPO's row.
    """
    hpo_col_name = 'src_hpo_id'
    selective_rows, total_hpo_id_columns = [], []

    for df_num, df in enumerate(dataframes):
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

    return hpo_id_col, selective_rows


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
    sorted_tables (lits): names of all the table types
        in alphabetical order
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

    # earlier code ensured all sheets have same rows - can just take first
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
        date_metric = 0
        for site in sorted_names:
            table_metrics = date_report[site]
            date_metric, num_iterated = 0, 0

            for table in table_metrics.keys():
                stat = table_metrics[table]
                if not math.isnan(stat):
                    date_metric += stat
                    num_iterated += 1

        if percentage and num_iterated > 0:  # not really used
            date_metric = date_metric / num_iterated
        elif percentage and num_iterated == 0:
            date_metric = float('NaN')

        date_report['aggregate_info'] = date_metric

    return site_and_date_info


def generate_weighted_average_table_sheet(
        file_names, date, table, new_col_info):
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
    new_col_info (list): shows the proportion of 'poor' records per HPO
        site

    :return:
    total_quality (float): indicates the overall proportion of well
        defined rows with respect to the total number of rows
    Function returns None object when it cannot calculate a weighted
        average
    """

    hpo_total_rows_by_date, valid_cols_tot = generate_hpo_contribution(
        file_names, 'total')

    first_underscore = True
    underscore_idx = 0

    for idx, char in enumerate(table):
        if first_underscore and char == '_':
            underscore_idx = idx
            first_underscore = False

    # FIXME: the analysis script needs to output to source with a consistent
    #  naming convention with respect to the table types
    if not first_underscore:  # no underscore in the table name
        table = table[0:underscore_idx]

    table_tot = table + "_total_row"

    if table_tot in valid_cols_tot:
        site_totals = hpo_total_rows_by_date[date][table_tot]

        total_table_rows_across_all_sites = 0
        total_poor_rows_across_all_sites = 0

        # can only count actual values
        for site_rows, site_err_rate in zip(site_totals, new_col_info):
            if not math.isnan(site_rows) and not math.isnan(site_err_rate):
                total_table_rows_across_all_sites += site_rows
                site_err_rate = site_err_rate / 100  # logged as a percent
                site_poor_rows = site_err_rate * site_rows
                total_poor_rows_across_all_sites += site_poor_rows

        if total_table_rows_across_all_sites > 0:
            total_quality = 100 * round(total_poor_rows_across_all_sites /
                                        total_table_rows_across_all_sites, 3)
        else:  # table only started to appear in later sheets
            return float('NaN')

        return total_quality
    else:  # no row count for table; cannot generate weighted average
        return None


def generate_table_dfs(sorted_names, sorted_tables,
                       ordered_dates_str, site_and_date_info,
                       percentage, file_names):
    """
    Function generates a unique dataframe containing data quality
    metrics. Each dataframe should match to the metrics for a
    particular table type.

    The ROWS of each dataframe should be each HPO site.

    NOTE: This function INTENTIONALLY excludes aggregate
    information generation. This is so it can be generated
    more efficiently down the line.

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

    :return:
    df_dictionary_by_table (dict): dictionary with key:value
        of table type: pandas df with the format described above
    """
    # create a new pandas df for each of the table types
    total_dfs = []
    num_tables = len(sorted_tables)

    for new_sheet in range(num_tables):
        new_df = pd.DataFrame({'hpo_ids': sorted_names})
        total_dfs.append(new_df)

    # df for each table
    for new_sheet_num, table in enumerate(sorted_tables):
        df_in_question = total_dfs[new_sheet_num]

        for date in ordered_dates_str:  # generate the columns
            new_col_info = []
            hpo_site_info = site_and_date_info[date]

            for site in sorted_names:  # add the rows for the column
                if site != 'aggregate_info':
                    hpo_table_info = hpo_site_info[site][table]

                    if not math.isnan(hpo_table_info):
                        new_col_info.append(hpo_table_info)
                    else:
                        new_col_info.append(float('NaN'))

            if not percentage:
                total = 0

                for site_val in new_col_info:
                    if not math.isnan(site_val):
                        total += site_val

                new_col_info.append(total)  # adding aggregate
            else:
                # FIXME: There is no way to actually create a weighted
                #  average with some of the tables whose row counts
                #  do not exist.

                weighted_avg = generate_weighted_average_table_sheet(
                    file_names, date, table, new_col_info)

                if weighted_avg is not None:  # successful calculation
                    new_col_info.append(weighted_avg)
                else:
                    new_col_info.append("N/A")

            df_in_question[date] = new_col_info

    df_dictionary_by_table = dict(zip(sorted_tables, total_dfs))
    return df_dictionary_by_table


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

    selective_rows (list): list of the strings that are in
        the HPO ID columns for some but not all of the sheets
    """

    num_files = len(file_names)
    dataframes = []

    for file_num in range(num_files):
        sheet = pd.read_excel(file_names[file_num], sheet_name)
        dataframes.append(sheet)

    hpo_id_col, selective_rows = generate_hpo_id_col(dataframes)

    return dataframes, hpo_id_col, selective_rows


def get_valid_columns(dataframes, contribution_type, row_sheet_name):
    """
    Function is used to determine the columns to be investigated in
    across all of the site report dataframes. The columns should all
    be the same in the 'source' sheet (otherwise an error is thrown).

    The columns of interest are those that can help elucidate an
    HPO site's relative contribution (total rows, well defined rows,
    etc.)

    :param
    dataframes (lst): list of pandas dataframes loaded from the Excel
        files generated from the analysis reports
    contribution_type (str): string representing the types of columns to
        look at for the dataframe. either can represent the 'total' row
        metrics or the 'error' metrics for a particular column.
    row_sheet_name (str): sheet name within the analytics files that
        show the total number of rows and the number of well defined
        rows

    :return:
    valid_cols (lst): list of the columns that are consistent across all
        of the sheets and relevant to the HPO weighting report needed
    """
    valid_cols = []

    # find the columns you want to investigate
    for df in dataframes:
        for column in df:
            if contribution_type == 'total' and len(column) > 9 and \
                    column[-9:] == 'total_row':
                valid_cols.append(column)
            elif contribution_type == 'valid' and len(column) > 16 and \
                    column[-16:] == 'well_defined_row':
                valid_cols.append(column)

    valid_cols = list(dict.fromkeys(valid_cols))
    valid_cols.sort()

    return valid_cols


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
        metrics or the 'error' metrics for a particular column.

    :return:
    hpo_contributions_by_date (dictionary): dictionary with the following
    key:value pairings
        a. date:following dictionary
        b. table type: list with the relative contribution of each HPO
            with respect to the number of rows it contributes for the
            particular table type. the sequence of the list follows
            the alphabetical order of all the HPO sites.

    valid_cols (lst): list of the table types to be iterated over
    """
    hpo_contributions_by_date = {}  # key:value will be date:dict
    row_sheet_name = 'concept'

    dataframes, hpo_id_col, selective_rows = load_total_row_sheet(
        file_names, row_sheet_name)

    valid_cols = get_valid_columns(dataframes, contribution_type,
                                   row_sheet_name)

    for number, sheet in enumerate(dataframes):  # for each date
        num_chars_to_chop = 5  # get rid of .xlsx
        date = file_names[number][:-num_chars_to_chop]
        total_per_sheet = {}

        for table_num, table_type in enumerate(valid_cols):  # for each table
            rows_for_table = []

            for hpo in hpo_id_col:  # follows alphabetical order
                hpo_row_idx = find_hpo_row(sheet, hpo, date, selective_rows,
                                           row_sheet_name)

                if hpo == 'aggregate counts':  # will be added later
                    pass
                elif table_type not in sheet:
                    pass
                elif hpo_row_idx is None:
                    rows_for_table.append(float('NaN'))
                else:
                    rows_for_hpo = sheet[table_type][hpo_row_idx]

                    try:  # later sheets put in the number of rows as a str
                        rows_for_hpo = float(rows_for_hpo)
                    except ValueError:
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


"""
This part of the script deals with making sheets where the
    a. SITES are SHEETS
    b. TABLES are ROWS
"""


def generate_site_dfs(sorted_names, sorted_tables,
                      ordered_dates_str, site_and_date_info,
                      percentage, file_names, analytics_type):
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

    :return:
    df_dictionary_by_site (dict): dictionary with key:value
        of table type: pandas df with the format described above
    """
    total_dfs = []
    sorted_tables.append('total')

    # ignoring 'total' and 'aggregate info'
    tables_only, hpo_names_only = sorted_tables[:-1], sorted_names[:-1]

    # df generation for each HPO and 'total'
    for new_sheet in range(len(hpo_names_only)):
        new_df = pd.DataFrame({'table_type': sorted_tables})
        total_dfs.append(new_df)

    for new_sheet_num, site in enumerate(hpo_names_only):
        df_in_question = total_dfs[new_sheet_num]

        for date in ordered_dates_str:
            tot_errs_date = 0
            new_col_info = []
            site_info = site_and_date_info[date][site]

            for table in tables_only:  # populating the row
                new_col_info.append(site_info[table])
                tot_errs_date += site_info[table]

            # creating the 'aggregate' statistic
            if not percentage:
                new_col_info.append(tot_errs_date)
            else:
                weighted_errs = determine_overall_percent_of_an_hpo(
                    site_and_date_info, file_names, sorted_names,
                    date, site)

                weighted_succ = 100 - weighted_errs

                new_col_info.append(weighted_succ)

            df_in_question[date] = new_col_info

    # make the aggregate sheet
    aggregate_dfs = generate_aggregate_sheets(
        file_names, tables_only, site_and_date_info,
        ordered_dates_str, percentage, analytics_type,
        sorted_names)

    total_dfs.extend(aggregate_dfs)

    if len(aggregate_dfs) == 3:
        sorted_names.extend(['poorly_defined_rows_total',
                             'total_rows'])

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
    weighted.

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


def generate_aggregate_data_completeness_sheet(
        file_names, ordered_dates_str):
    """
    Function is used to generate two 'aggregate data' sheets that
    show the data quality for various tables across the various
    dates. The two sheets generated show:
        a. the total number of 'poorly defined' rows for each of the
           table types

        b. the relative proportion by which each table contributes to
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
    # need to generate aggregate df before any dates
    hpo_total_rows_by_date, valid_cols_tot = generate_hpo_contribution(
        file_names, 'total')

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
        errs_by_table, total_by_table = [], []  # parallel the alphabetical order of tables

        for table_tot, table_valid in zip(valid_cols_tot[:-1], valid_cols_val):
            # these are lists, each idx represents a site
            site_totals = hpo_total_rows_by_date[date][table_tot]
            site_valids = hpo_errors_by_date[date][table_valid]

            site_totals = [x if isinstance(x, float) and not
            math.isnan(x) else 0 for x in site_totals]
            site_valids = [x if isinstance(x, float) and not
            math.isnan(x) else 0 for x in site_valids]

            site_total_for_table = sum(site_totals)
            total_rows += site_total_for_table

            # TODO: Might want to clean up the multiple if/else statements
            # NOTE: if/else to avoid problems with adding NaN but also not have 0 vals

            if site_total_for_table > 0:
                total_by_table.append(site_total_for_table)
            else:
                total_by_table.append(float('NaN'))

            # b/c valids are well_defined rows. sum across all sites
            table_errs = (site_total_for_table - sum(site_valids))
            total_errs += table_errs

            if table_errs > 0:
                errs_by_table.append(table_errs)
            else:
                errs_by_table.append(float('NaN'))

        if total_errs > 0:
            errs_by_table.append(total_errs)
        else:
            errs_by_table.append(float('NaN'))

        if total_rows > 0:
            total_by_table.append(total_rows)
        else:
            total_by_table.append(float('NaN'))

        aggregate_df_total[date] = total_by_table  # list the parallels alphabetical order
        aggregate_df_error[date] = errs_by_table
        # now all as a proportion

        succ_rate_by_table = []
        for errors, total in zip(errs_by_table, total_by_table):
            error_rate = round(errors / total * 100, 2)
            success_rate = 100 - error_rate
            succ_rate_by_table.append(success_rate)

        aggregate_df_proportion[date] = succ_rate_by_table
    return total_dfs


def determine_overall_percent_of_an_hpo(
        site_and_date_info, file_names, sorted_names,
        date, site):
    """
    This function is used to determine the aggregate 'percent'
    metric. This is calculated by giving more weight to categories
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

    incidence_for_site = site_and_date_info[date]

    tot_rows_for_date, tot_errors_for_date = 0, 0

    # FIXME: Figure out how to 'kick out' of the for loop when you
    #  correctly identify the site

    # this is iterated first b/c total rows per site is a list that parallels the site names
    for table, table_row_total in zip(sorted_tables[:-1], valid_cols_tot):
        total_rows_per_site = hpo_total_rows_by_date[date][table_row_total]

        for site_name, site_rows in zip(sorted_names, total_rows_per_site):
            if site == site_name and not math.isnan(site_rows):  # found the site in question
                site_succ_rate = incidence_for_site[site][table]
                if not math.isnan(site_rows) and not math.isnan(site_succ_rate):
                    tot_rows_for_date += site_rows
                    site_err_rate = (100 - site_succ_rate) / 100

                    tot_table_errs_for_date = site_err_rate * site_rows
                    tot_errors_for_date += tot_table_errs_for_date

    if tot_rows_for_date > 0:
        total_err_rate = tot_errors_for_date / tot_rows_for_date * 100
        total_err_rate = round(total_err_rate, 2)
    else:  # new site; no rows for first date(s)
        total_err_rate = float('NaN')

    return total_err_rate


def determine_weighted_average_of_percent(
        site_and_date_info, ordered_dates_str,
        sorted_tables, file_names, sorted_names):
    """
    This function determines the overall data quality for each
    of the table types for a particular date. This function ensures
    that the relative contribution of each HPO site is factored in
    and thus gives us a better idea of the 'overall health' of a
    table.

    :param
    site_and_date_info (dict): dictionary with key:value
        of date:additional dictionaries that contain metrics
        for each HPO's data quality by type
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
        metrics for the data quality for each table. put in a list
        for consistency with how it will be added with respect to
        the other generated dataframes.
    """

    hpo_total_rows_by_date, valid_cols_tot = generate_hpo_contribution(
        file_names, 'total')

    sorted_tables.append('total')  # use relevant tables

    # NOTE: sorted tables should have all tables that ever appear on
    # the relevant sheet
    aggregate_df_weighted = pd.DataFrame({'table_type': sorted_tables})

    total_rows = valid_cols_tot
    total_rows.sort()

    for date in ordered_dates_str:
        error_amounts_for_date = []
        incidence_for_site = site_and_date_info[date]
        tot_rows_for_date, tot_errors_for_date = 0, 0

        # do not take the 'total' for the sorted_tables
        for table, table_row_tot in zip(sorted_tables[:-1], total_rows):
            tot_errors_for_table, tot_rows_for_table = 0, 0
            total_rows_per_site = hpo_total_rows_by_date[date][table_row_tot]

            for site, site_rows in zip(sorted_names[:-1], total_rows_per_site):
                site_err_rate = incidence_for_site[site][table]

                # FIXME: Most sites have a NaN error rate; cannot calculate
                #  a meaningful total error rate
                if not math.isnan(site_err_rate) and not math.isnan(site_rows):
                    site_err_rate = site_err_rate / 100  # convert from percent
                    site_err_tot = site_err_rate * site_rows

                    tot_rows_for_table += site_rows
                    tot_rows_for_date += site_rows

                    tot_errors_for_table += site_err_tot
                    tot_errors_for_date += site_err_tot
            # now all the error rates for each table type
            if tot_rows_for_table > 0:
                err_rate_table = tot_errors_for_table / tot_rows_for_table
                err_rate_table = round(err_rate_table, 3)
                error_amounts_for_date.append(err_rate_table)
            else:
                error_amounts_for_date.append(float('NaN'))

        if tot_rows_for_date > 0:
            tot_errors_for_date = tot_errors_for_date / tot_rows_for_date
            tot_errors_for_date = round(tot_errors_for_date, 3)
            # overall error rate for the date
            error_amounts_for_date.append(tot_errors_for_date)
        else:
            error_amounts_for_date.append(float('NaN'))

        aggregate_df_weighted[date] = error_amounts_for_date

    return [aggregate_df_weighted]


def generate_aggregate_sheets(file_names, sorted_tables, site_and_date_info,
                              ordered_dates_str, percentage, analytics_type,
                              sorted_names):
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
    :return:
    total_dfs (lst): list of pandas dataframes that document the
        data quality of the various data types
    """
    if analytics_type in ['source', 'concept']:
        total_dfs = generate_aggregate_data_completeness_sheet(
            file_names, ordered_dates_str)
    elif percentage:
        total_dfs = determine_weighted_average_of_percent(
            site_and_date_info, ordered_dates_str, sorted_tables,
            file_names, sorted_names)
    else:  # just really summing the numbers
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
    output_choice (string): determines which variable (either
        distinct site or distince talbe) will serve as a
        'anchor' to separate out the sheets
    """
    output_prompt = "\nWould you prefer to generate: \n" \
                    "A. {} sheets detailing the data quality for " \
                    "each table. The HPO IDs would be displayed " \
                    "as rows. \nor \n" \
                    "B. {} sheets detailing the data quality for " \
                    "each HPO site. The table type would be " \
                    "displayed as rows. This will also include 1-3 " \
                    "table(s) with statistics on the aggregate data " \
                    "for each table type on each date.".format(
        len(sorted_tables), len(sorted_names))

    user_input = input(output_prompt).lower()
    output_choice_dict = {'a': 'table_sheets',
                          'b': 'hpo_sheets'}

    while user_input not in output_choice_dict.keys():
        print("\nInvalid choice. Please specify a letter that corresponds "
              "to an appropriate output type.\n")
        user_input = input(output_prompt).lower()

    output_choice = output_choice_dict[user_input]

    return output_choice


cwd = os.getcwd()

# NOTE: This is hard-coded in rather than asking the user to specify the
# file names. This can be modified in future iterations if needed.
report1 = 'july_15_2019.xlsx'
report2 = 'july_23_2019.xlsx'
report3 = 'august_05_2019.xlsx'
report4 = 'august_13_2019.xlsx'
report5 = 'august_19_2019.xlsx'
# report1 = 'march_27_2019.xlsx'  # CDR releases
# report2 = 'may_10_2019.xlsx'
# report3 = 'august_02_2019.xlsx'

report_names = [report1, report2, report3, report4, report5]

analytics_choice, percent_bool, target_low = get_user_analysis_choice()

data_frames = load_files(analytics_choice, report_names)

# creating a consistent row/column label set
hpo_id_col, selective_rows = generate_hpo_id_col(data_frames)

# creating an organized dictionary with all of the information easily-indexed
site_and_date_info, all_tables = iterate_sheets(
    data_frames, hpo_id_col, selective_rows, percent_bool,
    analytics_choice, report_names)

# creating a consistent means to navigate the tables
ordered_dates, sorted_names, sorted_tables = sort_names_and_tables(
    site_and_date_info, all_tables)

# adding new aggregate metrics for each date; sum or average of HPO sites
site_and_date_info = add_aggregate_info(
    site_and_date_info, percent_bool, hpo_id_col)

# understanding which variable will distinguish new sheets
user_output_choice = understand_sheet_output_type(sorted_tables,
                                                  sorted_names)

# creating dataframes with site/table/date metrics
if user_output_choice == 'table_sheets':
    df_dict = generate_table_dfs(
        sorted_names, sorted_tables, ordered_dates,
        site_and_date_info, percent_bool, report_names)
elif user_output_choice == 'hpo_sheets':
    df_dict = generate_site_dfs(
        sorted_names, sorted_tables, ordered_dates,
        site_and_date_info, percent_bool, report_names,
        analytics_choice)

file_name = analytics_choice + "_" + user_output_choice + \
            "_data_analytics.xlsx"

writer = pd.ExcelWriter(file_name, engine='xlsxwriter')

for df_name, df in df_dict.items():
    df.to_excel(writer, sheet_name=df_name)

writer.save()
