"""
File is intended to store a number of functions that are
used to create the DataQualityMetric objects throughtout
the script.
"""
import pandas as pd


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


def get_info(
        sheet, row_num, percentage,
        sheet_name, columns_to_collect, target_low):
    """
    Function is used to create a dictionary that contains
    the number of flawed records for a particular site.

    Parameters
    ----------
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

    columns_to_collect (lst): contains the tables that should be
        documented for every table and at every date.

    target_low (bool): determines whether the number displayed
        should be considered a positive or negative metric

    Returns
    -------
    data_dictionary (dictionary): key:value pairs represent the
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

    data_dictionary = {}

    for col_label, number in data_info.iteritems():
        if col_label in columns_to_collect:

            # data for table for site does not exist
            if number is None or number == 'No Data':
                data_dictionary[col_label] = float('NaN')

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

                    # actual info to be logged if sensible data
                    elif percentage and target_low:  # proportion w/ errors
                        data_dictionary[col_label] = round(100 - number, 2)
                    elif percentage and not target_low:  # effective
                        data_dictionary[col_label] = round(number, 2)
                    elif not percentage and number > -1:
                        data_dictionary[col_label] = int(number)
        else:
            pass  # do nothing; do not want to document the column

    # adding all the tables for the HPO; maintaining consistency across all
    # HPOs for consistency and versatility
    for table in data_dictionary:
        if table not in data_dictionary.keys():
            data_dictionary[table] = float('NaN')

    return data_dictionary
