"""
File is intended to house the functions that are ultimately
used to create DataQualityMetric objects.
"""

from dictionaries_and_lists import \
    metric_type_to_english_dict, data_quality_dimension_dict, \
    columns_to_document_for_sheet_email, table_based_on_column_provided

from functions_to_create_dqm_objects import find_hpo_row, \
    get_info

from data_quality_metric_class import DataQualityMetric


def create_dqm_objects_for_sheet(
        dataframe, hpo_names, user_choice, metric_is_percent,
        target_low, date):
    """
    Function is used to create DataQualityMetric objects for all of
    the pertinent values on the various sheets being loaded.

    Parameters
    ---------
    dataframe (df): contains the information for a particular dimension
        of data quality on a particular date

    hpo_names (list): list of the strings that should go
        into an HPO ID column. for use in generating HPO objects.

    user_choice (string): represents the sheet from the analysis reports
        whose metrics will be compared over time

    metric_is_percent (bool): determines whether the data will be seen
        as 'percentage complete' or individual instances of a
        particular error

    target_low (bool): determines whether the number displayed should
        be considered a desirable or undesirable characteristic

    date (datetime): datetime object that represents the time that the
        data quality metric was documented (corresponding to the
        title of the file from which it was extracted)

    Returns
    -------
    dqm_objects (list): list of DataQualityMetrics objects
        these are objects that all should have the same
        metric_type, data_quality_dimension, and date attributes

    columns (list): the column names that whose data will be extracted.
        these will eventually be converted to either the rows of
        dataframes or the names of the different dataframes to be
        output.
    """
    # to instantiate dqm objects later on
    metric_type = metric_type_to_english_dict[user_choice]
    dqm_type = data_quality_dimension_dict[user_choice]
    columns = columns_to_document_for_sheet_email[user_choice]

    dqm_objects = []

    # for each HPO (row) in the dataframe
    for name in hpo_names:
        row_number = find_hpo_row(sheet=dataframe, hpo=name)

        data_dict = get_info(
            sheet=dataframe, row_num=row_number,
            percentage=metric_is_percent, sheet_name=user_choice,
            columns_to_collect=columns,
            target_low=target_low)

        # for each table / class (column) in the dataframe
        for table, data in data_dict.items():
            table_or_class_name = table_based_on_column_provided[table]

            new_dqm_object = DataQualityMetric(
                hpo=name, table_or_class=table_or_class_name,
                metric_type=metric_type,
                value=data, data_quality_dimension=dqm_type,
                date=date)

            dqm_objects.append(new_dqm_object)

    return dqm_objects, columns


def create_dqm_list(dfs, file_names, datetimes, user_choice,
                    percent_bool, target_low, hpo_names):
    """
    Function is used to create all of the possible 'DataQualityMetric'
    objects that are needed given all of the inputted data.

    Parameters
    ----------
    dfs (list): list of pandas dataframes. each dataframe contains
        info about data quality for all of the sites for a date. each
        index of the list should represent a particular date's metrics.

    file_names (list): list of the user-specified Excel files that are
        in the current directory. Files are analytics reports to be
        scanned.

    datetimes (list): list of datetime objects that
        represent the dates of the files that are being
        ingested

    user_choies (string): represents the sheet from the analysis reports
        whose metrics will be compared over time

    percent_bool (bool): determines whether the data will be seen
        as 'percentage complete' or individual instances of a
        particular error

    target_low (bool): determines whether the number displayed should
        be considered a desirable or undesirable characteristic

    hpo_names (list): list of the strings that should go
        into an HPO ID column. for use in generating subsequent
        dataframes.

    Return
    -------
    dqm_list (lst): list of DataQualityMetric objects
    """
    dqm_list = []

    # creating the DQM objects and assigning to HPOs
    for dataframe, file_name, date in zip(dfs, file_names, datetimes):
        dqm_objects, col_names = create_dqm_objects_for_sheet(
            dataframe=dataframe, hpo_names=hpo_names,
            user_choice=user_choice, metric_is_percent=percent_bool,
            target_low=target_low, date=date)

        dqm_list.extend(dqm_objects)

    return dqm_list
