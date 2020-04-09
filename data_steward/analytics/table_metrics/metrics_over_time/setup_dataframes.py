"""
Python file defines sets of functions that will be used
to set up the skeleton of the dataframes to be created.

This 'skeleton' will consist of columns that are
strings representing datetime objects.

The rows of the dataframes will be as follows:

    if the user selects 'HPO dataframes':
        the tables or classes
        each dataframe represents an HPO

    if the user selects 'table dataframes':
        the HPOs
        each dataframe represents a table or class
"""
import pandas as pd
from auxillary_aggregate_functions import find_relevant_tables_or_classes


def create_dataframe_skeletons(
        sheet_output, metric_dictionary, datetimes, hpo_names):
    """
    Function is used to create the 'skeletons' of the dataframes
    that will ultimately be produced. These dataframes are
    blank but have the appropriate column and row labels.


    Parameters
    ----------
    sheet_output (string): determines the type of 'output'
        to be generated (e.g. the sheets are HPOs or the
        sheets are tables)

    metric_dictionary (dict): has the following structure
        keys: all of the different metric_types possible
        values: all of the HPO objects that
            have that associated metric_type

    datetimes (list): list of datetime objects that
        represent the dates of the files that are being
        ingested

    hpo_names (list): list of the HPO names that are to be
        put into dataframes (either as the titles of the
        dataframe or the rows of a dataframe)

    Return
    ------
    dataframes_dict (dict): has the following structure
        key: the 'name' of the dataframe; either the name
            of a table/metric or the name of the HPO

        value: the 'skeleton' of the dataframe to be
            created

    tables_or_classes_for_metric (list): list of the
        tables or classes that apply to this particular
        metric
    """

    dts_string, tables_or_classes_for_metric, dataframes_dict = \
        setup_skeleton_function(
            datetimes=datetimes, hpo_names=hpo_names,
            metric_dictionary=metric_dictionary)

    if sheet_output == 'table_sheets':
        for table_or_class_name in tables_or_classes_for_metric:
            df = pd.DataFrame(
                index=hpo_names, columns=dts_string)

            dataframes_dict[table_or_class_name] = df

    elif sheet_output == 'hpo_sheets':

        # for the bottom of the dataframe
        tables_or_classes_for_metric.append(
            'aggregate_info')

        for hpo_name in hpo_names:

            df = pd.DataFrame(
                index=tables_or_classes_for_metric,
                columns=dts_string)

            dataframes_dict[hpo_name] = df

    else:
        raise Exception(
            """Bad parameter input for function
             organize_dataframes_master_function. Parameter provided
            was: {param}""".format(param=sheet_output))

    return dataframes_dict, tables_or_classes_for_metric


def setup_skeleton_function(
        datetimes, hpo_names, metric_dictionary):
    """
    Function is used to 'setup' the variables that will
    ultimately be used in the create_dataframe_skeletons
    function.

    Parameters
    ----------
    metric_dictionary (dict): has the following structure
        keys: all of the different metric_types possible
        values: all of the HPO objects that
            have that associated metric_type

    datetimes (list): list of datetime objects that
        represent the dates of the files that are being
        ingested

    hpo_names (list): list of the HPO names that are to be
        put into dataframes (either as the titles of the
        dataframe or the rows of a dataframe)

    Returns
    -------
    dts_string (list): list of the string objects, now
        converted into strings that can be used as
        column headers for each of the dataframes

    tables_or_classes_for_metric (list): list of the
        tables or classes that apply to this particular
        metric

    dataframes_dict (dict): blank dictionary; will ultimately
        be populated
    """
    dts_string = [
        date_obj.strftime('%m/%d/%Y') for date_obj in datetimes]

    # for final row or ultimate dataframe
    hpo_names.append('aggregate_info')
    tables_or_classes_for_metric = []  # start blank

    dataframes_dict = {}

    for metric, hpo_objs in metric_dictionary.items():

        if len(hpo_objs) > 0:  # relevant_metric
            tables_or_classes_for_metric = \
                find_relevant_tables_or_classes(
                    hpo_object_list=hpo_objs,
                    metric_type=metric)

    # double check that the tables exist
    assert tables_or_classes_for_metric, \
        "No HPO objects found for the provided metric"

    return dts_string, tables_or_classes_for_metric, dataframes_dict
