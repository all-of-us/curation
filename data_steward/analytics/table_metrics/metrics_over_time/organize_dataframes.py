"""
File is used to organize the information stored in:
    - HPO objects
    - DataQualityMetric objects
    - AggregateMetric objects

into pandas dataframes. These dataframes are intended
to display the information for HPOs on the different
data quality metrics based on the input from the
user.
"""

from setup_dataframes import create_dataframe_skeletons

from dictionaries_and_lists import \
    metric_type_to_english_dict, \
    unweighted_metric_already_integrated_for_hpo, \
    no_aggregate_metric_needed_for_table_sheets

from populate_dfs_with_aggregate_info import \
    create_aggregate_info_df, add_aggregate_to_end_of_table_class_df, \
    add_aggregate_to_end_of_hpo_df

import math

def organize_dataframes_master_function(
        sheet_output, metric_dictionary, datetimes, hpo_names,
        metric_choice, hpo_dictionary, aggregate_metrics):
    """
    Function is used to carry out the act of creating the
    dataframes with the data quality metrics that were
    associated with the different HPO objects.

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

    metric_choice (str): the type of analysis that the user
        wishes to perform. used to triage whether the function will
        create a 'weighted' or unweighted' metric

    hpo_dictionary (dict): has the following structure
        keys: all of the different HPO IDs
        values: all of the associated HPO objects that
            have that associated HPO ID

    aggregate_metrics (list): list of metrics objects
        (AggregateMetricForTableOrClass or
        AggregateMetricForHPO & AggregateMetricForDate)
        that contain all of the 'aggregate metrics' to be displayed

    Returns
    -------
    dataframes_dict (dict): has the following structure
        key: the 'name' of the dataframe; the name of
            the table/class or HPO

        value: the dataframe - now populated with the
            data from each HPO and the
            'aggregate metric'
    """

    dataframes_dict, tables_or_classes_for_metric = \
        create_dataframe_skeletons(
            sheet_output=sheet_output,
            metric_dictionary=metric_dictionary,
            datetimes=datetimes,
            hpo_names=hpo_names)

    if sheet_output == 'table_sheets':
        dataframes_dict = populate_table_df_rows(
            datetimes=datetimes,
            hpo_names=hpo_names,
            dataframes_dict=dataframes_dict,
            metric_choice=metric_choice,
            hpo_dictionary=hpo_dictionary,
            aggregate_metrics=aggregate_metrics)

    else:  # hpo_sheets - otherwise error in create_dataframe_skeletons
        dataframes_dict = populate_hpo_df_rows(
            datetimes=datetimes,
            tables_or_classes_for_metric=tables_or_classes_for_metric,
            dataframes_dict=dataframes_dict,
            metric_choice=metric_choice,
            hpo_dictionary=hpo_dictionary,
            aggregate_metrics=aggregate_metrics)

        dataframes_dict = create_aggregate_info_df(
            datetimes=datetimes,
            tables_or_classes_for_metric=tables_or_classes_for_metric,
            dataframes_dict=dataframes_dict,
            metric_choice=metric_choice,
            aggregate_metrics=aggregate_metrics)

    return dataframes_dict


def populate_table_df_rows(
        datetimes,
        hpo_names, dataframes_dict,
        metric_choice, hpo_dictionary,
        aggregate_metrics):
    """
    Function is used to populate each 'table/class'
    dataframe row-by row. Each row of this dataframe
    is a different HPO. Each column of this dataframe
    is a date.

    Parameters
    ----------
    datetimes (list): list of datetime objects that
        represent the dates of the files that are being
        ingested

    hpo_names (list): list of the HPO names that are to be
        put into dataframes (either as the titles of the
        dataframe or the rows of a dataframe)

    dataframes_dict (dict): has the following structure
        key: the 'name' of the dataframe; the name of
            the table/class

        value: the 'skeleton' of the dataframe to be
            created

    metric_choice (str): the type of analysis that the user
        wishes to perform.

    hpo_dictionary (dict): has the following structure
        keys: all of the different HPO IDs
        values: all of the associated HPO objects that
            have that associated HPO ID

    aggregate_metrics (list): list of metrics objects
        (AggregateMetricForTableOrClass)
        that contain all of the 'aggregate metrics' to be displayed

    Returns
    -------
    dataframes_dict (dict): has the following structure
        key: the 'name' of the dataframe; the name of
            the table/class

        value: the dataframe - now populated for each HPO
            (including the 'aggregate_metric' row) and
            each date
    """
    metric_choice_eng = metric_type_to_english_dict[metric_choice]

    # for each dataframe
    for table_class_name, df in dataframes_dict.items():

        # row by row - exclude aggregate_info
        for hpo in hpo_names[:-1]:
            row_to_place = []
            relevant_hpos = hpo_dictionary[hpo]

            # column by column
            for date in datetimes:

                for relevant_hpo_object in relevant_hpos:
                    relevant_dqms = \
                        relevant_hpo_object.use_string_to_get_relevant_objects(
                            metric=metric_choice_eng)

                    for dqm in relevant_dqms:
                        if dqm.date == date and \
                           dqm.table_or_class == table_class_name:
                            value = dqm.value

                            if math.isnan(value):
                                row_to_place.append(0)
                            else:
                                row_to_place.append(dqm.value)

            df.loc[hpo] = row_to_place


        df = add_aggregate_to_end_of_table_class_df(
            datetimes=datetimes,
            aggregate_metrics=aggregate_metrics,
            table_class_name=table_class_name,
            metric_choice=metric_choice_eng, df=df)

        # replace
        dataframes_dict[table_class_name] = df

    return dataframes_dict


def populate_hpo_df_rows(
        datetimes,
        tables_or_classes_for_metric, dataframes_dict,
        metric_choice, hpo_dictionary,
        aggregate_metrics):
    """
    Function is used to populate each 'table/class'
    dataframe row-by row. Each row of this dataframe
    is a different HPO. Each column of this dataframe
    is a date.

    Parameters
    ----------
    datetimes (list): list of datetime objects that
        represent the dates of the files that are being
        ingested

    tables_or_classes_for_metric (list): list of the
        table/class names that are to be
        put into dataframes (as the rows of a dataframe)

    dataframes_dict (dict): has the following structure
        key: the 'name' of the dataframe; the name of
            the table/class

        value: the 'skeleton' of the dataframe to be
            created

    metric_choice (str): the type of analysis that the user
        wishes to perform. used to triage whether the function
        will create a 'weighted' or unweighted' metric

    hpo_dictionary (dict): has the following structure
        keys: all of the different HPO IDs
        values: all of the associated HPO objects that
            have that associated HPO ID

    aggregate_metrics (list): list of metrics objects
        (AggregateMetricForHPO and AggregateMetricForDate)
        that contain all of the 'aggregate metrics' to be
        displayed

    Returns
    -------
    dataframes_dict (dict): has the following structure
        key: the 'name' of the dataframe; the name of
            the HPO

        value: the dataframe - now populated for each
            table/class (including the 'aggregate_metric' row)
            and each date
    """
    metric_choice_eng = metric_type_to_english_dict[metric_choice]

    # get key, values to iterate over - exclude aggregate_info
    hpo_names = list(dataframes_dict.keys())[:-1]
    dfs = list(dataframes_dict.values())[:-1]

    # for each dataframe
    for hpo_name, df in zip(hpo_names, dfs):

        hpo_objects = hpo_dictionary[hpo_name]

        # row by row - exclude aggregate_info
        for table_or_class in tables_or_classes_for_metric[:-1]:
            row_to_place = []

            # column by column
            for date in datetimes:

                for relevant_hpo_object in hpo_objects:
                    relevant_dqms = \
                        relevant_hpo_object.use_string_to_get_relevant_objects(
                            metric=metric_choice_eng)

                    for dqm in relevant_dqms:
                        if dqm.date == date and \
                           dqm.table_or_class == table_or_class:
                            row_to_place.append(dqm.value)

            df.loc[table_or_class] = row_to_place

        # need to add the 'aggregate metric'
        if metric_choice not in \
                unweighted_metric_already_integrated_for_hpo\
                and metric_choice not in \
                no_aggregate_metric_needed_for_table_sheets:
            df = add_aggregate_to_end_of_hpo_df(
                datetimes=datetimes,
                aggregate_metrics=aggregate_metrics,
                hpo_id=hpo_name, metric_choice=metric_choice_eng,
                df=df)
        else:
            # no need - already logged
            df = df.drop('aggregate_info')

        # replace
        dataframes_dict[hpo_name] = df

    return dataframes_dict
