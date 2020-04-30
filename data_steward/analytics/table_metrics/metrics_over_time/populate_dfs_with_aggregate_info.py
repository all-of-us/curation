"""
Function is used to populate all of the dataframes with
'aggregate information'.

In the case of the 'table' dataframes, the aggregate information
goes at the bottom of each date column.

In the case of the 'HPO' dataframes, the aggregate information:
- sometimes is embedded in the middle of the rows
- is sometimes calculated separately an appended to the bottom
  of the HPO dataframe
- has its own, separate dataframe that contains 'aggregate
  information' that spans all of the sites.

This was kept separately from the organize_dataframes file
in order to increase readability of the file.
"""

from aggregate_metric_classes import \
    AggregateMetricForHPO, AggregateMetricForDate,\
    AggregateMetricForTableOrClass

from dictionaries_and_lists import \
    metric_type_to_english_dict, \
    unweighted_metric_already_integrated_for_hpo, \
    no_aggregate_metric_needed_for_table_sheets

from messages import err_message_agg_for_table, \
    err_message_agg_for_date, \
    err_message_agg_for_hpo


def add_aggregate_to_end_of_table_class_df(
        datetimes, aggregate_metrics, table_class_name,
        metric_choice, df):
    """
    Function is used to add the 'aggregate metrics'
    to the bottom of a dataframe where:
        a. the HPOs are rows
        b. dates are columns
        c. the title of the df is the particular
            table/class being investigated

    Parameters
    ----------
    datetimes (list): list of datetime objects that
        represent the dates of the files that are being
        ingested

    aggregate_metrics (list): list of metrics objects
        (AggregateMetricForTableOrClass)
        that contain all of the 'aggregate metrics' to
        be displayed

    table_class_name (string): the table or the class
        whose 'dataframe' is being generated

    metric_choice (str): the type of analysis that the user
        wishes to perform. used to triage whether the function will
        create a 'weighted' or unweighted' metric

    df (df): dataframe that has all of the metrics for the
        HPOs generated and populated with the exception
        of the 'aggregate' metric

    Returns
    -------
    df (df): the dataframe provided but now with the 'aggregate'
        metric placed at the bottom of the dataframe for each
        column
    """
    row_to_place = []

    for date in datetimes:
        agg_metric_found = False

        for aggregate_metric in aggregate_metrics:

            # what means that it is the right metric for the
            # particular row/column combo in the dataframe
            if (aggregate_metric.date == date) and \
                (aggregate_metric.table_or_class_name ==
                    table_class_name) and \
                    (aggregate_metric.metric_type == metric_choice):

                agg_metric_found = True

                # duplicates - want the total number of records
                if metric_choice == 'Duplicate Records':
                    aggregate_rate = aggregate_metric.num_pertinent_rows
                else:
                    aggregate_rate = aggregate_metric.overall_rate

                row_to_place.append(aggregate_rate)

        assert agg_metric_found, \
            err_message_agg_for_table.format(
                date=date, table_class_name=table_class_name,
                metric_type=metric_choice)

    df.loc['aggregate_info'] = row_to_place

    return df


def add_aggregate_to_end_of_hpo_df(
        datetimes, aggregate_metrics, hpo_id,
        metric_choice, df):
    """
    Function is used to add the 'aggregate metrics'
    to the bottom of a dataframe where:
        a. the tables/classes are rows
        b. dates are columns
        c. the title of the df is the particular
            HPO being investigated

    Parameters
    ----------
    datetimes (list): list of datetime objects that
        represent the dates of the files that are being
        ingested

    aggregate_metrics (list): list of metrics objects
        (AggregateMetricForHPO)
        that contain all of the 'aggregate metrics' to
        be displayed

    hpo_id (string): the ID of the HPO whose dataframe
        is being generated

    metric_choice (str): the type of analysis that the user
        wishes to perform. used to triage whether the function will
        create a 'weighted' or unweighted' metric

    df (df): dataframe that has all of the metrics for HPO
        generated and populated with the exception
        of the 'aggregate' metric

    Returns
    -------
    df (df): the dataframe provided but now with the 'aggregate'
        metric placed at the bottom of the dataframe for each
        column
    """
    row_to_place = []

    for date in datetimes:
        agg_metric_found = False

        for aggregate_metric in aggregate_metrics:

            # what means that it is the right metric for the
            # particular row/column combo in the dataframe
            if isinstance(
                    aggregate_metric, AggregateMetricForHPO) and \
                (aggregate_metric.date == date) and \
                (aggregate_metric.hpo_name ==
                    hpo_id) and \
                    (aggregate_metric.metric_type == metric_choice):

                agg_metric_found = True

                # duplicates - want the total number of records
                if metric_choice == 'Duplicate Records':
                    aggregate_rate = aggregate_metric.num_pertinent_rows
                else:
                    aggregate_rate = aggregate_metric.overall_rate

                # populating index-by-index
                row_to_place.append(aggregate_rate)

        assert agg_metric_found, \
            err_message_agg_for_hpo.format(
                date=date, hpo_name=hpo_id,
                metric_type=metric_choice)

    df.loc['aggregate_info'] = row_to_place

    return df


def create_aggregate_info_df(
        datetimes, tables_or_classes_for_metric,
        dataframes_dict, metric_choice,
        aggregate_metrics):
    """
    Function is designed to create an ultimate 'aggregate metrics'
    dataframe. This dataframe is to be generated when
    the user selects the 'HPO output' option at the beginning of
    the script.

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
        wishes to perform.

    aggregate_metrics (list): list of metrics objects
        (AggregateMetricForTableOrClass)
        that contain all of the 'aggregate metrics' to be
        displayed

    Returns
    -------
    dataframes_dict (dict): has the following structure
    key: the 'name' of the dataframe; the name of
        the table/class

    value: the 'skeleton' of the dataframe to be
        created

    now has the aggregate information dataframe
    """
    df_of_interest = dataframes_dict['aggregate_info']

    metric_choice_eng = metric_type_to_english_dict[metric_choice]

    # will be added later - not an aggregate_metric object
    if 'aggregate_info' in tables_or_classes_for_metric:
        tables_or_classes_for_metric.remove('aggregate_info')

    # each row
    for table_or_class in tables_or_classes_for_metric:
        new_row = []

        # each column
        for date in datetimes:
            agg_metric_found = False

            for aggregate_metric in aggregate_metrics:

                # what means that it is the right metric for the
                # particular row/column combo in the dataframe
                if isinstance(
                        aggregate_metric, AggregateMetricForTableOrClass) \
                        and \
                        (aggregate_metric.date == date) \
                        and \
                        (aggregate_metric.metric_type ==
                         metric_choice_eng) \
                        and \
                        (aggregate_metric.table_or_class_name == table_or_class):

                    agg_metric_found = True

                    # duplicates - want the total number of records
                    if metric_choice_eng == 'Duplicate Records':
                        aggregate_rate = aggregate_metric.num_pertinent_rows
                    else:
                        aggregate_rate = aggregate_metric.overall_rate

                    # populating index-by-index
                    new_row.append(aggregate_rate)

            assert agg_metric_found, \
                err_message_agg_for_table.format(
                    date=date, metric_type=metric_choice_eng,
                    table_or_class=table_or_class)

        df_of_interest.loc[table_or_class] = new_row

    # need to separately calculate the aggregate metric
    # for the day
    if metric_choice not in \
            unweighted_metric_already_integrated_for_hpo\
            and metric_choice not in \
            no_aggregate_metric_needed_for_table_sheets:
        final_row = make_aggregate_row_for_aggregate_df(
            datetimes=datetimes, metric_choice=metric_choice_eng,
            aggregate_metrics=aggregate_metrics)

        df_of_interest.loc['aggregate_info'] = final_row
    else:
        # no need - already logged
        df_of_interest = df_of_interest.drop('aggregate_info')

    # resetting accordingly
    dataframes_dict['aggregate_info'] = df_of_interest

    return dataframes_dict


def make_aggregate_row_for_aggregate_df(
        datetimes, metric_choice,
        aggregate_metrics):
    """
    Function is designed to create the ultimate/final
    row of the 'aggregate information' dataframe. This
    row should span all of the tables and HPOs for a
    particular date/metric type combination (e.g. the
    total number of duplicates across all tables
    for a particular date).


    Parameters
    ----------
    datetimes (list): list of datetime objects that
        represent the dates of the files that are being
        ingested

    metric_choice (str): the type of analysis that the user
        wishes to perform.

    aggregate_metrics (list): list of metrics objects
        (AggregateMetricForTableOrClass)
        that contain all of the 'aggregate metrics' to be
        displayed

    Return
    ------
    row (list): contains all of the relevant information from
        the AggregateMetricForDate
    """

    row = []

    # each column
    for date in datetimes:
        agg_metric_found = False

        for aggregate_metric in aggregate_metrics:

            # what means that it is the right metric for the
            # particular row/column combo in the dataframe
            # note: not checking table/class
            if isinstance(
                aggregate_metric, AggregateMetricForDate) and \
                (aggregate_metric.date == date) and \
                    (aggregate_metric.metric_type == metric_choice):

                agg_metric_found = True

                # duplicates - want the total number of records
                if metric_choice == 'Duplicate Records':
                    aggregate_rate = aggregate_metric.num_pertinent_rows
                else:
                    aggregate_rate = aggregate_metric.overall_rate

                # populating index-by-index
                row.append(aggregate_rate)

        # want to ensure every date has a value
        assert agg_metric_found, \
            err_message_agg_for_date.format(
                date=date, metric_type=metric_choice)

    return row
