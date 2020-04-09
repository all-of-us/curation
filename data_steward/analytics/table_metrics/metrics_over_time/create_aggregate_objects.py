"""
These series of functions are used in the creation of
AggregateMetric objects. These are similar to the regular
DataQualityMetric objects except they contain additional
information to better convey the scale in terms of the
number of rows available across all of the different
sites.

Please see the AggregateMetric class documentation for
further information.
"""

from unweighted_aggregate_metric_functions import \
    create_unweighted_aggregate_metrics_for_tables, \
    create_unweighted_aggregate_metrics_for_hpos, \
    create_unweighted_aggregate_metric_for_dates

from weighted_aggregate_metric_functions import \
    create_weighted_aggregate_metric_for_dates,\
    create_weighted_aggregate_metrics_for_hpos, \
    create_weighted_aggregate_metrics_for_tables

from dictionaries_and_lists import \
    metrics_to_weight, \
    unweighted_metric_already_integrated_for_hpo, \
    aggregate_metric_class_names


def create_aggregate_metric_master_function(
        metric_dictionary, hpo_dictionary,
        sheet_output, datetimes, metric_choice):
    """
    Function is used to identify which type of AggregateMetric
    object to make. The type of AggregateMetric object to create
    is contingent upon the user_choice parameter. See the
    aggregate_metric_classes.py documentation on the attributes
    of the objects to be instantiated.

    Parameters
    ---------
    metric_dictionary (dict): has the following structure
        keys: all of the different metric_types possible
        values: all of the HPO objects that
            have that associated metric_type

    hpo_dictionary (dict): has the following structure
        keys: all of the different HPO IDs
        values: all of the associated HPO objects that
            have that associated HPO ID

    sheet_output (string): determines the type of 'output'
        to be generated (e.g. the sheets are HPOs or the
        sheets are tables)

    datetimes (list): list of datetime objects that
        represent the dates of the files that are being
        ingested

    metric_choice (str): the type of analysis that the user
        wishes to perform. used to triage whether the function will
        create a 'weighted' or unweighted' metric

    Returns
    -------
    aggregate_metrics (list): list of metrics objects
        (AggregateMetricForTableOrClass or
        AggregateMetricForHPO & AggregateMetricForDate)
        that contain all of the 'aggregate metrics' to be displayed
    """
    if metric_choice in metrics_to_weight:
        aggregate_metrics = create_weighted_aggregate_metrics(
            sheet_output=sheet_output,
            metric_dictionary=metric_dictionary,
            datetimes=datetimes, hpo_dictionary=hpo_dictionary)

    else:
        aggregate_metrics = create_unweighted_aggregate_metrics(
            sheet_output=sheet_output,
            metric_dictionary=metric_dictionary,
            datetimes=datetimes,
            hpo_dictionary=hpo_dictionary,
            metric_choice=metric_choice)

    return aggregate_metrics


def create_weighted_aggregate_metrics(
        sheet_output, metric_dictionary, datetimes, hpo_dictionary):
    """
    Function is used to create 'weighted' aggregate metrics that can
    be useful in terms of data =analysis

    Parameters
    ---------
    metric_dictionary (dict): has the following structure
        keys: all of the different metric_types possible
        values: all of the HPO objects that
            have that associated metric_type

    hpo_dictionary (dict): has the following structure
        keys: all of the different HPO IDs
        values: all of the associated HPO objects that
            have that associated HPO ID

    sheet_output (string): determines the type of 'output'
        to be generated (e.g. the sheets are HPOs or the
        sheets are tables)

    datetimes (list): list of datetime objects that
        represent the dates of the files that are being
        ingested

    Returns
    -------
    aggregate_metrics (list): list of metrics objects
        (AggregateMetricForTableOrClass or
        AggregateMetricForHPO & AggregateMetricForDate)
        that contain all of the 'aggregate metrics' to be displayed
    """
    # will always need the table aggregate_metrics
    aggregate_metrics = create_weighted_aggregate_metrics_for_tables(
            metric_dictionary=metric_dictionary,
            datetimes=datetimes)

    if sheet_output == 'hpo_sheets':
        hpo_aggregate_metrics = create_weighted_aggregate_metrics_for_hpos(
            hpo_dictionary=hpo_dictionary,
            datetimes=datetimes,
            metric_dictionary=metric_dictionary)

        aggregate_metrics.extend(hpo_aggregate_metrics)

        agg_met_for_dates = create_weighted_aggregate_metric_for_dates(
            aggregate_metrics=aggregate_metrics)

        aggregate_metrics.extend(agg_met_for_dates)

    elif sheet_output != 'table_sheets':
        raise Exception(
            """Bad parameter input for function
             create_aggregate_master_function. Parameter provided
            was: {param}""".format(param=sheet_output))

    return aggregate_metrics


def create_unweighted_aggregate_metrics(
        sheet_output, metric_dictionary, datetimes, hpo_dictionary,
        metric_choice):
    """
    Function is used to create 'unweighted' aggregate metrics
    that can be useful in terms of data analysis.

    Parameters
    ---------
    metric_dictionary (dict): has the following structure
        keys: all of the different metric_types possible
        values: all of the HPO objects that
            have that associated metric_type

    hpo_dictionary (dict): has the following structure
        keys: all of the different HPO IDs
        values: all of the associated HPO objects that
            have that associated HPO ID

    sheet_output (string): determines the type of 'output'
        to be generated (e.g. the sheets are HPOs or the
        sheets are tables)

    datetimes (list): list of datetime objects that
        represent the dates of the files that are being
        ingested

    metric_choice (str): the type of analysis that the user
        wishes to perform. used to NOT generate an aggregate
        HPO metric if there already is an 'all' that exists
        as a class.

    Returns
    -------
    aggregate_metrics (list): list of metrics objects
        (AggregateMetricForTableOrClass or
        AggregateMetricForHPO & AggregateMetricForDate)
        that contain all of the 'aggregate metrics' to be displayed
    """

    # going to appear in the 'aggregate_information' dataframe
    aggregate_metrics = create_unweighted_aggregate_metrics_for_tables(
        metric_dictionary=metric_dictionary, datetimes=datetimes)

    if sheet_output == 'hpo_sheets':

        # already includes 'AggregateMetricForDate' objects
        aggregate_metrics_hpos = hpo_sheets_chosen_create_uw_ams(
            metric_choice=metric_choice,
            hpo_dictionary=hpo_dictionary,
            datetimes=datetimes,
            metric_dictionary=metric_dictionary)

        aggregate_metrics.extend(aggregate_metrics_hpos)

    elif sheet_output != 'table_sheets':
        raise Exception(
            """Bad parameter input for function
             create_aggregate_master_function. Parameter provided
            was: {param}""".format(param=sheet_output))

    return aggregate_metrics


def hpo_sheets_chosen_create_uw_ams(
        metric_choice, hpo_dictionary, datetimes, metric_dictionary):
    """
    Function is used to triage how to create 'aggregate metrics'
    for HPO sheets based on the kind of user input provided.

    In cases where the 'aggregate metric' already exists in the
    DataQualityMetrics (e.g. 'All Measurements' for measurement
    integration), one should only create aggregate metrics
    for each class AND use the metrics where the class is an
    'aggregate metric' to make an 'AggregateMetricForDate'
    object.

    Otherwise, one should create aggregate dataquality metrics
    for each HPO that spans every table/class (as opposed to
    leveraging existing DataQualityMetric objects).

    Parameters
    ---------
    metric_dictionary (dict): has the following structure
        keys: all of the different metric_types possible
        values: all of the HPO objects that
            have that associated metric_type

    hpo_dictionary (dict): has the following structure
        keys: all of the different HPO IDs
        values: all of the associated HPO objects that
            have that associated HPO ID

    datetimes (list): list of datetime objects that
        represent the dates of the files that are being
        ingested

    metric_choice (str): the type of analysis that the user
        wishes to perform. used to NOT generate an aggregate
        HPO metric if there already is an 'all' that exists
        as a class.

    Returns
    -------
    aggregate_metrics (list): list of metrics objects
        (AggregateMetricForHPO & AggregateMetricForDate)
        that contain all of the 'aggregate metrics' to be displayed
    """

    # case where the metric already does not already exist as a DQM object
    if metric_choice not in unweighted_metric_already_integrated_for_hpo:

        # creates what will appear at the bottom of each HPO dataframe
        # if it already is not a 'baked in' metric
        aggregate_metrics = create_unweighted_aggregate_metrics_for_hpos(
            hpo_dictionary=hpo_dictionary,
            datetimes=datetimes, metric_dictionary=metric_dictionary)

        # below specifically creates what would be at the bottom of the
        # aggregate dataframe - essentially the aggregate across all
        # tables, HPOs, and dates for the same metric type
        agg_met_for_dates = create_unweighted_aggregate_metric_for_dates(
            aggregate_metrics=aggregate_metrics)

        aggregate_metrics.extend(agg_met_for_dates)

        return aggregate_metrics
    else:
        # information logged in AggregateMetricForTable objects - empty list
        # to signify that nothing needs to be appended
        return []
