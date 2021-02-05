"""
This file is used to store 'auxillary' functions that are used
to create AggregateMetric objects. These auxillary functions should
be useful in ensuring that the main create_aggregate_objects.py
file remains uncluttered and readable.
"""


def find_relevant_tables_or_classes(
        hpo_object_list, metric_type):
    """
    This function is used to find the tables that should be
    either triaged into separate AggregateMetric objects
    (as in the case of AggregateMetricForTable) or iterated
    over (as in the case of AggregateMetricForHPO).

    Parameters
    ----------
    hpo_object_list (list): HPO objects that will be iterated
        over. we intend to return the tables that exist across
        all of the DataQualityMetrics for the particular
        metric_type

    metric_type (string): shows the kind of metric that is
        being determined (e.g. duplicate records)

    Return
    ------
    tables_for_metric (list): contains all of the tables
        that exist across all of the HPO objects for
        the specified metric_type
    """

    # need to create tables for each metric - varies by metric
    tables_or_classes_for_metric = []

    for hpo_object in hpo_object_list:
        relevant_dqms = hpo_object.use_string_to_get_relevant_objects(
            metric=metric_type)

        for dqm in relevant_dqms:
            table_or_class = dqm.table_or_class

            if table_or_class not in tables_or_classes_for_metric:
                tables_or_classes_for_metric.append(table_or_class)

    return tables_or_classes_for_metric


def get_stats_for_weighted_hpo_aggregate_metric(
        hpo_object, metric, date, hpo_name, tables_or_classes_counted,
        total_rows, pertinent_rows):
    """
    Function is used once an HPO is found and warrants its own
    AggregateMetricForHPO because it has a unique set of date
    and metric parameters.

    Parameters
    ----------
    hpo_object (HPO): object of class HPO that has all the
        information we want to sort across (and ultimately
        average across all of the applicable tables)

    metric (string): represents the kind of metric that
        is to be investigated (e.g. duplicates)

    date (datetime): the datetime that should be unique
        for the AggregateMetricForHPO to be created.

    hpo_name (string): name of the HPO object

    tables_or_classes_counted (list): list of tables that
        should not be counted in the 'overall tally'.
        this is used to prevent the same table from being
        counted more than once

    total_rows (float): starts at zero. goal is to add the
        total number of rows that span the
        HPO across all of the tables

    pertinent_rows (float): starts at zero. goal is to add
        the total number of rows that either
        contribute to either the 'success' or failure rate

    Returns
    -------
    total_rows (float): total number of rows that span the
        HPO across all of the tables

    pertinent_rows (float): total number of rows that either
        contribute to either the 'success' or failure rate

    tables_counted (list): list of tables that should not
        be counted in the 'overall tally'. now also contains
        the tables that contributed to the overall tally for
        the particular HPO on the particular date
    """
    relevant_dqms = hpo_object.use_string_to_get_relevant_objects(
                                metric=metric)

    for dqm in relevant_dqms:

        # regardless of dqm.table_or_class
        if (dqm.date == date and
            dqm.hpo == hpo_name and
            dqm.metric_type == metric) and \
                (hpo_object.date == date) and \
                dqm.table_or_class not in tables_or_classes_counted:

            table_or_class = dqm.table_or_class
            metric_type = dqm.metric_type

            hpo_pert_rows, hpo_total_rows = \
                hpo_object.use_table_or_class_name_to_find_rows(
                    table_or_class=table_or_class,
                    metric=metric_type)

            total_rows += float(hpo_total_rows)
            pertinent_rows += float(hpo_pert_rows)

            # prevent double counting
            tables_or_classes_counted.append(dqm.table_or_class)

    return total_rows, pertinent_rows, tables_or_classes_counted


def get_stats_for_weighted_table_aggregate_metric(
        hpo_object, metric_type, date, table_or_class, hpos_counted,
        total_rows, pertinent_rows):
    """
    Function is used once an table is found and warrants its own
    AggregateMetricForTable because it has a unique set of date
    and metric parameters.

    Parameters
    ----------
    hpo_object (HPO): object of class HPO that has all the
        information we want to sort across (and ultimately
        average across all of the applicable tables)

    metric (string): represents the kind of metric that
        is to be investigated (e.g. duplicates)

    table_or_class (string): the table or class whose
        'aggregate metric' is being calculated
        (e.g. 'Drug Exposure' or 'ACE Inhibitor')

    date (datetime): the datetime that should be unique
        for the AggregateMetricForTableOrClass
        to be created.

    hpo_name (string): name of the HPO object

    hpos_counted (list): list of HPOs that should not
        be counted in the 'overall tally'. this is used to
        prevent the same HPO from being counted more than
        once

    total_rows (float): starts at zero. goal is to add the
        total number of rows that span the
        table across all of the HPOs

    pertinent_rows (float): starts at zero. goal is to add
        the total number of rows that either
        contribute to either the 'success' or failure rate

    Returns
    -------
    total_rows (float): total number of rows that span the
        table across all of the HPOs

    pertinent_rows (float): total number of rows that either
        contribute to either the 'success' or failure rate

    hpos_counted (list): list of HPOs that should not
        be counted in the 'overall tally'. now also contains
        the HPOs that contributed to the overall tally for
        the particular HPO on the particular date
    """

    relevant_dqms = hpo_object.use_string_to_get_relevant_objects(
        metric=metric_type)

    for dqm in relevant_dqms:

        # regardless of dqm.hpo
        # warrants 'counting' towards the metric to create
        if (dqm.metric_type == metric_type and
            dqm.date == date and
            dqm.table_or_class == table_or_class) and \
                hpo_object.name not in hpos_counted:

            hpo_pert_rows, hpo_total_rows = \
                hpo_object.use_table_or_class_name_to_find_rows(
                    table_or_class=table_or_class,
                    metric=metric_type)

            # float conversion for consistency
            total_rows += float(hpo_total_rows)
            pertinent_rows += float(hpo_pert_rows)

    hpos_counted.append(hpo_object.name)  # prevent from counting again

    return hpos_counted, total_rows, pertinent_rows


def find_unique_dates_and_metrics(aggregate_metrics):
    """
    Function is used to find all of the unique dates
    and metrics. Each date/metric combination should
    warrant its own AggregateMetricForDate object.

    Parameter
    ---------
    aggregate_metrics (list): contains AggregateMetricForHPO
        objects that reflect each date, metric, and HPO combination
        (regardless of table)

    Return
    ------
    dates (list): list of the unique dates

    metrics (list): list of the metric

    agg_metrics_for_dates: blank list to add AggregateMetricForHPO
        objects. will ultimately be the length of
        len(dates) x len(metrics)
    """
    # C + D - determine the number to make
    dates, metrics = [], []

    for agg_hpo_metric in aggregate_metrics:
        metric_date = agg_hpo_metric.date
        metric_type = agg_hpo_metric.metric_type

        if metric_date not in dates:
            dates.append(metric_date)
        if metric_type not in metrics:
            metrics.append(metric_type)

    agg_metrics_for_dates = []

    return dates, metrics, agg_metrics_for_dates


def get_stats_for_unweighted_table_aggregate_metric(
        hpo_object, metric_type, date, table_or_class,
        hpos_counted, unweighted_metrics_for_hpos):
    """
    Function is used once a table is found and warrants its own
    AggregateMetricForTable because it has a unique set of date
    and metric parameters.

    NOTE: this is similar to
        'get_stats_for_weighted_table_aggregate_metric'
        but DOES NOT give different weights to HPOs

    Parameters
    ----------
    hpo_object (HPO): object of class HPO that has all the
        information we want to sort across (and ultimately
        average across all of the applicable tables)

    metric_type (string): represents the kind of metric that
        is to be investigated (e.g. duplicates)

    date (datetime): the datetime that should be unique
        for the AggregateMetricForTable to be created.

    table_or_class (string): the table whose 'aggregate
        metric' is being calculated

    hpos_counted (list): list of HPOs that should not
        be counted in the 'overall tally'. this is used to
        prevent the same HPO from being counted more than
        once

    unweighted_metrics_for_hpos (list): growing list of
        unweighted metrics (across all the HPOs) that
        could be used for the ultimate calculation

    Returns
    -------
    unweighted_metrics_for_hpos (list): list of the
        DataQualityMetric objects' values. This will
        eventually be averaged to create a 'total
        unweighted' aggregate metric

    hpos_counted (list): list of HPOs that should not
        be counted in the 'overall tally'. now also contains
        the HPOs that contributed to the overall tally for
        the particular HPO on the particular date
    """

    relevant_dqms = hpo_object.use_string_to_get_relevant_objects(
        metric=metric_type)

    for dqm in relevant_dqms:

        # regardless of dqm.hpo
        # warrants 'counting' towards the metric to create
        if (dqm.metric_type == metric_type and
            dqm.date == date and
            dqm.table_or_class == table_or_class) and \
                hpo_object.name not in hpos_counted:

            value = dqm.value
            unweighted_metrics_for_hpos.append(value)

    hpos_counted.append(hpo_object.name)

    return unweighted_metrics_for_hpos, hpos_counted


def get_stats_for_unweighted_hpo_aggregate_metric(
        hpo_object, metric, date, hpo_name,
        tables_and_classes_counted, statistics_to_average):
    """
    Function is used once an HPO is found and warrants its own
    AggregateMetricForHPO because it has a unique set of date
    and metric parameters.

    This function, however, differs from
    get_stats_for_weighted_hpo_aggregate_metric in that it
    weights all of the different classes equally.
    The other function instead creates an 'aggregate'
    metric and weights the tables/categories by their
    relative row contributions.

    Parameters
    ----------
    hpo_object (HPO): object of class HPO that has all the
        information we want to sort across (and ultimately
        average across all of the applicable tables)

    metric (string): represents the kind of metric that
        is to be investigated (e.g. duplicates)

    date (datetime): the datetime that should be unique
        for the AggregateMetricForHPO to be created.

    hpo_name (string): name of the HPO object

    tables_counted (list): list of tables that should not
        be counted in the 'overall tally'. this is used to
        prevent the same table from being counted more than
        once

    statistics_to_average (list): list of the 'values'
        associated with various DQM objects. this is
        to grow across the metric (through all tables)
        for the HPO.

    Returns
    -------
    statistics_to_average (list): list of the 'values'
        associated with the HPO object for the relevant
        data quality metrics. these will all ultimately
        be averaged to create an 'aggregate unweighted'
        data quality metric for the HPO.

    tables_and_classes_counted (list): list of tables that
        should not be counted in the 'overall tally'.
        now also contains the tables that contributed to
        the overall tally for the particular HPO on the
        particular date
    """
    relevant_dqms = hpo_object.use_string_to_get_relevant_objects(
                                metric=metric)

    for dqm in relevant_dqms:

        # regardless of dqm.table_or_class
        if (dqm.date == date and
            dqm.hpo == hpo_name and
            dqm.metric_type == metric) and \
                (hpo_object.date == date) and \
                dqm.table_or_class not in tables_and_classes_counted:

            # add the value
            statistics_to_average.append(dqm.value)

            # prevent double counting
            tables_and_classes_counted.append(dqm.table_or_class)

    return statistics_to_average, tables_and_classes_counted
