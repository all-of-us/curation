"""
File is used to store the functions that are used to create
unweighted aggregate metrics. These are metrics that do
NOT weight the sites' contributions to the overall metric
based on row count.

These functions are called in
the create_aggregate_objects file and harness many of the
functions in the auxillary_aggregate_functions file.
"""

from aggregate_metric_classes import \
    AggregateMetricForTableOrClass, \
    AggregateMetricForHPO, AggregateMetricForDate


from auxillary_aggregate_functions import \
    find_relevant_tables_or_classes, \
    get_stats_for_unweighted_table_aggregate_metric, \
    get_stats_for_unweighted_hpo_aggregate_metric, \
    find_unique_dates_and_metrics

import numpy as np

def create_unweighted_aggregate_metrics_for_tables(
        metric_dictionary, datetimes):
    """
    Function is intended to create 'aggregate' data quality
    metrics that can be applied to a specific data quality metric
    for a particular date (across all HPOs).

    This metric is NOT weighted. This means that all of the HPOs
    should ultimately contribute equally to the ending metric.

    Parameters
    ----------
    metric_dictionary (dict): has the following structure
        keys: all of the different metric_types possible
        values: all of the HPO objects that
            have that associated metric_type

    datetimes (list): list of datetime objects that
        represent the dates of the files that are being
        ingested

    Returns
    -------
    new_agg_metrics (list): contains new
    AggregateMetricForTableOrClass objects that span all of the
    desired tables.
    """
    # create a metric type for each
    #    a. metric
    #    b. date
    #    c. table

    new_agg_metrics = []

    # A - really will only go into for applicable metric
    for metric_type, hpo_object_list in metric_dictionary.items():

        tables_for_metric = find_relevant_tables_or_classes(
            hpo_object_list=hpo_object_list, metric_type=metric_type)
        # now we know the tables and dates for all of the metrics

        # B.
        for date in datetimes:

            # C
            for table_or_class in tables_for_metric:

                # need to specify - only check the relevant metric
                if len(hpo_object_list) > 0:

                    unweighted_metrics_for_hpos = []

                    for hpo_object in hpo_object_list:
                        hpos_counted = []  # to avoid repeats

                        unweighted_metrics_for_hpos, hpos_counted = \
                            get_stats_for_unweighted_table_aggregate_metric(
                                hpo_object=hpo_object,
                                metric_type=metric_type, date=date,
                                table_or_class=table_or_class,
                                hpos_counted=hpos_counted,
                                unweighted_metrics_for_hpos=
                                unweighted_metrics_for_hpos)

                    # setting = 0 to show unweighted
                    total_rows, pertinent_rows = 0, 0

                    # 'unweighted' aspect comes in - simple mean
                    overall_rate = np.nansum(unweighted_metrics_for_hpos) / \
                        len(unweighted_metrics_for_hpos)

                    new_uw_agg_metric = AggregateMetricForTableOrClass(
                        date=date, table_or_class_name=table_or_class,
                        metric_type=metric_type, num_total_rows=total_rows,
                        num_pertinent_rows=pertinent_rows)

                    new_uw_agg_metric.manually_set_overall_rate(
                        rate=overall_rate)

                    new_agg_metrics.append(new_uw_agg_metric)

    return new_agg_metrics


def create_unweighted_aggregate_metrics_for_hpos(
        hpo_dictionary, datetimes, metric_dictionary):
    """
    Function is intended to create 'aggregate' data quality
    metrics that can be applied to a specific data quality metric
    for a particular HPO (across all tables).

    These metrics, however, should NOT be weighted. Each HPO
    should contribute equally regardless of the number of
    rows.

    Parameters
    ----------
    hpo_dictionary (dict): has the following structure
        keys: all of the different HPO IDs
        values: all of the associated HPO objects that
            have that associated HPO ID

    datetimes (list): list of datetime objects that
        represent the dates of the files that are being
        ingested

    metric_dictionary (dict): has the following structure
        keys: all of the different metric_types possible
        values: all of the HPO objects that
            have that associated metric_type

    Returns
    -------
    new_aggregate_metrics (list): contains AggregateMetricForHPO
        objects that reflect each date, metric, and HPO combination
        (regardless of table). Again, this is unweighted.
    """

    # create a metric type for each
    #    a. HPO
    #    b. date
    #    b. metric

    new_agg_metrics = []

    # A.
    for hpo, hpo_objects in hpo_dictionary.items():

        # B.
        for date in datetimes:

            # C.
            for metric in metric_dictionary:

                # need to specify - only check the relevant metric
                if len(hpo_objects) > 0:

                    # where all of the statistics for the metric
                    # (across all tables) will be housed
                    statistics_to_average = []

                    for hpo_object in hpo_objects:

                        # tables_counted to avoid double-counting
                        # want to exclude device exposure for now
                        tables_and_classes_counted = ['Device Exposure']

                        if hpo_object.date == date:

                            statistics_to_average, \
                                tables_and_classes_counted = \
                                get_stats_for_unweighted_hpo_aggregate_metric(
                                    hpo_object=hpo_object, metric=metric,
                                    date=date, hpo_name=hpo_object.name,
                                    tables_and_classes_counted=
                                    tables_and_classes_counted,
                                    statistics_to_average=
                                    statistics_to_average)

                    # setting = 0 to show unweighted
                    total_rows, pertinent_rows = 0, 0

                    # 'unweighted' aspect comes in - simple mean
                    overall_rate = sum(statistics_to_average) / \
                        len(statistics_to_average)

                    new_agg_metric = AggregateMetricForHPO(
                        date=date, hpo_name=hpo, metric_type=metric,
                        num_total_rows=total_rows,
                        num_pertinent_rows=pertinent_rows)

                    new_agg_metric.manually_set_overall_rate(
                        rate=overall_rate)

                    new_agg_metrics.append(new_agg_metric)

    # finished the loop - now has all the aggregate metrics
    return new_agg_metrics


def create_unweighted_aggregate_metric_for_dates(
        aggregate_metrics):
    """
    This function is designed to create a special 'total'
    AggregateMetricForDate for a particular metric for each date.

    This is intended to show the relative
    count/success rate/failure rate:
        a. across all tables
        b. across all HPOs
        c. on the same date
        d. on the same metric type

    This 'unweighted' metric is intended to not give more
    preference to either tables or HPOs that have higher
    row counts.

    Parameters
    ----------
    aggregate_metrics (list): contains AggregateMetricForHPO
        objects that reflect each date, metric, and HPO combination
        (regardless of table). These are 'unweighted' objects and
        have 0s for their 'pertinent' and 'total' row counts.

    Return
    ------
    agg_metrics_for_dates (list): contains the
        AggregateMetricForDate objects that we laid out above.
    """
    dates, metrics, agg_metrics_for_dates = \
        find_unique_dates_and_metrics(aggregate_metrics=aggregate_metrics)

    # show that this is across all tables and HPOs
    table_or_class = 'aggregate_info'

    # should ultimately be len(dates) x len(metrics) AMFD objects
    for date in dates:
        for metric in metrics:

            # set these as 0s to delineate that these are 'unweighted'
            num_total_rows, num_pertinent_rows = 0, 0
            values = []  # collect all of the values

            # find the relevant metrics - add if relevant
            for agg_hpo_metric in aggregate_metrics:
                if agg_hpo_metric.date == date \
                        and agg_hpo_metric.metric_type == metric:

                    value_for_hpo = agg_hpo_metric.overall_rate

                    values.append(value_for_hpo)

            # here's where the 'unweighted' aspect comes in - simple mean
            overall_rate = sum(values) / len(values)

            amfd = AggregateMetricForDate(
                date=date, metric_type=metric,
                num_total_rows=num_total_rows,
                num_pertinent_rows=num_pertinent_rows,
                table_or_class=table_or_class)

            amfd.manually_set_overall_rate(rate=overall_rate)

            agg_metrics_for_dates.append(amfd)

    return agg_metrics_for_dates
