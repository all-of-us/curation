"""
File is used to store the functions that are used to create
weighted aggregate metrics. These are metrics that
weight the sites' contributions to the overall metric
based on row count.

These functions are called in
the create_aggregate_objects file and harness many of the
functions in the auxillary_aggregate_functions file.
"""

from aggregate_metric_classes import AggregateMetricForTableOrClass, \
    AggregateMetricForHPO, AggregateMetricForDate

from auxillary_aggregate_functions import \
    find_relevant_tables_or_classes, \
    get_stats_for_weighted_table_aggregate_metric, \
    get_stats_for_weighted_hpo_aggregate_metric, \
    find_unique_dates_and_metrics, \
    get_stats_for_unweighted_table_aggregate_metric


def create_weighted_aggregate_metrics_for_tables(
        metric_dictionary, datetimes):
    """
    Function is intended to create 'aggregate' data quality
    metrics that can be applied to a specific data quality metric
    for a particular date (across all HPOs). This metric will
    be weighted so that HPOs that contribute more rows for each
    table contribute more to the overall metric than HPOs that
    have fewer rows.

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
    new_aggregate_metrics (list): contains AggregateMetricForTable
        objects that reflect each date, metric, and table combination
    """

    # create a metric type for each
    #    a. metric
    #    b. date
    #    c. table

    new_agg_metrics = []

    # A - really will only go into for applicable metric
    for metric_type, hpo_object_list in metric_dictionary.items():

        tables_or_classes_for_metric = find_relevant_tables_or_classes(
            hpo_object_list=hpo_object_list, metric_type=metric_type)
        # now we know the tables and dates for all of the metrics

        # B.
        for date in datetimes:

            # C
            for table_or_class_name in tables_or_classes_for_metric:
                # to add to the new object's attributes
                total_rows, pertinent_rows = 0, 0

                hpos_counted = []  # need to prevent double-counting

                # now we need to find the relevant DataQualityMetric objects
                for hpo_object in hpo_object_list:

                    if hpo_object.date == date:
                        hpos_counted, total_rows, pertinent_rows = \
                            get_stats_for_weighted_table_aggregate_metric(
                                hpo_object=hpo_object,
                                metric_type=metric_type,
                                date=date,
                                table_or_class=table_or_class_name,
                                hpos_counted=hpos_counted,
                                total_rows=total_rows,
                                pertinent_rows=pertinent_rows)

                # actually create the metric - culled for all three dimensions

                new_am = AggregateMetricForTableOrClass(
                    date=date,
                    table_or_class_name=table_or_class_name,
                    metric_type=metric_type,
                    num_total_rows=total_rows,
                    num_pertinent_rows=pertinent_rows)

                new_agg_metrics.append(new_am)

    # finished the loop - now has all the aggregate metrics
    return new_agg_metrics


def create_weighted_aggregate_metrics_for_hpos(
        hpo_dictionary, datetimes, metric_dictionary):
    """
    Function is intended to create 'aggregate' data quality
    metrics that can be applied to a specific data quality metric
    for a particular HPO (across all tables).

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
        (regardless of table)
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

                total_rows, pertinent_rows = 0, 0

                # need to specify - only check the relevant metric
                if len(hpo_objects) > 0:

                    for hpo_object in hpo_objects:

                        # want to exclude device exposure for now
                        tables_or_classes_counted = ['Device Exposure']

                        if hpo_object.date == date:

                            total_rows, pertinent_rows, \
                                tables_or_classes_counted = \
                                get_stats_for_weighted_hpo_aggregate_metric(
                                    hpo_object=hpo_object, metric=metric,
                                    date=date, hpo_name=hpo_object.name,
                                    tables_or_classes_counted=
                                    tables_or_classes_counted,
                                    total_rows=total_rows,
                                    pertinent_rows=pertinent_rows)

                new_agg_metric = AggregateMetricForHPO(
                    date=date, hpo_name=hpo, metric_type=metric,
                    num_total_rows=total_rows,
                    num_pertinent_rows=pertinent_rows)

                new_agg_metrics.append(new_agg_metric)

    # finished the loop - now has all the aggregate metrics
    return new_agg_metrics


def create_weighted_aggregate_metric_for_dates(aggregate_metrics):
    """
    This function is designed to create a special 'total'
    AggregateMetricForDate for a particular metric for each date.

    This is intended to show the relative
    count/success rate/failure rate:
        a. across all tables
        b. across all HPOs
        c. on the same date
        d. on the same metric type

    Parameters
    ----------
    aggregate_metrics (list): contains AggregateMetricForHPO
        objects that reflect each date, metric, and HPO combination
        (regardless of table)

    Return
    ------
    agg_metrics_for_dates (list): contains the
        AggregateMetricForDate objects that we laid out above.
    """
    dates, metrics, agg_metrics_for_dates = \
        find_unique_dates_and_metrics(aggregate_metrics=aggregate_metrics)

    # should ultimately be len(dates) x len(metrics) AMFD objects
    for date in dates:
        for metric in metrics:
            num_pertinent_rows, num_total_rows = 0, 0

            # find the relevant metrics - add if relevant
            for agg_hpo_metric in aggregate_metrics:
                if agg_hpo_metric.date == date \
                        and agg_hpo_metric.metric_type == metric\
                        and isinstance(agg_hpo_metric, AggregateMetricForHPO):

                    hpo_total_rows = agg_hpo_metric.num_total_rows
                    hpo_pert_rows = agg_hpo_metric.num_pertinent_rows

                    num_pertinent_rows += hpo_pert_rows
                    num_total_rows += hpo_total_rows

            amfd = AggregateMetricForDate(
                date=date, metric_type=metric,
                num_total_rows=num_total_rows,
                num_pertinent_rows=num_pertinent_rows,
                table_or_class='aggregate_info')

            agg_metrics_for_dates.append(amfd)

    return agg_metrics_for_dates
