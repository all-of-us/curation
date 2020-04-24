"""
This file is to sequester functions that are used to 'cross reference'
between older metrics sheets and newer metrics sheets. These are
taken away from the 'main' create_dq_issue_site_dfs.py file to improve
readability.
"""

from general_functions import load_files
import pandas as pd


def cross_reference_old_metrics(failing_metrics, old_failing_metrics,
                                prev_dashboard):
    """
    Function is used to determine if the 'failing metrics' for a particular
    site are 'new' (appeared in the most recent iteration) or if they
    are 'old' (existed in the last iteration). If the metric is deemed to
    be 'old', the first_reported metric for the 'newer' version is reset
    to the former first_reported metric.

    Parameters
    ----------
    failing_metrics (list): contains DataQualityMetric objects that are
        known to 'fail' with respect to the thresholds established.
        these are the metrics from the newer file.

    old_failing_metrics (list): contains DataQualityMetric objects
        that are known to 'fail' with respect to the thresholds
        established. these are the metrics from the older file.

    prev_dashboard (string): name of the 'old' dashboards that
        should reside in an Excel file in the current directory.
        these dashboards will be necessary to update the
        'first_reported' aspect of DataQualityMetric objects.

    Returns
    -------
    failing_metrics (list): now contains the DataQuality objects
        but has the updated first_reported attribute
    """
    # can only iterate if something to report
    if failing_metrics is not None:
        for idx, new_metric in enumerate(failing_metrics):
            found_in_old = False

            try:
                for old_metric in old_failing_metrics:

                    # all attributes except value or first reported
                    metrics_the_same = (
                        new_metric.hpo == old_metric.hpo and
                        new_metric.table_or_class == old_metric.table_or_class and
                        new_metric.metric_type == old_metric.metric_type and
                        new_metric.data_quality_dimension ==
                        old_metric.data_quality_dimension and
                        new_metric.link == old_metric.link)

                    if metrics_the_same:
                            found_in_old = True

                if found_in_old:
                    # found the metric in previous sheet - need to find the
                    # original report date and change accordingly
                    reported_date = find_report_date(
                        new_metric=new_metric,
                        prev_dashboards=prev_dashboard)

                    new_metric.first_reported = reported_date

                    # be sure to replace appropriately
                    failing_metrics[idx] = new_metric
            except TypeError:
                pass  # means no 'old metrics' failed

    return failing_metrics


def find_report_date(prev_dashboards, new_metric):
    """
    Function is used to look into a previous report

    Parameters
    ----------
    prev_dashboard (string): name of the 'old' dashboards that
        should reside in an Excel file in the current directory.
        these dashboards will be necessary to update the
        'first_reported' aspect of DataQualityMetric objects.

    new_metric (DataQualityMetric): object whose 'counterpart'
        in the 'dashboard' needs to be found in order to
        report out the date

    Returns
    -------
    report_date (datetime): date in the previous dashboard for the
        particular data quality metric that was found to be erroneous
    """
    sheet_name = new_metric.hpo
    sheet = load_files(sheet_name=sheet_name, file_name=prev_dashboards)
    # now we have the sheet in question - should be easy to find to row

    report_date = None  # default - should be changed to datetime object

    for index, row in sheet.iterrows():

        # same standards as employed by cross_reference_old_metrics
        same_hpo = (row['HPO'] == new_metric.hpo)
        same_table = (row['Table/Class'] == new_metric.table_or_class)
        same_mt = (row['Metric Type'] == new_metric.metric_type)
        same_dqd = (row['Data Quality Dimension'] ==
                    new_metric.data_quality_dimension)
        same_link = (row['Link'] == new_metric.link)

        correct_row = (
             same_hpo and same_table and same_mt and
             same_dqd and same_link)

        # get the date
        if correct_row:
            # should be a timestamp
            report_date = row['First Reported']

    # check that it is reassigned - just in case
    assert isinstance(report_date, pd.Timestamp), \
        "Date not found in the old dashboard. This applies to" \
        "the following DataQualityMetric object: {dq}".format(
            dq=new_metric.print_dqd_attributes()
        )

    return report_date
