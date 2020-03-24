"""
This file is to sequester functions that are used to 'cross reference'
between older metrics sheets and newer metrics sheets. These are
taken away from the 'main' create_dq_issue_site_dfs.py file to improve
readability.
"""

from general_functions import load_files
from dictionaries_and_lists import english_to_metric_type_dict
import datetime


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

    for idx, new_metric in enumerate(failing_metrics):
        found_in_old = False

        for old_metric in old_failing_metrics:

            # all attributes except value or first reported
            metrics_the_same = (
                new_metric.hpo == old_metric.hpo and
                new_metric.table == old_metric.table and
                new_metric.metric_type == old_metric.metric_type and
                new_metric.data_quality_dimension ==
                old_metric.data_quality_dimension and
                    new_metric.link == old_metric.link

            )

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

    return failing_metrics


def find_report_date(prev_dashboards, new_metric):
    """
    Function is used to look into a previous reporrt

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
    date (datetime): date in the previous dashboard for the particular
        data quality metric that was found to be erroneous
    """
    sheet_name = new_metric.hpo
    sheet = load_files(sheet_name=sheet_name, file_name=prev_dashboards)
    # now we have the sheet in question - should be easy to find to row

    date = None  # default - should be changed to datetime object

    for index, row in sheet.iterrows():

        # ensure we can compare to teh attribute of a DQM object
        metric_type_english = row['Metric Type']
        metric_type_raw = english_to_metric_type_dict[metric_type_english]

        # same standards as employed by cross_reference_old_metrics
        correct_row = (
            row['HPO'] == new_metric.hpo and
            row['Table'] == new_metric.table and
            metric_type_raw == new_metric.metric_type and
            row['Data Quality Dimension'] == new_metric.data_quality_dimension and
            row['Link'] == new_metric.link)

        # get the date
        if correct_row:
            date_string = row['First Reported']
            date = datetime.strptime(date_string, '%Y-%m-%d')

    # check that it is reassigned - just in case
    assert isinstance(date, datetime), \
        "Date now found in the old dashboard. This applies to" \
        "the following DataQualityMetric object: {dq}".format(
            dq=new_metric.print_dqd_attributes()
        )

    return date
