"""
This file is to sequester functions that are used to 'cross reference'
between older metrics sheets and newer metrics sheets. These are
taken away from the 'main' create_dq_issue_site_dfs.py file to improve
readability.
"""

from general_functions import load_files
import pandas as pd
import datetime
from dictionaries_and_lists import new_metric_types
import constants


# ### changed the below cell's  metrics_the_same part re:link !!! 

def cross_reference_old_metrics(
        failing_metrics, old_failing_metrics,
        prev_dashboard, new_hpo_ids, excel_file_name):
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

    new_hpo_ids (list): contains the IDs of the HPOs that are
        new to the latest 'analytics report' and therefore are
        not contained in the previous 'panels'.

    excel_file_name (str): the name of the most recent 'analytics'
        report. contains the 'date' that will be assigned to
        novel data quality issues.

    Returns
    -------
    failing_metrics (list): now contains the DataQuality objects
        but has the updated first_reported attribute.
    """
    # can only iterate if something to report
    if failing_metrics is not None:
        
        # check if the old issue is fixed
        for idx, old_metric in enumerate(old_failing_metrics):
            old_fixed = False
            for new_metric in failing_metrics:
                metrics_the_same = (
                    new_metric.hpo.lower() ==
                    old_metric.hpo.lower() and

                    new_metric.table_or_class.lower() ==
                    old_metric.table_or_class.lower() and

                    new_metric.metric_type.lower() ==
                    old_metric.metric_type.lower() and

                    new_metric.data_quality_dimension.lower() ==
                    old_metric.data_quality_dimension.lower() and

                    new_metric.link.lower() ==
                    old_metric.link.lower())

                if metrics_the_same == False:
                    # this means this old issue has been fixed
                    old_fixed = True
            # this is suppressing drug and measurement integration
            if old_fixed:
                del old_failing_metrics[idx]
        
        # check if the new metric existed in old metric & update the first reported date
        for idx, new_metric in enumerate(failing_metrics):
            found_in_old = False

            try:
                for old_metric in old_failing_metrics:

                    # all attributes except value or first reported
                    metrics_the_same = (
                        new_metric.hpo.lower() ==
                        old_metric.hpo.lower() and

                        new_metric.table_or_class.lower() ==
                        old_metric.table_or_class.lower() and

                        new_metric.metric_type.lower() ==
                        old_metric.metric_type.lower() and

                        new_metric.data_quality_dimension.lower() ==
                        old_metric.data_quality_dimension.lower() and

                        new_metric.link.lower() ==
                        old_metric.link.lower())

                    if metrics_the_same:
                        found_in_old = True

                if found_in_old:
                    # found the metric in previous sheet - need to find the
                    # original report date and change accordingly
                    reported_date = find_report_date(
                        new_metric=new_metric,
                        prev_dashboards=prev_dashboard,
                        new_hpo_ids=new_hpo_ids,
                        excel_file_name=excel_file_name)

                    new_metric.first_reported = reported_date

                    # be sure to replace appropriately
                    failing_metrics[idx] = new_metric
                    
            except TypeError:
                pass  # means no 'old metrics' failed

    
    return failing_metrics


def find_report_date(
        prev_dashboards, new_metric, new_hpo_ids, excel_file_name):
    """
    Function is used to look into a previous report.

    Parameters
    ----------
    prev_dashboard (string): name of the 'old' dashboards that
        should reside in an Excel file in the current directory.
        these dashboards will be necessary to update the
        'first_reported' aspect of DataQualityMetric objects.

    new_metric (DataQualityMetric): object whose 'counterpart'
        in the 'dashboard' needs to be found in order to
        report out the date.

    new_hpo_ids (list): contains the IDs of the HPOs that are
        new to the latest 'analytics report' and therefore are
        not contained in the previous 'panels'

    excel_file_name (str): the name of the most recent 'analytics'
        report. contains the 'date' that will be assigned to
        novel data quality issues.

    Returns
    -------
    report_date (datetime): date in the previous dashboard for the
        particular data quality metric that was found to be erroneous.
    """
    sheet_name = new_metric.hpo

    if new_metric.hpo in new_hpo_ids:
        # new HPO site - means that the issue must have
        # originated in the latest 'analytics report' and
        # did not exist in the previous sheet

        date_str = excel_file_name[:-5]  # take off the .xlsx
        date = datetime.datetime.strptime(date_str, constants.date_format)
        date = pd.Timestamp(date)
        report_date = date

    else:
        sheet = load_files(
            sheet_name=sheet_name, file_name=prev_dashboards)
        # now we have the sheet in question - should be easy to find to row

        report_date = None  # default - should be changed to datetime object

        for index, row in sheet.iterrows():

            # same standards as employed by cross_reference_old_metrics
            same_hpo = (
                row[constants.hpo_col_name].lower() ==
                new_metric.hpo.lower())
            same_table = (
                row[constants.table_class_col_name].lower() ==
                new_metric.table_or_class.lower())
            same_mt = (
                row[constants.metric_type_col_name].lower() ==
                new_metric.metric_type.lower())
            same_dqd = (
                row[constants.data_quality_dimension_col_name].lower() ==
                new_metric.data_quality_dimension.lower())
            same_link = (
                row[constants.link_col_name].lower() ==
                new_metric.link.lower())

            correct_row = (
                 same_hpo and same_table and same_mt and
                 same_dqd and same_link)
            
            #print(row[constants.link_col_name].lower(), new_metric.link.lower())  ############
            '''
            if same_link == False:
                print(same_hpo, same_table, same_mt, same_dqd, same_link)
                old = row[constants.link_col_name].lower()
                new = new_metric.link.lower()
                for i in range(len(old)):
                    if old[i] != new[i]: print(old[i:i+10], "new:", new[i:i+10])
            ''' 
            # get the date
            if correct_row:
                report_date = row[constants.first_reported_col_name]

    if new_metric.metric_type not in new_metric_types:
        # ensure reassignment
        assert isinstance(report_date, pd.Timestamp), \
            f"""
            Date not found in the old dashboard. This applies to
            the following DataQualityMetric object:
            {new_metric.print_dqd_attributes()}"""

    else:
        # NOTE: the metric being investigated is new and therefore
        # should not be expected to appear on an old 'data_quality
        # _issues' panel. please be sure to delete a 'new metric'
        # in the corresponding list once it appears in old panels
        report_date = new_metric.first_reported

    return report_date
