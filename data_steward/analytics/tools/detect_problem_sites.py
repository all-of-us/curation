"""
Developed with Python 2.7

Goals
-----
This program should be used to detect sites with problematic
data quality trends. The DRC (Columbia University Medical Center)
should intervene with these sites and help them address their
problems reaching the goals of AoU.

Sites that require interventions are those that:
    a. have not had ANY improvements in data quality during a
       specified length of time
    b. have not reached the thresholds communicated to them by
       CUMC


ASSUMPTIONS
-----------
This program relies on the output from two other programs that
can also be found in the curation GitHub:
    - metrics_over_time.py
    - delta_metrics_over_time.py

metrics_over_time.py will allow us to see if the sites are
reaching their data quality metrics.

delta_metrics_over_time.py will allow us to more easily see
the 'change' in data quality for a particular site from
report-to-report.

This program runs on the assumptions that the reports will
generate the same tabs for the same metric type generated.
This is validated in delta_metrics_over_time.py.

IMPORTANT NOTES
---------------
This program should be:
    a. versatile
        the benchmarks that sites should reach can be modified
        new benchmarks can be added if needed
    b. standardized
        all sites should be held to the same AoU standards

FIXME
-----
1. Determine whether or not the threshold for 'maximum' values
(e.g. duplicates) should also apply to the 'total' row that
is at the bottom of the sheet
    Currently excluded

2. Determine whether we should 'tag' a site for improvement
in a particular area if even ONE of the tables is stagnant
    Currently the site is only flagged for a particular
    metric if NONE of the tables see improvement over
    an extended period of time
"""

import pandas as pd
import xlrd


def get_metric_files_and_tabs():
    """
    Function should be used to organize the files that will
        be used to detect sites that require intervention.
        This function also organizes the tabs of those files
        so we have a comprehensive list of the HPO sites.

    :return:
    all_files (dict):
        metric (e.g. concept): list
            the list that is the value has both of the file
            names that are required to run the analysis

    metric_goals(dict):
        metric (e.g. concept): value
            the value is the 'goal' of the metric (e.g. a value
            of 0 means the 'ideal' is to ultimately have 0 of
            that instance)

    want_above_metric (dict):
        metric (e.g. concept): bool
            the bool indicates if the metric is a 'minimum'
            that the sight should strive towards. False indicates
            that the number in metric_goals is a 'maximum.'
    """
    metric_goals = {
        'concept': 70,  # percent
        'duplicates': 1500,  # total count
        'end_before_begin': 15  # percent
    }

    want_above_metric = {
        'concept': True,
        'duplicates': False,
        'end_before_begin': False
    }

    all_files = {}

    for metric in sorted(metric_goals.keys()):
        metric_files = []

        analytics_data = metric + '_hpo_sheets_data_analytics.xlsx'
        analytics_change = metric + '_weekly_changes_hpo_sheets.xlsx'

        metric_files.append(analytics_data)
        metric_files.append(analytics_change)

        all_files[metric] = metric_files

    return all_files, metric_goals, want_above_metric


def find_hpos_not_meeting_benchmarks(hpo_sites,
                                     files, metric_goals,
                                     want_above_metric):
    """
    Function is used to identify the HPOs that are not
        meeting at least one of the metrics established
        by the HPO. This is based on the most recent
        analytics report.

    :param
    hpo_site_names (list): list of all the HPO sites whose
    metrics need to be investigated

    files (dict):
        metric (e.g. concept): list
            the list that has both of the file names
            required to run the analysis

    metric_goals(dict):
        metric (e.g. concept): value
            the value is the 'goal' of the metric that the site
            should aim to be above or below (depending on
            the value in want_above_metric)

    want_above_metric (dict):
        metric (e.g. concept): bool
            the bool indicates if the metric is a 'minimum'
            that the sight should strive towards. False indicates
            that the number in metric_goals is a 'maximum.'

    :return
    problem_hpos (list): contains the HPOs that are
        not meeting the thresholds (either maximum or minimum)
        established by the DRC
    """
    problem_hpos = {}

    for hpo in hpo_sites:
        problem_metrics_for_hpo = []
        for metric in sorted(metric_goals.keys()):
            file_to_use = files[metric][0]

            threshold = metric_goals[metric]
            threshold_is_min = want_above_metric[metric]

            df = pd.read_excel(file_to_use, sheet_name=hpo)
            last_col = df.iloc[:, -1]
            last_row_idx = len(last_col) - 1

            for row_num, value in enumerate(last_col):
                if metric not in problem_metrics_for_hpo:
                    if threshold_is_min and (value < threshold):
                        problem_metrics_for_hpo.append(metric)

                    # do not want to log if the 'total' row
                    elif not threshold_is_min and (value > threshold) \
                            and (row_num != last_row_idx):
                        problem_metrics_for_hpo.append(metric)

        if problem_metrics_for_hpo:
            problem_hpos[hpo] = problem_metrics_for_hpo

    return problem_hpos


def find_stagnant_hpos(problem_hpos, files, num_reports,
                       want_above_metric):
    """
    Function is used to find HPOs who fail to meet benchmarks
        established by the DRC and have not made and progress
        towards improving their data quality for a set number
        of time.

    :param
    problem_hpos (list): contains the HPOs that are
        not meeting the thresholds (either maximum or minimum)
        established by the DRC

    files (dict):
        metric (e.g. concept): list
            the list has both of the file names that are
            required to run the analysis

    num_reports (int): the number of final columns (number of
        reports in the data analysis scripts) to look through
        to ensure full indexing

    want_above_metric (dict):
        metric (e.g. concept): bool
            the bool indicates if the metric is a 'minimum'
            that the sight should strive towards. False indicates
            that the number in metric_goals is a 'maximum.'

    :return:
    problem_hpos_with_stagnation (dict):
        hpo: list
            the inner list are the metrics that were deemed
            problematic based on the most recent analytics
            report AND have not had any improvement in the
            past x reports (x = num_reports).
    """
    problem_hpos_with_stagnation = {}

    for hpo, failing_metrics in problem_hpos.items():
        metrics_with_no_progress = []

        for metric in failing_metrics:
            file_to_use = files[metric][1]
            threshold_is_min = want_above_metric[metric]

            df = pd.read_excel(file_to_use, sheet_name=hpo)
            saw_improvement = False

            num_cols = len(df.columns)
            col_idxs = list(range(0, num_cols))
            earliest_col = num_cols - num_reports
            cols_to_investigate = col_idxs[num_cols - 1: earliest_col - 1: -1]

            for report_col in cols_to_investigate:
                col_name = df.columns[report_col]
                col = df[col_name]

                for table_value in col:  # detecting positive changes in DQ
                    if threshold_is_min and (table_value > 0):
                        saw_improvement = True
                    elif not threshold_is_min and (table_value < 0):
                        saw_improvement = True

            if not saw_improvement:  # no positive changes - log
                metrics_with_no_progress.append(metric)

        if metrics_with_no_progress:  # there was at least one prob metric
            problem_hpos_with_stagnation[hpo] = metrics_with_no_progress

    return problem_hpos_with_stagnation


file_names, metrics_thresholds, metric_direction = get_metric_files_and_tabs()

# get a metric whose analytics report only has all of the HPOs and an 'aggregate'
# info tab. allows you to take the names of all of the tabs except for the last tab
metric_with_only_agg_info = 'end_before_begin'
sheet_with_hpos = file_names[metric_with_only_agg_info][0]

all_tabs = xlrd.open_workbook(sheet_with_hpos,
                              on_demand=True).sheet_names()

hpo_site_names = all_tabs[:-1]  # take off aggregate_info

problem_hpos_and_metric_failures = \
    find_hpos_not_meeting_benchmarks(
        hpo_site_names, file_names, metrics_thresholds, metric_direction)

num_reports_before_stagnant = 3  # reports generally run on weekly basis

stagnant_problem_hpos = find_stagnant_hpos(
    problem_hpos_and_metric_failures, file_names, num_reports_before_stagnant,
    metric_direction)

text_file = open("output.txt", "w")

for site, problem_metrics in stagnant_problem_hpos.items():
    problems = ""
    for num, metric_type in enumerate(problem_metrics):
        problems += "{}. {}\n".format(num + 1, metric_type)

    end_message = "{} has issues that have not been improved upon " \
                  "in {} reports in the following:\n".format(
        site, num_reports_before_stagnant) + problems \
                  + "\n"

    text_file.write(end_message)

text_file.close()
