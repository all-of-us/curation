"""
This program generates an e-mail from a properly-formatted Excel
file. The e-mail should contain information regarding data quality
for the various AoU HPO sites.

Assumptions
-----------
1. Excel file in question is also imported into this current directory

Notes
-----
1. This code is not particularly efficient as it generates an HPO object
for every HPO represented (even though only one HPO object is ultimately
used to generate the message).

At the moment, this is not of major concern because the additional 40+
HPO objects only take 2-3 additional seconds to generate and would
introduce several inconsistencies with the metrics_over_time script.

It is important to note, however, that if this is to be implemented
on a larger scale, we should consider refactoring the functions
that make DataQualityMetric/HPO objects.

Code was developed with respect to PEP8 standards
"""
from startup_functions import startup, \
    convert_file_names_to_datetimes

from create_dqms import create_dqm_list

from functions_to_create_hpo_objects import establish_hpo_objects, \
    add_dqm_to_hpo_objects, add_number_total_rows_for_hpo_and_date, \
    sort_hpos_into_dicts

from organize_relevant_dqms import create_string_for_failing_metrics

from messages import introduction, sign_off, sign_off, link

from dictionaries_and_lists import full_names

from contact_list import recipient_dict

report1 = 'april_17_2020.xlsx'

report_names = [report1]

sheet_names = [
    'concept', 'data_after_death', 'date_datetime_disparity',
    'drug_routes', 'drug_success', 'duplicates',
    'end_before_begin', 'measurement_units', 'erroneous_dates',
    'sites_measurement', 'person_id_failure_rate']


def create_hpo_objects(dqm_objects, file_names, datetimes):
    """
    Function is used to create the various 'HPO' objects
    that will be used to eventually populate the sheets.

    Parameters
    ---------
    dqm_objects (list): list of DataQualityMetric objects.
        these will eventually be associated to their respective
        HPO objects.

    file_names (list): list of the strings that indicate
        the names of the files being ingested. these
        in sequential order.

    datetimes (list): list of datetime objects that
        represent the dates of the files that are being
        ingested

    Return
    ------
    hpo_objects (list): contains all of the HPO objects. the
        DataQualityMetric objects will now be associated to
        the HPO objects appropriately.

    NOTE
    ----
    The DQM objects that are being established would only
    have 'metrics' that are associated with the user's choice
    of analytics output.
    """
    blank_hpo_objects = establish_hpo_objects(
        dqm_objects=dqm_objects)

    hpo_objects = add_dqm_to_hpo_objects(
        dqm_objects=dqm_objects, hpo_objects=blank_hpo_objects)

    for date in datetimes:
        hpo_objects = \
            add_number_total_rows_for_hpo_and_date(
                hpos=hpo_objects,
                date_names=file_names,
                date=date)

    return hpo_objects


def assemble_final_messages(unique_metrics, hpo_id):
    """
    This function is used to assemble and print the
    final message displayed on the outputted email

    Parameters
    ----------
    unique_metrics (str): string that contains
        all the 'erroneous' data quality metrics and
        tables/classes affected. this effectively serves
        as the body of the paragraph

    hpo_id (str): represents the HPO string that the
        user entered
    """
    ehr_site = full_names[hpo_id]
    name = "Noah Engel"
    num_metrics = len(sheet_names)
    date = "April 17th, 2020"

    message = introduction.format(
        ehr_site=ehr_site, name=name,
        num_metrics=num_metrics, date=date)

    message += unique_metrics

    message += sign_off.format(
        link=link)

    relevant_persons = recipient_dict[hpo_id]

    message += """\n
    Email Title:
    ------------
    Data Check Feedback (April 2020) - {site}""".format(
        site=ehr_site)

    message += """\n
    Contact the following individuals:
    ----------------------------------
    {relevant_persons}
    """.format(relevant_persons=relevant_persons)

    print(message)


def main():
    """
    Function that executes the entirety of the program.
    """
    all_objects = []

    for metric_choice in sheet_names:
        dfs, hpo_names, target_low, percent_bool = \
            startup(file_names=report_names,
                    metric_choice=metric_choice)

        file_names, datetimes = convert_file_names_to_datetimes(
            file_names=report_names)

        dqm_list = create_dqm_list(
            dfs=dfs, file_names=file_names, datetimes=datetimes,
            user_choice=metric_choice, percent_bool=percent_bool,
            target_low=target_low, hpo_names=hpo_names)

        hpo_objects = create_hpo_objects(
            dqm_objects=dqm_list, file_names=file_names,
            datetimes=datetimes)

        all_objects.extend(hpo_objects)

    unique_metrics, hpo_id = create_string_for_failing_metrics(
        hpo_objects=all_objects)

    assemble_final_messages(unique_metrics=unique_metrics, hpo_id=hpo_id)

if __name__ == "__main__":
    main()
