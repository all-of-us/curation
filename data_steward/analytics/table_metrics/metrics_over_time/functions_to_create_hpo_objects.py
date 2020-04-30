"""
File is intended to store a number of functions that are
used to create the HPO objects throughtout the script.
"""

from dictionaries_and_lists import full_names, \
    row_count_col_names, metric_type_to_english_dict
from hpo_class import HPO
from startup_functions import load_files
from functions_to_create_dqm_objects import find_hpo_row, \
    get_info
import datetime

def establish_hpo_objects(dqm_objects):
    """
    Function is used as a 'launch pad' for all of the other functions
    that create HPO objects based on the various DataQualityMetric
    objects

    Parameters
    ----------
    dqm_objects (list): list of DataQualityMetric objects.
        these will eventually be associated to their respective
        HPO objects.

    Return
    ------
    blank_hpo_objects (list): list of the blank HPO objects. there
        should be a unique (and mostly empty) object for each HPO
        and date (total length should be #HPOs times #dates)
    """
    names_to_establish = []
    dates_to_establish = []
    blank_hpo_objects = []

    for obj in dqm_objects:
        name = obj.hpo
        date = obj.date

        if name not in names_to_establish:
            names_to_establish.append(name)

        if date not in dates_to_establish:
            dates_to_establish.append(date)

    # create unique object for each HPO and date
    for hpo_name in names_to_establish:
        full_name = full_names[hpo_name]

        for date in dates_to_establish:

            hpo = HPO(
              name=hpo_name, full_name=full_name,
              date=date,

              # all of the metric objects to be left blank
              # for the time being

              concept_success=[], duplicates=[],
              end_before_begin=[], data_after_death=[],
              route_success=[], unit_success=[],
              measurement_integration=[], ingredient_integration=[],
              date_datetime_disp=[], erroneous_dates=[],
              person_id_failure=[], achilles_errors=[])

            blank_hpo_objects.append(hpo)

    return blank_hpo_objects


def add_dqm_to_hpo_objects(dqm_objects, hpo_objects):
    """
    This function is designed to leverage the internal
    HPO.add_metric_with_string() function to further
    establish what data quality metrics are associated
    with each of the HPO/date combinations.

    Parameters
    ----------
    dqm_objects (list): list of DataQualityMetric objects.
        these will eventually be associated to their respective
        HPO objects.

    hpo_objects (list): list of the blank HPO objects. there
        should be a unique (and mostly empty) object for each HPO
        and date (total length should be #HPOs times #dates)

    Returns
    -------
    hpo_objects (list): list of the HPO objects originally
        provided to the function. these objects, however,
        now have 'metrtics' provisioned accordingly
    """
    for dqm in dqm_objects:
        hpo_name_for_metric = dqm.hpo
        metric_name = dqm.metric_type
        date_for_metric = dqm.date

        for hpo in hpo_objects:
            if hpo.name == hpo_name_for_metric and \
               hpo.date == date_for_metric:
                    hpo.add_metric_with_string(
                        metric=metric_name,
                        dq_object=dqm)

    return hpo_objects


def add_number_total_rows_for_hpo_and_date(
        hpos, date_names, date):
    """
    Function is used to add further attributes to the HPO
    objects. These are the attributes pertaining to the number
    of rows in each of the tables. These row counts should be
    stored in the 'concept' sheet.

    Parameters
    ----------
    hpos (list): list of the HPO objects. these should
        already have the name and date established at
        the minimum.

    date_names (list): list of the strings that indicate
        the names of the files being ingested. these
        in sequential order.

    date (list): datetime object that is used to ensure
        that data quality metrics are being associated
        with the HPO object that is associated with their
        respective date

    Returns
    -------
    hpos (list): list of the HPO objects. now should have the
        attributes for the number of rows filled in.
    """
    sheet_name = 'concept'  # where row count is stored
    dfs = load_files(
        user_choice=sheet_name, file_names=date_names)

    dates_objs = []

    for date_str in date_names:
        date_str = date_str[:-5]  # get rid of extension
        date_obj = datetime.datetime.strptime(date_str, '%B_%d_%Y')
        dates_objs.append(date_obj)

    chosen_idx = -1

    for idx, date_object in enumerate(dates_objs):
        if date_object == date:
            chosen_idx = idx

    assert chosen_idx > -1, "Invalid Date: {date}".format(
        date=date
    )

    df_for_date = dfs[chosen_idx]

    for hpo in hpos:
        if hpo.date == date:

            hpo_name = hpo.name
            hpo_row = find_hpo_row(
                sheet=df_for_date, hpo=hpo_name)

            num_rows_dictionary = get_info(
                sheet=df_for_date, row_num=hpo_row,
                percentage=False, sheet_name=sheet_name,
                columns_to_collect=row_count_col_names,
                target_low=False)

            for table_name, value in num_rows_dictionary.items():
                hpo.add_row_count_with_string(
                    table=table_name, value=value
                )

    return hpos


def sort_hpos_into_dicts(
        hpo_objects, hpo_names, user_choice):
    """
    Function is used to sort the newly-created HPO objects
    into dictionaries where the keys can be used to access
    pertinent DataQualityMetric objects. The keys for the
    dictionaries can either be the HPO sites or the metric
    type that is being investigated.

    This can ultimately decrease the number of iterations
    needed to create 'aggregate' data quality metric objects.

    Parameters
    ----------
    hpo_objects (list): list of HPO objects. These HPO objects
        should have their associated DataQualityMetric objects
        and attached row counts.

    hpo_names (list): list of the HPO names that are to be
        put into dataframes (either as the titles of the
        dataframe or the rows of a dataframe)

    user_choice (str): the data quality metric the user wants to
        investigate

    Returns
    -------
    hpo_dictionary (dict): has the following structure
        keys: all of the different HPO IDs
        values: all of the associated HPO objects that
            have that associated HPO ID
    """
    # want to have an aggregate
    metrics_to_instantiate = []
    metric_dictionary, hpo_dictionary = {}, {}
    metric = metric_type_to_english_dict[user_choice]

    # creating the keys for the metric dictionary
    for hpo_obj in hpo_objects:
        dqm_lst = hpo_obj.use_string_to_get_relevant_objects(
            metric=metric)
        for dqm in dqm_lst:
            metric_type = dqm.metric_type
            if metric_type not in metrics_to_instantiate:
                metrics_to_instantiate.append(metric_type)

    # create the 'metric key' dictionary
    for metric_type in metrics_to_instantiate:
        relevant_hpo_objs = []
        for hpo_obj in hpo_objects:
            dqm_lst = hpo_obj.use_string_to_get_relevant_objects(
                metric=metric)

            for dqm in dqm_lst:
                dqm_metric = dqm.metric_type

                if dqm_metric == metric_type:
                    relevant_hpo_objs.append(hpo_obj)

        metric_dictionary[metric_type] = relevant_hpo_objs

    # creating the 'HPO' dictionary
    for hpo_name in hpo_names:
        relevant_hpo_objs = []
        for hpo_obj in hpo_objects:
            hpo_id = hpo_obj.name

            if hpo_id == hpo_name:
                relevant_hpo_objs.append(hpo_obj)

        hpo_dictionary[hpo_name] = relevant_hpo_objs

    return metric_dictionary, hpo_dictionary
