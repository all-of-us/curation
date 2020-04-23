"""
File is intended to create dataframes for each of the sites to better catalog
their respective data quality issues. The output of this file should
be useful for uploading to a project management tool that could then be
provisioned to the different sites.
This project management tool could then, in turn, enable sites to more easily
identify their data quality issues and allow the DRC to more easily track
HPO engagement.
For a full description of this issue, please see EDQ-427.
Start Date: 03/24/2020 (v1)
NOTE: For 'seed' data_quality_issues/analytics report files,
please see the 'baseline summary reports' folder in the
internal DRC drive. This will allow someone to run this script.
"""

from dictionaries_and_lists import relevant_links, full_names, \
    desired_columns_dict, data_quality_dimension_dict, \
    table_or_class_based_on_column_provided, metric_names, \
    metric_type_to_english_dict

from class_definitions import HPO, DataQualityMetric

from general_functions import load_files, \
    generate_hpo_id_col, find_hpo_row, get_err_rate, sort_and_convert_dates

from cross_reference_functions import cross_reference_old_metrics

import pandas as pd

old_dashboards = 'march_26_2020_data_quality_issues.xlsx'

old_excel_file_name = 'march_19_2020.xlsx'
excel_file_name = 'april_17_2020.xlsx'

metric_names = list(metric_names.keys())  # sheets to be investigated


def create_hpo_objects(file_name):
    """
    Function is used to establish the HPO objects that will
    ultimately carry all of the data from the sheet.

    Parameters
    ----------
    file_name (str): the date of the file that is being used to generate
        the data quality issue frames

    Returns
    -------
    hpo_objects (lst): list of HPO objects (see class_definitions.py)
        that will be used and ultimately populated with the
        data quality metrics

    hpo_id_column (lst): list of the hpo_ids that will eventually
        each be associated with its own dataframe
    """
    # creating the various HPO objects
    hpo_id_column = generate_hpo_id_col(file_name)
    hpo_objects = []

    for hpo_id in hpo_id_column:

        # keeping the lists empty - to be filled later with
        # DataQualityMetric objects
        # lists cannot be 'default values' for a class because they
        # are mutable so they all need to be manually specified

        hpo = HPO(
            name=hpo_id, full_name=full_names[hpo_id],
            concept_success=[], duplicates=[],
            end_before_begin=[], data_after_death=[],
            route_success=[], unit_success=[], measurement_integration=[],
            ingredient_integration=[], date_datetime_disparity=[],
            erroneous_dates=[], person_id_failure_rate=[])

        hpo_objects.append(hpo)

    return hpo_objects, hpo_id_column


def populate_hpo_objects_with_dq_metrics(
        hpo_objects, metrics, file_name, date):
    """
    Function is used to take the HPO objects created in a previous
    function (create_hpo_objects) and associate them with
    DataQualityMetric objects that contain the relevant pieces
    of information from the selected sheet.

    Parameters
    ----------
    hpo_objects (lst): list of HPO objects (see class_definitions.py)
        that will be used and ultimately populated with the
        data quality metrics

    metric_names (lst): list of the sheets that will be used to
        identify the data quality metrics for each of the HPO
        and DataQualityMetric objects

    file_name (str): the date of the file that is being used to generate
        the data quality issue frames

    date (datetime): datetime object that corresponds to the date that
        the file is named after

    Returns
    -------
    hpo_objects (lst): list of HPO objects (see class_definitions.py)
        that now have the appropriate DataQualityMetric objects
    """

    # start with analyzing each metric first - minimizes 'loads'
    for metric in metrics:
        sheet = load_files(sheet_name=metric, file_name=file_name)

        for hpo in hpo_objects:
            hpo_name = hpo.name
            row_num = find_hpo_row(sheet, hpo_name)

            # what we are looking for within each analytics sheet
            desired_columns = desired_columns_dict[metric]

            all_dqds_for_hpo_for_metric = []  # list of objects - to be filled

            for column_for_table in desired_columns:
                err_rate = get_err_rate(sheet, row_num, metric,
                                        hpo_name, column_for_table)

                data_quality_dimension = DataQualityMetric(
                    hpo=hpo_name,
                    table_or_class=
                    table_or_class_based_on_column_provided[column_for_table],
                    metric_type=metric_type_to_english_dict[metric],
                    value=err_rate,
                    first_reported=date,
                    data_quality_dimension=data_quality_dimension_dict[metric],
                    link=relevant_links[metric])

                # adding to a list of the same metric type for the same site
                all_dqds_for_hpo_for_metric.append(data_quality_dimension)

            # now we have objects for all of the data quality metrics for
                # a. each site
                # b. each table
            # for a particular data quality metric - should now assign to HPO

            for metric_object in all_dqds_for_hpo_for_metric:
                hpo.add_attribute_with_string(
                    metric=metric_object.metric_type, dq_object=metric_object)

    return hpo_objects


def create_hpo_problem_dfs(hpo_objects, old_hpo_objects, hpo_id_column,
                           prev_dashboards):
    """
    Function is used to actually create the output Pandas dataframes
    that catalogue the problems for each site. There should be one
    dataframe for each HPO object. Each row of the dataframe should
    more or less contain the information stored in a
    DataQualityMetric object.

    Parameters
    ----------
    hpo_objects (lst): list of HPO objects (see class_definitions.py)
        that will be used and ultimately populated with the
        data quality metrics

    old_hpo_objects (lst): list of HPO objects
        (see class_defintions.py) that will be used to determine
        if a particular data quality issue is 'old' or 'new'

    hpo_id_column (lst): list of the hpo_ids that will eventually
        each be associated with its own dataframe

    prev_dashboards (string): name of the 'old' dashboards that
        should reside in an Excel file in the current directory.
        these dashboards will be necessary to update the
        'first_reported' aspect of DataQualityMetric objects.

    Returns
    -------
    df_dictionary_by_site (dict): dictionary with structure:
        keys: HPO ids
        values: dataframes containing the data quality issues
            for said site. the rows are each unique data
            quality issues. the columns are the attributes
            of DataQualityMetric objects
    """

    total_dfs = []
    sample_dqd_object = hpo_objects[0].concept_success[0]
    attribute_names = sample_dqd_object.get_list_of_attribute_names()

    # instantiating the appropriate number of dataframes
    for _ in range(len(hpo_objects)):
        new_df = pd.DataFrame(columns=attribute_names)
        total_dfs.append(new_df)

    for hpo, old_hpo, df in zip(hpo_objects, old_hpo_objects, total_dfs):
        assert hpo.name == old_hpo.name, \
            "New HPO added: {hpo_name}. Please re-run the " \
            "'old' Excel file in order to ensure consistency in" \
            "HPO names".format(hpo_name=hpo.name)

        # now have list of DataQualityMetric objects
        failing_metrics = hpo.find_failing_metrics()
        old_failing_metrics = old_hpo.find_failing_metrics()

        # assign failing metrics the correct 'first_reported' date
        failing_metrics = cross_reference_old_metrics(
            failing_metrics, old_failing_metrics,
            prev_dashboards)

        # can only iterate if problem exists
        if failing_metrics:
            for row_idx, failed_metric in enumerate(failing_metrics):
                attributes = failed_metric.get_attributes_in_order()

                df.loc[row_idx] = attributes

    df_dictionary_by_site = dict(zip(hpo_id_column, total_dfs))

    return df_dictionary_by_site


def main():
    """
    Function that executes the entirety of the program.
    """

    # getting datetime objects
    file_names = [old_excel_file_name, excel_file_name]
    date_objects = sort_and_convert_dates(file_names)

    # getting the 'old' objects from previous file
    old_hpo_objects, old_hpo_id_column = create_hpo_objects(
        file_name=old_excel_file_name
    )

    old_hpo_objects = populate_hpo_objects_with_dq_metrics(
        # dfk
        hpo_objects=old_hpo_objects, metrics=metric_names,
        file_name=old_excel_file_name, date=date_objects[0]
    )

    # getting the 'new' objects from the current file
    hpo_objects, hpo_id_column = create_hpo_objects(
        file_name=excel_file_name)

    hpo_objects = populate_hpo_objects_with_dq_metrics(
        hpo_objects=hpo_objects, metrics=metric_names,
        file_name=excel_file_name, date=date_objects[1])

    df_dict = create_hpo_problem_dfs(
        hpo_objects=hpo_objects,
        old_hpo_objects=old_hpo_objects,
        hpo_id_column=hpo_id_column,
        prev_dashboards=old_dashboards)

    # cut off previous extension
    output_file_name = excel_file_name[:-5] + \
        "_data_quality_issues.xlsx"

    writer = pd.ExcelWriter(output_file_name, engine='xlsxwriter')

    for df_name, dataframe in df_dict.items():
        dataframe.to_excel(writer, sheet_name=df_name)

    writer.save()


if __name__ == "__main__":
    main()
