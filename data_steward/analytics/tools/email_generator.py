"""
This program generates an e-mail from a properly-formatted Excel
file. The e-mail should contain information regarding data quality
for the various AoU HPO sites.
Assumptions
-----------
1. Excel file in question is also imported into this current directory
2. Script also stored with introduction.txt, great_job.txt, and contact_list.py
Code was developed with respect to PEP8 standards
"""
from __future__ import print_function
import os
from io import open

import pandas as pd

from contact_list import recipient_dict


def determine_row(sheet, site_hpo_id):
    """
    Function is used to find the row in the sheet where you can find all
    the error information for the HPO ID in question
    :param
    Sheet (dataframe): pandas dataframe to traverse.
                       Represents a sheet with numbers indicating
                       data incompleteness.
    hpo_id (string): string representing the HPO ID used to generate the e-mail

    :returns
    row_idx (int): row index on the particular sheet where
                   you can find the information regarding data incompleteness
    """
    row_num = 9999999
    hpo_id_col = sheet['src_hpo_id']

    for idx, site_id in enumerate(hpo_id_col):
        if site_hpo_id == site_id:
            row_num = idx

    if row_num == 9999999:  # never reassigned
        raise ValueError("The HPO ID was not found in the Excel file.")

    return row_num


def get_info(sheet, row_num, percentage, succ_rate):
    """
    Function is used to create a dictionary that contains
    the number of flawed records for a particular site.

    :param
    sheet (dataframe): pandas dataframe to traverse. Represents a
        sheet with numbers indicating data incompleteness.

    row_num (int): row (0-index) with all of the information for
                   the specified site's data quality

    percentage (boolean): used to determine whether or not the
        number is a 'flawed' record count versus the percentage
        of 'acceptable' records

    succ_rate (boolean): used to determine if the sheet should only
        be looking for the columns of the analytics
        report that end with "success_rate"

    :return:
    err_dictionary (dictionary): key:value pairs represent the
        column and and number that represents the quality of the data
    """
    data_info = sheet.iloc[row_num, :]  # series, row labels and values
    err_dictionary = {}

    for col_label, number in data_info.items():
        if succ_rate:
            if len(col_label) > 12:
                if col_label[-12:] == 'success_rate':
                    try:
                        number = float(number)
                        if number < 100:  # concept rate is below 100%
                            err_dictionary[col_label] = round(100 - number, 1)
                    except TypeError:  # failed to convert to float
                        pass
                    except ValueError:  # 'no info' case
                        pass
        else:
            if col_label == 'Unnamed: 0':  # first column with row indexes
                pass
            else:
                try:
                    number = float(number)
                    if percentage and number < 100:  # proportion w/ errors
                        err_dictionary[col_label] = round(100 - number, 1)
                    elif not percentage and number > 0:
                        err_dictionary[col_label] = int(number)
                except TypeError:  # failed to convert to float
                    pass
                except ValueError:  # cases of 'no info'
                    pass

    return err_dictionary


def determine_parameters(sheet_name):
    """
    Function is used to determine what parameters should be used
    in the get_info function based on the type of data quality
    error that is being investigated

    :param
    sheet_name (string): sheet that is being 'investigated' to
        determine the data quality metrics for a particular site

    :return:
    perc (boolean): used to determine whether or not the
        number is a 'flawed' record count versus the percentage
        of 'acceptable' records

    succ_rate_string (boolean): used to determine if the sheet should
        only be looking for the columns of the analytics
        report that end with "success_rate"
    """

    if sheet_name in ['duplicates']:
        perc = False
        succ_rate_string = False

    # NOTE: below would also have 'drug_concept_integration' and
    # 'integration_measurement_concept'
    elif sheet_name in ['end_before_begin']:
        perc = True
        succ_rate_string = False

    elif sheet_name in ['unit_integration', 'concept', 'drug_routes']:
        perc = True
        succ_rate_string = True

    else:  # does not apply; populate with 0 to throw error down the line
        perc, succ_rate_string = 0, 0

    return perc, succ_rate_string


# NOTE: I had trouble with character conversion when I wrote my
# messages directly in Pycharm text files. I recommend writing
# notes in other software environments (e.g. NotePad) and
# copying and pasting them to text files in this directory


def make_print_msg_specific(
        integration_rate, starting_msg, err_type):
    """
    Function is used to make the print message more specific
    by replacing 'generic' phrases with phrases that more
    completely explain the data quality issue at play.

    :param
    integration_rate (bool): determines if the data quality
        metric to be printed is an 'integration rate' rather
        than a problem with data quality. This warrants a
        change in how the message will be printed.

    starting_msg (str): the message to build off that
        will ultimately be displayed

    err_type (str): indicates the type of error metric that is
        being reported. Used to change what is printed so it is
        more appropriate.

    :return:
    starting_msg (str): the message with the data quality issue
        that now has a more specific indicator for the
        problem at hand
    """
    if integration_rate:
        # only one issue; make first informative
        starting_msg = starting_msg.replace(
            'of data)^', 'of expected concepts are not '
                         'successfully integrated)')

        # series of issues; make first informative
        starting_msg = starting_msg.replace(
            'of data),^', 'of expected concepts are not '
                          'successfully integrated),')

        # do not make non-first messages overly long
        starting_msg = starting_msg.replace(
            'of data', ' of concepts not integrated')

    elif err_type in ['concept']:
        starting_msg = starting_msg.replace(
            'of data)^', 'of concept_ids are not '
            'properly mapped)')

        starting_msg = starting_msg.replace(
            'of data),^', 'of concept_ids are not '
            'properly mapped),')

        starting_msg = starting_msg.replace(
            'of data', 'of concept_ids are not '
            'properly mapped')

    elif err_type in ['drug_routes']:
        starting_msg = starting_msg.replace(
            'of data)^', 'of route_concept_ids '
            'are not properly populated)'
        )

        starting_msg = starting_msg.replace(
            'of data),^', 'of route_concept_ids '
            'are not properly populated),'
        )

        starting_msg = starting_msg.replace(
            'of data', 'of drugs'
        )

    elif err_type in ['end_before_begin']:
        starting_msg = starting_msg.replace(
            'of data)^', 'of end dates precede '
                         'start dates')

        starting_msg = starting_msg.replace(
            'of data),^', 'of end dates precede '
                          'start dates')

    # get rid of lingering underscores
    starting_msg = starting_msg.replace('^', '')

    return starting_msg


def print_error_info(error_dict, starting_msg, percent,
                     err_type, integration_rate):
    """
    Function is used to create a string to display the error
    information for each of the tables belonging to a particular
    sheet.

    :param
    err_dictionary (dictionary): key:value pairs represent the
        column and and number that represents the quality
        of the data

    starting_msg (str): the message to build off that
        will ultimately be displayed

    percent (bool): determines whether the metrics to be displayed
        should have a percent sign following their metric

    err_type (str): indicates the type of error metric that is
        being reported. Used to change what is printed so it is
        more appropriate.

    integration_rate (bool): determines if the data quality metric
        to be printed is an 'integration rate' rather than a problem
        with data quality. This warrants a change in how the message
        will be printed.

    :return:
    starting_msg (str): the message that will display the metrics
        regarding data quality
    """
    num_errs = len(error_dict)
    error_number_in_type = 1
    first_instance = True

    for table, value in error_dict.items():
        if num_errs == 1 or (num_errs == 2 and error_number_in_type == 1):
            starting_msg += " {} ({}% of data)".format(
                table, value)
            if num_errs == 1:
                starting_msg += "."
        elif error_number_in_type < num_errs:  # in a series
            starting_msg += " {} ({}% of data),".format(table, value)
        else:  # last in a series (2 or more errors total)
            starting_msg += " and {} ({}% of data).".format(table, value)

        if first_instance:  # so the first instance can be replaced
            starting_msg += "^"
        first_instance = False

        error_number_in_type += 1

    starting_msg = make_print_msg_specific(
        integration_rate, starting_msg, err_type)

    # get rid of the percent if it should display the number of instances
    if not percent:
        starting_msg = starting_msg.replace('% of data', '')

    starting_msg = starting_msg.replace('_success_rate', '')

    return starting_msg


###########################################################################
# No longer defining functions                                            #
# We are now doing everything that comes BEFORE we print the message      #
###########################################################################


# 1. Loading the files
cwd = os.getcwd()
excel_file_name = cwd + "\\november_04_2019.xlsx"  # change for each email date

sheet_names = ['duplicates', 'end_before_begin', 'concept', 'unit_integration',
               'drug_routes']

# currently excluded from the script
# 'integration_measurement_concept', 'drug_concept_integration'

sheets = []

for sheet_name in sheet_names:
    df = pd.read_excel(excel_file_name, sheet_name)
    sheets.append(df)


# 2. Seeing which site the user wants to look at
prompt = "Please input the site ID of the HPO site that " \
         "you would like to use for an auto-generated e-mail. " \
         "(e.g. nyc_hh)\n"

hpo_id = input(prompt)
hpo_id = hpo_id.lower()  # case sensitivity

while hpo_id not in recipient_dict:
    print("HPO ID not found.")
    hpo_id = input(prompt)


# 3. Getting the actual data quality metrics
err_dictionaries = {}
for sheet_name, df in zip(sheet_names, sheets):
    try:
        row_idx = determine_row(df, hpo_id)

        percentage, succ_rate = determine_parameters(sheet_name)

        err_dict = get_info(df, row_idx, percentage=percentage,
                            succ_rate=succ_rate)

        err_dictionaries[sheet_name] = err_dict
    except ValueError:  # HPO ID not found
        err_dictionaries[sheet_name] = []  # blank error sheet


# 4. Getting the full HPO name; should be same in all DFs
sample_df = sheets[0]
row_idx = determine_row(sample_df, hpo_id)
hpo_full_name = 9999  # should be reset
row_info = sample_df.iloc[row_idx, :]  # series, row labels and values

for col_name, val in row_info.items():
    if col_name == 'HPO':
        hpo_full_name = val

if hpo_full_name == 9999:
    raise ValueError("No 'HPO' column title detected"
                     "in sheet {}".format(sheet_names[0]))


# 5. Want to know the number of sheets w/ errors for numbering purposes
num_prob_sheets = 0
for _, err_dict in err_dictionaries.items():
    if err_dict:
        num_prob_sheets += 1


###########################################################################
# Now we are actually printing out the 'problem' information for the site #
# This is admittedly clunky but I decided to not implement a standard     #
# function in order to allow the text feel more 'natural.'                #
###########################################################################


# same for all of the sites
intro = open('introduction.txt', 'r', encoding='utf-8')
intro_txt = intro.read()
intro_txt = intro_txt.replace("[EHR site]", hpo_full_name)
intro_txt = intro_txt.replace("{}", "Noah Engel")
print(intro_txt)


# Site is great
if num_prob_sheets == 0:
    great_job = open('great_job.txt', 'r')
    great_job_txt = great_job.read()
    print(great_job_txt)
else:
    starting_number = 1  # value for the 'problem number'

    print("We found the following with your most recent "
          "submission: \n")

    dups = 'duplicates'
    if err_dictionaries[dups]:
        duplicates_err = str(starting_number) + \
                         ". There are duplicate rows in the " \
                         "following table(s):"
        duplicates_err = print_error_info(
            err_dictionaries[dups], duplicates_err,
            percent=False, err_type=dups, integration_rate=False)
        starting_number += 1  # increment for future texts if applicable
        print(duplicates_err + "\n")

    ebb = 'end_before_begin'
    if err_dictionaries[ebb]:
        end_before_err = str(starting_number) + \
                         ". There are end dates before the start " \
                         "dates in the following table(s):"
        end_before_err = print_error_info(
            err_dictionaries[ebb], end_before_err,
            percent=True, err_type=dups, integration_rate=False)
        starting_number += 1
        print(end_before_err + "\n")

    c = 'concept'
    if err_dictionaries[c]:
        concept_err = str(starting_number) + \
            ". The concept success rates for some of the tables are " \
            "below 100%. This affects the following table(s):"

        concept_err = print_error_info(
            err_dictionaries[c], concept_err, percent=True,
            err_type=c, integration_rate=False)

        follow_up = "\n\nPlease note that the DRC does NOT expect " \
                    "concept success rates of 100% at this time. We " \
                    "expect a concept success rate of 90% or " \
                    "higher for each table. If you find that a particular " \
                    "table (or set of tables) has a concept success rate " \
                    "below 80%, it may be worth investigating your " \
                    "ETL to see how you can improve your mapping. We " \
                    "at the DRC can help with some mapping issues you might face."

        concept_err += follow_up

        print(concept_err + "\n")
        starting_number += 1

    unit = 'unit_integration'
    if err_dictionaries[unit]:
        unit_err = str(starting_number) + \
            ". Several vital measurements do not have the 'unit' " \
            "field populated with a standard 'unit_concept_id'. " \
            "This affects {}% of the instances of the selected " \
            "measurements. Please see the data quality description " \
            "link at the bottom of this e-mail to find the list of " \
            "specified measurements.".format(err_dictionaries[unit][
                    'unit_success_rate'])

        print(unit_err + "\n")
        starting_number += 1

    dr = 'drug_routes'
    if err_dictionaries[dr]:
        dr_err = str(starting_number) + \
            ". Several vital drugs do not have the 'route' " \
            "field populated with a standard 'route_concept_id'. " \
            "This affects the following classes of drugs:"

        dr_err = print_error_info(
            err_dictionaries[dr], dr_err, percent=True,
            err_type=dr, integration_rate=False)

        starting_number += 1
        print(dr_err + "\n")


"""
NOTE: The section below is used to populate information about the
'integration' of ingredients from various drug classes and measurements.
These are currently excluded from the e-mail, however, because the
means we are calculating these metrics right now are not necesssarily
efficient and are potentially deflating our perception of a site's
data completeness.

We will continue to improve the upstream analysis script to improve
these metrics. When we are more confident in these statistics, we
can use them to understand a site's data completeness and
re-integrate these comments into the script.
"""

# meas = 'integration_measurement_concept'
# if err_dictionaries[meas]:
#     meas_err = str(starting_number) + \
#         ". Several expected measurement concepts do not exist in " \
#         "your 'measurement' table. The following measurement " \
#         "classes have 'gaps' in that not all of the expected " \
#         "measurements are found:"
#
#     meas_err = print_error_info(
#         err_dictionaries[meas], meas_err, percent=True,
#         err_type=meas, integration_rate=True)
#
#     print(meas_err + "\n")
#     starting_number += 1
#
# drug = 'drug_concept_integration'
# if err_dictionaries[drug]:
#     drug_err = str(starting_number) + \
#         ". Several expected drug ingredients do not exist " \
#         "in your 'drug exposure' table. The following drug " \
#         "classes have 'gaps' in that not all of the expected " \
#         "drug ingredients are found:"
#
#     drug_err = print_error_info(
#         err_dictionaries[drug], drug_err, percent=True,
#         err_type=drug, integration_rate=True)
#
#     print(drug_err + "\n")
#     starting_number += 1


dq_description_link = 'https://docs.google.com/document/d/1Jq0n' \
                      'pMH9fOqpM7zMeXkddjFDZVVL7TrWOism0Ld4lAY/edit#'

gh_prompt = "If have questions about our metrics, " \
            "please consult the information sheet at the " \
            "following website: {}\n".format(dq_description_link)


# NOTE: this is a back-up
# important = "At this time, it is most important to address the following " \
#             "issues \n" \
#             "a. Duplicate records\n" \
#             "b. End dates preceding start dates\n" \
#             "c. Concept success rates\n" \
#             "If you received feedback on any of the above issue(s), " \
#             "please consider investigating your uploads and routinely " \
#             "uploading updated files so we can continue to track your " \
#             "progress. \n"
#
# less_imp = "If you received feedback on any other data quality metric, " \
#            "please use this as a 'baseline' and do not feel " \
#            "obligated to address the issue in the immediate future. \n"


github_link = 'https://github.com/all-of-us/curation/' \
              'tree/develop/data_steward/analytics'


query_p = "Finally, if you would like to see the queries that generate " \
          "these analytics, please visit the following site: {}\n".format(
            github_link)

sign_off = "Please do not hesitate to reach out to the DRC if you have any " \
           "questions related to All of Us at " \
           "datacuration@researchallofus.org."

print(gh_prompt)
print(query_p)
print(sign_off)

# now let's print the contacts
print("\nContact the following individual(s):")
print(recipient_dict[hpo_id])

print("\n")
print("Title: ")
title = "Data Check Feedback - [{}]".format(hpo_full_name)
print(title)
