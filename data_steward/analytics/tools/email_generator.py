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

import pandas as pd
from contact_list import recipient_dict
from io import open

cwd = os.getcwd()
excel_file_name = cwd + "\\august_26_2019.xlsx"  # change for each email date

duplicates = pd.read_excel(excel_file_name,
                           sheet_name="duplicates")
end_before_begin = pd.read_excel(excel_file_name,
                                 sheet_name="end_before_begin")
concept = pd.read_excel(excel_file_name,
                        sheet_name="concept")

prompt = "Please input the site ID of the HPO site that " \
         "you would like to use for an auto-generated e-mail. " \
         "(e.g. nyc_hh)\n"

hpo_id = input(prompt)
hpo_id = hpo_id.lower()  # case sensitivity

while hpo_id not in recipient_dict:
    print("HPO ID not found.")
    hpo_id = input(prompt)


# now we have the excel files and the appropriate HPO site


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
                          number is a 'flawed' record count versus
                          the percentage of 'acceptable' records
    succ_rate (boolean): used to determine if the sheet should only
                         be looking for the columns of the analytics
                         report that end with "success_rate"
    :return:
    err_dictionary (dictionary): key:value pairs represent the
                                 column and and number that represents
                                 the quality of the data
    """
    data_info = sheet.iloc[row_num, :]  # series, row labels and values
    err_dictionary = {}

    for col_label, number in data_info.iteritems():
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


row_idx_dups = determine_row(duplicates, hpo_id)
duplicate_err_dict = get_info(duplicates, row_idx_dups, percentage=False,
                              succ_rate=False)

# let's get the full name of the HPO. should be same across all sheets
hpo_full_name = 9999  # should be reset
row_info = duplicates.iloc[row_idx_dups, :]  # series, row labels and values

for col_name, val in row_info.iteritems():
    if col_name == 'HPO':
        hpo_full_name = val

if hpo_full_name == 9999:
    raise ValueError("No 'HPO' Column Title detected")

row_idx_end_before_begin = determine_row(end_before_begin, hpo_id)
end_before_begin_dict = get_info(end_before_begin,
                                 row_idx_end_before_begin, percentage=True,
                                 succ_rate=False)

row_idx_concept = determine_row(concept, hpo_id)
concept_dict = get_info(concept, row_idx_concept, percentage=True, succ_rate=True)

# RECAP: dictionaries storing the data quality errors for that particular site

# want to know the number of sheets w/ errors for numbering purposes
num_prob_sheets = 0
for sheet_info in [duplicate_err_dict,
                   end_before_begin_dict, concept_dict]:
    if sheet_info:
        num_prob_sheets += 1


# NOTE: I had trouble with character conversion when I wrote my
# messages directly in Pycharm text files. I recommend writing
# notes in other software environments (e.g. NotePad) and
# copying and pasting them to text files in this directory


def print_error_info(error_dict, starting_msg, percent):
    """
    Function is used to create a string to display the error
    information for each of the tables belonging to a particular sheet.
    :param
    err_dictionary (dictionary): key:value pairs represent the
                                 column and and number that
                                 represents the quality of the data
    starting_msg (str): the message to build off that
                        will ultimately be displayed
    percent (bool): determines whether the metrics to be displayed
                    should have a percent sign following their metric
    :return:
    starting_msg (str): the message that will display the metrics
                        regarding data quality
    """
    num_errs = len(error_dict)
    error_number_in_type = 1

    for table, value in error_dict.items():
        if num_errs == 1 or (num_errs == 2 and error_number_in_type == 1):
            starting_msg += " {} ({}% of data)".format(table, value)
            if num_errs == 1:
                starting_msg += "."
        elif error_number_in_type < num_errs:  # in a series
            starting_msg += " {} ({}% of data),".format(table, value)
        else:  # last in a series (2 or more errors total)
            starting_msg += " and {} ({}% of data).".format(table, value)

        error_number_in_type += 1

    # get rid of the percent if it should display the number of instances
    if not percent:
        starting_msg = starting_msg.replace('% of data', '')

    return starting_msg


# same for all of the sites
intro = open('introduction.txt', 'r', encoding='utf-8')
intro_txt = intro.read()
intro_txt = intro_txt.replace("[EHR site]", hpo_full_name)
print(intro_txt)

# Site is great
if num_prob_sheets == 0:
    great_job = open('great_job.txt', 'r')
    great_job_txt = great_job.read()
    print(great_job_txt)
else:
    starting_number = 1  # value for the 'problem number'

    print("We found the following error(s) with your most recent "
          "submission: \n")

    if duplicate_err_dict:  # there are problems with the error dict
        duplicates_err = str(starting_number) + \
                         ". There are row duplicates in the " \
                         "following table(s):"
        duplicates_err = print_error_info(duplicate_err_dict,
                                          duplicates_err, percent=False)
        starting_number += 1  # increment for future texts if applicable
        print(duplicates_err + "\n")

    if end_before_begin_dict:
        end_before_err = str(starting_number) + \
                         ". There are end dates before the start dates in the " \
                         "following table(s):"
        end_before_err = print_error_info(end_before_begin_dict,
                                          end_before_err, percent=True)
        starting_number += 1
        print(end_before_err + "\n")

    if concept_dict:
        concept_err = str(starting_number) + \
                      ". The concept success rates for some of " \
                      "the tables is below 100%. This affects the " \
                      "following table(s):"
        concept_err = print_error_info(concept_dict,
                                       concept_err,
                                       percent=True)
        print(concept_err + "\n")

github_link = 'https://github.com/all-of-us/curation/' \
              'tree/develop/data_steward/analytics'

sign_off = "Please aim to fix the issue(s) (if applicable) " \
           "from this emails and continue to routinely upload " \
           "your files so we can continue to track your " \
           "progress. You may also see the queries used " \
           "to generate these analytics at the following site: {}. " \
           "Do not hesitate to reach out to the " \
           "curation team if you have any " \
           "questions. ".format(github_link)

print(sign_off)

# now let's print the contacts
print("\nContact the following individual(s):")
print(recipient_dict[hpo_id])

print("\n")
print("Title: ")
title = "Data Check Feedback - [{}]".format(hpo_full_name)
print(title)
