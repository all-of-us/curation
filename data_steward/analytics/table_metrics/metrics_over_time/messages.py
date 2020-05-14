"""
File is intended to store the 'messages' (e.g. the prompts that the
user sees, error messages, etc.) that are used in the script

Messages
-------
analysis_type_prompt: used to determine what data quality metric the user
    would like to analyze

output_prompt: allows the user to decide whether s/he wants 'table-based'
    sheets or 'HPO-based' sheets

err_message_agg_for_table: displays the appropriate error message when the
    AggregateMetricForTableOrClass can not be found with the designated
    attributes

err_message_agg_for_date: displays the appropriate error message when the
    AggregateMetricForDate can not be found with the designated
    attributes

err_message_agg_for_hpo: displays the appropriate error message when the
    AggregateMetricForHPO can not be found with the designated
    attributes

fnf_error: error message to display when a FileNotFoundError is
    encountered

"""

analysis_type_prompt = """
What kind of analysis over time report would you like
to generate for each site?

A. Duplicates
B. Amount of data following death dates
C. Amount of data with end dates preceding start dates
D. Success rate for concept_id field
E. Population of the 'unit' field in the measurement table (
   only for specified measurements)
F. Population of the 'route' field in the drug exposure table
G. Percentage of expected drug ingredients observed
H. Percentage of expected measurements observed
I. Date consistency across tables
J. Date/datetime inconsistencies
K. Erroneous dates
L. Person ID failure rate
M. Number of ACHILLES Errors

Please specify your choice by typing the corresponding letter.
"""

output_prompt = \
    """
Would you prefer to generate:
A. {} sheets detailing the data quality for each table.
The HPO IDs would be displayed as rows.

or

B. {} sheets detailing the data quality for each HPO site.
The table type would be displayed as rows. This will
also include 1-3 table(s) with statistics on the
aggregate data for each table type on each date.
"""

err_message_agg_for_table = """
AggregateMetricForTableOrClass object not found for
the following combination:
    Date: {date}
    Metric Type: {metric_type}
    Table Or Class: {table_or_class}
"""

err_message_agg_for_date = """
AggregateMetricForDate object not found for the
following combination:
    Date: {date}
    Metric Type: {metric_type}
"""

err_message_agg_for_hpo = """
AggregateMetricForHPO object not found for the
following combination:
    Date: {date}
    HPO Name: {hpo_name}
    Metric Type: {metric_type}
"""

fnf_error = """
{file} not found in the current directory: {cwd}.
Please ensure that the file names are
consistent between the Python script and the
file name in your current directory.
"""
