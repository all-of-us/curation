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

analysis_type_prompt = \
        "\nWhat kind of analysis over time report would you like " \
        "to generate for each site?\n\n" \
        "A. Duplicates\n" \
        "B. Amount of data following death dates\n" \
        "C. Amount of data with end dates preceding start dates\n" \
        "D. Success rate for concept_id field\n" \
        "E. Population of the 'unit' field in the measurement table (" \
        "only for specified measurements)\n" \
        "F. Population of the 'route' field in the drug exposure table\n" \
        "G. Percentage of expected drug ingredients observed\n" \
        "H. Percentage of expected measurements observed\n" \
        "I. Date consistency across tables \n" \
        "J. Date/datetime inconsistencies \n\n" \
        "Please specify your choice by typing the corresponding letter."

output_prompt = \
    "\nWould you prefer to generate: \n" \
    "A. {} sheets detailing the data quality for each table. " \
    "The HPO IDs would be displayed as rows. \nor \n" \
    "B. {} sheets detailing the data quality for each HPO site. " \
    "The table type would be displayed as rows. This will " \
    "also include 1-3 table(s) with statistics on the " \
    "aggregate data for each table type on each date."


err_message_agg_for_table = \
    "AggregateMetricForTableOrClass object not found for " \
    "the following combination:" \
    "\n\tDate: {date}" \
    "\n\tMetric Type: {metric_type}" \
    "\n\tTable Or Class: {table_or_class}"

err_message_agg_for_date = \
    "AggregateMetricForDate object not found for the " \
    "following combination:" \
    "\n\tDate: {date}" \
    "\n\tMetric Type: {metric_type}"

err_message_agg_for_hpo = \
    "AggregateMetricForHPO object not found for the " \
    "following combination:" \
    "\n\tDate: {date}" \
    "\n\tHPO Name: {hpo_name}" \
    "\n\tMetric Type: {metric_type}"

fnf_error = \
    "{file} not found in the current directory: {cwd}. " \
    "Please ensure that the file names are " \
    "consistent between the Python script and the " \
    "file name in your current directory."
