"""
File is intended to store some of the messages to be put into the program.
These messages can include:
    - Error messages
    - Text for the introduction/conclusion of the email
"""

introduction = \
    "Dear {ehr_site},\n\n" \
    "My name is {name} and I am a Data Analyst at Columbia University Medical Center with the All of Us precision medicine initiative. CUMC has been reviewing your file submissions for the past few weeks and would like to share its findings with you.\n\n" \
    "In this e-mail, we will list out any errors that apply to your most recent submission (as of {date}) that fall below the DRC's standards for the specified data quality metrics. Similarly, we will provide you with a 'dashboard' that lists out these errors in an Excel format.\n\n" \
    "Finally, we will provide you with images that display data quality values for all {num_metrics} monitored metrics, regardless of whether or not they meet the DRC's standards as noted here.\n\n"

fnf_error = \
    "{file} not found in the current directory: {cwd}. " \
    "Please ensure that the file names are " \
    "consistent between the Python script and the " \
    "file name in your current directory."

great_job = """
    All of the issues regarding the 11 monitored metrics have met or
    exceeded the DRC's expectations.
    """

sign_off = """
If have questions about our metrics, please consult the AoU EHR website at this link: {link} under the 'Data Quality Metrics' tab at the top of the page. This site contains descriptions, videos, and SQL queries that can help you troubleshoot your data quality.

Finally, please be sure to visit the 'results.html' file populated in your Google bucket to find other potential errors related to your EHR submissions.

Please also do not hesitate to reach out to the DRC at datacuration@researchallofus.org with any questions.

Best regards,
Noah Engel
Data Analyst | Columbia University Medical Center\n
"""

link = "https://sites.google.com/view/ehrupload"
