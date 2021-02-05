"""
File is intended to store some of the messages to be put into the program.
These messages can include:
    - Error messages
    - Text for the introduction/conclusion of the email
"""

introduction = """
Dear {ehr_site},

My name is {name} and I am a Data Analyst at Columbia University Medical Center with the All of Us initiative. Thank you so much for all your help and contributions to the All of Us program. We appreciate your hard work and effort, and would like to share some findings with you. 

In this e-mail, we will list out any issues that apply to your most recent submission (as of {date}) that fall below the DRC's standards for the specified data quality metrics. Similarly, we will provide you with a 'panel' that lists out these issues in an Excel format, tracking the first time of their appearance.

Finally, we will provide you with images that display data quality values for all monitored metrics, regardless of whether or not they meet the DRC's standards as noted here.\n\n"""

fnf_error = \
    """{file} not found in the current directory: {cwd}.
    Please ensure that the file names are
    consistent between the Python script and the
    file name in your current directory."""

great_job = """
All of the issues regarding the 12 monitored metrics have met or exceeded the DRC's expectations.
 """

sign_off = """
If you have questions about our metrics, please consult the AoU EHR website at this link: {link} under the 'Data Quality Metrics > Weekly Data Quality Metrics' tab at the top right corner of the page. This site contains descriptions, videos, and SQL queries that can help you troubleshoot your data quality.

Please also be sure to visit the 'results.html' file populated in your Google bucket to find other potential errors related to your EHR submissions.

Finally, do not hesitate to reach out to the DRC at {email} with any questions. We are also available for individual conference calls to walk through the report. Thank you for working with us on improving data quality.

Best regards,
Hongjue Wang
Data Analyst, All of Us Research Program
Columbia University Irving Medical Center\n
"""

link = "https://sites.google.com/view/ehrupload"

email = "datacuration@researchallofus.org"
