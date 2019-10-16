from IPython.display import display, HTML
from IPython.display import Markdown as md


def dataframe(df):
    if len(df) == 0:
        html = HTML('<div class="alert alert-info">There are no records in the dataframe.</div>')
    else:
        html = HTML(df.to_html())
    display(html)
