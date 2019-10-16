from IPython.display import display, HTML
from IPython.display import Markdown as md
import qgrid


def dataframe(df):
    if len(df) == 0:
        html = HTML('<div class="alert alert-info">There are no records in the dataframe.</div>')
        return display(html)
    else:
        return qgrid.show_grid(df, )
