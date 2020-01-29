from IPython.display import display, HTML
from IPython.display import Markdown as md
import qgrid

DEFAULT_GRID_OPTIONS = dict(enableColumnReorder=True)


def dataframe(df, grid_options=None):
    if grid_options is None:
        grid_options = DEFAULT_GRID_OPTIONS
    if len(df) == 0:
        html = HTML(
            '<div class="alert alert-info">There are no records in the dataframe.</div>'
        )
        return display(html)
    else:
        return qgrid.show_grid(df, grid_options=grid_options)
