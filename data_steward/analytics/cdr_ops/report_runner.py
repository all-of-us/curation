# -*- coding: utf-8 -*-
"""Main `papermill` interface."""

import os
import sys
import tempfile
from stat import S_ISFIFO
import nbclient
import nbconvert
import nbformat
import traceback

import logging
from pathlib import PurePath

import click

import platform

from papermill.execute import execute_notebook
from papermill.inspection import display_notebook_help
from papermill.exceptions import PapermillExecutionError
import jupytext
from nbconvert import HTMLExporter

from papermill import __version__ as papermill_version

click.disable_unicode_literals_warning = True

INPUT_PIPED = S_ISFIFO(os.fstat(0).st_mode)
OUTPUT_PIPED = not sys.stdout.isatty()


def print_papermill_version(ctx, param, value):
    if not value:
        return
    print("{version} from {path} ({pyver})".format(
        version=papermill_version,
        path=__file__,
        pyver=platform.python_version()))
    ctx.exit()


def create_ipynb_from_py(py_path):
    """Create an .ipynb notebook file from a Jupytext .py file

    :param py_path: path to a Jupytext-generated .py file
    :type py_path: path-like
    :return: path to a newly created .ipynb file
    :rtype: path-like
    """
    py_path = PurePath(py_path)  # if not already
    converted_nb = jupytext.read(py_path)

    ipynb_path = py_path.with_suffix('.ipynb')
    jupytext.write(converted_nb, ipynb_path)

    return ipynb_path


class NotebookFileParamType(click.Path):
    name = "notebook_file"

    def __init__(self, exists=False, extension_whitelist=['.ipynb', '.py']):
        """Custom click param type that checks notebook format

        :param exists: indicates if file must already exist, defaults to False
        :type exists: bool, optional
        :param extension_whitelist: list of allowed file extensions, defaults to ['.ipynb', '.py']
        :type extension_whitelist: list, optional
        """
        super().__init__(exists=exists, dir_okay=False)
        self.extension_whitelist = extension_whitelist

    def convert(self, value, param, ctx):
        super().convert(value, param, ctx)
        extension_whitelist = self.extension_whitelist
        try:
            if value:
                p = PurePath(value)

                ext = p.suffix
                if value and ext not in extension_whitelist:
                    self.fail(
                        f'expected notebook file to have extension in {extension_whitelist}',
                        param, ctx)

                return p
            else:
                return value

        except Exception as e:
            self.fail(e, param, ctx)


@click.command(context_settings=dict(help_option_names=['-h', '--help']))
@click.pass_context
@click.argument('notebook_path',
                type=NotebookFileParamType(
                    exists=True, extension_whitelist=['.ipynb', '.py']))
@click.argument('output_path',
                default="",
                type=NotebookFileParamType(
                    exists=False,
                    extension_whitelist=['.ipynb', '.py', '.html']))
@click.option(
    '--help-notebook',
    is_flag=True,
    default=False,
    help='Display parameters information for the given notebook path.',
)
@click.option('--parameters',
              '-p',
              nargs=2,
              multiple=True,
              help='Parameters to pass to the parameters cell.')
@click.option('--progress-bar/--no-progress-bar',
              default=None,
              help="Flag for turning on the progress bar.")
@click.option(
    '--log-output/--no-log-output',
    default=False,
    help="Flag for writing notebook output to the configured logger.",
)
@click.option(
    '--log-level',
    type=click.Choice(
        ['NOTSET', 'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']),
    default='INFO',
    help='Set log level',
)
def papermill(click_ctx, notebook_path, output_path, help_notebook, parameters,
              progress_bar, log_output, log_level):
    """This utility executes a single notebook.
    Papermill takes a source notebook, applies parameters to the source
    notebook, executes the notebook with the specified kernel, and saves the
    output in the destination notebook.
    The NOTEBOOK_PATH is expected to point to an .ipynb file or a Jupytext-generated
    .py file. 
    The OUTPUT_PATH is optional. If empty, it will output to an html file named
    [NOTEBOOK PATH].html. If valued, path must end in .html, .ipynb, .py.
    """
    # if not help_notebook:
    #     required_output_path = not (INPUT_PIPED or OUTPUT_PIPED)
    #     if required_output_path and not output_path:
    #         raise click.UsageError("Missing argument 'OUTPUT_PATH'")

    if progress_bar is None:
        progress_bar = not log_output

    # logging.basicConfig(level=log_level, format="%(message)s")

    # Read in Parameters
    parameters_final = {}
    for name, value in parameters or []:
        parameters_final[name] = _resolve_type(value)

    #Create a corresponding ipynb file for a py file in order to be inspected
    surrogate_notebook_path = notebook_path
    if notebook_path.suffix == '.py':
        surrogate_notebook_path = create_ipynb_from_py(notebook_path)

    if help_notebook:
        sys.exit(
            display_notebook_help(click_ctx, str(surrogate_notebook_path),
                                  parameters_final))

    #Assign default output_path if not already provided
    if not output_path:
        output_path = notebook_path.with_suffix('.html')
        print(output_path)

    #Create temporary ipynb files to output notebooks,
    #   although they may not necessarily be final output
    output_conversion_req = False
    surrogate_output_path = output_path
    if output_path.suffix == '.ipynb':
        surrogate_output_path = PurePath(output_path)
        output_conversion_req = False
    elif output_path.suffix == '.py':
        surrogate_output_f = tempfile.NamedTemporaryFile(suffix='.ipynb')
        surrogate_output_path = PurePath(surrogate_output_f.name)
        output_conversion_req = True
    elif output_path.suffix == '.html':
        surrogate_output_f = tempfile.NamedTemporaryFile(suffix='.ipynb')
        surrogate_output_path = PurePath(surrogate_output_f.name)
        output_conversion_req = True

    try:
        execute_notebook(input_path=str(surrogate_notebook_path),
                         output_path=str(surrogate_output_path),
                         parameters=parameters_final,
                         progress_bar=progress_bar,
                         log_output=log_output)
    except nbclient.exceptions.DeadKernelError:
        # Exiting with a special exit code for dead kernels
        traceback.print_exc()
        sys.exit(138)
    except PapermillExecutionError:
        traceback.print_exc()

    #Export notebook, even if errors occurred during execution
    if output_conversion_req:
        if output_path.suffix == '.py':
            written_nb = jupytext.read(surrogate_output_path)
            jupytext.write(written_nb, output_path)

        elif output_path.suffix == '.html':
            html_exporter = HTMLExporter()
            html_exporter.template_name = 'classic'
            with open(surrogate_output_path, 'r') as f:
                written_nb = nbformat.reads(f.read(), as_version=4)
            (body, resources) = html_exporter.from_notebook_node(written_nb)
            with open(output_path, 'w') as f:
                f.write(body)

    print(f'Exported notebook to {output_path}')


def _resolve_type(value):
    if value == "True":
        return True
    elif value == "False":
        return False
    elif value == "None":
        return None
    elif _is_int(value):
        return int(value)
    elif _is_float(value):
        return float(value)
    else:
        return value


def _is_int(value):
    """Use casting to check if value can convert to an `int`."""
    try:
        int(value)
    except ValueError:
        return False
    else:
        return True


def _is_float(value):
    """Use casting to check if value can convert to a `float`."""
    try:
        float(value)
    except ValueError:
        return False
    else:
        return True


if __name__ == '__main__':
    papermill()