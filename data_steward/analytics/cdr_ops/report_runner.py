# -*- coding: utf-8 -*-
"""Main `papermill` interface."""

import os
import sys
import tempfile
from stat import S_ISFIFO
import nbclient
import nbconvert
import traceback

import base64
import logging
from pathlib import PurePath

import click

import yaml
import platform

from papermill.execute import execute_notebook
from papermill.inspection import display_notebook_help
import jupytext

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


class NotebookFileParamType(click.Path):
    name = "notebook_file"

    def __init__(self, exists=False):
        super().__init__(exists=exists, dir_okay=False)

    def convert(self, value, param, ctx):
        super().convert(value, param, ctx)
        extension_whitelist = ['.ipynb', '.py', '.json']
        try:
            p = PurePath(value)
            # if self.exists and not p.exists():
            #     self.fail(f'file {value} does not exist', param, ctx)

            # if p.is_dir():
            #     self.fail()

            ext = p.suffix
            if ext not in extension_whitelist:
                self.fail(
                    f'expected notebook file to have extension in {extension_whitelist}',
                    param, ctx)

            return p

        except Exception as e:
            self.fail(e, param, ctx)


@click.command(context_settings=dict(help_option_names=['-h', '--help']))
@click.pass_context
@click.argument('notebook_path',
                required=not INPUT_PIPED,
                type=NotebookFileParamType(exists=True))
@click.argument('output_path', default="", type=click.Path())
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
    """This utility executes a single notebook in a subprocess.
    Papermill takes a source notebook, applies parameters to the source
    notebook, executes the notebook with the specified kernel, and saves the
    output in the destination notebook.
    The NOTEBOOK_PATH and OUTPUT_PATH can now be replaced by `-` representing
    stdout and stderr, or by the presence of pipe inputs / outputs.
    Meaning that
    `<generate input>... | papermill | ...<process output>`
    with `papermill - -` being implied by the pipes will read a notebook
    from stdin and write it out to stdout.
    """
    if not help_notebook:
        required_output_path = not (INPUT_PIPED or OUTPUT_PIPED)
        if required_output_path and not output_path:
            raise click.UsageError("Missing argument 'OUTPUT_PATH'")

    if INPUT_PIPED and notebook_path and not output_path:
        input_path = '-'
        output_path = notebook_path
    else:
        input_path = notebook_path or '-'
        output_path = output_path or '-'

    if output_path == '-':

        # Reduce default log level if we pipe to stdout
        if log_level == 'INFO':
            log_level = 'ERROR'

    elif progress_bar is None:
        progress_bar = not log_output

    logging.basicConfig(level=log_level, format="%(message)s")

    # Read in Parameters
    parameters_final = {}
    for name, value in parameters or []:
        parameters_final[name] = _resolve_type(value)

    if help_notebook:
        sys.exit(
            display_notebook_help(click_ctx, notebook_path, parameters_final))

    if notebook_path.suffix == '.ipynb':
        pass
    elif notebook_path.suffix == '.py':
        converted_nb = jupytext.read(notebook_path)
        converted_nb_path = f'{notebook_path.stem}2.ipynb'
        converted_nb_f = tempfile.NamedTemporaryFile(suffix='.ipynb')
        jupytext.write(converted_nb, converted_nb_f.name)
        input_path = converted_nb_f.name

    try:
        execute_notebook(input_path=input_path,
                         output_path=output_path,
                         parameters=parameters_final,
                         progress_bar=progress_bar,
                         log_output=log_output)
    except nbclient.exceptions.DeadKernelError:
        # Exiting with a special exit code for dead kernels
        traceback.print_exc()
        sys.exit(138)


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