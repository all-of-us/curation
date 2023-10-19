# System imports
import re
import sys
import logging
import copy
import argparse
from enum import Enum
from pathlib import Path, PurePath
from collections import OrderedDict
from typing import List, Tuple, Dict

# Papermill and Jupytext imports
from papermill.execute import execute_notebook
from papermill import inspect_notebook
from papermill.exceptions import PapermillExecutionError
import jupytext
import nbformat
import nbclient
from nbconvert import HTMLExporter

# Project imports
from utils import pipeline_logging

LOGGER = logging.getLogger(__name__)
IPYNB_SUFFIX = '.ipynb'
HTML_SUFFIX = '.html'
PARAMETER_DEFAULT = 'default'
PARAMETER_REQUIRED = 'required'
PARAMETER_NONE_VALUE = 'None'


def create_ipynb_from_py(py_path) -> str:
    """Create an .ipynb notebook file from a Jupytext .py file

    :param py_path: path to a Jupytext-generated .py file
    :type py_path: path-like
    :return: path to a newly created .ipynb file
    :rtype: path-like
    """
    py_path = PurePath(py_path)  # if not already
    converted_nb = jupytext.read(py_path)
    ipynb_path = py_path.with_suffix(IPYNB_SUFFIX)
    jupytext.write(converted_nb, ipynb_path)
    return str(ipynb_path)


def create_html_from_ipynb(surrogate_output_path):
    """
    Create a html page from the output of the jupyter notebook
    :param surrogate_output_path:
    :return:
    """
    # Convert output ipynb to html
    output_path = PurePath(surrogate_output_path).with_suffix(HTML_SUFFIX)

    html_exporter = HTMLExporter()
    html_exporter.template_name = 'classic'
    with open(surrogate_output_path, 'r', encoding='utf-8') as f:
        written_nb = nbformat.reads(f.read(), as_version=4)
        (body, resources) = html_exporter.from_notebook_node(written_nb)
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(body)

    LOGGER.info(f'Notebook exported to {output_path}')

    return True


def infer_required(param_properties: OrderedDict) -> OrderedDict:
    """
    This function infers whether the notebook parameter is required or not based on the following
    heuristics: if the default value is 'None' (notebook translates None to a string version of
    None) or '""' or '\'\'' (string consists of double quotes or single quotes only)

    :param param_properties:
    :return:
    """

    def is_required(param_value):
        return (param_value is None) or (param_value == PARAMETER_NONE_VALUE) \
               or (not re.sub('["\']', '', param_value))

    ordered_dict_copy = copy.deepcopy(param_properties)
    for key, value in param_properties.items():
        if key == PARAMETER_DEFAULT:
            required = is_required(value)
            ordered_dict_copy[PARAMETER_REQUIRED] = required
            break
    return ordered_dict_copy


def infer_notebook_params(notebook_path) -> List[Tuple[str, OrderedDict]]:
    """
    A helper function to infer the notebook params

    :param notebook_path:
    :return:
    """

    return [(name, infer_required(properties))
            for name, properties in inspect_notebook(notebook_path).items()]


def display_notebook_help(notebook_path):
    """
    A helper function to display

    :param notebook_path:
    :return:
    """
    print(f'Parameters inferred for notebook {PurePath(notebook_path).stem}:')
    for _, properties in infer_notebook_params(notebook_path):
        type_repr = properties["inferred_type_name"]
        if type_repr == "None":
            type_repr = "Unknown type"

        definition = "  {}: {} (default {}, required={})".format(
            properties["name"], type_repr, properties["default"],
            properties['required'])
        if len(definition) > 30:
            if len(properties["help"]):
                param_help = "".join(
                    (definition, "\n", 34 * " ", properties["help"]))
            else:
                param_help = definition
        else:
            param_help = "{:<34}{}".format(definition, properties["help"])

        print(f'{param_help}')


def is_parameter_required(properties: OrderedDict):
    """
    This functions checks if the notebook parameter is required
    :param properties: the properties associated with the parameter
    :return:
    """
    for key, value in properties.items():
        if key == PARAMETER_REQUIRED:
            return value
    return True


def validate_notebook_params(notebook_path, provided_params: Dict[str, str]):
    """
    This function validates the provided parameters passed to the notebook

    :param notebook_path:
    :param provided_params: provided parameters from the arg parser
    :return:
    """

    notebook_param_dict = dict(infer_notebook_params(notebook_path))

    missing_parameters = [
        (name, properties)
        for name, properties in notebook_param_dict.items()
        if (name not in provided_params) & (is_parameter_required(properties))
    ]

    missing_values = [(param, value)
                      for param, value in provided_params.items()
                      if value in ("", None)]

    unknown_parameters = [(name, value)
                          for name, value in provided_params.items()
                          if name not in notebook_param_dict]

    if missing_parameters:
        for name, param in missing_parameters:
            LOGGER.error(
                f'Missing the parameter {name} for notebook {PurePath(notebook_path).stem}'
            )

    if missing_values:
        for name, value in missing_values:
            LOGGER.error(f'Missing value for the parameter {name} for notebook '
                         f'{PurePath(notebook_path).stem}')

    if unknown_parameters:
        for name, value in unknown_parameters:
            LOGGER.error(
                f'Unknown parameters provided: {name}={value} for notebook '
                f'{PurePath(notebook_path).stem}')

    return (not missing_parameters) & (not missing_values) & (
        not unknown_parameters)


# Usage: report_runner.py [OPTIONS] NOTEBOOK_PATH [OUTPUT_PATH]

# Parameters inferred for notebook 'rdr_export_qc.ipynb':
#   project_id: Unknown type (default "")
#   old_rdr: Unknown type (default "")
#   new_rdr: Unknown type (default "")


def main(notebook_jupytext_path, params, output_path, help_notebook=False):
    """

    :param notebook_jupytext_path:
    :param params:
    :param output_path:
    :param help_notebook:
    :return:
    """

    # Output name defaults to ipynb_path if the output_path is an empty string

    # Convert py to ipynb
    surrogate_input_path = create_ipynb_from_py(notebook_jupytext_path)
    surrogate_output_path = str(
        PurePath(output_path if output_path else notebook_jupytext_path).
        with_suffix(IPYNB_SUFFIX))

    if help_notebook:
        display_notebook_help(surrogate_input_path)
        sys.exit(0)

    if not validate_notebook_params(surrogate_input_path, params):
        display_notebook_help(surrogate_input_path)
        sys.exit(1)

    try:
        # Pass ipynb to papermill
        execute_notebook(surrogate_input_path,
                         surrogate_output_path,
                         parameters=params)
    except nbclient.exceptions.DeadKernelError as e:
        # Exiting with a special exit code for dead kernels
        LOGGER.error(e)
        sys.exit(138)
    except PapermillExecutionError as e:
        LOGGER.error(e)
        create_html_from_ipynb(surrogate_output_path)
    else:
        create_html_from_ipynb(surrogate_output_path)


class FileType(Enum):
    INPUT = 'input'
    OUTPUT = 'output'


class NotebookFileParamType(object):

    def __init__(self, file_type: FileType):
        self._file_type = file_type

    def __call__(self, value, **kwargs):
        if (self._file_type == FileType.INPUT) and (not Path(value).exists()):
            raise argparse.ArgumentTypeError(
                f'{value} is not a valid input path')

        if self._file_type == FileType.OUTPUT:
            # If the parent folder doesn't exist, the validation should fail. The only exception
            # is when the output folder is an empty string, in which case it infers the output
            # path based on the input path
            if (not Path(value).parent.exists()) and value:
                raise argparse.ArgumentTypeError(
                    f'The parent folder {Path(value).parent} folder doesn`t exist'
                )

        return value


if __name__ == '__main__':
    pipeline_logging.configure(logging.INFO, add_console_handler=True)

    parser = argparse.ArgumentParser(
        description=
        "Executes a jupyter notebook with parameters and outputs results to HTML."
    )
    parser.add_argument('notebook_path',
                        help='A .py jupytext file',
                        type=NotebookFileParamType(FileType.INPUT))
    parser.add_argument(
        '--output_path',
        default="",
        type=NotebookFileParamType(FileType.OUTPUT),
        help=
        'An output .html file. If not provided, defaults to an html path with name of [notebook path]'
    )
    parser.add_argument(
        '--help_notebook',
        action='store_true',
        help="Lists the accepted parameters for [notebook_path]")
    parser.add_argument(
        '--params',
        '-p',
        nargs=2,
        action='append',
        metavar=('PARAM_NAME', 'PARAM_VALUE'),
        help="A parameter to pass to [notebook path] (multiple may be provided)"
    )

    args = parser.parse_args()
    parsed_params = dict(args.params) if args.params else dict()

    main(args.notebook_path, parsed_params, args.output_path,
         args.help_notebook)
