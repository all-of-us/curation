import argparse
from papermill.execute import execute_notebook
# from papermill.inspection import  display_notebook_help
from papermill import inspect_notebook
from papermill.exceptions import PapermillExecutionError
import jupytext
from pathlib import PurePath
from nbconvert import HTMLExporter
import nbconvert
import nbformat
import sys
import logging
import copy
from collections import OrderedDict
from typing import List, Tuple

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
    with open(surrogate_output_path, 'r') as f:
        written_nb = nbformat.reads(f.read(), as_version=4)
        (body, resources) = html_exporter.from_notebook_node(written_nb)
    with open(output_path, 'w') as f:
        f.write(body)

    LOGGER.info(f'Notebook exported to {output_path}')

    return True


def infer_notebook_params(notebook_path) -> List[Tuple[str, OrderedDict]]:
    """
    A helper function to infer the notebook params 

    :param notebook_path: 
    :return: 
    """

    def infer_required(ordered_dict: OrderedDict) -> OrderedDict:
        ordered_dict_copy = copy.deepcopy(ordered_dict)
        for key, value in ordered_dict.items():
            if key == PARAMETER_DEFAULT:
                required = (value == PARAMETER_NONE_VALUE) or (
                    not value.replace('"', ''))
                ordered_dict_copy[PARAMETER_REQUIRED] = required
                break
        return ordered_dict_copy

    return [(name, infer_required(properties))
            for name, properties in inspect_notebook(notebook_path).items()]


def display_notebook_help(notebook_path):
    """
    A helper function to display  

    :param notebook_path: 
    :return: 
    """
    LOGGER.info(
        f'Parameters inferred for notebook {PurePath(notebook_path).stem}:')
    for _, properties in infer_notebook_params(notebook_path):
        properties = ', '.join(
            [f'{key}={value}' for key, value in properties.items()])
        LOGGER.info(f'Parameter name: {properties}')


def validate_notebook_params(notebook_path, provided_params):
    """
    This function validates the provided parameters passed to the notebook 
    
    :param notebook_path: 
    :param provided_params: provided parameters from the arg parser 
    :return: 
    """

    def is_parameter_required(properties: OrderedDict):
        for key, value in properties.items():
            if key == PARAMETER_REQUIRED:
                return value

    notebook_param_dict = dict(infer_notebook_params(notebook_path))

    missing_parameters = [
        (name, properties)
        for name, properties in notebook_param_dict.items()
        if (name not in provided_params) & (is_parameter_required(properties))
    ]

    missing_values = [
        (param, value) for param, value in provided_params.items() if not value
    ]

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
        sys.exit(display_notebook_help(surrogate_input_path))

    if not validate_notebook_params(surrogate_input_path, params):
        display_notebook_help(surrogate_input_path)
        sys.exit('Missing required parameters')

    try:
        # Pass ipynb to papermill
        execute_notebook(surrogate_input_path,
                         surrogate_output_path,
                         parameters=params)
        create_html_from_ipynb(surrogate_output_path)
    except Exception as e:
        LOGGER.error(e)


if __name__ == '__main__':
    pipeline_logging.configure(logging.INFO, add_console_handler=True)

    parser = argparse.ArgumentParser()
    parser.add_argument('--notebook_path',
                        help='A .py jupytext file.',
                        required=True)
    parser.add_argument('--output_path',
                        default="",
                        help='An output .html file')
    parser.add_argument('--help_notebook', action='store_true')
    parser.add_argument('--params', '-p', nargs=2, action='append')

    args = parser.parse_args()
    parsed_params = dict(args.params) if args.params else dict()

    main(args.notebook_path, parsed_params, args.output_path,
         args.help_notebook)
