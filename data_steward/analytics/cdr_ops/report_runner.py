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


def display_notebook_help(notebook_path, params):
    expected_params = inspect_notebook(notebook_path)
    notebook_name = PurePath(notebook_path).stem
    print(f'Parameters inferred for notebook {notebook_name}:')
    for name, param in expected_params.items():
        print(name)


# Usage: report_runner.py [OPTIONS] NOTEBOOK_PATH [OUTPUT_PATH]

# Parameters inferred for notebook 'rdr_export_qc.ipynb':
#   project_id: Unknown type (default "")
#   old_rdr: Unknown type (default "")
#   new_rdr: Unknown type (default "")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('notebook_path', help='A .py jupytext file.')
    parser.add_argument('output_path', help='An output .html file')
    parser.add_argument('--help_notebook', action='store_true')
    parser.add_argument('--params', '-p', nargs=2, action='append')

    args = parser.parse_args()
    notebook_path = args.notebook_path
    output_path = args.output_path
    help_notebook = args.help_notebook
    params = dict(args.params)

    #Convert py to ipynb
    create_ipynb_from_py(notebook_path)
    notebook_path = PurePath(notebook_path).with_suffix('.ipynb')

    if help_notebook:
        sys.exit(display_notebook_help(str(notebook_path), params))

    surrogate_output_path = PurePath(output_path).with_suffix('.ipynb')

    try:
        #Pass ipynb to papermill
        execute_notebook(str(notebook_path),
                         str(surrogate_output_path),
                         parameters=params)
    except Exception as e:
        print(e)

    #Convert output ipynb to html

    html_exporter = HTMLExporter()
    html_exporter.template_name = 'classic'
    with open(surrogate_output_path, 'r') as f:
        written_nb = nbformat.reads(f.read(), as_version=4)
        (body, resources) = html_exporter.from_notebook_node(written_nb)
    with open(output_path, 'w') as f:
        f.write(body)


if __name__ == '__main__':
    main()