import os
import unittest
from tempfile import NamedTemporaryFile
from pathlib import PurePath
from bs4 import BeautifulSoup as bs

from analytics.cdr_ops.report_runner import IPYNB_SUFFIX, HTML_SUFFIX, main

TEST_NOTEBOOK = """
# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.7.1
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# + tags=["parameters"]
project_id = ''
dataset_id = ''
table_name = ''
# -

print(
    f'project_id={project_id}, dataset_id={dataset_id}, table_name={table_name}'
)
"""


class ReportRunnerTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.temp_notebook_py_file = NamedTemporaryFile('w',
                                                        suffix='.py',
                                                        delete=True)
        self.temp_notebook_py_file.write(TEST_NOTEBOOK.strip())
        self.temp_notebook_py_file.flush()

        self.notebook_py_path = self.temp_notebook_py_file.name
        self.notebook_ipynb_path = PurePath(
            self.notebook_py_path).with_suffix(IPYNB_SUFFIX)
        self.notebook_html_path = PurePath(
            self.notebook_py_path).with_suffix(HTML_SUFFIX)

        self.parameters = {
            'project_id': 'project_id',
            'dataset_id': 'dataset_id',
            'table_name': 'condition'
        }

    def tearDown(self):
        # This removes the python file automatically
        self.temp_notebook_py_file.close()
        # Remove the ipynb and html files
        os.remove(self.notebook_ipynb_path)
        os.remove(self.notebook_html_path)

    def test_main(self):
        # Running the notebook and saving to the HTML page
        main(self.notebook_py_path, self.parameters, self.notebook_py_path)

        # Testing the content of the HTML page
        with open(self.notebook_html_path, 'r') as f:
            soup = bs(f, parser="lxml", features="lxml")
            all_div_elements = soup.findAll('div', {"class": "output_text"})
            self.assertEqual(len(all_div_elements), 1)
            all_pre_elements = all_div_elements[0].findAll('pre')
            self.assertEqual(len(all_pre_elements), 1)
            actual = all_pre_elements[0].get_text().strip()
            expected = ', '.join(
                [f'{k}={v}' for k, v in self.parameters.items()])
            self.assertEqual(actual, expected)
