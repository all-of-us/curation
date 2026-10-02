"""
Unit test for the common module.
"""
# Python imports
import unittest

# Third party imports
import jinja2

# Project imports
from common import JINJA_ENV


class JinjaEnvTest(unittest.TestCase):

    def test_unsupplied_variable_raises(self):
        """
        A variable the render call does not supply must raise.

        Without this policy jinja renders the missing name as an empty string.
        BigQuery accepts a table reference whose project segment is empty and
        resolves it against the job's default project, so a mismatched render
        keyword would retarget the query with nothing raised.
        """
        template = JINJA_ENV.from_string(
            'SELECT 1 FROM `{{project}}.{{dataset}}.observation`')

        with self.assertRaises(jinja2.UndefinedError):
            template.render(dataset='rdr')

        with self.assertRaises(jinja2.UndefinedError):
            template.render(project='aou-res-curation-prod')

    def test_supplied_variables_render(self):
        """
        A fully supplied template still renders, and renders qualified.
        """
        template = JINJA_ENV.from_string(
            'SELECT 1 FROM `{{project}}.{{dataset}}.observation`')

        rendered = template.render(project='aou-res-curation-prod',
                                   dataset='rdr')

        self.assertEqual(
            rendered, 'SELECT 1 FROM `aou-res-curation-prod.rdr.observation`')
