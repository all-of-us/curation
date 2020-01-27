# Python imports
import os
import unittest

# Third party imports

# Project imports
from validation import achilles
from validation import sql_wrangle


class AchillesTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print(
            '\n**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.achilles_analysis_count = 134
        self.hpo_id = 'foo_bar'

    def tearDown(self):
        pass

    def test_get_run_analysis_commands(self):
        # pre-conditions
        cmd_iter = achilles._get_run_analysis_commands(self.hpo_id)
        commands = list(cmd_iter)

        # test
        self.assertEqual(len(commands), self.achilles_analysis_count)

        for command in commands:
            is_temp = sql_wrangle.is_to_temp_table(command)
            self.assertFalse(is_temp, command)
