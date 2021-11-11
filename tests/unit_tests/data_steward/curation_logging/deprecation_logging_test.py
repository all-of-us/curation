import sys
import unittest
from contextlib import contextmanager
from io import StringIO

from deprecated import deprecated


class DeprecationLoggingTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        pass

    def test_deprecation_warning(self):

        with self.assertWarns(DeprecationWarning) as warn:
            self.deprecated_method()
            self.assertIn('@@@@@', str(warn.warnings[0].message))

    @deprecated(reason='@@@@@')
    def deprecated_method(self):
        pass
