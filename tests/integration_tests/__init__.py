import unittest

# Hides first line of function docstring in test output
unittest.TestCase.shortDescription = lambda x: '\t'
