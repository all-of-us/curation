# Python imports
from unittest import TestCase

# Third party imports

# Project imports
from gcloud.bq import BigQueryClient


class BQCTest(TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')
