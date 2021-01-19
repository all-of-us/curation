# Python imports
import os
from unittest import TestCase

# Third party imports
import google.cloud.bigquery as gbq

# Project imports
import app_identity
import retraction.retract_utils as ru


class RetractUtilsTest(TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.project_id = app_identity.get_application_id()
        self.dataset_id = os.environ.get('UNIONED_DATASET_ID')
        self.client = gbq.Client()

    def test_is_labeled_deid(self):
        dataset = self.client.get_dataset(
            f'{self.project_id}.{self.dataset_id}')
        dataset.labels = {"de_identified": "true"}
        self.client.update_dataset(dataset, ["labels"])
        actual = ru.is_labeled_deid(self.client, self.project_id,
                                    self.dataset_id)
        self.assertTrue(actual)

        dataset = self.client.get_dataset(
            f'{self.project_id}.{self.dataset_id}')
        dataset.labels = {"de_identified": "false"}
        self.client.update_dataset(dataset, ["labels"])
        actual = ru.is_labeled_deid(self.client, self.project_id,
                                    self.dataset_id)
        self.assertFalse(actual)

        dataset = self.client.get_dataset(
            f'{self.project_id}.{self.dataset_id}')
        dataset.labels = {"de_identified": None}
        self.client.update_dataset(dataset, ["labels"])
        actual = ru.is_labeled_deid(self.client, self.project_id,
                                    self.dataset_id)
        self.assertIsNone(actual)
