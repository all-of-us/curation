# Python imports
from unittest import TestCase
import os
import typing

# Third party imports

# Project imports
from gcloud.bq import BigQueryClient
import resources


class DummyClient(BigQueryClient):
    """
    A class which inherits all of StorageClient but doesn't authenticate
    """

    # pylint: disable=super-init-not-called
    def __init__(self):
        pass

    def _get_all_field_types(self,) -> typing.FrozenSet[str]:
        """
        Helper to get all field types referenced in fields (json) files

        :return: names of all types in fields files
        """
        all_field_types = set()
        for _, dir_paths, files in os.walk(resources.fields_path):
            for dir_path in dir_paths:
                for fields_file in files:
                    table, _ = os.path.splitext(fields_file)
                    try:
                        fields = resources.fields_for(table, sub_path=dir_path)
                    except RuntimeError:
                        pass
                    else:
                        for field in fields:
                            all_field_types.add(field.get('type'))
        return frozenset(all_field_types)


class BQCTest(TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        self.client = DummyClient()

    def test_get_table_schema(self):
        actual_fields = self.client.get_table_schema(
            'digital_health_sharing_status')

        for field in actual_fields:
            if field.field_type.upper() == "RECORD":
                self.assertEqual(len(field.fields), 2)

    def test_to_standard_sql_type(self):
        # All types used in schema files should successfully map to standard sql types
        all_field_types = self.client._get_all_field_types()
        for field_type in all_field_types:
            result = self.client._to_standard_sql_type(field_type)
            self.assertTrue(result)

        # Unknown types should raise ValueError
        with self.assertRaises(ValueError) as c:
            self.client._to_standard_sql_type('unknown_type')
            self.assertEqual(str(c.exception),
                             f'unknown_type is not a valid field type')