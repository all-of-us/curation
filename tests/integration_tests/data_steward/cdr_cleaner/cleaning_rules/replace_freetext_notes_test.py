"""
Original Issues: DC-3607
"""

# Python Imports
import os
from datetime import date, datetime

# Project Imports
from common import NOTE
from common import JINJA_ENV
from app_identity import PROJECT_ID
from cdr_cleaner.cleaning_rules.replace_freetext_notes \
    import ReplaceFreeTextNotes
from tests.integration_tests.data_steward.cdr_cleaner.cleaning_rules.bigquery_tests_base \
    import BaseTest

from dateutil.parser import parse
import pytz


class ReplaceFreeTextNotesTest(BaseTest.CleaningRulesTestBase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

        super().initialize_class_vars()

        # Set the test project identifier
        cls.project_id = os.environ.get(PROJECT_ID)

        # Set the expected test datasets
        cls.dataset_id = os.environ.get('COMBINED_DATASET_ID')
        cls.sandbox_id = cls.dataset_id + '_sandbox'
        cls.rule_instance = ReplaceFreeTextNotes(cls.project_id, cls.dataset_id,
                                                 cls.sandbox_id)

        # Generates list of fully qualified table names and their corresponding sandbox table names
        for table_name in [NOTE]:
            cls.fq_table_names.append(
                f'{cls.project_id}.{cls.dataset_id}.{table_name}')

        # Add _logging_standard_concept_id_replacement to fq_sandbox_table_names for cleanup in
        # teardown
        # cls.fq_sandbox_table_names.append(
        #     f'{cls.project_id}.{cls.sandbox_id}.# {SRC_CONCEPT_ID_TABLE_NAME}')

        # call super to set up the client, create datasets
        cls.up_class = super().setUpClass()

    def setUp(self):

        # Create tables required for the test
        super().setUp()

        self._date = date(2023, 1, 1)
        self._datetime = parse('2023-01-01 00:00:00 UTC').astimezone(pytz.utc)

        note_table_data_template = JINJA_ENV.from_string("""
        INSERT INTO `{{project_id}}.{{dataset_id}}.note`
        (
                note_id,
                person_id,
                note_date,
                note_datetime,
                note_type_concept_id,
                note_class_concept_id,
                note_title,
                note_text,
                encoding_concept_id,
                language_concept_id
        )
        VALUES
              (1, 10, DATE('2023-01-01'), TIMESTAMP('2023-01-01 00:00:00'), 111, 222, 'note_title entry', 'NO_TEXT', 333, 444)
              -- (2, 20, DATE('2023-01-01'), TIMESTAMP('2023-01-01 00:00:00'), 111, 222, 'NO_TEXT', 'note_text entry', 333, 444), --
              -- (3, 30, DATE('2023-01-01'), TIMESTAMP('2023-01-01 00:00:00'), 111, 222, 'note_title entry', 'note_text entry', 333, 444), --
              -- (4, 40, DATE('2023-00-01'), TIMESTAMP('2023-11-28 16:22:00'), 111, 222, 'NO_TEXT', 'NO_TEXT', 333, 444) --
        """)

        insert_note_data_query = note_table_data_template.render(
            project_id=self.project_id, dataset_id=self.dataset_id)

        # Load test data
        self.load_test_data([f'{insert_note_data_query}'])

    def test_replace_freetext_notes(self):
        """
        note:
        record 1: a note_text entry thats not NO_TEXT
        record 2: a note_title entry thats not NO_TEXT
        record 3: both note_text and note_title have entries other than NO_TEXT
        record 4: both note_text and note_tile are correct and contain hold NO_TEXT
        NO_TEXT values
        """

        # Expected results list
        tables_and_counts = [{
            'fq_table_name':
                f'{self.project_id}.{self.dataset_id}.{NOTE}',
            'loaded_ids': [1,],  # 2, 3, 4],
            'fields': [
                'note_id', 'person_id', 'note_date', 'note_datetime',
                'note_type_concept_id', 'note_class_concept_id', 'note_title',
                'note_text', 'encoding_concept_id', 'language_concept_id'
            ],
            'cleaned_values': [
                (1, 10, self._date, self._datetime, 111, 222, 'NO_TEXT', 'NO_TEXT', 333, 444),
                (2, 20, self._date, self._datetime, 111, 222, 'NO_TEXT', 'NO_TEXT', 333, 444),
                (3, 30, self._date, self._datetime, 111, 222, 'NO_TEXT', 'NO_TEXT', 333, 444),
                (4, 40, self._date, self._datetime, 111, 222, 'NO_TEXT', 'NO_TEXT', 333, 444),
            ]
        }]

        self.default_test(tables_and_counts)