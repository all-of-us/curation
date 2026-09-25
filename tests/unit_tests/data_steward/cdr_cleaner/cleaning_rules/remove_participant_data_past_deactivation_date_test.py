"""
Unit tests for the `rdr_sandbox_dataset_id` contract of
RemoveParticipantDataPastDeactivationDate.

The parameter names the RDR stage sandbox holding the adult to pediatric pairs.
It has no default, so `get_custom_kwargs` fails a run that omits it rather than
letting the rule cascade to nobody.

Original Issues: DL2486
"""
# Python imports
import unittest

# Project imports
from cdr_cleaner.clean_cdr_engine import get_custom_kwargs
from cdr_cleaner.cleaning_rules.remove_participant_data_past_deactivation_date import (
    RemoveParticipantDataPastDeactivationDate)


class RemoveParticipantDataPastDeactivationDateTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def test_omitting_the_rdr_sandbox_dataset_fails_and_names_it(self):
        """
        Omitting the RDR stage sandbox fails at construction, naming it.
        """
        with self.assertRaises(ValueError) as context:
            get_custom_kwargs(RemoveParticipantDataPastDeactivationDate,
                              api_project_id='foo-project-id',
                              rdr_dataset_id='foo_rdr',
                              table_namer='unioned')

        self.assertIn('rdr_sandbox_dataset_id', str(context.exception))
        self.assertIn(RemoveParticipantDataPastDeactivationDate.__name__,
                      str(context.exception))

    def test_supplying_the_rdr_sandbox_dataset_is_accepted(self):
        """
        Supplying it succeeds, so the failure above is about that parameter.
        """
        kwargs = get_custom_kwargs(RemoveParticipantDataPastDeactivationDate,
                                   api_project_id='foo-project-id',
                                   rdr_dataset_id='foo_rdr',
                                   rdr_sandbox_dataset_id='foo_rdr_sandbox',
                                   table_namer='unioned')

        self.assertEqual(kwargs['rdr_sandbox_dataset_id'], 'foo_rdr_sandbox')

    def test_the_parameter_has_no_default(self):
        """
        The fail-fast depends on the missing default, so assert it directly.
        """
        import inspect

        parameter = inspect.signature(RemoveParticipantDataPastDeactivationDate
                                     ).parameters['rdr_sandbox_dataset_id']

        self.assertIs(parameter.default, inspect.Parameter.empty)
        # Positional, because `reporter.get_stage_elements` instantiates rules
        # positionally.
        self.assertIs(parameter.kind, inspect.Parameter.POSITIONAL_OR_KEYWORD)
