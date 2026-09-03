"""
Unit test for the constructor contract of RemoveParticipantDataPastDeactivationDate.

The rule reads the adult to pediatric pairs from a lookup written at the RDR
stage, and the fitbit dataset carries neither `fact_relationship` nor `person`,
so the dataset holding that lookup has to be named explicitly. Giving the
parameter a default would let the rule construct with no linkage and cascade to
nobody, silently. `get_custom_kwargs` raises only for a parameter that has none,
so the absence of a default is what makes the failure loud. That is the contract
these tests hold in place.

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
        A run that does not supply the RDR stage sandbox must fail at
        construction naming the parameter, rather than proceeding against a
        lookup it cannot read.
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
        The same call succeeds once the parameter is supplied, so the assertion
        above is about that one parameter and not about the call shape.
        """
        kwargs = get_custom_kwargs(RemoveParticipantDataPastDeactivationDate,
                                   api_project_id='foo-project-id',
                                   rdr_dataset_id='foo_rdr',
                                   rdr_sandbox_dataset_id='foo_rdr_sandbox',
                                   table_namer='unioned')

        self.assertEqual(kwargs['rdr_sandbox_dataset_id'], 'foo_rdr_sandbox')

    def test_the_parameter_has_no_default(self):
        """
        The fail-fast above is bought entirely by the absence of a default, so
        assert that directly. A later edit adding `=None` would otherwise turn
        the two tests above green by accident only if the caller kept passing it.
        """
        import inspect

        parameter = inspect.signature(RemoveParticipantDataPastDeactivationDate
                                     ).parameters['rdr_sandbox_dataset_id']

        self.assertIs(parameter.default, inspect.Parameter.empty)
        # Positional rather than keyword-only, because `reporter.get_stage_elements`
        # instantiates every rule with positional arguments.
        self.assertIs(parameter.kind, inspect.Parameter.POSITIONAL_OR_KEYWORD)
