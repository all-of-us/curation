"""
The indicators-of-birth concept set for the CT+ date-of-birth add-on dataset.

The CT+ date-of-birth dataset carries the event rows that indicate a birth alongside
the person-level date of birth. Those rows stay flagged ct_plus_suppressed in the
privacy CSVs, because they must not reach the mainline, so the flag cannot release
them. Instead the CT+ rules that would remove them leave them inline, and
split_ct_plus_dob_indicators.py moves them into the add-on at the end of the run:

1. BirthInformationSuppressionCtPlus removes them for AIAN and pediatric participants,
   who cannot receive the add-on, before any rule below runs.
2. YearOfBirthRecordsSuppressionCtPlus and the CT+ privacy suppression variants leave
   them in place for everyone else.
3. split_ct_plus_dob_indicators.py copies them out of the re-key output.

Every step reads the set from here, so the rows a rule releases and the rows the split
claims cannot drift apart.

Original Issues: DL2495
"""

# Third party imports
import pandas as pd

# Project imports
from cdr_cleaner.cleaning_rules.deid.concept_suppression import CT_PLUS_SUPPRESSED
from resources import (CT_ADDITIONAL_PRIVACY_CONCEPTS_PATH,
                       CT_OBSERVATION_PRIVACY_CONCEPTS_PATH,
                       CT_RETROACTIVE_PRIVACY_CONCEPTS_PATH,
                       CT_RT_PUBLICLY_REPORTABLE_CONCEPTS_PATH)

CONCEPT_ID = 'concept_id'
PRIVACY_RULE = 'privacy_rule'

# The birth-indicating labels as the four CSVs spell them. The retroactive file uses
# title-case prose for the same categories. Mixed labels carrying abortion, free text
# or location are left out: those categories stay suppressed in CT+.
DOB_INDICATOR_LABELS = frozenset([
    'liveborn',
    'liveborn_infants',
    'perinatal',
    'stillborn',
    'birth.csv',
    'delivery-proc.csv',
    'perinatal.csv',
    'liveborn; multiples',
    'liveborn; date of birth',
    'Liveborn infants',
    'Perinatal (including birth trauma)',
])

# ICD9CM 763, 763.2, 763.3 and 763.4, perinatal codes filed under the cross-category
# '06apr20 NIH List' label, so no birth label reaches them.
NIH_LIST_PERINATAL_CONCEPT_IDS = frozenset(
    [44826952, 44823422, 44829274, 44837410])

# The PPI date-of-birth question, which BirthInformationSuppression removes and no CSV
# lists. Its other two concepts are in the CSVs under 'liveborn; date of birth'.
PPI_DATE_OF_BIRTH_CONCEPT_ID = 1585259

PRIVACY_CONCEPT_PATHS = [
    CT_ADDITIONAL_PRIVACY_CONCEPTS_PATH,
    CT_OBSERVATION_PRIVACY_CONCEPTS_PATH,
    CT_RT_PUBLICLY_REPORTABLE_CONCEPTS_PATH,
    CT_RETROACTIVE_PRIVACY_CONCEPTS_PATH,
]


def get_dob_indicator_concept_ids(paths=None):
    """
    The concepts the CT+ date-of-birth dataset delivers as event rows.

    A concept carrying a birth label that also carries a still-suppressed row under any
    other label, other than the NIH list rows above, is left out: suppressed wins, so a
    concept that indicates abortion as well as a birth stays suppressed. Another label
    flagged expanded does not conflict, since that category is released anyway.

    :param paths: privacy CSVs to read, defaulting to the four CT-consumed ones
    :return: sorted list of concept ids
    """
    frames = [
        pd.read_csv(path,
                    usecols=[CONCEPT_ID, PRIVACY_RULE, CT_PLUS_SUPPRESSED])
        for path in (paths or PRIVACY_CONCEPT_PATHS)
    ]
    df = pd.concat(frames, ignore_index=True).dropna(subset=[CONCEPT_ID])
    df[CONCEPT_ID] = df[CONCEPT_ID].astype(int)

    is_birth = df[PRIVACY_RULE].isin(DOB_INDICATOR_LABELS)
    is_nih_perinatal = df[CONCEPT_ID].isin(NIH_LIST_PERINATAL_CONCEPT_IDS)
    # Compared as text, so a blank cell counts as suppressed rather than failing
    is_expanded = df[CT_PLUS_SUPPRESSED].astype(str).str.lower() == 'false'

    candidates = set(df.loc[is_birth,
                            CONCEPT_ID]) | set(NIH_LIST_PERINATAL_CONCEPT_IDS)
    conflicting = set(df.loc[~is_birth & ~is_nih_perinatal & ~is_expanded,
                             CONCEPT_ID])

    return sorted((candidates - conflicting) | {PPI_DATE_OF_BIRTH_CONCEPT_ID})


def drop_dob_indicators(df):
    """
    Remove the indicators-of-birth concepts from a suppression lookup frame.

    Used by the CT+ privacy suppression variants, so a row is suppressed only for a
    still-suppressed concept outside the set. A birth row that also carries such a
    concept in another column is still removed.

    :param df: concept rows read from one or more privacy CSVs
    :return: the rows whose concept_id is not in the set
    """
    return df[~df[CONCEPT_ID].isin(get_dob_indicator_concept_ids())]
