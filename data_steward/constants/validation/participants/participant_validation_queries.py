RDR_SEX = 'rdr_sex'
EHR_SEX = 'ehr_sex'
MATCH_STATUS = 'match_status'
MATCH = 'match'
NO_MATCH = 'no_match'
MISSING_EHR = 'missing_ehr'
MISSING_RDR = 'missing_rdr'
MATCH_STATUS_PAIRS = 'match_status_pairs'

GENDER_MATCH = [{
    MATCH_STATUS:
        MATCH,
    MATCH_STATUS_PAIRS: [{
        RDR_SEX: "SexAtBirth_Male",
        EHR_SEX: "MALE"
    }, {
        RDR_SEX: "SexAtBirth_Female",
        EHR_SEX: "FEMALE"
    }, {
        RDR_SEX: "SexAtBirth_SexAtBirthNoneOfThese",
        EHR_SEX: "UNKNOWN"
    }, {
        RDR_SEX: "SexAtBirth_SexAtBirthNoneOfThese",
        EHR_SEX: "OTHER"
    }, {
        RDR_SEX: "SexAtBirth_SexAtBirthNoneOfThese",
        EHR_SEX: "AMBIGUOUS"
    }]
}, {
    MATCH_STATUS:
        NO_MATCH,
    MATCH_STATUS_PAIRS: [{
        RDR_SEX: "PMI_Skip",
        EHR_SEX: "AMBIGUOUS"
    }, {
        RDR_SEX: "SexAtBirth_Male",
        EHR_SEX: "UNKNOWN"
    }, {
        RDR_SEX: "SexAtBirth_Male",
        EHR_SEX: "AMBIGUOUS"
    }, {
        RDR_SEX: "SexAtBirth_Male",
        EHR_SEX: "Gender unknown"
    }, {
        RDR_SEX: "SexAtBirth_Male",
        EHR_SEX: "Gender unspecified"
    }, {
        RDR_SEX: "SexAtBirth_Male",
        EHR_SEX: "FEMALE"
    }, {
        RDR_SEX: "SexAtBirth_Male",
        EHR_SEX: "OTHER"
    }, {
        RDR_SEX: "SexAtBirth_Female",
        EHR_SEX: "UNKNOWN"
    }, {
        RDR_SEX: "SexAtBirth_Female",
        EHR_SEX: "Gender unknown"
    }, {
        RDR_SEX: "SexAtBirth_Female",
        EHR_SEX: "AMBIGUOUS"
    }, {
        RDR_SEX: "SexAtBirth_Female",
        EHR_SEX: "Gender unspecified"
    }, {
        RDR_SEX: "SexAtBirth_Female",
        EHR_SEX: "OTHER"
    }, {
        RDR_SEX: "SexAtBirth_Female",
        EHR_SEX: "MALE"
    }, {
        RDR_SEX: "SexAtBirth_Intersex",
        EHR_SEX: "AMBIGUOUS"
    }, {
        RDR_SEX: "SexAtBirth_Intersex",
        EHR_SEX: "Gender unknown"
    }, {
        RDR_SEX: "SexAtBirth_Intersex",
        EHR_SEX: "Gender unspecified"
    }, {
        RDR_SEX: "SexAtBirth_Intersex",
        EHR_SEX: "FEMALE"
    }, {
        RDR_SEX: "SexAtBirth_Intersex",
        EHR_SEX: "MALE"
    }, {
        RDR_SEX: "SexAtBirth_Intersex",
        EHR_SEX: "UNKNOWN"
    }, {
        RDR_SEX: "SexAtBirth_Intersex",
        EHR_SEX: "OTHER"
    }, {
        RDR_SEX: "SexAtBirth_SexAtBirthNoneOfThese",
        EHR_SEX: "Gender unknown"
    }, {
        RDR_SEX: "SexAtBirth_SexAtBirthNoneOfThese",
        EHR_SEX: "Gender unspecified"
    }, {
        RDR_SEX: "SexAtBirth_SexAtBirthNoneOfThese",
        EHR_SEX: "MALE"
    }, {
        RDR_SEX: "SexAtBirth_SexAtBirthNoneOfThese",
        EHR_SEX: "FEMALE"
    }]
}, {
    MATCH_STATUS:
        MISSING_EHR,
    MATCH_STATUS_PAIRS: [{
        RDR_SEX: "SexAtBirth_Male",
        EHR_SEX: "No matching concept"
    }, {
        RDR_SEX: "SexAtBirth_Female",
        EHR_SEX: "No matching concept"
    }, {
        RDR_SEX: "SexAtBirth_Intersex",
        EHR_SEX: "No matching concept"
    }, {
        RDR_SEX: "SexAtBirth_SexAtBirthNoneOfThese",
        EHR_SEX: "No matching concept"
    }]
}, {
    MATCH_STATUS:
        MISSING_RDR,
    MATCH_STATUS_PAIRS: [{
        RDR_SEX: "UNSET",
        EHR_SEX: "Gender unspecified"
    }, {
        RDR_SEX: "UNSET",
        EHR_SEX: "AMBIGUOUS"
    }, {
        RDR_SEX: "UNSET",
        EHR_SEX: "FEMALE"
    }, {
        RDR_SEX: "UNSET",
        EHR_SEX: "Gender unknown"
    }, {
        RDR_SEX: "UNSET",
        EHR_SEX: "UNKNOWN"
    }, {
        RDR_SEX: "UNSET",
        EHR_SEX: "No matching concept"
    }, {
        RDR_SEX: "UNSET",
        EHR_SEX: "OTHER"
    }, {
        RDR_SEX: "UNSET",
        EHR_SEX: "MALE"
    }, {
        RDR_SEX: "PMI_Skip",
        EHR_SEX: "No matching concept"
    }, {
        RDR_SEX: "PMI_Skip",
        EHR_SEX: "FEMALE"
    }, {
        RDR_SEX: "PMI_Skip",
        EHR_SEX: "Gender unknown"
    }, {
        RDR_SEX: "PMI_Skip",
        EHR_SEX: "Gender unspecified"
    }, {
        RDR_SEX: "PMI_Skip",
        EHR_SEX: "OTHER"
    }, {
        RDR_SEX: "PMI_Skip",
        EHR_SEX: "UNKNOWN"
    }, {
        RDR_SEX: "PMI_Skip",
        EHR_SEX: "MALE"
    }, {
        RDR_SEX: "PMI_PreferNotToAnswer",
        EHR_SEX: "No matching concept"
    }, {
        RDR_SEX: "PMI_PreferNotToAnswer",
        EHR_SEX: "MALE"
    }, {
        RDR_SEX: "PMI_PreferNotToAnswer",
        EHR_SEX: "UNKNOWN"
    }, {
        RDR_SEX: "PMI_PreferNotToAnswer",
        EHR_SEX: "OTHER"
    }, {
        RDR_SEX: "PMI_PreferNotToAnswer",
        EHR_SEX: "Gender unspecified"
    }, {
        RDR_SEX: "PMI_PreferNotToAnswer",
        EHR_SEX: "Gender unknown"
    }, {
        RDR_SEX: "PMI_PreferNotToAnswer",
        EHR_SEX: "AMBIGUOUS"
    }, {
        RDR_SEX: "PMI_PreferNotToAnswer",
        EHR_SEX: "FEMALE"
    }]
}]


def get_gender_comparision_case_statement():
    conditions = []
    for match in GENDER_MATCH:
        and_conditions = []
        for dict in match[MATCH_STATUS_PAIRS]:
            and_conditions.append(
                f'(rdr_sex = \'{dict[RDR_SEX]}\' AND ehr_sex = \'{dict[EHR_SEX]}\')'
            )
        all_matches = ' OR '.join(and_conditions)
        conditions.append(f'WHEN {all_matches} THEN \'{match[MATCH_STATUS]}\'')
    return '\n'.join(conditions)
