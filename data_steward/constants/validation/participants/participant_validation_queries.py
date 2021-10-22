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
        RDR_SEX: ["SexAtBirth_Male"],
        EHR_SEX: ["MALE"]
    }, {
        RDR_SEX: ["SexAtBirth_Female"],
        EHR_SEX: ["FEMALE"]
    }, {
        RDR_SEX: ["SexAtBirth_SexAtBirthNoneOfThese"],
        EHR_SEX: ["UNKNOWN", "OTHER", "AMBIGUOUS"]
    }]
}, {
    MATCH_STATUS:
        NO_MATCH,
    MATCH_STATUS_PAIRS: [{
        RDR_SEX: ["SexAtBirth_Male"],
        EHR_SEX: [
            "UNKNOWN", "Gender unknown", "AMBIGUOUS", "Gender unspecified",
            "OTHER", "FEMALE"
        ]
    }, {
        RDR_SEX: ["SexAtBirth_Female"],
        EHR_SEX: [
            "UNKNOWN", "Gender unknown", "AMBIGUOUS", "Gender unspecified",
            "OTHER", "MALE"
        ]
    }, {
        RDR_SEX: ["SexAtBirth_Intersex"],
        EHR_SEX: [
            "AMBIGUOUS", "Gender unknown", "Gender unspecified", "FEMALE",
            "MALE", "UNKNOWN", "OTHER"
        ]
    }, {
        RDR_SEX: ["SexAtBirth_SexAtBirthNoneOfThese"],
        EHR_SEX: ["FEMALE", "MALE", "Gender unspecified", "Gender unknown"]
    }]
}, {
    MATCH_STATUS:
        MISSING_EHR,
    MATCH_STATUS_PAIRS: [{
        RDR_SEX: [
            "SexAtBirth_Male", "SexAtBirth_Female", "SexAtBirth_Intersex",
            "SexAtBirth_SexAtBirthNoneOfThese"
        ],
        EHR_SEX: ["No matching concept"]
    }]
}, {
    MATCH_STATUS:
        MISSING_RDR,
    MATCH_STATUS_PAIRS: [{
        RDR_SEX: ["UNSET", "PMI_Skip", "PMI_PreferNotToAnswer"],
        EHR_SEX: [
            "MALE", "OTHER", "Gender unspecified", "AMBIGUOUS", "FEMALE",
            "UNKNOWN", "Gender unknown", "No matching concept"
        ]
    }]
}]


def get_gender_comparison_case_statement():
    conditions = []
    for match in GENDER_MATCH:
        and_conditions = []
        for dict_ in match[MATCH_STATUS_PAIRS]:
            and_conditions.append(
                f"(rdr_sex in {[pair.lower() for pair in dict_[RDR_SEX]]} AND ehr_sex in {[pair.lower() for pair in dict_[EHR_SEX]]})"
            )
        all_matches = ' OR '.join(and_conditions)
        all_matches = all_matches.replace('[', '(').replace(']', ')')
        conditions.append(f'WHEN {all_matches} THEN \'{match[MATCH_STATUS]}\'')
    return ' \n'.join(conditions)
