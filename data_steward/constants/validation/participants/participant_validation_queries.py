import re

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

# State abbreviations. Used to validate state abbreviations
STATE_ABBREVIATIONS = [
    'al',
    'ak',
    'az',
    'ar',
    'ca',
    'co',
    'ct',
    'de',
    'fl',
    'ga',
    'hi',
    'id',
    'il',
    'in',
    'ia',
    'ks',
    'ky',
    'la',
    'me',
    'md',
    'ma',
    'mi',
    'mn',
    'ms',
    'mo',
    'mt',
    'ne',
    'nv',
    'nh',
    'nj',
    'nm',
    'ny',
    'nc',
    'nd',
    'oh',
    'ok',
    'or',
    'pa',
    'ri',
    'sc',
    'sd',
    'tn',
    'tx',
    'ut',
    'vt',
    'va',
    'wa',
    'wv',
    'wi',
    'wy',
    # Commonwealth/Territory:
    'as',
    'dc',
    'fm',
    'gu',
    'mh',
    'mp',
    'pw',
    'pr',
    'vi',
    # Military "State":
    'aa',
    'ae',
    'ap',
]

AT = '@'

NUMERIC_ENDINGS_REGEX = '(\d+)(st|nd|rd|th)'

COMPILED_NUMERIC_ENDINGS_REGEX = re.compile(NUMERIC_ENDINGS_REGEX)

ALPHA_NUMERIC = '(\d+)[a-zA-Z]+'

COMPILED_ALPHA_NUMERIC = re.compile(ALPHA_NUMERIC)

ADDRESS_ABBREVIATIONS = {
    'aly': 'alley',
    'anx': 'annex',
    'apt': 'apartment',
    'ave': 'avenue',
    'bch': 'beach',
    'bldg': 'building',
    'blvd': 'boulevard',
    'bnd': 'bend',
    'btm': 'bottom',
    'cir': 'circle',
    'ct': 'court',
    'co': 'county',
    'ctr': 'center',
    'dr': 'drive',
    'e': 'east',
    'expy': 'expressway',
    'hts': 'heights',
    'hwy': 'highway',
    'is': 'island',
    'jct': 'junction',
    'lk': 'lake',
    'ln': 'lane',
    'mtn': 'mountain',
    'n': 'north',
    'ne': 'northeast',
    'num': 'number',
    'nw': 'northwest',
    'pkwy': 'parkway',
    'pl': 'place',
    'plz': 'plaza',
    'po': 'post office',
    'rd': 'road',
    'rdg': 'ridge',
    'rr': 'rural route',
    'rm': 'room',
    's': 'south',
    'se': 'southeast',
    'sq': 'square',
    'st': 'street',
    'str': 'street',
    'sta': 'station',
    'ste': 'suite',
    'sw': 'southwest',
    'ter': 'terrace',
    'tpke': 'turnpike',
    'trl': 'trail',
    'vly': 'valley',
    'w': 'west',
    'way': 'way',
}

CITY_ABBREVIATIONS = {
    'st': 'saint',
    'afb': 'air force base',
}


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


def get_state_abbreviations():
    """ Returns lowercase state abbreviations separated by comma.
    e.g. 'al','ak','az',...
    """
    return ','.join(f"'{state}'" for state in STATE_ABBREVIATIONS)

def get_abbreviation_replace_statement(abbreviations):
    """[summary]

    Args:
        abbreviations ([type]): [description]
    """
    pass