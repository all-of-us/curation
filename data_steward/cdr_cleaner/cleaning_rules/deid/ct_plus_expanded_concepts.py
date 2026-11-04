"""
The expanded concept set of the CT+ per-element add-on datasets.

A concept is expanded when CT suppresses it and CT+ releases it to studies entitled to
its data element. The CT+ rules leave these rows inline, so they reach the re-key output
with every value-level transform applied, and split_ct_plus_expanded_concepts.py moves
them into one dataset per data element at the end of the run:

1. AianExpandedConceptSuppressionCtPlus removes AIAN participants' rows, since AIAN
   participants are in no add-on and the mainline must equal CT.
2. split_ct_plus_expanded_concepts.py copies the rest out of the re-key output, leaving
   out the pediatric cohort, whose rows the pediatrics add-on holds.

Both read the lookup built here, so the rows the rule removes and the rows the split
claims cannot drift apart. The set has two sources:

- the rows flagged ct_plus_suppressed = false on the four CT-consumed privacy CSVs;
- the concepts MotorVehicleAccidentSuppression and VehicularAccidentConceptSuppression
  reach. Neither runs in CT+, and both build their lists from the vocabulary rather
  than a CSV, so most of those rows are on no CSV.

A row is expanded only where a CT rule would have removed it, since the mainline must
keep the rows CT keeps. So each source applies to the tables and columns of the rule it
stands in for: the two vehicle lists reach fewer tables than the CSVs do.

That parity is row-level only. CreateDerivedTables, CalculatePrimaryDeathRecord and
NoDataAfterDeath run while these rows are inline, and the mainline assembly recomputes
none of them. So the eras, observation_period and primary_death_record still reflect
expanded rows after the split, and rows NoDataAfterDeath removed on an expanded death
record stay removed.

Original Issues: DL-2538
"""
# Third party imports
import pandas as pd

# Project imports
from cdr_cleaner.cleaning_rules.deid.concept_suppression import (
    CT_PLUS_SUPPRESSED, lookup_load_job_config)
from cdr_cleaner.cleaning_rules.deid.ct_plus_dob_indicators import (
    CONCEPT_ID, PRIVACY_CONCEPT_PATHS, PRIVACY_RULE)
from cdr_cleaner.cleaning_rules.deid.motor_vehicle_accident_suppression import \
    MOTOR_VEHICLE_ACCIDENT_CONCEPT_QUERY
from cdr_cleaner.cleaning_rules.vehicular_accident_concept_suppression import \
    VEHICULAR_ACCIDENTS_CONCEPT_QUERY
from common import (AOU_DEATH, AOU_REQUIRED, CDM_TABLES, CONDITION_OCCURRENCE,
                    CT_PLUS_EXPANDED_CONCEPTS, DEATH, JINJA_ENV, NOTE, NOTE_NLP,
                    PERSON)
from resources import (CT_PLUS_LABEL_MAP_PATH, csv_to_list,
                       get_concept_id_fields, has_domain_table_id)

DATA_ELEMENT = 'data_element'
SOURCE = 'source'

MOTOR_VEHICLE_ACCIDENT = 'motor_vehicle_accident'

# Which rule a lookup row stands in for, and so which tables it applies to
SOURCE_CSV = 'csv'
SOURCE_MOTOR_VEHICLE = 'motor_vehicle'
SOURCE_VEHICULAR = 'vehicular'

# Sandbox tables the two vehicle queries write. Not the rules' own names, so a CT stage
# sharing the sandbox dataset never reads this run's table or the reverse.
VEHICLE_LOOKUPS = {
    SOURCE_MOTOR_VEHICLE: 'ct_plus_motor_vehicle_concepts',
    SOURCE_VEHICULAR: 'ct_plus_vehicular_concepts',
}

# Separates the labels of a row carrying more than one
LABEL_SEPARATOR = ';'

INSERT_VEHICLE_CONCEPTS = JINJA_ENV.from_string("""
INSERT INTO `{{project}}.{{sandbox_dataset}}.{{lookup}}`
  (concept_id, data_element, source)
SELECT DISTINCT concept_id, '{{data_element}}', '{{source}}'
FROM `{{project}}.{{sandbox_dataset}}.{{vehicle_lookup}}`
""")

EXPANDED_CONCEPT_IDS = JINJA_ENV.from_string("""
SELECT concept_id
FROM `{{project}}.{{sandbox_dataset}}.{{lookup}}`
WHERE source IN ('{{sources | join("', '")}}')
{% if data_element %}  AND data_element = '{{data_element}}'{% endif %}
""")

# note_nlp has no person_id, so its rows are attributed through their note. A LEFT JOIN,
# so an orphaned row reaches the split's checks instead of dropping out of them.
OWNER_JOIN = JINJA_ENV.from_string("""
LEFT JOIN `{{project}}.{{dataset}}.{{note}}` AS owner
  ON owner.note_id = {{alias}}.note_id
""")


def get_expanded_concept_labels(paths=None):
    """
    The labels of every row flagged expanded on the privacy CSVs.

    :param paths: privacy CSVs to read, defaulting to the four CT-consumed ones
    :return: frame of concept_id and privacy_rule, one row per concept and label
    """
    frames = [
        pd.read_csv(path,
                    usecols=[CONCEPT_ID, PRIVACY_RULE, CT_PLUS_SUPPRESSED])
        for path in (paths or PRIVACY_CONCEPT_PATHS)
    ]
    df = pd.concat(frames, ignore_index=True).dropna(subset=[CONCEPT_ID])
    # Compared as text, so a blank cell counts as suppressed rather than failing
    df = df[df[CT_PLUS_SUPPRESSED].astype(str).str.lower() == 'false']

    return pd.DataFrame({
        CONCEPT_ID: df[CONCEPT_ID].astype(int),
        PRIVACY_RULE: df[PRIVACY_RULE].fillna('')
    }).drop_duplicates()


def get_label_map(path=None):
    """
    :param path: label map CSV, defaulting to the copy of the warehouse seed
    :return: {privacy_rule label: data element}
    """
    return {
        row[PRIVACY_RULE]: row[DATA_ELEMENT]
        for row in csv_to_list(path or CT_PLUS_LABEL_MAP_PATH)
    }


def resolve_concept_elements(labels, label_map):
    """
    Resolve each expanded concept to every data element its labels name.

    A blank label is refused, not matched to the map's (unlabelled) row. The registry
    marks that element suppressed, so a row carrying it must not reach an add-on.

    :param labels: frame of concept_id and privacy_rule
    :param label_map: {privacy_rule label: data element}
    :return: frame of concept_id and data_element, one row per pair
    :raises ValueError: naming every label that resolves to no element. A concept left
        unresolved would either stay in the mainline or be lost.
    """
    pairs = set()
    unmapped = set()
    for concept_id, privacy_rule in labels.itertuples(index=False):
        for label in str(privacy_rule).split(LABEL_SEPARATOR):
            label = label.strip()
            if label in label_map:
                pairs.add((concept_id, label_map[label]))
            else:
                unmapped.add(label)

    if unmapped:
        raise ValueError(
            f'Expanded concepts carry labels the label map does not resolve: '
            f'{sorted(unmapped)}. Refresh the map from the warehouse seed, or add '
            f'the label there.')

    return pd.DataFrame(sorted(pairs), columns=[CONCEPT_ID, DATA_ELEMENT])


def get_expanded_tables():
    """
    The tables CT's concept suppression rules reach, with the columns they read.

    Follows AbstractConceptSuppression's filter. person is left out, as in
    CTAdditionalPrivacyConceptSuppression: the rule never removes a person row.

    :return: {table: concept_id columns}
    """
    tables = {}
    for table in sorted(set(CDM_TABLES + [AOU_DEATH]) - {PERSON}):
        concept_fields = get_concept_id_fields(table)
        if concept_fields and (has_domain_table_id(table) or
                               table in [AOU_DEATH, DEATH]):
            tables[table] = concept_fields
    return tables


def get_lookup_sources(table):
    """
    :param table: table name
    :return: the lookup sources applying to the table, matching the CT rules that
        reach it
    """
    sources = [SOURCE_CSV]
    if table in AOU_REQUIRED + [AOU_DEATH]:
        sources.append(SOURCE_MOTOR_VEHICLE)
    if table == CONDITION_OCCURRENCE:
        sources.append(SOURCE_VEHICULAR)
    return sources


def expanded_row(project_id,
                 sandbox_dataset_id,
                 table,
                 concept_fields,
                 alias,
                 data_element=None):
    """
    :param project_id: project holding the lookup
    :param sandbox_dataset_id: dataset holding the lookup
    :param table: table the predicate is applied to
    :param concept_fields: concept_id columns to test
    :param alias: the table's alias in the query
    :param data_element: restrict to one element, or None for the whole set
    :return: the predicate matching a row carrying an expanded concept
    """
    concept_ids = EXPANDED_CONCEPT_IDS.render(
        project=project_id,
        sandbox_dataset=sandbox_dataset_id,
        lookup=CT_PLUS_EXPANDED_CONCEPTS,
        sources=get_lookup_sources(table),
        data_element=data_element)
    return '(' + ' OR '.join(
        f'{alias}.{field} IN ({concept_ids})' for field in concept_fields) + ')'


def get_row_owner(project_id, dataset_id, table, alias):
    """
    Say how to reach the participant a row belongs to.

    :param project_id: project holding the table
    :param dataset_id: dataset holding the table
    :param table: table name
    :param alias: the table's alias in the query
    :return: (join clause, or '' when not needed; person_id expression)
    """
    if table != NOTE_NLP:
        return '', f'{alias}.person_id'

    return OWNER_JOIN.render(project=project_id,
                             dataset=dataset_id,
                             note=NOTE,
                             alias=alias), 'owner.person_id'


def build_expanded_lookup(client, project_id, vocabulary_dataset_id,
                          sandbox_dataset_id):
    """
    Write the expanded concept lookup, replacing any earlier one.

    :param client: BigQueryClient
    :param project_id: project holding both datasets
    :param vocabulary_dataset_id: dataset holding the vocabulary tables
    :param sandbox_dataset_id: dataset the lookup is written to
    :return: sorted data elements in the lookup
    :raises RuntimeError: if nothing is flagged expanded, which means the CT+ flips are
        missing rather than that there is nothing to do
    :raises ValueError: if a label resolves to no element
    """
    concepts = resolve_concept_elements(get_expanded_concept_labels(),
                                        get_label_map())
    if concepts.empty:
        raise RuntimeError(
            f'No privacy CSV row has {CT_PLUS_SUPPRESSED} = false, so every add-on '
            f'would be empty. The CT+ flips are missing.')
    concepts[SOURCE] = SOURCE_CSV

    client.load_table_from_dataframe(
        concepts,
        f'{project_id}.{sandbox_dataset_id}.{CT_PLUS_EXPANDED_CONCEPTS}',
        job_config=lookup_load_job_config()).result()

    client.query(
        MOTOR_VEHICLE_ACCIDENT_CONCEPT_QUERY.render(
            project=project_id,
            dataset=vocabulary_dataset_id,
            sandbox_dataset=sandbox_dataset_id,
            concept_suppression_lookup=VEHICLE_LOOKUPS[SOURCE_MOTOR_VEHICLE])
    ).result()
    client.query(
        VEHICULAR_ACCIDENTS_CONCEPT_QUERY.render(
            project_id=project_id,
            dataset_id=vocabulary_dataset_id,
            sandbox_id=sandbox_dataset_id,
            concept_suppression_lookup_table=VEHICLE_LOOKUPS[SOURCE_VEHICULAR])
    ).result()

    for source, vehicle_lookup in VEHICLE_LOOKUPS.items():
        client.query(
            INSERT_VEHICLE_CONCEPTS.render(
                project=project_id,
                sandbox_dataset=sandbox_dataset_id,
                lookup=CT_PLUS_EXPANDED_CONCEPTS,
                data_element=MOTOR_VEHICLE_ACCIDENT,
                source=source,
                vehicle_lookup=vehicle_lookup)).result()

    return sorted(set(concepts[DATA_ELEMENT]) | {MOTOR_VEHICLE_ACCIDENT})
