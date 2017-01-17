CREATE TABLE hpo_schema.person
(
  person_id                   INTEGER     NOT NULL,
  gender_concept_id           INTEGER     NOT NULL,
  year_of_birth               INTEGER     NOT NULL,
  month_of_birth              INTEGER     NULL,
  day_of_birth                INTEGER     NULL,
  time_of_birth               VARCHAR(10) NULL,
  race_concept_id             INTEGER     NOT NULL,
  ethnicity_concept_id        INTEGER     NOT NULL,
  location_id                 INTEGER     NULL,
  provider_id                 INTEGER     NULL,
  care_site_id                INTEGER     NULL,
  person_source_value         VARCHAR(50) NULL,
  gender_source_value         VARCHAR(50) NULL,
  gender_source_concept_id    INTEGER     NULL,
  race_source_value           VARCHAR(50) NULL,
  race_source_concept_id      INTEGER     NULL,
  ethnicity_source_value      VARCHAR(50) NULL,
  ethnicity_source_concept_id INTEGER     NULL
);

CREATE TABLE hpo_schema.observation_period
(
  observation_period_id         INTEGER     NOT NULL,
  person_id                     INTEGER     NOT NULL,
  observation_period_start_date DATE        NOT NULL,
  observation_period_end_date   DATE        NOT NULL,
  period_type_concept_id        INTEGER     NOT NULL
);

CREATE TABLE hpo_schema.specimen
(
  specimen_id                 INTEGER     NOT NULL,
  person_id                   INTEGER     NOT NULL,
  specimen_concept_id         INTEGER     NOT NULL,
  specimen_type_concept_id    INTEGER     NOT NULL,
  specimen_date               DATE        NOT NULL,
  specimen_time               VARCHAR(10) NULL,
  quantity                    FLOAT       NULL,
  unit_concept_id             INTEGER     NULL,
  anatomic_site_concept_id    INTEGER     NULL,
  disease_status_concept_id   INTEGER     NULL,
  specimen_source_id          VARCHAR(50) NULL,
  specimen_source_value       VARCHAR(50) NULL,
  unit_source_value           VARCHAR(50) NULL,
  anatomic_site_source_value  VARCHAR(50) NULL,
  disease_status_source_value VARCHAR(50) NULL
);

CREATE TABLE hpo_schema.death
(
  person_id               INTEGER     NOT NULL,
  death_date              DATE        NOT NULL,
  death_type_concept_id   INTEGER     NOT NULL,
  cause_concept_id        INTEGER     NULL,
  cause_source_value      VARCHAR(50) NULL,
  cause_source_concept_id INTEGER     NULL
);

CREATE TABLE hpo_schema.visit_occurrence
(
  visit_occurrence_id     INTEGER     NOT NULL,
  person_id               INTEGER     NOT NULL,
  visit_concept_id        INTEGER     NOT NULL,
  visit_start_date        DATE        NOT NULL,
  visit_start_time        VARCHAR(10) NULL,
  visit_end_date          DATE        NOT NULL,
  visit_end_time          VARCHAR(10) NULL,
  visit_type_concept_id   INTEGER     NOT NULL,
  provider_id             INTEGER     NULL,
  care_site_id            INTEGER     NULL,
  visit_source_value      VARCHAR(50) NULL,
  visit_source_concept_id INTEGER     NULL
);

CREATE TABLE hpo_schema.procedure_occurrence
(
  procedure_occurrence_id     INTEGER     NOT NULL,
  person_id                   INTEGER     NOT NULL,
  procedure_concept_id        INTEGER     NOT NULL,
  procedure_date              DATE        NOT NULL,
  procedure_type_concept_id   INTEGER     NOT NULL,
  modifier_concept_id         INTEGER     NULL,
  quantity                    INTEGER     NULL,
  provider_id                 INTEGER     NULL,
  visit_occurrence_id         INTEGER     NULL,
  procedure_source_value      VARCHAR(50) NULL,
  procedure_source_concept_id INTEGER     NULL,
  qualifier_source_value      VARCHAR(50) NULL
);

CREATE TABLE hpo_schema.drug_exposure
(
  drug_exposure_id         INTEGER      NOT NULL,
  person_id                INTEGER      NOT NULL,
  drug_concept_id          INTEGER      NOT NULL,
  drug_exposure_start_date DATE         NOT NULL,
  drug_exposure_end_date   DATE         NULL,
  drug_type_concept_id     INTEGER      NOT NULL,
  stop_reason              VARCHAR(20)  NULL,
  refills                  INTEGER      NULL,
  quantity                 FLOAT        NULL,
  days_supply              INTEGER      NULL,
  sig                      VARCHAR(MAX) NULL,
  route_concept_id         INTEGER      NULL,
  effective_drug_dose      FLOAT        NULL,
  dose_unit_concept_id     INTEGER      NULL,
  lot_number               VARCHAR(50)  NULL,
  provider_id              INTEGER      NULL,
  visit_occurrence_id      INTEGER      NULL,
  drug_source_value        VARCHAR(50)  NULL,
  drug_source_concept_id   INTEGER      NULL,
  route_source_value       VARCHAR(50)  NULL,
  dose_unit_source_value   VARCHAR(50)  NULL
);

CREATE TABLE hpo_schema.device_exposure
(
  device_exposure_id         INTEGER      NOT NULL,
  person_id                  INTEGER      NOT NULL,
  device_concept_id          INTEGER      NOT NULL,
  device_exposure_start_date DATE         NOT NULL,
  device_exposure_end_date   DATE         NULL,
  device_type_concept_id     INTEGER      NOT NULL,
  unique_device_id           VARCHAR(50)  NULL,
  quantity                   INTEGER      NULL,
  provider_id                INTEGER      NULL,
  visit_occurrence_id        INTEGER      NULL,
  device_source_value        VARCHAR(100) NULL,
  device_source_concept_id   INTEGER      NULL
);

CREATE TABLE hpo_schema.condition_occurrence
(
  condition_occurrence_id     INTEGER     NOT NULL,
  person_id                   INTEGER     NOT NULL,
  condition_concept_id        INTEGER     NOT NULL,
  condition_start_date        DATE        NOT NULL,
  condition_end_date          DATE        NULL,
  condition_type_concept_id   INTEGER     NOT NULL,
  stop_reason                 VARCHAR(20) NULL,
  provider_id                 INTEGER     NULL,
  visit_occurrence_id         INTEGER     NULL,
  condition_source_value      VARCHAR(50) NULL,
  condition_source_concept_id INTEGER     NULL
);

CREATE TABLE hpo_schema.measurement
(
  measurement_id                INTEGER     NOT NULL,
  person_id                     INTEGER     NOT NULL,
  measurement_concept_id        INTEGER     NOT NULL,
  measurement_date              DATE        NOT NULL,
  measurement_time              VARCHAR(10) NULL,
  measurement_type_concept_id   INTEGER     NOT NULL,
  operator_concept_id           INTEGER     NULL,
  value_as_number               FLOAT       NULL,
  value_as_concept_id           INTEGER     NULL,
  unit_concept_id               INTEGER     NULL,
  range_low                     FLOAT       NULL,
  range_high                    FLOAT       NULL,
  provider_id                   INTEGER     NULL,
  visit_occurrence_id           INTEGER     NULL,
  measurement_source_value      VARCHAR(50) NULL,
  measurement_source_concept_id INTEGER     NULL,
  unit_source_value             VARCHAR(50) NULL,
  value_source_value            VARCHAR(50) NULL
);

CREATE TABLE hpo_schema.note
(
  note_id              INTEGER      NOT NULL,
  person_id            INTEGER      NOT NULL,
  note_date            DATE         NOT NULL,
  note_time            VARCHAR(10)  NULL,
  note_type_concept_id INTEGER      NOT NULL,
  note_text            VARCHAR(MAX) NOT NULL,
  provider_id          INTEGER      NULL,
  visit_occurrence_id  INTEGER      NULL,
  note_source_value    VARCHAR(50)  NULL
);

CREATE TABLE hpo_schema.observation
(
  observation_id                INTEGER     NOT NULL,
  person_id                     INTEGER     NOT NULL,
  observation_concept_id        INTEGER     NOT NULL,
  observation_date              DATE        NOT NULL,
  observation_time              VARCHAR(10) NULL,
  observation_type_concept_id   INTEGER     NOT NULL,
  value_as_number               FLOAT       NULL,
  value_as_string               VARCHAR(60) NULL,
  value_as_concept_id           INTEGER     NULL,
  qualifier_concept_id          INTEGER     NULL,
  unit_concept_id               INTEGER     NULL,
  provider_id                   INTEGER     NULL,
  visit_occurrence_id           INTEGER     NULL,
  observation_source_value      VARCHAR(50) NULL,
  observation_source_concept_id INTEGER     NULL,
  unit_source_value             VARCHAR(50) NULL,
  qualifier_source_value        VARCHAR(50) NULL
);

CREATE TABLE hpo_schema.fact_relationship
(
  domain_concept_id_1     INTEGER     NOT NULL,
  fact_id_1               INTEGER     NOT NULL,
  domain_concept_id_2     INTEGER     NOT NULL,
  fact_id_2               INTEGER     NOT NULL,
  relationship_concept_id INTEGER     NOT NULL
);

CREATE TABLE hpo_schema.drug_era
(
  drug_era_id         INTEGER NOT NULL,
  person_id           INTEGER NOT NULL,
  drug_concept_id     INTEGER NOT NULL,
  drug_era_start_date DATE    NOT NULL,
  drug_era_end_date   DATE    NOT NULL,
  drug_exposure_count INTEGER NULL,
  gap_days            INTEGER NULL
);

CREATE TABLE hpo_schema.dose_era
(
  dose_era_id         INTEGER NOT NULL,
  person_id           INTEGER NOT NULL,
  drug_concept_id     INTEGER NOT NULL,
  unit_concept_id     INTEGER NOT NULL,
  dose_value          FLOAT   NOT NULL,
  dose_era_start_date DATE    NOT NULL,
  dose_era_end_date   DATE    NOT NULL
);

CREATE TABLE hpo_schema.condition_era
(
  condition_era_id           INTEGER NOT NULL,
  person_id                  INTEGER NOT NULL,
  condition_concept_id       INTEGER NOT NULL,
  condition_era_start_date   DATE    NOT NULL,
  condition_era_end_date     DATE    NOT NULL,
  condition_occurrence_count INTEGER NULL
)