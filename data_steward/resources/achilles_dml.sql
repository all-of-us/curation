
--populate the tables with names of analyses

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name)
	values (0, 'Source name');

--000. PERSON statistics

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name)
	values (1, 'Number of persons');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name)
	values (2, 'Number of persons by gender', 'gender_concept_id');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name)
	values (3, 'Number of persons by year of birth', 'year_of_birth');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name)
	values (4, 'Number of persons by race', 'race_concept_id');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name)
	values (5, 'Number of persons by ethnicity', 'ethnicity_concept_id');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name)
	values (7, 'Number of persons with invalid provider_id');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name)
	values (8, 'Number of persons with invalid location_id');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name)
	values (9, 'Number of persons with invalid care_site_id');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name, stratum_2_name)
	values (10, 'Number of all persons by year of birth by gender', 'year_of_birth', 'gender_concept_id');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name, stratum_2_name)
	values (11, 'Number of non-deceased persons by year of birth by gender', 'year_of_birth', 'gender_concept_id');
	
insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name, stratum_2_name)
  values (12, 'Number of persons by race and ethnicity','race_concept_id','ethnicity_concept_id');


--100. OBSERVATION_PERIOD (joined to PERSON)

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name)
	values (101, 'Number of persons by age, with age at first observation period', 'age');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name, stratum_2_name)
	values (102, 'Number of persons by gender by age, with age at first observation period', 'gender_concept_id', 'age');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name)
	values (103, 'Distribution of age at first observation period');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name)
	values (104, 'Distribution of age at first observation period by gender', 'gender_concept_id');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name)
	values (105, 'Length of observation (days) of first observation period');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name)
	values (106, 'Length of observation (days) of first observation period by gender', 'gender_concept_id');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name)
	values (107, 'Length of observation (days) of first observation period by age decile', 'age decile');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name)
	values (108, 'Number of persons by length of observation period, in 30d increments', 'Observation period length 30d increments');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name)
	values (109, 'Number of persons with continuous observation in each year', 'calendar year');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name)
	values (110, 'Number of persons with continuous observation in each month', 'calendar month');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name)
	values (111, 'Number of persons by observation period start month', 'calendar month');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name)
	values (112, 'Number of persons by observation period end month', 'calendar month');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name)
	values (113, 'Number of persons by number of observation periods', 'number of observation periods');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name)
	values (114, 'Number of persons with observation period before year-of-birth');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name)
	values (115, 'Number of persons with observation period end < observation period start');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name, stratum_2_name, stratum_3_name)
	values (116, 'Number of persons with at least one day of observation in each year by gender and age decile', 'calendar year', 'gender_concept_id', 'age decile');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name)
	values (117, 'Number of persons with at least one day of observation in each month', 'calendar month');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name)
  values (118, 'Number of observation periods with invalid person_id');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name)
  values (119, 'Number of observation period records by period_type_concept_id','period_type_concept_id');




--200- VISIT_OCCURRENCE


insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name)
	values (200, 'Number of persons with at least one visit occurrence, by visit_concept_id', 'visit_concept_id');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name)
	values (201, 'Number of visit occurrence records, by visit_concept_id', 'visit_concept_id');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name, stratum_2_name)
	values (202, 'Number of persons by visit occurrence start month, by visit_concept_id', 'visit_concept_id', 'calendar month');	

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name)
	values (203, 'Number of distinct visit occurrence concepts per person');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name, stratum_2_name, stratum_3_name, stratum_4_name)
	values (204, 'Number of persons with at least one visit occurrence, by visit_concept_id by calendar year by gender by age decile', 'visit_concept_id', 'calendar year', 'gender_concept_id', 'age decile');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name, stratum_2_name)
	values (206, 'Distribution of age by visit_concept_id', 'visit_concept_id', 'gender_concept_id');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name)
	values (207, 'Number of visit records with invalid person_id');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name)
	values (208, 'Number of visit records outside valid observation period');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name)
	values (209, 'Number of visit records with end date < start date');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name)
	values (210, 'Number of visit records with invalid care_site_id');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name)
	values (211, 'Distribution of length of stay by visit_concept_id', 'visit_concept_id');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name, stratum_2_name, stratum_3_name )
	values (212, 'Number of persons with at least one visit occurrence, by calendar year by gender by age decile', 'calendar year', 'gender_concept_id', 'age decile');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name)
	values (220, 'Number of visit occurrence records by visit occurrence start month', 'calendar month');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name)
	values (221, 'Number of persons by visit start year', 'calendar year');



--300- PROVIDER
insert into synpuf_100.achilles_analysis (analysis_id, analysis_name)
	values (300, 'Number of providers');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name)
	values (301, 'Number of providers by specialty concept_id', 'specialty_concept_id');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name)
	values (302, 'Number of providers with invalid care site id');



--400- CONDITION_OCCURRENCE

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name)
	values (400, 'Number of persons with at least one condition occurrence, by condition_concept_id', 'condition_concept_id');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name)
	values (401, 'Number of condition occurrence records, by condition_concept_id', 'condition_concept_id');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name, stratum_2_name)
	values (402, 'Number of persons by condition occurrence start month, by condition_concept_id', 'condition_concept_id', 'calendar month');	

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name)
	values (403, 'Number of distinct condition occurrence concepts per person');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name, stratum_2_name, stratum_3_name, stratum_4_name)
	values (404, 'Number of persons with at least one condition occurrence, by condition_concept_id by calendar year by gender by age decile', 'condition_concept_id', 'calendar year', 'gender_concept_id', 'age decile');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name, stratum_2_name)
	values (405, 'Number of condition occurrence records, by condition_concept_id by condition_type_concept_id', 'condition_concept_id', 'condition_type_concept_id');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name, stratum_2_name)
	values (406, 'Distribution of age by condition_concept_id', 'condition_concept_id', 'gender_concept_id');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name)
	values (409, 'Number of condition occurrence records with invalid person_id');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name)
	values (410, 'Number of condition occurrence records outside valid observation period');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name)
	values (411, 'Number of condition occurrence records with end date < start date');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name)
	values (412, 'Number of condition occurrence records with invalid provider_id');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name)
	values (413, 'Number of condition occurrence records with invalid visit_id');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name)
	values (420, 'Number of condition occurrence records by condition occurrence start month', 'calendar month');	

--500- DEATH

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name)
	values (500, 'Number of persons with death, by cause_concept_id', 'cause_concept_id');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name)
	values (501, 'Number of records of death, by cause_concept_id', 'cause_concept_id');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name)
	values (502, 'Number of persons by death month', 'calendar month');	

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name, stratum_2_name, stratum_3_name)
	values (504, 'Number of persons with a death, by calendar year by gender by age decile', 'calendar year', 'gender_concept_id', 'age decile');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name)
	values (505, 'Number of death records, by death_type_concept_id', 'death_type_concept_id');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name)
	values (506, 'Distribution of age at death by gender', 'gender_concept_id');


insert into synpuf_100.achilles_analysis (analysis_id, analysis_name)
	values (509, 'Number of death records with invalid person_id');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name)
	values (510, 'Number of death records outside valid observation period');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name)
	values (511, 'Distribution of time from death to last condition');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name)
	values (512, 'Distribution of time from death to last drug');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name)
	values (513, 'Distribution of time from death to last visit');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name)
	values (514, 'Distribution of time from death to last procedure');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name)
	values (515, 'Distribution of time from death to last observation');


--600- PROCEDURE_OCCURRENCE



insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name)
	values (600, 'Number of persons with at least one procedure occurrence, by procedure_concept_id', 'procedure_concept_id');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name)
	values (601, 'Number of procedure occurrence records, by procedure_concept_id', 'procedure_concept_id');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name, stratum_2_name)
	values (602, 'Number of persons by procedure occurrence start month, by procedure_concept_id', 'procedure_concept_id', 'calendar month');	

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name)
	values (603, 'Number of distinct procedure occurrence concepts per person');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name, stratum_2_name, stratum_3_name, stratum_4_name)
	values (604, 'Number of persons with at least one procedure occurrence, by procedure_concept_id by calendar year by gender by age decile', 'procedure_concept_id', 'calendar year', 'gender_concept_id', 'age decile');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name, stratum_2_name)
	values (605, 'Number of procedure occurrence records, by procedure_concept_id by procedure_type_concept_id', 'procedure_concept_id', 'procedure_type_concept_id');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name, stratum_2_name)
	values (606, 'Distribution of age by procedure_concept_id', 'procedure_concept_id', 'gender_concept_id');



insert into synpuf_100.achilles_analysis (analysis_id, analysis_name)
	values (609, 'Number of procedure occurrence records with invalid person_id');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name)
	values (610, 'Number of procedure occurrence records outside valid observation period');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name)
	values (612, 'Number of procedure occurrence records with invalid provider_id');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name)
	values (613, 'Number of procedure occurrence records with invalid visit_id');


insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name)
	values (620, 'Number of procedure occurrence records  by procedure occurrence start month', 'calendar month');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name, stratum_2_name)
	values (691, 'Number of persons that have at least x procedures', 'procedure_id', 'procedure_count');

--700- DRUG_EXPOSURE


insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name)
	values (700, 'Number of persons with at least one drug exposure, by drug_concept_id', 'drug_concept_id');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name)
	values (701, 'Number of drug exposure records, by drug_concept_id', 'drug_concept_id');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name, stratum_2_name)
	values (702, 'Number of persons by drug exposure start month, by drug_concept_id', 'drug_concept_id', 'calendar month');	

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name)
	values (703, 'Number of distinct drug exposure concepts per person');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name, stratum_2_name, stratum_3_name, stratum_4_name)
	values (704, 'Number of persons with at least one drug exposure, by drug_concept_id by calendar year by gender by age decile', 'drug_concept_id', 'calendar year', 'gender_concept_id', 'age decile');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name, stratum_2_name)
	values (705, 'Number of drug exposure records, by drug_concept_id by drug_type_concept_id', 'drug_concept_id', 'drug_type_concept_id');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name, stratum_2_name)
	values (706, 'Distribution of age by drug_concept_id', 'drug_concept_id', 'gender_concept_id');



insert into synpuf_100.achilles_analysis (analysis_id, analysis_name)
	values (709, 'Number of drug exposure records with invalid person_id');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name)
	values (710, 'Number of drug exposure records outside valid observation period');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name)
	values (711, 'Number of drug exposure records with end date < start date');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name)
	values (712, 'Number of drug exposure records with invalid provider_id');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name)
	values (713, 'Number of drug exposure records with invalid visit_id');



insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name)
	values (715, 'Distribution of days_supply by drug_concept_id', 'drug_concept_id');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name)
	values (716, 'Distribution of refills by drug_concept_id', 'drug_concept_id');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name)
	values (717, 'Distribution of quantity by drug_concept_id', 'drug_concept_id');


insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name)
	values (720, 'Number of drug exposure records  by drug exposure start month', 'calendar month');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name, stratum_2_name)
	values (791, 'Number of persons that have at least x drug exposures', 'drug_concept_id', 'drug_count');

--800- OBSERVATION


insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name)
	values (800, 'Number of persons with at least one observation occurrence, by observation_concept_id', 'observation_concept_id');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name)
	values (801, 'Number of observation occurrence records, by observation_concept_id', 'observation_concept_id');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name, stratum_2_name)
	values (802, 'Number of persons by observation occurrence start month, by observation_concept_id', 'observation_concept_id', 'calendar month');	

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name)
	values (803, 'Number of distinct observation occurrence concepts per person');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name, stratum_2_name, stratum_3_name, stratum_4_name)
	values (804, 'Number of persons with at least one observation occurrence, by observation_concept_id by calendar year by gender by age decile', 'observation_concept_id', 'calendar year', 'gender_concept_id', 'age decile');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name, stratum_2_name)
	values (805, 'Number of observation occurrence records, by observation_concept_id by observation_type_concept_id', 'observation_concept_id', 'observation_type_concept_id');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name, stratum_2_name)
	values (806, 'Distribution of age by observation_concept_id', 'observation_concept_id', 'gender_concept_id');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name, stratum_2_name)
	values (807, 'Number of observation occurrence records, by observation_concept_id and unit_concept_id', 'observation_concept_id', 'unit_concept_id');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name)
	values (809, 'Number of observation records with invalid person_id');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name)
	values (810, 'Number of observation records outside valid observation period');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name)
	values (812, 'Number of observation records with invalid provider_id');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name)
	values (813, 'Number of observation records with invalid visit_id');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name)
	values (814, 'Number of observation records with no value (numeric, string, or concept)');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name)
	values (815, 'Distribution of numeric values, by observation_concept_id and unit_concept_id');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name)
	values (820, 'Number of observation records  by observation start month', 'calendar month');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name, stratum_2_name)
	values (891, 'Number of persons that have at least x observations', 'observation_concept_id', 'observation_count');

--900- DRUG_ERA


insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name)
	values (900, 'Number of persons with at least one drug era, by drug_concept_id', 'drug_concept_id');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name)
	values (901, 'Number of drug era records, by drug_concept_id', 'drug_concept_id');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name, stratum_2_name)
	values (902, 'Number of persons by drug era start month, by drug_concept_id', 'drug_concept_id', 'calendar month');	

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name)
	values (903, 'Number of distinct drug era concepts per person');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name, stratum_2_name, stratum_3_name, stratum_4_name)
	values (904, 'Number of persons with at least one drug era, by drug_concept_id by calendar year by gender by age decile', 'drug_concept_id', 'calendar year', 'gender_concept_id', 'age decile');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name, stratum_2_name)
	values (906, 'Distribution of age by drug_concept_id', 'drug_concept_id', 'gender_concept_id');
	
insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name)
	values (907, 'Distribution of drug era length, by drug_concept_id', 'drug_concept_id');	

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name)
	values (908, 'Number of drug eras without valid person');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name)
	values (909, 'Number of drug eras outside valid observation period');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name)
	values (910, 'Number of drug eras with end date < start date');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name)
	values (920, 'Number of drug era records  by drug era start month', 'calendar month');

--1000- CONDITION_ERA


insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name)
	values (1000, 'Number of persons with at least one condition era, by condition_concept_id', 'condition_concept_id');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name)
	values (1001, 'Number of condition era records, by condition_concept_id', 'condition_concept_id');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name, stratum_2_name)
	values (1002, 'Number of persons by condition era start month, by condition_concept_id', 'condition_concept_id', 'calendar month');	

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name)
	values (1003, 'Number of distinct condition era concepts per person');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name, stratum_2_name, stratum_3_name, stratum_4_name)
	values (1004, 'Number of persons with at least one condition era, by condition_concept_id by calendar year by gender by age decile', 'condition_concept_id', 'calendar year', 'gender_concept_id', 'age decile');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name, stratum_2_name)
	values (1006, 'Distribution of age by condition_concept_id', 'condition_concept_id', 'gender_concept_id');
	
insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name)
	values (1007, 'Distribution of condition era length, by condition_concept_id', 'condition_concept_id');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name)
	values (1008, 'Number of condition eras without valid person');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name)
	values (1009, 'Number of condition eras outside valid observation period');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name)
	values (1010, 'Number of condition eras with end date < start date');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name)
	values (1020, 'Number of condition era records by condition era start month', 'calendar month');



--1100- LOCATION


insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name)
	values (1100, 'Number of persons by location 3-digit zip', '3-digit zip');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name)
	values (1101, 'Number of persons by location state', 'state');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name)
	values (1102, 'Number of care sites by location 3-digit zip', '3-digit zip');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name)
	values (1103, 'Number of care sites by location state', 'state');


--1200- CARE_SITE

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name)
	values (1200, 'Number of persons by place of service', 'place_of_service_concept_id');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name)
	values (1201, 'Number of visits by place of service', 'place_of_service_concept_id');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name)
	values (1202, 'Number of care sites by place of service', 'place_of_service_concept_id');


--1300- ORGANIZATION

--NOT APPLICABLE IN CDMV5
--insert into synpuf_100.ACHILLES_analysis (analysis_id, analysis_name, stratum_1_name)
--	values (1300, 'Number of organizations by place of service', 'place_of_service_concept_id');


--1400- PAYOR_PLAN_PERIOD

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name)
	values (1406, 'Length of payer plan (days) of first payer plan period by gender', 'gender_concept_id');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name)
	values (1407, 'Length of payer plan (days) of first payer plan period by age decile', 'age_decile');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name)
	values (1408, 'Number of persons by length of payer plan period, in 30d increments', 'payer plan period length 30d increments');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name)
	values (1409, 'Number of persons with continuous payer plan in each year', 'calendar year');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name)
	values (1410, 'Number of persons with continuous payer plan in each month', 'calendar month');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name)
	values (1411, 'Number of persons by payer plan period start month', 'calendar month');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name)
	values (1412, 'Number of persons by payer plan period end month', 'calendar month');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name)
	values (1413, 'Number of persons by number of payer plan periods', 'number of payer plan periods');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name)
	values (1414, 'Number of persons with payer plan period before year-of-birth');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name)
	values (1415, 'Number of persons with payer plan period end < payer plan period start');

--1500- DRUG_COST



insert into synpuf_100.achilles_analysis (analysis_id, analysis_name)
	values (1500, 'Number of drug cost records with invalid drug exposure id');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name)
	values (1501, 'Number of drug cost records with invalid payer plan period id');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name)
	values (1502, 'Distribution of paid copay, by drug_concept_id', 'drug_concept_id');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name)
	values (1503, 'Distribution of paid coinsurance, by drug_concept_id', 'drug_concept_id');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name)
	values (1504, 'Distribution of paid toward deductible, by drug_concept_id', 'drug_concept_id');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name)
	values (1505, 'Distribution of paid by payer, by drug_concept_id', 'drug_concept_id');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name)
	values (1506, 'Distribution of paid by coordination of benefit, by drug_concept_id', 'drug_concept_id');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name)
	values (1507, 'Distribution of total out-of-pocket, by drug_concept_id', 'drug_concept_id');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name)
	values (1508, 'Distribution of total paid, by drug_concept_id', 'drug_concept_id');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name)
	values (1509, 'Distribution of ingredient_cost, by drug_concept_id', 'drug_concept_id');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name)
	values (1510, 'Distribution of dispensing fee, by drug_concept_id', 'drug_concept_id');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name)
	values (1511, 'Distribution of average wholesale price, by drug_concept_id', 'drug_concept_id');


--1600- PROCEDURE_COST



insert into synpuf_100.achilles_analysis (analysis_id, analysis_name)
	values (1600, 'Number of procedure cost records with invalid procedure occurrence id');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name)
	values (1601, 'Number of procedure cost records with invalid payer plan period id');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name)
	values (1602, 'Distribution of paid copay, by procedure_concept_id', 'procedure_concept_id');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name)
	values (1603, 'Distribution of paid coinsurance, by procedure_concept_id', 'procedure_concept_id');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name)
	values (1604, 'Distribution of paid toward deductible, by procedure_concept_id', 'procedure_concept_id');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name)
	values (1605, 'Distribution of paid by payer, by procedure_concept_id', 'procedure_concept_id');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name)
	values (1606, 'Distribution of paid by coordination of benefit, by procedure_concept_id', 'procedure_concept_id');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name)
	values (1607, 'Distribution of total out-of-pocket, by procedure_concept_id', 'procedure_concept_id');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name)
	values (1608, 'Distribution of total paid, by procedure_concept_id', 'procedure_concept_id');

--NOT APPLICABLE FOR OMOP CDM v5
--insert into synpuf_100.ACHILLES_analysis (analysis_id, analysis_name, stratum_1_name)
--	values (1609, 'Number of records by disease_class_concept_id', 'disease_class_concept_id');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name)
	values (1610, 'Number of records by revenue_code_concept_id', 'revenue_code_concept_id');


--1700- COHORT

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name)
	values (1700, 'Number of records by cohort_concept_id', 'cohort_concept_id');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name)
	values (1701, 'Number of records with cohort end date < cohort start date');

--1800- MEASUREMENT


insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name)
	values (1800, 'Number of persons with at least one measurement occurrence, by measurement_concept_id', 'measurement_concept_id');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name)
	values (1801, 'Number of measurement occurrence records, by measurement_concept_id', 'measurement_concept_id');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name, stratum_2_name)
	values (1802, 'Number of persons by measurement occurrence start month, by measurement_concept_id', 'measurement_concept_id', 'calendar month');	

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name)
	values (1803, 'Number of distinct mesurement occurrence concepts per person');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name, stratum_2_name, stratum_3_name, stratum_4_name)
	values (1804, 'Number of persons with at least one mesurement  occurrence, by measurement_concept_id by calendar year by gender by age decile', 'measurement_concept_id', 'calendar year', 'gender_concept_id', 'age decile');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name, stratum_2_name)
	values (1805, 'Number of measurement occurrence records, by measurement_concept_id by measurement_type_concept_id', 'measurement_concept_id', 'measurement_type_concept_id');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name, stratum_2_name)
	values (1806, 'Distribution of age by measurement_concept_id', 'measurement_concept_id', 'gender_concept_id');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name, stratum_2_name)
	values (1807, 'Number of measurement occurrence records, by measurement_concept_id and unit_concept_id', 'measurement_concept_id', 'unit_concept_id');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name)
	values (1809, 'Number of measurement records with invalid person_id');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name)
	values (1810, 'Number of measurement records outside valid observation period');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name)
	values (1812, 'Number of measurement records with invalid provider_id');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name)
	values (1813, 'Number of measurement records with invalid visit_id');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name)
	values (1814, 'Number of measurement records with no value (numeric, string, or concept)');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name)
	values (1815, 'Distribution of numeric values, by measurement_concept_id and unit_concept_id');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name)
	values (1816, 'Distribution of low range, by measurement_concept_id and unit_concept_id');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name)
	values (1817, 'Distribution of high range, by observation_concept_id and unit_concept_id');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name)
	values (1818, 'Number of measurement records below/within/above normal range, by measurement_concept_id and unit_concept_id');


insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name)
	values (1820, 'Number of measurement records  by measurement start month', 'calendar month');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name)
	values (1821, 'Number of measurement records with no numeric value');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name, stratum_2_name)
	values (1891, 'Number of persons that have at least x measurements', 'measurement_concept_id', 'measurement_count');

--1900 REPORTS

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name, stratum_2_name)
	values (1900, 'Source values mapped to concept_id 0 by table, by source_value', 'table_name', 'source_value');


--2000 Iris (and possibly other new measures) integrated into Achilles

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name)
	values (2000, 'Number of patients with at least 1 Dx and 1 Rx');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name)
	values (2001, 'Number of patients with at least 1 Dx and 1 Proc');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name)
	values (2002, 'Number of patients with at least 1 Meas, 1 Dx and 1 Rx');
	
insert into synpuf_100.achilles_analysis (analysis_id, analysis_name)
	values (2003, 'Number of patients with at least 1 Visit');


--2100- DEVICE_EXPOSURE


insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name)
	values (2100, 'Number of persons with at least one device exposure, by device_concept_id', 'device_concept_id');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name)
	values (2101, 'Number of device exposure records, by device_concept_id', 'device_concept_id');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name, stratum_2_name)
	values (2102, 'Number of persons by device records  start month, by device_concept_id', 'device_concept_id', 'calendar month');	

--2103 was not implemented at this point

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name, stratum_2_name, stratum_3_name, stratum_4_name)
	values (2104, 'Number of persons with at least one device exposure, by device_concept_id by calendar year by gender by age decile', 'device_concept_id', 'calendar year', 'gender_concept_id', 'age decile');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name, stratum_2_name)
	values (2105, 'Number of device exposure records, by device_concept_id by device_type_concept_id', 'device_concept_id', 'device_type_concept_id');



--2200- NOTE


insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name)
	values (2200, 'Number of persons with at least one note by  note_type_concept_id', 'note_type_concept_id');

insert into synpuf_100.achilles_analysis (analysis_id, analysis_name, stratum_1_name)
	values (2201, 'Number of note records, by note_type_concept_id', 'note_type_concept_id');




--end of importing values into analysis lookup table

--


/* create achilles_results nonclustered indexes if dbms is PDW */

--


/****
7. generate results for analysis_results


****/

--
-- 0	cdm name, version of Achilles and date when pre-computations were executed
insert into synpuf_100.achilles_results (analysis_id, stratum_1, stratum_2, stratum_3,count_value)
select  0 as analysis_id, CAST('my_source'  AS STRING) as stratum_1, CAST('1.4.6'  AS STRING) as stratum_2, CAST(CURRENT_DATE()  AS STRING) as stratum_3, COUNT(distinct person_id) as count_value
 from  synpuf_100.person;

insert into synpuf_100.achilles_results_dist (analysis_id, stratum_1, count_value)
select  0 as analysis_id, CAST('my_source'  AS STRING) as stratum_1, COUNT(distinct person_id) as count_value
 from  synpuf_100.person;

--


/********************************************

ACHILLES Analyses on PERSON table

*********************************************/



--
-- 1	Number of persons
insert into synpuf_100.achilles_results (analysis_id, count_value)
select  1 as analysis_id, COUNT(distinct person_id) as count_value
 from  synpuf_100.person;
--


--
-- 2	Number of persons by gender
insert into synpuf_100.achilles_results (analysis_id, stratum_1, count_value)
 select  2 as analysis_id, CAST(gender_concept_id  AS STRING) as stratum_1, COUNT(distinct person_id) as count_value
  from  synpuf_100.person
 group by  2 ;
--



--
-- 3	Number of persons by year of birth
insert into synpuf_100.achilles_results (analysis_id, stratum_1, count_value)
 select  3 as analysis_id, CAST(year_of_birth  AS STRING) as stratum_1, COUNT(distinct person_id) as count_value
  from  synpuf_100.person
 group by  2 ;
--


--
-- 4	Number of persons by race
insert into synpuf_100.achilles_results (analysis_id, stratum_1, count_value)
 select  4 as analysis_id, CAST(race_concept_id  AS STRING) as stratum_1, COUNT(distinct person_id) as count_value
  from  synpuf_100.person
 group by  2 ;
--



--
-- 5	Number of persons by ethnicity
insert into synpuf_100.achilles_results (analysis_id, stratum_1, count_value)
 select  5 as analysis_id, CAST(ethnicity_concept_id  AS STRING) as stratum_1, COUNT(distinct person_id) as count_value
  from  synpuf_100.person
 group by  2 ;
--





--
-- 7	Number of persons with invalid provider_id
insert into synpuf_100.achilles_results (analysis_id, count_value)
select  7 as analysis_id, COUNT(p1.person_id) as count_value
 from  synpuf_100.person p1
	left join synpuf_100.provider pr1
	on p1.provider_id = pr1.provider_id
where p1.provider_id is not null
	and pr1.provider_id is null
;
--



--
-- 8	Number of persons with invalid location_id
insert into synpuf_100.achilles_results (analysis_id, count_value)
select  8 as analysis_id, COUNT(p1.person_id) as count_value
 from  synpuf_100.person p1
	left join synpuf_100.location l1
	on p1.location_id = l1.location_id
where p1.location_id is not null
	and l1.location_id is null
;
--


--
-- 9	Number of persons with invalid care_site_id
insert into synpuf_100.achilles_results (analysis_id, count_value)
select  9 as analysis_id, COUNT(p1.person_id) as count_value
 from  synpuf_100.person p1
	left join synpuf_100.care_site cs1
	on p1.care_site_id = cs1.care_site_id
where p1.care_site_id is not null
	and cs1.care_site_id is null
;
--



--
-- 10	Number of all persons by year of birth and by gender
insert into synpuf_100.achilles_results (analysis_id, stratum_1, stratum_2, count_value)
 select  10 as analysis_id, CAST(year_of_birth  AS STRING) as stratum_1, CAST(gender_concept_id  AS STRING) as stratum_2, COUNT(distinct person_id) as count_value
  from  synpuf_100.person
 group by  2, 3 ;
--


--
-- 11	Number of non-deceased persons by year of birth and by gender
insert into synpuf_100.achilles_results (analysis_id, stratum_1, stratum_2, count_value)
 select  11 as analysis_id, CAST(year_of_birth  AS STRING) as stratum_1, CAST(gender_concept_id  AS STRING) as stratum_2, COUNT(distinct person_id) as count_value
  from  synpuf_100.person
where person_id not in (select  person_id  from  synpuf_100.death)
 group by  2, 3 ;
--



--
-- 12	Number of persons by race and ethnicity
insert into synpuf_100.achilles_results (analysis_id, stratum_1, stratum_2, count_value)
 select  12 as analysis_id, CAST(race_concept_id  AS STRING) as stratum_1, CAST(ethnicity_concept_id  AS STRING) as stratum_2, COUNT(distinct person_id) as count_value
  from  synpuf_100.person
 group by  2, 3 ;
--

/********************************************

ACHILLES Analyses on OBSERVATION_PERIOD table

*********************************************/

--
-- 101	Number of persons by age, with age at first observation period
insert into synpuf_100.achilles_results (analysis_id, stratum_1, count_value)
 select  101 as analysis_id, CAST(EXTRACT(YEAR from op1.index_date) - p1.year_of_birth  AS STRING) as stratum_1, COUNT(p1.person_id) as count_value
  from  synpuf_100.person p1
	inner join ( select  person_id, min(observation_period_start_date) as index_date   from  synpuf_100.observation_period  group by  1 ) op1
	on p1.person_id = op1.person_id
 group by  2 ;
--



--
-- 102	Number of persons by gender by age, with age at first observation period
insert into synpuf_100.achilles_results (analysis_id, stratum_1, stratum_2, count_value)
 select  102 as analysis_id, CAST(p1.gender_concept_id  AS STRING) as stratum_1, CAST(EXTRACT(YEAR from op1.index_date) - p1.year_of_birth  AS STRING) as stratum_2, COUNT(p1.person_id) as count_value
  from  synpuf_100.person p1
	inner join ( select  person_id, min(observation_period_start_date) as index_date   from  synpuf_100.observation_period  group by  1 ) op1
	on p1.person_id = op1.person_id
 group by  p1.gender_concept_id, 3 ;
--


--
-- 103	Distribution of age at first observation period
INTO temp.tempresults
   WITH rawdata  as ( select  p.person_id as person_id, min(EXTRACT(YEAR from observation_period_start_date)) - p.year_of_birth  as age_value   from  synpuf_100.person p
  join synpuf_100.observation_period op on p.person_id = op.person_id
   group by  p.person_id, p.year_of_birth
 ), overallstats  as (select  cast(avg(1.0 * age_value)  as float64)  as avg_value, cast(STDDEV(age_value)  as float64)  as stdev_value, min(age_value)  as min_value, max(age_value)  as max_value, COUNT(*)  as total  from  rawdata
), agestats  as ( select  age_value as age_value, COUNT(*)  as total, row_number() over (order by age_value)  as rn   from  rawdata
   group by  1 ), agestatsprior  as ( select  s.age_value as age_value, s.total as total, sum(p.total)  as accumulated   from  agestats s
  join agestats p on p.rn <= s.rn
   group by  s.age_value, s.total, s.rn
 )
 select  103 as analysis_id, o.total as count_value, o.min_value, o.max_value, o.avg_value, o.stdev_value, min(case when p.accumulated >= .50 * o.total then age_value end) as median_value, min(case when p.accumulated >= .10 * o.total then age_value end) as p10_value, min(case when p.accumulated >= .25 * o.total then age_value end) as p25_value, min(case when p.accumulated >= .75 * o.total then age_value end) as p75_value, min(case when p.accumulated >= .90 * o.total then age_value end) as p90_value
  FROM  agestatsprior p
cross join overallstats o
 group by  o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
 ;

insert into synpuf_100.achilles_results_dist (analysis_id, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value)
select  analysis_id, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
 from  temp.tempresults
;

truncate table temp.tempresults;
drop table temp.tempresults;
--



--
-- 104	Distribution of age at first observation period by gender
INTO temp.tempresults
   WITH rawdata  as ( select  p.gender_concept_id as gender_concept_id, min(EXTRACT(YEAR from observation_period_start_date)) - p.year_of_birth  as age_value   from  synpuf_100.person p
	join synpuf_100.observation_period op on p.person_id = op.person_id
	 group by  p.person_id, p.gender_concept_id, p.year_of_birth
 ), overallstats  as ( select  gender_concept_id as gender_concept_id, cast(avg(1.0 * age_value)  as float64)  as avg_value, cast(STDDEV(age_value)  as float64)  as stdev_value, min(age_value)  as min_value, max(age_value)  as max_value, COUNT(*)  as total   from  rawdata
   group by  1 ), agestats  as ( select  gender_concept_id as gender_concept_id, age_value as age_value, COUNT(*)  as total, row_number() over (order by age_value)  as rn   from  rawdata
   group by  1, 2 ), agestatsprior  as ( select  s.gender_concept_id as gender_concept_id, s.age_value as age_value, s.total as total, sum(p.total)  as accumulated   from  agestats s
  join agestats p on s.gender_concept_id = p.gender_concept_id and p.rn <= s.rn
   group by  s.gender_concept_id, s.age_value, s.total, s.rn
 )
 select  104 as analysis_id, CAST(o.gender_concept_id  AS STRING) as stratum_1, o.total as count_value, o.min_value, o.max_value, o.avg_value, o.stdev_value, min(case when p.accumulated >= .50 * o.total then age_value end) as median_value, min(case when p.accumulated >= .10 * o.total then age_value end) as p10_value, min(case when p.accumulated >= .25 * o.total then age_value end) as p25_value, min(case when p.accumulated >= .75 * o.total then age_value end) as p75_value, min(case when p.accumulated >= .90 * o.total then age_value end) as p90_value
  FROM  agestatsprior p
join overallstats o on p.gender_concept_id = o.gender_concept_id
 group by  o.gender_concept_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
 ;

insert into synpuf_100.achilles_results_dist (analysis_id, stratum_1, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value)
select  analysis_id, stratum_1, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
 from  temp.tempresults
;

truncate table temp.tempresults;
drop table temp.tempresults;
--

--
-- 105	Length of observation (days) of first observation period
INTO temp.tempresults
   WITH rawdata  as (select  count_value
   as count_value  from  (
    select  DATE_DIFF(cast(op.observation_period_end_date as date), cast(op.observation_period_start_date as date), DAY) as count_value, row_number() over (partition by op.person_id order by op.observation_period_start_date asc) as rn
     from  synpuf_100.observation_period op
	) op
	where op.rn = 1
), overallstats  as (select  cast(avg(1.0 * count_value)  as float64)  as avg_value, cast(STDDEV(count_value)  as float64)  as stdev_value, min(count_value)  as min_value, max(count_value)  as max_value, COUNT(*)  as total  from  rawdata
), statsview  as ( select  count_value as count_value, COUNT(*)  as total, row_number() over (order by count_value)  as rn   from  (
    select  DATE_DIFF(cast(op.observation_period_end_date as date), cast(op.observation_period_start_date as date), DAY) as count_value, row_number() over (partition by op.person_id order by op.observation_period_start_date asc) as rn
     from  synpuf_100.observation_period op
	) op
  where op.rn = 1
   group by  1 ), priorstats  as ( select  s.count_value as count_value, s.total as total, sum(p.total)  as accumulated   from  statsview s
  join statsview p on p.rn <= s.rn
   group by  s.count_value, s.total, s.rn
 )
 select  105 as analysis_id, o.total as count_value, o.min_value, o.max_value, o.avg_value, o.stdev_value, min(case when p.accumulated >= .50 * o.total then count_value end) as median_value, min(case when p.accumulated >= .10 * o.total then count_value end) as p10_value, min(case when p.accumulated >= .25 * o.total then count_value end) as p25_value, min(case when p.accumulated >= .75 * o.total then count_value end) as p75_value, min(case when p.accumulated >= .90 * o.total then count_value end) as p90_value
  FROM  priorstats p
cross join overallstats o
 group by  o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
 ;

insert into synpuf_100.achilles_results_dist (analysis_id, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value)
select  analysis_id, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
 from  temp.tempresults
;

truncate table temp.tempresults;
drop table temp.tempresults;
--


--
-- 106	Length of observation (days) of first observation period by gender
INTO temp.tempresults
   WITH rawdata as (select  p.gender_concept_id as gender_concept_id, op.count_value
   as count_value  from  (
    select  person_id, DATE_DIFF(cast(op.observation_period_end_date as date), cast(op.observation_period_start_date as date), DAY) as count_value, row_number() over (partition by op.person_id order by op.observation_period_start_date asc) as rn
     from  synpuf_100.observation_period op
	) op
  join synpuf_100.person p on op.person_id = p.person_id
	where op.rn = 1
), overallstats  as ( select  gender_concept_id as gender_concept_id, cast(avg(1.0 * count_value)  as float64)  as avg_value, cast(STDDEV(count_value)  as float64)  as stdev_value, min(count_value)  as min_value, max(count_value)  as max_value, COUNT(*)  as total   from  rawdata
   group by  1 ), statsview  as ( select  gender_concept_id as gender_concept_id, count_value as count_value, COUNT(*)  as total, row_number() over (order by count_value)  as rn   from  rawdata
   group by  1, 2 ), priorstats  as ( select  s.gender_concept_id as gender_concept_id, s.count_value as count_value, s.total as total, sum(p.total)  as accumulated   from  statsview s
  join statsview p on s.gender_concept_id = p.gender_concept_id and p.rn <= s.rn
   group by  s.gender_concept_id, s.count_value, s.total, s.rn
 )
 select  106 as analysis_id, CAST(o.gender_concept_id  AS STRING) as gender_concept_id, o.total as count_value, o.min_value, o.max_value, o.avg_value, o.stdev_value, min(case when p.accumulated >= .50 * o.total then count_value end) as median_value, min(case when p.accumulated >= .10 * o.total then count_value end) as p10_value, min(case when p.accumulated >= .25 * o.total then count_value end) as p25_value, min(case when p.accumulated >= .75 * o.total then count_value end) as p75_value, min(case when p.accumulated >= .90 * o.total then count_value end) as p90_value
  FROM  priorstats p
join overallstats o on p.gender_concept_id = o.gender_concept_id
 group by  o.gender_concept_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
 ;

insert into synpuf_100.achilles_results_dist (analysis_id, stratum_1, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value)
select  analysis_id, gender_concept_id, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
 from  temp.tempresults
;

truncate table temp.tempresults;
drop table temp.tempresults;

--

--
-- 107	Length of observation (days) of first observation period by age decile

INTO temp.tempresults
   WITH rawdata  as (select  floor((EXTRACT(YEAR from op.observation_period_start_date) - p.year_of_birth)/10)  as age_decile, DATE_DIFF(cast(op.observation_period_end_date as date), cast(op.observation_period_start_date as date), DAY)  as count_value  from  (
    select  person_id, op.observation_period_start_date, op.observation_period_end_date, row_number() over (partition by op.person_id order by op.observation_period_start_date asc) as rn
     from  synpuf_100.observation_period op
  ) op
  join synpuf_100.person p on op.person_id = p.person_id
  where op.rn = 1
), overallstats  as ( select  age_decile as age_decile, cast(avg(1.0 * count_value)  as float64)  as avg_value, cast(STDDEV(count_value)  as float64)  as stdev_value, min(count_value)  as min_value, max(count_value)  as max_value, COUNT(*)  as total   from  rawdata
   group by  1 ), statsview  as ( select  age_decile as age_decile, count_value as count_value, COUNT(*)  as total, row_number() over (order by count_value)  as rn   from  rawdata
   group by  1, 2 ), priorstats  as ( select  s.age_decile as age_decile, s.count_value as count_value, s.total as total, sum(p.total)  as accumulated   from  statsview s
  join statsview p on s.age_decile = p.age_decile and p.rn <= s.rn
   group by  s.age_decile, s.count_value, s.total, s.rn
 )
 select  107 as analysis_id, CAST(o.age_decile  AS STRING) as age_decile, o.total as count_value, o.min_value, o.max_value, o.avg_value, o.stdev_value, min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value, min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value, min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value, min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value, min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
  FROM  priorstats p
join overallstats o on p.age_decile = o.age_decile
 group by  o.age_decile, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
 ;

insert into synpuf_100.achilles_results_dist (analysis_id, stratum_1, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value)
select  analysis_id, age_decile, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
 from  temp.tempresults
;

truncate table temp.tempresults;
drop table temp.tempresults;

--


--
-- 108	Number of persons by length of observation period, in 30d increments
insert into synpuf_100.achilles_results (analysis_id, stratum_1, count_value)
 select  108 as analysis_id, CAST(floor(DATE_DIFF(cast(op1.observation_period_end_date as date), cast(op1.observation_period_start_date as date), DAY)/30)  AS STRING) as stratum_1, COUNT(distinct p1.person_id) as count_value
  from  synpuf_100.person p1
	inner join 
	(select  person_id, observation_period_start_date, observation_period_end_date, row_number() over (partition by person_id order by observation_period_start_date asc) as rn1
		  from  synpuf_100.observation_period
	) op1
	on p1.person_id = op1.person_id
	where op1.rn1 = 1
 group by  2 ;
--




--
-- 109	Number of persons with continuous observation in each year
-- Note: using temp table instead of nested query because this gives vastly improved performance in Oracle

DROP TABLE IF EXISTS temp.temp_dates;

INTO temp.temp_dates
  SELECT distinct  EXTRACT(YEAR from observation_period_start_date) as obs_year, parse_date('%Y%m%d', concat(concat(CAST(EXTRACT(YEAR from observation_period_start_date)  AS STRING), '01'), '01')) as obs_year_start, parse_date('%Y%m%d', concat(concat(CAST(EXTRACT(YEAR from observation_period_start_date)  AS STRING), '12'), '31')) as obs_year_end
  FROM  synpuf_100.observation_period
;

insert into synpuf_100.achilles_results (analysis_id, stratum_1, count_value)
 select  109 as analysis_id, CAST(obs_year  AS STRING) as stratum_1, COUNT(distinct person_id) as count_value
  from  synpuf_100.observation_period,
	temp.temp_dates
where  
		observation_period_start_date <= obs_year_start
	and 
		observation_period_end_date >= obs_year_end
 group by  2 ;

truncate table temp.temp_dates;
drop table temp.temp_dates;
--


--
-- 110	Number of persons with continuous observation in each month
-- Note: using temp table instead of nested query because this gives vastly improved performance in Oracle

DROP TABLE IF EXISTS temp.temp_dates;

INTO temp.temp_dates
  SELECT distinct  EXTRACT(YEAR from observation_period_start_date)*100 + EXTRACT(MONTH from observation_period_start_date) as obs_month, parse_date('%Y%m%d', concat(concat(CAST(EXTRACT(YEAR from observation_period_start_date)  AS STRING), SUBSTR(concat('0', CAST(EXTRACT(MONTH from observation_period_start_date)  AS STRING)),-2)), '01'))
  as obs_month_start, DATE_ADD(cast(DATE_ADD(cast(parse_date('%Y%m%d', concat(concat(CAST(EXTRACT(YEAR from observation_period_start_date)  AS STRING), SUBSTR(concat('0', CAST(EXTRACT(MONTH from observation_period_start_date)  AS STRING)),-2)), '01')) as date), interval 1 MONTH) as date), interval -1 DAY) as obs_month_end
  FROM  synpuf_100.observation_period
;


insert into synpuf_100.achilles_results (analysis_id, stratum_1, count_value)
 select  110 as analysis_id, CAST(obs_month  AS STRING) as stratum_1, COUNT(distinct person_id) as count_value
  from  synpuf_100.observation_period,
	temp.temp_dates
where 
		observation_period_start_date <= obs_month_start
	and
		observation_period_end_date >= obs_month_end
 group by  2 ;

truncate table temp.temp_dates;
drop table temp.temp_dates;
--



--
-- 111	Number of persons by observation period start month
insert into synpuf_100.achilles_results (analysis_id, stratum_1, count_value)
 select  111 as analysis_id, CAST(EXTRACT(YEAR from observation_period_start_date)*100 + EXTRACT(MONTH from observation_period_start_date)  AS STRING) as stratum_1, COUNT(distinct op1.person_id) as count_value
  from  synpuf_100.observation_period op1
 group by  2 ;
--



--
-- 112	Number of persons by observation period end month
insert into synpuf_100.achilles_results (analysis_id, stratum_1, count_value)
 select  112 as analysis_id, CAST(EXTRACT(YEAR from observation_period_end_date)*100 + EXTRACT(MONTH from observation_period_end_date)  AS STRING) as stratum_1, COUNT(distinct op1.person_id) as count_value
  from  synpuf_100.observation_period op1
 group by  2 ;
--


--
-- 113	Number of persons by number of observation periods
insert into synpuf_100.achilles_results (analysis_id, stratum_1, count_value)
 select  113 as analysis_id, CAST(op1.num_periods  AS STRING) as stratum_1, COUNT(distinct op1.person_id) as count_value
  from  ( select  person_id, COUNT(observation_period_start_date) as num_periods   from  synpuf_100.observation_period  group by  1 ) op1
 group by  op1.num_periods
 ;
--

--
-- 114	Number of persons with observation period before year-of-birth
insert into synpuf_100.achilles_results (analysis_id, count_value)
select  114 as analysis_id, COUNT(distinct p1.person_id) as count_value
 from 
	synpuf_100.person p1
	inner join ( select  person_id, min(EXTRACT(YEAR from observation_period_start_date)) as first_obs_year   from  synpuf_100.observation_period  group by  1 ) op1
	on p1.person_id = op1.person_id
where p1.year_of_birth > op1.first_obs_year
;
--

--
-- 115	Number of persons with observation period end < start
insert into synpuf_100.achilles_results (analysis_id, count_value)
select  115 as analysis_id, COUNT(op1.person_id) as count_value
 from 
	synpuf_100.observation_period op1
where op1.observation_period_end_date < op1.observation_period_start_date
;
--



--
-- 116	Number of persons with at least one day of observation in each year by gender and age decile
-- Note: using temp table instead of nested query because this gives vastly improved performance in Oracle

DROP TABLE IF EXISTS temp.temp_dates;

INTO temp.temp_dates
  SELECT distinct  EXTRACT(YEAR from observation_period_start_date) as obs_year 
  FROM  
  synpuf_100.observation_period
;

insert into synpuf_100.achilles_results (analysis_id, stratum_1, stratum_2, stratum_3, count_value)
 select  116 as analysis_id, CAST(t1.obs_year  AS STRING) as stratum_1, CAST(p1.gender_concept_id  AS STRING) as stratum_2, CAST(floor((t1.obs_year - p1.year_of_birth)/10)  AS STRING) as stratum_3, COUNT(distinct p1.person_id) as count_value
  from  synpuf_100.person p1
	inner join 
  synpuf_100.observation_period op1
	on p1.person_id = op1.person_id
	,
	temp.temp_dates t1 
where EXTRACT(YEAR from op1.observation_period_start_date) <= t1.obs_year
	and EXTRACT(YEAR from op1.observation_period_end_date) >= t1.obs_year
 group by  t1.obs_year, p1.gender_concept_id, 4 ;

truncate table temp.temp_dates;
drop table temp.temp_dates;
--


--
-- 117	Number of persons with at least one day of observation in each year by gender and age decile
-- Note: using temp table instead of nested query because this gives vastly improved performance in Oracle

DROP TABLE IF EXISTS temp.temp_dates;

INTO temp.temp_dates
  SELECT distinct  EXTRACT(YEAR from observation_period_start_date)*100 + EXTRACT(MONTH from observation_period_start_date)  as obs_month
  FROM  
  synpuf_100.observation_period
;

insert into synpuf_100.achilles_results (analysis_id, stratum_1, count_value)
 select  117 as analysis_id, CAST(t1.obs_month  AS STRING) as stratum_1, COUNT(distinct op1.person_id) as count_value
  from  synpuf_100.observation_period op1,
	temp.temp_dates t1 
where EXTRACT(YEAR from observation_period_start_date)*100 + EXTRACT(MONTH from observation_period_start_date) <= t1.obs_month
	and EXTRACT(YEAR from observation_period_end_date)*100 + EXTRACT(MONTH from observation_period_end_date) >= t1.obs_month
 group by  t1.obs_month
 ;

truncate table temp.temp_dates;
drop table temp.temp_dates;
--


--
-- 118  Number of observation period records with invalid person_id
insert into synpuf_100.achilles_results (analysis_id, count_value)
select  118 as analysis_id, COUNT(op1.person_id) as count_value
 from 
  synpuf_100.observation_period op1
  left join synpuf_100.person p1
  on p1.person_id = op1.person_id
where p1.person_id is null
;
--

--
-- 119  Number of observation period records by period_type_concept_id
insert into synpuf_100.achilles_results (analysis_id, stratum_1,count_value)
 select  119 as analysis_id, CAST(op1.period_type_concept_id  AS STRING) as stratum_1, COUNT(*) as count_value
  from  synpuf_100.observation_period op1
 group by  op1.period_type_concept_id
 ;
--


/********************************************

ACHILLES Analyses on VISIT_OCCURRENCE table

*********************************************/


--
-- 200	Number of persons with at least one visit occurrence, by visit_concept_id
insert into synpuf_100.achilles_results (analysis_id, stratum_1, count_value)
 select  200 as analysis_id, CAST(vo1.visit_concept_id  AS STRING) as stratum_1, COUNT(distinct vo1.person_id) as count_value
  from  synpuf_100.visit_occurrence vo1
 group by  vo1.visit_concept_id
 ;
--


--
-- 201	Number of visit occurrence records, by visit_concept_id
insert into synpuf_100.achilles_results (analysis_id, stratum_1, count_value)
 select  201 as analysis_id, CAST(vo1.visit_concept_id  AS STRING) as stratum_1, COUNT(vo1.person_id) as count_value
  from  synpuf_100.visit_occurrence vo1
 group by  vo1.visit_concept_id
 ;
--



--
-- 202	Number of persons by visit occurrence start month, by visit_concept_id
insert into synpuf_100.achilles_results (analysis_id, stratum_1, stratum_2, count_value)
 select  202 as analysis_id, CAST(vo1.visit_concept_id  AS STRING) as stratum_1, CAST(EXTRACT(YEAR from visit_start_date)*100 + EXTRACT(MONTH from visit_start_date)  AS STRING) as stratum_2, COUNT(distinct person_id) as count_value
  from  synpuf_100.visit_occurrence vo1
 group by  vo1.visit_concept_id, 3 ;
--



--
-- 203	Number of distinct visit occurrence concepts per person

INTO temp.tempresults
   WITH rawdata as ( select  vo1.person_id as person_id, COUNT(distinct vo1.visit_concept_id)  as count_value   from  synpuf_100.visit_occurrence vo1
		 group by  vo1.person_id
 ), overallstats  as (select  cast(avg(1.0 * count_value)  as float64)  as avg_value, cast(STDDEV(count_value)  as float64)  as stdev_value, min(count_value)  as min_value, max(count_value)  as max_value, COUNT(*)  as total  from  rawdata
), statsview  as ( select  count_value as count_value, COUNT(*)  as total, row_number() over (order by count_value)  as rn   from  rawdata
   group by  1 ), priorstats  as ( select  s.count_value as count_value, s.total as total, sum(p.total)  as accumulated   from  statsview s
  join statsview p on p.rn <= s.rn
   group by  s.count_value, s.total, s.rn
 )
 select  203 as analysis_id, o.total as count_value, o.min_value, o.max_value, o.avg_value, o.stdev_value, min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value, min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value, min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value, min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value, min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
  FROM  priorstats p
cross join overallstats o
 group by  o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
 ;

insert into synpuf_100.achilles_results_dist (analysis_id, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value)
select  analysis_id, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
 from  temp.tempresults
;

truncate table temp.tempresults;
drop table temp.tempresults;

--



--
-- 204	Number of persons with at least one visit occurrence, by visit_concept_id by calendar year by gender by age decile
insert into synpuf_100.achilles_results (analysis_id, stratum_1, stratum_2, stratum_3, stratum_4, count_value)
 select  204 as analysis_id, CAST(vo1.visit_concept_id  AS STRING) as stratum_1, CAST(EXTRACT(YEAR from visit_start_date)  AS STRING) as stratum_2, CAST(p1.gender_concept_id  AS STRING) as stratum_3, CAST(floor((EXTRACT(YEAR from visit_start_date) - p1.year_of_birth)/10)  AS STRING) as stratum_4, COUNT(distinct p1.person_id) as count_value
  from  synpuf_100.person p1
inner join
synpuf_100.visit_occurrence vo1
on p1.person_id = vo1.person_id
 group by  vo1.visit_concept_id, 3, p1.gender_concept_id, 5 ;
--





--
-- 206	Distribution of age by visit_concept_id

INTO temp.tempresults
   WITH rawdata as (select  vo1.visit_concept_id as stratum1_id, p1.gender_concept_id as stratum2_id, vo1.visit_start_year - p1.year_of_birth  as count_value  from  synpuf_100.person p1
	inner join 
  (
		 select  person_id, visit_concept_id, min(EXTRACT(YEAR from visit_start_date)) as visit_start_year
		  from  synpuf_100.visit_occurrence
		 group by  1, 2 ) vo1 on p1.person_id = vo1.person_id
), overallstats  as ( select  stratum1_id as stratum1_id, stratum2_id as stratum2_id, cast(avg(1.0 * count_value)  as float64)  as avg_value, cast(STDDEV(count_value)  as float64)  as stdev_value, min(count_value)  as min_value, max(count_value)  as max_value, COUNT(*)  as total   from  rawdata
	 group by  1, 2 ), statsview  as ( select  stratum1_id as stratum1_id, stratum2_id as stratum2_id, count_value as count_value, COUNT(*)  as total, row_number() over (partition by stratum1_id, stratum2_id order by count_value)  as rn   from  rawdata
   group by  1, 2, 3 ), priorstats  as ( select  s.stratum1_id as stratum1_id, s.stratum2_id as stratum2_id, s.count_value as count_value, s.total as total, sum(p.total)  as accumulated   from  statsview s
  join statsview p on s.stratum1_id = p.stratum1_id and s.stratum2_id = p.stratum2_id and p.rn <= s.rn
   group by  s.stratum1_id, s.stratum2_id, s.count_value, s.total, s.rn
 )
 select  206 as analysis_id, CAST(o.stratum1_id  AS STRING) as stratum1_id, CAST(o.stratum2_id  AS STRING) as stratum2_id, o.total as count_value, o.min_value, o.max_value, o.avg_value, o.stdev_value, min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value, min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value, min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value, min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value, min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
  FROM  priorstats p
join overallstats o on p.stratum1_id = o.stratum1_id and p.stratum2_id = o.stratum2_id 
 group by  o.stratum1_id, o.stratum2_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
 ;

insert into synpuf_100.achilles_results_dist (analysis_id, stratum_1, stratum_2, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value)
select  analysis_id, stratum1_id, stratum2_id, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
 from  temp.tempresults
;

truncate table temp.tempresults;
drop table temp.tempresults;

--


--
--207	Number of visit records with invalid person_id
insert into synpuf_100.achilles_results (analysis_id, count_value)
select  207 as analysis_id, COUNT(vo1.person_id) as count_value
 from 
	synpuf_100.visit_occurrence vo1
	left join synpuf_100.person p1
	on p1.person_id = vo1.person_id
where p1.person_id is null
;
--


--
--208	Number of visit records outside valid observation period
insert into synpuf_100.achilles_results (analysis_id, count_value)
select  208 as analysis_id, COUNT(vo1.person_id) as count_value
 from 
	synpuf_100.visit_occurrence vo1
	left join synpuf_100.observation_period op1
	on op1.person_id = vo1.person_id
	and vo1.visit_start_date >= op1.observation_period_start_date
	and vo1.visit_start_date <= op1.observation_period_end_date
where op1.person_id is null
;
--

--
--209	Number of visit records with end date < start date
insert into synpuf_100.achilles_results (analysis_id, count_value)
select  209 as analysis_id, COUNT(vo1.person_id) as count_value
 from 
	synpuf_100.visit_occurrence vo1
where visit_end_date < visit_start_date
;
--

--
--210	Number of visit records with invalid care_site_id
insert into synpuf_100.achilles_results (analysis_id, count_value)
select  210 as analysis_id, COUNT(vo1.person_id) as count_value
 from 
	synpuf_100.visit_occurrence vo1
	left join synpuf_100.care_site cs1
	on vo1.care_site_id = cs1.care_site_id
where vo1.care_site_id is not null
	and cs1.care_site_id is null
;
--


--
-- 211	Distribution of length of stay by visit_concept_id
INTO temp.tempresults
   WITH rawdata as (select  visit_concept_id as stratum_id, DATE_DIFF(cast(visit_end_date as date), cast(visit_start_date as date), DAY)  as count_value  from  synpuf_100.visit_occurrence
), overallstats  as ( select  stratum_id as stratum_id, cast(avg(1.0 * count_value)  as float64)  as avg_value, cast(STDDEV(count_value)  as float64)  as stdev_value, min(count_value)  as min_value, max(count_value)  as max_value, COUNT(*)  as total   from  rawdata
   group by  1 ), statsview  as ( select  stratum_id as stratum_id, count_value as count_value, COUNT(*)  as total, row_number() over (order by count_value)  as rn   from  rawdata
   group by  1, 2 ), priorstats  as ( select  s.stratum_id as stratum_id, s.count_value as count_value, s.total as total, sum(p.total)  as accumulated   from  statsview s
  join statsview p on s.stratum_id = p.stratum_id and p.rn <= s.rn
   group by  s.stratum_id, s.count_value, s.total, s.rn
 )
 select  211 as analysis_id, CAST(o.stratum_id  AS STRING) as stratum_id, o.total as count_value, o.min_value, o.max_value, o.avg_value, o.stdev_value, min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value, min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value, min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value, min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value, min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
  FROM  priorstats p
join overallstats o on p.stratum_id = o.stratum_id
 group by  o.stratum_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
 ;

insert into synpuf_100.achilles_results_dist (analysis_id, stratum_1, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value)
select  analysis_id, stratum_id, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
 from  temp.tempresults
;

truncate table temp.tempresults;
drop table temp.tempresults;

--


--
-- 212	Number of persons with at least one visit occurrence by calendar year by gender by age decile
insert into synpuf_100.achilles_results (analysis_id, stratum_1, stratum_2, stratum_3, count_value)
 select  212 as analysis_id, CAST(EXTRACT(YEAR from visit_start_date)  AS STRING), CAST(p1.gender_concept_id  AS STRING) as stratum_2, CAST(floor((EXTRACT(YEAR from visit_start_date) - p1.year_of_birth)/10)  AS STRING) as stratum_3, COUNT(distinct p1.person_id) as count_value
  from  synpuf_100.person p1
inner join
synpuf_100.visit_occurrence vo1
on p1.person_id = vo1.person_id
 group by  2, p1.gender_concept_id, 4 ;
--


--
-- 220	Number of visit occurrence records by condition occurrence start month
insert into synpuf_100.achilles_results (analysis_id, stratum_1, count_value)
 select  220 as analysis_id, CAST(EXTRACT(YEAR from visit_start_date)*100 + EXTRACT(MONTH from visit_start_date)  AS STRING) as stratum_1, COUNT(person_id) as count_value
  from  synpuf_100.visit_occurrence vo1
 group by  2 ;
--


--
-- 221	Number of persons by visit start year 
insert into synpuf_100.achilles_results (analysis_id, stratum_1, count_value)
 select  221 as analysis_id, CAST(EXTRACT(YEAR from visit_start_date)  AS STRING) as stratum_1, COUNT(distinct person_id) as count_value
  from  synpuf_100.visit_occurrence vo1
 group by  2 ;
--





/********************************************

ACHILLES Analyses on PROVIDER table

*********************************************/


--
-- 300	Number of providers
insert into synpuf_100.achilles_results (analysis_id, count_value)
select  300 as analysis_id, COUNT(distinct provider_id) as count_value
 from  synpuf_100.provider;
--


--
-- 301	Number of providers by specialty concept_id
insert into synpuf_100.achilles_results (analysis_id, stratum_1, count_value)
 select  301 as analysis_id, CAST(specialty_concept_id  AS STRING) as stratum_1, COUNT(distinct provider_id) as count_value
  from  synpuf_100.provider
 group by  2 ;
--

--
-- 302	Number of providers with invalid care site id
insert into synpuf_100.achilles_results (analysis_id, count_value)
select  302 as analysis_id, COUNT(provider_id) as count_value
 from  synpuf_100.provider p1
	left join synpuf_100.care_site cs1
	on p1.care_site_id = cs1.care_site_id
where p1.care_site_id is not null
	and cs1.care_site_id is null
;
--



/********************************************

ACHILLES Analyses on CONDITION_OCCURRENCE table

*********************************************/


--
-- 400	Number of persons with at least one condition occurrence, by condition_concept_id
insert into synpuf_100.achilles_results (analysis_id, stratum_1, count_value)
 select  400 as analysis_id, CAST(co1.condition_concept_id  AS STRING) as stratum_1, COUNT(distinct co1.person_id) as count_value
  from  synpuf_100.condition_occurrence co1
 group by  co1.condition_concept_id
 ;
--


--
-- 401	Number of condition occurrence records, by condition_concept_id
insert into synpuf_100.achilles_results (analysis_id, stratum_1, count_value)
 select  401 as analysis_id, CAST(co1.condition_concept_id  AS STRING) as stratum_1, COUNT(co1.person_id) as count_value
  from  synpuf_100.condition_occurrence co1
 group by  co1.condition_concept_id
 ;
--



--
-- 402	Number of persons by condition occurrence start month, by condition_concept_id
insert into synpuf_100.achilles_results (analysis_id, stratum_1, stratum_2, count_value)
 select  402 as analysis_id, CAST(co1.condition_concept_id  AS STRING) as stratum_1, CAST(EXTRACT(YEAR from condition_start_date)*100 + EXTRACT(MONTH from condition_start_date)  AS STRING) as stratum_2, COUNT(distinct person_id) as count_value
  from  synpuf_100.condition_occurrence co1
 group by  co1.condition_concept_id, 3 ;
--



--
-- 403	Number of distinct condition occurrence concepts per person
INTO temp.tempresults
   WITH rawdata as ( select  person_id as person_id, COUNT(distinct condition_concept_id)  as count_value   from  synpuf_100.condition_occurrence
	 group by  1 ), overallstats  as (select  cast(avg(1.0 * count_value)  as float64)  as avg_value, cast(STDDEV(count_value)  as float64)  as stdev_value, min(count_value)  as min_value, max(count_value)  as max_value, COUNT(*)  as total  from  rawdata
), statsview  as ( select  count_value as count_value, COUNT(*)  as total, row_number() over (order by count_value)  as rn   from  rawdata
   group by  1 ), priorstats  as ( select  s.count_value as count_value, s.total as total, sum(p.total)  as accumulated   from  statsview s
  join statsview p on p.rn <= s.rn
   group by  s.count_value, s.total, s.rn
 )
 select  403 as analysis_id, o.total as count_value, o.min_value, o.max_value, o.avg_value, o.stdev_value, min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value, min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value, min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value, min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value, min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
  FROM  priorstats p
cross join overallstats o
 group by  o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
 ;

insert into synpuf_100.achilles_results_dist (analysis_id, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value)
select  analysis_id, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
 from  temp.tempresults
;

truncate table temp.tempresults;
drop table temp.tempresults;

--



--
-- 404	Number of persons with at least one condition occurrence, by condition_concept_id by calendar year by gender by age decile
insert into synpuf_100.achilles_results (analysis_id, stratum_1, stratum_2, stratum_3, stratum_4, count_value)
 select  404 as analysis_id, CAST(co1.condition_concept_id  AS STRING) as stratum_1, CAST(EXTRACT(YEAR from condition_start_date)  AS STRING) as stratum_2, CAST(p1.gender_concept_id  AS STRING) as stratum_3, CAST(floor((EXTRACT(YEAR from condition_start_date) - p1.year_of_birth)/10)  AS STRING) as stratum_4, COUNT(distinct p1.person_id) as count_value
  from  synpuf_100.person p1
inner join
synpuf_100.condition_occurrence co1
on p1.person_id = co1.person_id
 group by  co1.condition_concept_id, 3, p1.gender_concept_id, 5 ;
--

--
-- 405	Number of condition occurrence records, by condition_concept_id by condition_type_concept_id
insert into synpuf_100.achilles_results (analysis_id, stratum_1, stratum_2, count_value)
 select  405 as analysis_id, CAST(co1.condition_concept_id  AS STRING) as stratum_1, CAST(co1.condition_type_concept_id  AS STRING) as stratum_2, COUNT(co1.person_id) as count_value
  from  synpuf_100.condition_occurrence co1
 group by  co1.condition_concept_id, co1.condition_type_concept_id
 ;
--



--
-- 406	Distribution of age by condition_concept_id
INTO temp.rawdata_406
  SELECT co1.condition_concept_id as subject_id, p1.gender_concept_id, (co1.condition_start_year - p1.year_of_birth) as count_value
  FROM  synpuf_100.person p1
inner join 
(
	 select  person_id, condition_concept_id, min(EXTRACT(YEAR from condition_start_date)) as condition_start_year
	  from  synpuf_100.condition_occurrence
	 group by  1, 2 ) co1 on p1.person_id = co1.person_id
;

INTO temp.tempresults
   WITH overallstats  as ( select  subject_id  as stratum1_id, gender_concept_id  as stratum2_id, cast(avg(1.0 * count_value)  as float64)  as avg_value, cast(STDDEV(count_value)  as float64)  as stdev_value, min(count_value)  as min_value, max(count_value)  as max_value, COUNT(*)  as total   from  temp.rawdata_406
	 group by  1, 2 ), statsview  as ( select  subject_id  as stratum1_id, gender_concept_id  as stratum2_id, count_value as count_value, COUNT(*)  as total, row_number() over (partition by subject_id, gender_concept_id order by count_value)  as rn   from  temp.rawdata_406
   group by  1, 2, 3 ), priorstats  as ( select  s.stratum1_id as stratum1_id, s.stratum2_id as stratum2_id, s.count_value as count_value, s.total as total, sum(p.total)  as accumulated   from  statsview s
  join statsview p on s.stratum1_id = p.stratum1_id and s.stratum2_id = p.stratum2_id and p.rn <= s.rn
   group by  s.stratum1_id, s.stratum2_id, s.count_value, s.total, s.rn
 )
 select  406 as analysis_id, CAST(o.stratum1_id  AS STRING) as stratum1_id, CAST(o.stratum2_id  AS STRING) as stratum2_id, o.total as count_value, o.min_value, o.max_value, o.avg_value, o.stdev_value, min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value, min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value, min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value, min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value, min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
  FROM  priorstats p
join overallstats o on p.stratum1_id = o.stratum1_id and p.stratum2_id = o.stratum2_id 
 group by  o.stratum1_id, o.stratum2_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
 ;

insert into synpuf_100.achilles_results_dist (analysis_id, stratum_1, stratum_2, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value)
select  analysis_id, stratum1_id, stratum2_id, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
 from  temp.tempresults
;

truncate table temp.tempresults;
drop table temp.tempresults;

truncate table temp.rawdata_406;
drop table temp.rawdata_406;

--


--
-- 409	Number of condition occurrence records with invalid person_id
insert into synpuf_100.achilles_results (analysis_id, count_value)
select  409 as analysis_id, COUNT(co1.person_id) as count_value
 from 
	synpuf_100.condition_occurrence co1
	left join synpuf_100.person p1
	on p1.person_id = co1.person_id
where p1.person_id is null
;
--


--
-- 410	Number of condition occurrence records outside valid observation period
insert into synpuf_100.achilles_results (analysis_id, count_value)
select  410 as analysis_id, COUNT(co1.person_id) as count_value
 from 
	synpuf_100.condition_occurrence co1
	left join synpuf_100.observation_period op1
	on op1.person_id = co1.person_id
	and co1.condition_start_date >= op1.observation_period_start_date
	and co1.condition_start_date <= op1.observation_period_end_date
where op1.person_id is null
;
--


--
-- 411	Number of condition occurrence records with end date < start date
insert into synpuf_100.achilles_results (analysis_id, count_value)
select  411 as analysis_id, COUNT(co1.person_id) as count_value
 from 
	synpuf_100.condition_occurrence co1
where co1.condition_end_date < co1.condition_start_date
;
--


--
-- 412	Number of condition occurrence records with invalid provider_id
insert into synpuf_100.achilles_results (analysis_id, count_value)
select  412 as analysis_id, COUNT(co1.person_id) as count_value
 from 
	synpuf_100.condition_occurrence co1
	left join synpuf_100.provider p1
	on p1.provider_id = co1.provider_id
where co1.provider_id is not null
	and p1.provider_id is null
;
--

--
-- 413	Number of condition occurrence records with invalid visit_id
insert into synpuf_100.achilles_results (analysis_id, count_value)
select  413 as analysis_id, COUNT(co1.person_id) as count_value
 from 
	synpuf_100.condition_occurrence co1
	left join synpuf_100.visit_occurrence vo1
	on co1.visit_occurrence_id = vo1.visit_occurrence_id
where co1.visit_occurrence_id is not null
	and vo1.visit_occurrence_id is null
;
--

--
-- 420	Number of condition occurrence records by condition occurrence start month
insert into synpuf_100.achilles_results (analysis_id, stratum_1, count_value)
 select  420 as analysis_id, CAST(EXTRACT(YEAR from condition_start_date)*100 + EXTRACT(MONTH from condition_start_date)  AS STRING) as stratum_1, COUNT(person_id) as count_value
  from  synpuf_100.condition_occurrence co1
 group by  2 ;
--



/********************************************

ACHILLES Analyses on DEATH table

*********************************************/



--
-- 500	Number of persons with death, by cause_concept_id
insert into synpuf_100.achilles_results (analysis_id, stratum_1, count_value)
 select  500 as analysis_id, CAST(d1.cause_concept_id  AS STRING) as stratum_1, COUNT(distinct d1.person_id) as count_value
  from  synpuf_100.death d1
 group by  d1.cause_concept_id
 ;
--


--
-- 501	Number of records of death, by cause_concept_id
insert into synpuf_100.achilles_results (analysis_id, stratum_1, count_value)
 select  501 as analysis_id, CAST(d1.cause_concept_id  AS STRING) as stratum_1, COUNT(d1.person_id) as count_value
  from  synpuf_100.death d1
 group by  d1.cause_concept_id
 ;
--



--
-- 502	Number of persons by condition occurrence start month
insert into synpuf_100.achilles_results (analysis_id, stratum_1, count_value)
 select  502 as analysis_id, CAST(EXTRACT(YEAR from death_date)*100 + EXTRACT(MONTH from death_date)  AS STRING) as stratum_1, COUNT(distinct person_id) as count_value
  from  synpuf_100.death d1
 group by  2 ;
--



--
-- 504	Number of persons with a death, by calendar year by gender by age decile
insert into synpuf_100.achilles_results (analysis_id, stratum_1, stratum_2, stratum_3, count_value)
 select  504 as analysis_id, CAST(EXTRACT(YEAR from death_date)  AS STRING) as stratum_1, CAST(p1.gender_concept_id  AS STRING) as stratum_2, CAST(floor((EXTRACT(YEAR from death_date) - p1.year_of_birth)/10)  AS STRING) as stratum_3, COUNT(distinct p1.person_id) as count_value
  from  synpuf_100.person p1
inner join
synpuf_100.death d1
on p1.person_id = d1.person_id
 group by  2, p1.gender_concept_id, 4 ;
--

--
-- 505	Number of death records, by death_type_concept_id
insert into synpuf_100.achilles_results (analysis_id, stratum_1, count_value)
 select  505 as analysis_id, CAST(death_type_concept_id  AS STRING) as stratum_1, COUNT(person_id) as count_value
  from  synpuf_100.death d1
 group by  2 ;
--



--
-- 506	Distribution of age by condition_concept_id

INTO temp.tempresults
   WITH rawdata as (select  p1.gender_concept_id as stratum_id, d1.death_year - p1.year_of_birth  as count_value  from  synpuf_100.person p1
  inner join
  ( select  person_id, min(EXTRACT(YEAR from death_date)) as death_year
    from  synpuf_100.death
   group by  1 ) d1
  on p1.person_id = d1.person_id
), overallstats  as ( select  stratum_id as stratum_id, cast(avg(1.0 * count_value)  as float64)  as avg_value, cast(STDDEV(count_value)  as float64)  as stdev_value, min(count_value)  as min_value, max(count_value)  as max_value, COUNT(*)  as total   from  rawdata
   group by  1 ), statsview  as ( select  stratum_id as stratum_id, count_value as count_value, COUNT(*)  as total, row_number() over (order by count_value)  as rn   from  rawdata
   group by  1, 2 ), priorstats  as ( select  s.stratum_id as stratum_id, s.count_value as count_value, s.total as total, sum(p.total)  as accumulated   from  statsview s
  join statsview p on s.stratum_id = p.stratum_id and p.rn <= s.rn
   group by  s.stratum_id, s.count_value, s.total, s.rn
 )
 select  506 as analysis_id, CAST(o.stratum_id  AS STRING) as stratum_id, o.total as count_value, o.min_value, o.max_value, o.avg_value, o.stdev_value, min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value, min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value, min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value, min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value, min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
  FROM  priorstats p
join overallstats o on p.stratum_id = o.stratum_id
 group by  o.stratum_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
 ;

insert into synpuf_100.achilles_results_dist (analysis_id, stratum_1, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value)
select  analysis_id, stratum_id, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
 from  temp.tempresults
;

truncate table temp.tempresults;

drop table temp.tempresults;
--



--
-- 509	Number of death records with invalid person_id
insert into synpuf_100.achilles_results (analysis_id, count_value)
select  509 as analysis_id, COUNT(d1.person_id) as count_value
 from 
	synpuf_100.death d1
		left join synpuf_100.person p1
		on d1.person_id = p1.person_id
where p1.person_id is null
;
--



--
-- 510	Number of death records outside valid observation period
insert into synpuf_100.achilles_results (analysis_id, count_value)
select  510 as analysis_id, COUNT(d1.person_id) as count_value
 from 
	synpuf_100.death d1
		left join synpuf_100.observation_period op1
		on d1.person_id = op1.person_id
		and d1.death_date >= op1.observation_period_start_date
		and d1.death_date <= op1.observation_period_end_date
where op1.person_id is null
;
--


--
-- 511	Distribution of time from death to last condition
insert into synpuf_100.achilles_results_dist (analysis_id, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value)
select  511 as analysis_id, COUNT(count_value) as count_value, min(count_value) as min_value, max(count_value) as max_value, cast(avg(1.0*count_value)  as float64) as avg_value, cast(STDDEV(count_value)  as float64) as stdev_value, max(case when p1<=0.50 then count_value else -9999 end) as median_value, max(case when p1<=0.10 then count_value else -9999 end) as p10_value, max(case when p1<=0.25 then count_value else -9999 end) as p25_value, max(case when p1<=0.75 then count_value else -9999 end) as p75_value, max(case when p1<=0.90 then count_value else -9999 end) as p90_value
 from 
(
select  DATE_DIFF(cast(t0.max_date as date), cast(d1.death_date as date), DAY) as count_value, 1.0*(row_number() over (order by DATE_DIFF(cast(t0.max_date as date), cast(d1.death_date as date), DAY)))/(COUNT(*) over () + 1) as p1
 from  synpuf_100.death d1
	inner join
	(
		 select  person_id, max(condition_start_date) as max_date
		  from  synpuf_100.condition_occurrence
		 group by  1 ) t0 on d1.person_id = t0.person_id
) t1
;
--


--
-- 512	Distribution of time from death to last drug
INTO temp.tempresults
   WITH rawdata as (select  DATE_DIFF(cast(t0.max_date as date), cast(d1.death_date as date), DAY)  as count_value  from  synpuf_100.death d1
  inner join
	(
		 select  person_id, max(drug_exposure_start_date) as max_date
		  from  synpuf_100.drug_exposure
		 group by  1 ) t0
	on d1.person_id = t0.person_id
), overallstats  as (select  cast(avg(1.0 * count_value)  as float64)  as avg_value, cast(STDDEV(count_value)  as float64)  as stdev_value, min(count_value)  as min_value, max(count_value)  as max_value, COUNT(*)  as total  from  rawdata
), statsview  as ( select  count_value as count_value, COUNT(*)  as total, row_number() over (order by count_value)  as rn   from  rawdata
   group by  1 ), priorstats  as ( select  s.count_value as count_value, s.total as total, sum(p.total)  as accumulated   from  statsview s
  join statsview p on p.rn <= s.rn
   group by  s.count_value, s.total, s.rn
 )
 select  512 as analysis_id, o.total as count_value, o.min_value, o.max_value, o.avg_value, o.stdev_value, min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value, min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value, min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value, min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value, min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
  FROM  priorstats p
cross join overallstats o
 group by  o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
 ;

insert into synpuf_100.achilles_results_dist (analysis_id, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value)
select  analysis_id, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
 from  temp.tempresults
;

truncate table temp.tempresults;

drop table temp.tempresults;


--


--
-- 513	Distribution of time from death to last visit
INTO temp.tempresults
   WITH rawdata as (select  DATE_DIFF(cast(t0.max_date as date), cast(d1.death_date as date), DAY)  as count_value  from  synpuf_100.death d1
	inner join
	(
		 select  person_id, max(visit_start_date) as max_date
		  from  synpuf_100.visit_occurrence
		 group by  1 ) t0
	on d1.person_id = t0.person_id
), overallstats  as (select  cast(avg(1.0 * count_value)  as float64)  as avg_value, cast(STDDEV(count_value)  as float64)  as stdev_value, min(count_value)  as min_value, max(count_value)  as max_value, COUNT(*)  as total  from  rawdata
), statsview  as ( select  count_value as count_value, COUNT(*)  as total, row_number() over (order by count_value)  as rn   from  rawdata
   group by  1 ), priorstats  as ( select  s.count_value as count_value, s.total as total, sum(p.total)  as accumulated   from  statsview s
  join statsview p on p.rn <= s.rn
   group by  s.count_value, s.total, s.rn
 )
 select  513 as analysis_id, o.total as count_value, o.min_value, o.max_value, o.avg_value, o.stdev_value, min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value, min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value, min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value, min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value, min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
  FROM  priorstats p
cross join overallstats o
 group by  o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
 ;

insert into synpuf_100.achilles_results_dist (analysis_id, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value)
select  analysis_id, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
 from  temp.tempresults
;

truncate table temp.tempresults;

drop table temp.tempresults;

--


--
-- 514	Distribution of time from death to last procedure
INTO temp.tempresults
   WITH rawdata as (select  DATE_DIFF(cast(t0.max_date as date), cast(d1.death_date as date), DAY)  as count_value  from  synpuf_100.death d1
	inner join
	(
		 select  person_id, max(procedure_date) as max_date
		  from  synpuf_100.procedure_occurrence
		 group by  1 ) t0
	on d1.person_id = t0.person_id
), overallstats  as (select  cast(avg(1.0 * count_value)  as float64)  as avg_value, cast(STDDEV(count_value)  as float64)  as stdev_value, min(count_value)  as min_value, max(count_value)  as max_value, COUNT(*)  as total  from  rawdata
), statsview  as ( select  count_value as count_value, COUNT(*)  as total, row_number() over (order by count_value)  as rn   from  rawdata
   group by  1 ), priorstats  as ( select  s.count_value as count_value, s.total as total, sum(p.total)  as accumulated   from  statsview s
  join statsview p on p.rn <= s.rn
   group by  s.count_value, s.total, s.rn
 )
 select  514 as analysis_id, o.total as count_value, o.min_value, o.max_value, o.avg_value, o.stdev_value, min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value, min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value, min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value, min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value, min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
  FROM  priorstats p
cross join overallstats o
 group by  o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
 ;

insert into synpuf_100.achilles_results_dist (analysis_id, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value)
select  analysis_id, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
 from  temp.tempresults
;

truncate table temp.tempresults;

drop table temp.tempresults;

--


--
-- 515	Distribution of time from death to last observation
INTO temp.tempresults
   WITH rawdata as (select  DATE_DIFF(cast(t0.max_date as date), cast(d1.death_date as date), DAY)  as count_value  from  synpuf_100.death d1
	inner join
	(
		 select  person_id, max(observation_date) as max_date
		  from  synpuf_100.observation
		 group by  1 ) t0
	on d1.person_id = t0.person_id
), overallstats  as (select  cast(avg(1.0 * count_value)  as float64)  as avg_value, cast(STDDEV(count_value)  as float64)  as stdev_value, min(count_value)  as min_value, max(count_value)  as max_value, COUNT(*)  as total  from  rawdata
), statsview  as ( select  count_value as count_value, COUNT(*)  as total, row_number() over (order by count_value)  as rn   from  rawdata
   group by  1 ), priorstats  as ( select  s.count_value as count_value, s.total as total, sum(p.total)  as accumulated   from  statsview s
  join statsview p on p.rn <= s.rn
   group by  s.count_value, s.total, s.rn
 )
 select  515 as analysis_id, o.total as count_value, o.min_value, o.max_value, o.avg_value, o.stdev_value, min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value, min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value, min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value, min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value, min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
  FROM  priorstats p
cross join overallstats o
 group by  o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
 ;

insert into synpuf_100.achilles_results_dist (analysis_id, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value)
select  analysis_id, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
 from  temp.tempresults
;

truncate table temp.tempresults;
drop table temp.tempresults;


--



/********************************************

ACHILLES Analyses on PROCEDURE_OCCURRENCE table

*********************************************/



--
-- 600	Number of persons with at least one procedure occurrence, by procedure_concept_id
insert into synpuf_100.achilles_results (analysis_id, stratum_1, count_value)
 select  600 as analysis_id, CAST(po1.procedure_concept_id  AS STRING) as stratum_1, COUNT(distinct po1.person_id) as count_value
  from  synpuf_100.procedure_occurrence po1
 group by  po1.procedure_concept_id
 ;
--


--
-- 601	Number of procedure occurrence records, by procedure_concept_id
insert into synpuf_100.achilles_results (analysis_id, stratum_1, count_value)
 select  601 as analysis_id, CAST(po1.procedure_concept_id  AS STRING) as stratum_1, COUNT(po1.person_id) as count_value
  from  synpuf_100.procedure_occurrence po1
 group by  po1.procedure_concept_id
 ;
--



--
-- 602	Number of persons by procedure occurrence start month, by procedure_concept_id
insert into synpuf_100.achilles_results (analysis_id, stratum_1, stratum_2, count_value)
 select  602 as analysis_id, CAST(po1.procedure_concept_id  AS STRING) as stratum_1, CAST(EXTRACT(YEAR from procedure_date)*100 + EXTRACT(MONTH from procedure_date)  AS STRING) as stratum_2, COUNT(distinct person_id) as count_value
  from  synpuf_100.procedure_occurrence po1
 group by  po1.procedure_concept_id, 3 ;
--



--
-- 603	Number of distinct procedure occurrence concepts per person
INTO temp.tempresults
   WITH rawdata as ( select  COUNT(distinct po.procedure_concept_id)  as count_value   from  synpuf_100.procedure_occurrence po
	 group by  po.person_id
 ), overallstats  as (select  cast(avg(1.0 * count_value)  as float64)  as avg_value, cast(STDDEV(count_value)  as float64)  as stdev_value, min(count_value)  as min_value, max(count_value)  as max_value, COUNT(*)  as total  from  rawdata
), statsview  as ( select  count_value as count_value, COUNT(*)  as total, row_number() over (order by count_value)  as rn   from  rawdata
   group by  1 ), priorstats  as ( select  s.count_value as count_value, s.total as total, sum(p.total)  as accumulated   from  statsview s
  join statsview p on p.rn <= s.rn
   group by  s.count_value, s.total, s.rn
 )
 select  603 as analysis_id, o.total as count_value, o.min_value, o.max_value, o.avg_value, o.stdev_value, min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value, min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value, min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value, min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value, min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
  FROM  priorstats p
cross join overallstats o
 group by  o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
 ;

insert into synpuf_100.achilles_results_dist (analysis_id, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value)
select  analysis_id, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
 from  temp.tempresults
;

truncate table temp.tempresults;
drop table temp.tempresults;


--



--
-- 604	Number of persons with at least one procedure occurrence, by procedure_concept_id by calendar year by gender by age decile
insert into synpuf_100.achilles_results (analysis_id, stratum_1, stratum_2, stratum_3, stratum_4, count_value)
 select  604 as analysis_id, CAST(po1.procedure_concept_id  AS STRING) as stratum_1, CAST(EXTRACT(YEAR from procedure_date)  AS STRING) as stratum_2, CAST(p1.gender_concept_id  AS STRING) as stratum_3, CAST(floor((EXTRACT(YEAR from procedure_date) - p1.year_of_birth)/10)  AS STRING) as stratum_4, COUNT(distinct p1.person_id) as count_value
  from  synpuf_100.person p1
inner join
synpuf_100.procedure_occurrence po1
on p1.person_id = po1.person_id
 group by  po1.procedure_concept_id, 3, p1.gender_concept_id, 5 ;
--

--
-- 605	Number of procedure occurrence records, by procedure_concept_id by procedure_type_concept_id
insert into synpuf_100.achilles_results (analysis_id, stratum_1, stratum_2, count_value)
 select  605 as analysis_id, CAST(po1.procedure_concept_id  AS STRING) as stratum_1, CAST(po1.procedure_type_concept_id  AS STRING) as stratum_2, COUNT(po1.person_id) as count_value
  from  synpuf_100.procedure_occurrence po1
 group by  po1.procedure_concept_id, po1.procedure_type_concept_id
 ;
--



--
-- 606	Distribution of age by procedure_concept_id
INTO temp.rawdata_606
  SELECT po1.procedure_concept_id as subject_id, p1.gender_concept_id, po1.procedure_start_year - p1.year_of_birth as count_value
  FROM  synpuf_100.person p1
inner join
(
	 select  person_id, procedure_concept_id, min(EXTRACT(YEAR from procedure_date)) as procedure_start_year
	  from  synpuf_100.procedure_occurrence
	 group by  1, 2 ) po1 on p1.person_id = po1.person_id
;

INTO temp.tempresults
   WITH overallstats  as ( select  subject_id  as stratum1_id, gender_concept_id  as stratum2_id, cast(avg(1.0 * count_value)  as float64)  as avg_value, cast(STDDEV(count_value)  as float64)  as stdev_value, min(count_value)  as min_value, max(count_value)  as max_value, COUNT(*)  as total   from  temp.rawdata_606
	 group by  1, 2 ), statsview  as ( select  subject_id  as stratum1_id, gender_concept_id  as stratum2_id, count_value as count_value, COUNT(*)  as total, row_number() over (partition by subject_id, gender_concept_id order by count_value)  as rn   from  temp.rawdata_606
   group by  1, 2, 3 ), priorstats  as ( select  s.stratum1_id as stratum1_id, s.stratum2_id as stratum2_id, s.count_value as count_value, s.total as total, sum(p.total)  as accumulated   from  statsview s
  join statsview p on s.stratum1_id = p.stratum1_id and s.stratum2_id = p.stratum2_id and p.rn <= s.rn
   group by  s.stratum1_id, s.stratum2_id, s.count_value, s.total, s.rn
 )
 select  606 as analysis_id, CAST(o.stratum1_id  AS STRING) as stratum1_id, CAST(o.stratum2_id  AS STRING) as stratum2_id, o.total as count_value, o.min_value, o.max_value, o.avg_value, o.stdev_value, min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value, min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value, min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value, min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value, min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
  FROM  priorstats p
join overallstats o on p.stratum1_id = o.stratum1_id and p.stratum2_id = o.stratum2_id 
 group by  o.stratum1_id, o.stratum2_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
 ;

insert into synpuf_100.achilles_results_dist (analysis_id, stratum_1, stratum_2, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value)
select  analysis_id, stratum1_id, stratum2_id, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
 from  temp.tempresults
;

truncate table temp.tempresults;
drop table temp.tempresults;
truncate table temp.rawdata_606;
drop table temp.rawdata_606;

--

--
-- 609	Number of procedure occurrence records with invalid person_id
insert into synpuf_100.achilles_results (analysis_id, count_value)
select  609 as analysis_id, COUNT(po1.person_id) as count_value
 from 
	synpuf_100.procedure_occurrence po1
	left join synpuf_100.person p1
	on p1.person_id = po1.person_id
where p1.person_id is null
;
--


--
-- 610	Number of procedure occurrence records outside valid observation period
insert into synpuf_100.achilles_results (analysis_id, count_value)
select  610 as analysis_id, COUNT(po1.person_id) as count_value
 from 
	synpuf_100.procedure_occurrence po1
	left join synpuf_100.observation_period op1
	on op1.person_id = po1.person_id
	and po1.procedure_date >= op1.observation_period_start_date
	and po1.procedure_date <= op1.observation_period_end_date
where op1.person_id is null
;
--



--
-- 612	Number of procedure occurrence records with invalid provider_id
insert into synpuf_100.achilles_results (analysis_id, count_value)
select  612 as analysis_id, COUNT(po1.person_id) as count_value
 from 
	synpuf_100.procedure_occurrence po1
	left join synpuf_100.provider p1
	on p1.provider_id = po1.provider_id
where po1.provider_id is not null
	and p1.provider_id is null
;
--

--
-- 613	Number of procedure occurrence records with invalid visit_id
insert into synpuf_100.achilles_results (analysis_id, count_value)
select  613 as analysis_id, COUNT(po1.person_id) as count_value
 from 
	synpuf_100.procedure_occurrence po1
	left join synpuf_100.visit_occurrence vo1
	on po1.visit_occurrence_id = vo1.visit_occurrence_id
where po1.visit_occurrence_id is not null
	and vo1.visit_occurrence_id is null
;
--


--
-- 620	Number of procedure occurrence records by condition occurrence start month
insert into synpuf_100.achilles_results (analysis_id, stratum_1, count_value)
 select  620 as analysis_id, CAST(EXTRACT(YEAR from procedure_date)*100 + EXTRACT(MONTH from procedure_date)  AS STRING) as stratum_1, COUNT(person_id) as count_value
  from  synpuf_100.procedure_occurrence po1
 group by  2 ;
--


--
-- 691	Number of total persons that have at least x procedures
insert into synpuf_100.achilles_results (analysis_id, stratum_1, stratum_2, count_value)
 select  691 as analysis_id, procedure_concept_id as stratum_1, prc_cnt as stratum_2, sum(count(person_id))	over (partition by procedure_concept_id order by prc_cnt desc) as count_value
  from  (
	 select  p.procedure_concept_id, count(p.procedure_occurrence_id) as prc_cnt, p.person_id
	  from  synpuf_100.procedure_occurrence p 
	 group by  p.person_id, p.procedure_concept_id
 ) cnt_q
 group by  2, 3 ;
--

/********************************************

ACHILLES Analyses on DRUG_EXPOSURE table

*********************************************/




--
-- 700	Number of persons with at least one drug occurrence, by drug_concept_id
insert into synpuf_100.achilles_results (analysis_id, stratum_1, count_value)
 select  700 as analysis_id, CAST(de1.drug_concept_id  AS STRING) as stratum_1, COUNT(distinct de1.person_id) as count_value
  from  synpuf_100.drug_exposure de1
 group by  de1.drug_concept_id
 ;
--


--
-- 701	Number of drug occurrence records, by drug_concept_id
insert into synpuf_100.achilles_results (analysis_id, stratum_1, count_value)
 select  701 as analysis_id, CAST(de1.drug_concept_id  AS STRING) as stratum_1, COUNT(de1.person_id) as count_value
  from  synpuf_100.drug_exposure de1
 group by  de1.drug_concept_id
 ;
--



--
-- 702	Number of persons by drug occurrence start month, by drug_concept_id
insert into synpuf_100.achilles_results (analysis_id, stratum_1, stratum_2, count_value)
 select  702 as analysis_id, CAST(de1.drug_concept_id  AS STRING) as stratum_1, CAST(EXTRACT(YEAR from drug_exposure_start_date)*100 + EXTRACT(MONTH from drug_exposure_start_date)  AS STRING) as stratum_2, COUNT(distinct person_id) as count_value
  from  synpuf_100.drug_exposure de1
 group by  de1.drug_concept_id, 3 ;
--



--
-- 703	Number of distinct drug exposure concepts per person
INTO temp.tempresults
   WITH rawdata as (select  num_drugs  as count_value  from  (
		 select  de1.person_id, COUNT(distinct de1.drug_concept_id) as num_drugs
		  from  synpuf_100.drug_exposure de1
		 group by  de1.person_id
	 ) t0
), overallstats  as (select  cast(avg(1.0 * count_value)  as float64)  as avg_value, cast(STDDEV(count_value)  as float64)  as stdev_value, min(count_value)  as min_value, max(count_value)  as max_value, COUNT(*)  as total  from  rawdata
), statsview  as ( select  count_value as count_value, COUNT(*)  as total, row_number() over (order by count_value)  as rn   from  rawdata
   group by  1 ), priorstats  as ( select  s.count_value as count_value, s.total as total, sum(p.total)  as accumulated   from  statsview s
  join statsview p on p.rn <= s.rn
   group by  s.count_value, s.total, s.rn
 )
 select  703 as analysis_id, o.total as count_value, o.min_value, o.max_value, o.avg_value, o.stdev_value, min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value, min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value, min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value, min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value, min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
  FROM  priorstats p
cross join overallstats o
 group by  o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
 ;

insert into synpuf_100.achilles_results_dist (analysis_id, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value)
select  analysis_id, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
 from  temp.tempresults
;

truncate table temp.tempresults;
drop table temp.tempresults;

--



--
-- 704	Number of persons with at least one drug occurrence, by drug_concept_id by calendar year by gender by age decile
insert into synpuf_100.achilles_results (analysis_id, stratum_1, stratum_2, stratum_3, stratum_4, count_value)
 select  704 as analysis_id, CAST(de1.drug_concept_id  AS STRING) as stratum_1, CAST(EXTRACT(YEAR from drug_exposure_start_date)  AS STRING) as stratum_2, CAST(p1.gender_concept_id  AS STRING) as stratum_3, CAST(floor((EXTRACT(YEAR from drug_exposure_start_date) - p1.year_of_birth)/10)  AS STRING) as stratum_4, COUNT(distinct p1.person_id) as count_value
  from  synpuf_100.person p1
inner join
synpuf_100.drug_exposure de1
on p1.person_id = de1.person_id
 group by  de1.drug_concept_id, 3, p1.gender_concept_id, 5 ;
--

--
-- 705	Number of drug occurrence records, by drug_concept_id by drug_type_concept_id
insert into synpuf_100.achilles_results (analysis_id, stratum_1, stratum_2, count_value)
 select  705 as analysis_id, CAST(de1.drug_concept_id  AS STRING) as stratum_1, CAST(de1.drug_type_concept_id  AS STRING) as stratum_2, COUNT(de1.person_id) as count_value
  from  synpuf_100.drug_exposure de1
 group by  de1.drug_concept_id, de1.drug_type_concept_id
 ;
--



--
-- 706	Distribution of age by drug_concept_id
INTO temp.rawdata_706
  SELECT de1.drug_concept_id as subject_id, p1.gender_concept_id, de1.drug_start_year - p1.year_of_birth as count_value
  FROM  synpuf_100.person p1
inner join
(
	 select  person_id, drug_concept_id, min(EXTRACT(YEAR from drug_exposure_start_date)) as drug_start_year
	  from  synpuf_100.drug_exposure
	 group by  1, 2 ) de1 on p1.person_id = de1.person_id
;

INTO temp.tempresults
   WITH overallstats  as ( select  subject_id  as stratum1_id, gender_concept_id  as stratum2_id, cast(avg(1.0 * count_value)  as float64)  as avg_value, cast(STDDEV(count_value)  as float64)  as stdev_value, min(count_value)  as min_value, max(count_value)  as max_value, COUNT(*)  as total   from  temp.rawdata_706
	 group by  1, 2 ), statsview  as ( select  subject_id  as stratum1_id, gender_concept_id  as stratum2_id, count_value as count_value, COUNT(*)  as total, row_number() over (partition by subject_id, gender_concept_id order by count_value)  as rn   from  temp.rawdata_706
   group by  1, 2, 3 ), priorstats  as ( select  s.stratum1_id as stratum1_id, s.stratum2_id as stratum2_id, s.count_value as count_value, s.total as total, sum(p.total)  as accumulated   from  statsview s
  join statsview p on s.stratum1_id = p.stratum1_id and s.stratum2_id = p.stratum2_id and p.rn <= s.rn
   group by  s.stratum1_id, s.stratum2_id, s.count_value, s.total, s.rn
 )
 select  706 as analysis_id, CAST(o.stratum1_id  AS STRING) as stratum1_id, CAST(o.stratum2_id  AS STRING) as stratum2_id, o.total as count_value, o.min_value, o.max_value, o.avg_value, o.stdev_value, min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value, min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value, min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value, min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value, min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
  FROM  priorstats p
join overallstats o on p.stratum1_id = o.stratum1_id and p.stratum2_id = o.stratum2_id 
 group by  o.stratum1_id, o.stratum2_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
 ;

insert into synpuf_100.achilles_results_dist (analysis_id, stratum_1, stratum_2, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value)
select  analysis_id, stratum1_id, stratum2_id, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
 from  temp.tempresults
;


truncate table temp.rawdata_706;
drop table temp.rawdata_706;

truncate table temp.tempresults;
drop table temp.tempresults;

--



--
-- 709	Number of drug exposure records with invalid person_id
insert into synpuf_100.achilles_results (analysis_id, count_value)
select  709 as analysis_id, COUNT(de1.person_id) as count_value
 from 
	synpuf_100.drug_exposure de1
	left join synpuf_100.person p1
	on p1.person_id = de1.person_id
where p1.person_id is null
;
--


--
-- 710	Number of drug exposure records outside valid observation period
insert into synpuf_100.achilles_results (analysis_id, count_value)
select  710 as analysis_id, COUNT(de1.person_id) as count_value
 from 
	synpuf_100.drug_exposure de1
	left join synpuf_100.observation_period op1
	on op1.person_id = de1.person_id
	and de1.drug_exposure_start_date >= op1.observation_period_start_date
	and de1.drug_exposure_start_date <= op1.observation_period_end_date
where op1.person_id is null
;
--


--
-- 711	Number of drug exposure records with end date < start date
insert into synpuf_100.achilles_results (analysis_id, count_value)
select  711 as analysis_id, COUNT(de1.person_id) as count_value
 from 
	synpuf_100.drug_exposure de1
where de1.drug_exposure_end_date < de1.drug_exposure_start_date
;
--


--
-- 712	Number of drug exposure records with invalid provider_id
insert into synpuf_100.achilles_results (analysis_id, count_value)
select  712 as analysis_id, COUNT(de1.person_id) as count_value
 from 
	synpuf_100.drug_exposure de1
	left join synpuf_100.provider p1
	on p1.provider_id = de1.provider_id
where de1.provider_id is not null
	and p1.provider_id is null
;
--

--
-- 713	Number of drug exposure records with invalid visit_id
insert into synpuf_100.achilles_results (analysis_id, count_value)
select  713 as analysis_id, COUNT(de1.person_id) as count_value
 from 
	synpuf_100.drug_exposure de1
	left join synpuf_100.visit_occurrence vo1
	on de1.visit_occurrence_id = vo1.visit_occurrence_id
where de1.visit_occurrence_id is not null
	and vo1.visit_occurrence_id is null
;
--



--
-- 715	Distribution of days_supply by drug_concept_id
INTO temp.tempresults
   WITH rawdata as (select  drug_concept_id as stratum_id, days_supply  as count_value  from  synpuf_100.drug_exposure 
	where days_supply is not null
), overallstats  as ( select  stratum_id as stratum_id, cast(avg(1.0 * count_value)  as float64)  as avg_value, cast(STDDEV(count_value)  as float64)  as stdev_value, min(count_value)  as min_value, max(count_value)  as max_value, COUNT(*)  as total   from  rawdata
	 group by  1 ), statsview  as ( select  stratum_id as stratum_id, count_value as count_value, COUNT(*)  as total, row_number() over (order by count_value)  as rn   from  rawdata
   group by  1, 2 ), priorstats  as ( select  s.stratum_id as stratum_id, s.count_value as count_value, s.total as total, sum(p.total)  as accumulated   from  statsview s
  join statsview p on s.stratum_id = p.stratum_id and p.rn <= s.rn
   group by  s.stratum_id, s.count_value, s.total, s.rn
 )
 select  715 as analysis_id, CAST(o.stratum_id  AS STRING) as stratum_id, o.total as count_value, o.min_value, o.max_value, o.avg_value, o.stdev_value, min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value, min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value, min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value, min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value, min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
  FROM  priorstats p
join overallstats o on p.stratum_id = o.stratum_id
 group by  o.stratum_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
 ;

insert into synpuf_100.achilles_results_dist (analysis_id, stratum_1, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value)
select  analysis_id, stratum_id, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
 from  temp.tempresults
;

truncate table temp.tempresults;
drop table temp.tempresults;

--


--
-- 716	Distribution of refills by drug_concept_id
INTO temp.tempresults
   WITH rawdata as (select  drug_concept_id as stratum_id, refills  as count_value  from  synpuf_100.drug_exposure 
	where refills is not null
), overallstats  as ( select  stratum_id as stratum_id, cast(avg(1.0 * count_value)  as float64)  as avg_value, cast(STDDEV(count_value)  as float64)  as stdev_value, min(count_value)  as min_value, max(count_value)  as max_value, COUNT(*)  as total   from  rawdata
	 group by  1 ), statsview  as ( select  stratum_id as stratum_id, count_value as count_value, COUNT(*)  as total, row_number() over (order by count_value)  as rn   from  rawdata
   group by  1, 2 ), priorstats  as ( select  s.stratum_id as stratum_id, s.count_value as count_value, s.total as total, sum(p.total)  as accumulated   from  statsview s
  join statsview p on s.stratum_id = p.stratum_id and p.rn <= s.rn
   group by  s.stratum_id, s.count_value, s.total, s.rn
 )
 select  716 as analysis_id, CAST(o.stratum_id  AS STRING) as stratum_id, o.total as count_value, o.min_value, o.max_value, o.avg_value, o.stdev_value, min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value, min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value, min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value, min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value, min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
  FROM  priorstats p
join overallstats o on p.stratum_id = o.stratum_id
 group by  o.stratum_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
 ;

insert into synpuf_100.achilles_results_dist (analysis_id, stratum_1, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value)
select  analysis_id, stratum_id, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
 from  temp.tempresults
;

truncate table temp.tempresults;
drop table temp.tempresults;

--



--
-- 717	Distribution of quantity by drug_concept_id
INTO temp.tempresults
   WITH rawdata as (select  drug_concept_id as stratum_id, cast(quantity  as float64)  as count_value  from  synpuf_100.drug_exposure 
	where quantity is not null
), overallstats  as ( select  stratum_id as stratum_id, cast(avg(1.0 * count_value)  as float64)  as avg_value, cast(STDDEV(count_value)  as float64)  as stdev_value, min(count_value)  as min_value, max(count_value)  as max_value, COUNT(*)  as total   from  rawdata
	 group by  1 ), statsview  as ( select  stratum_id as stratum_id, count_value as count_value, COUNT(*)  as total, row_number() over (order by count_value)  as rn   from  rawdata
   group by  1, 2 ), priorstats  as ( select  s.stratum_id as stratum_id, s.count_value as count_value, s.total as total, sum(p.total)  as accumulated   from  statsview s
  join statsview p on s.stratum_id = p.stratum_id and p.rn <= s.rn
   group by  s.stratum_id, s.count_value, s.total, s.rn
 )
 select  717 as analysis_id, CAST(o.stratum_id  AS STRING) as stratum_id, o.total as count_value, o.min_value, o.max_value, o.avg_value, o.stdev_value, min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value, min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value, min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value, min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value, min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
  FROM  priorstats p
join overallstats o on p.stratum_id = o.stratum_id
 group by  o.stratum_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
 ;

insert into synpuf_100.achilles_results_dist (analysis_id, stratum_1, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value)
select  analysis_id, stratum_id, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
 from  temp.tempresults
;

truncate table temp.tempresults;
drop table temp.tempresults;


--


--
-- 720	Number of drug exposure records by condition occurrence start month
insert into synpuf_100.achilles_results (analysis_id, stratum_1, count_value)
 select  720 as analysis_id, CAST(EXTRACT(YEAR from drug_exposure_start_date)*100 + EXTRACT(MONTH from drug_exposure_start_date)  AS STRING) as stratum_1, COUNT(person_id) as count_value
  from  synpuf_100.drug_exposure de1
 group by  2 ;
--

--
-- 791	Number of total persons that have at least x drug exposures
insert into synpuf_100.achilles_results (analysis_id, stratum_1, stratum_2, count_value)
 select  791 as analysis_id, drug_concept_id as stratum_1, drg_cnt as stratum_2, sum(count(person_id))	over (partition by drug_concept_id order by drg_cnt desc) as count_value
  from  (
	 select  d.drug_concept_id, count(d.drug_exposure_id) as drg_cnt, d.person_id
	  from  synpuf_100.drug_exposure d 
	 group by  d.person_id, d.drug_concept_id
 ) cnt_q
 group by  2, 3 ;
--

/********************************************

ACHILLES Analyses on OBSERVATION table

*********************************************/



--
-- 800	Number of persons with at least one observation occurrence, by observation_concept_id
insert into synpuf_100.achilles_results (analysis_id, stratum_1, count_value)
 select  800 as analysis_id, CAST(o1.observation_concept_id  AS STRING) as stratum_1, COUNT(distinct o1.person_id) as count_value
  from  synpuf_100.observation o1
 group by  o1.observation_concept_id
 ;
--


--
-- 801	Number of observation occurrence records, by observation_concept_id
insert into synpuf_100.achilles_results (analysis_id, stratum_1, count_value)
 select  801 as analysis_id, CAST(o1.observation_concept_id  AS STRING) as stratum_1, COUNT(o1.person_id) as count_value
  from  synpuf_100.observation o1
 group by  o1.observation_concept_id
 ;
--



--
-- 802	Number of persons by observation occurrence start month, by observation_concept_id
insert into synpuf_100.achilles_results (analysis_id, stratum_1, stratum_2, count_value)
 select  802 as analysis_id, CAST(o1.observation_concept_id  AS STRING) as stratum_1, CAST(EXTRACT(YEAR from observation_date)*100 + EXTRACT(MONTH from observation_date)  AS STRING) as stratum_2, COUNT(distinct person_id) as count_value
  from  synpuf_100.observation o1
 group by  o1.observation_concept_id, 3 ;
--



--
-- 803	Number of distinct observation occurrence concepts per person
INTO temp.tempresults
   WITH rawdata as (select  num_observations  as count_value  from  (
  	 select  o1.person_id, COUNT(distinct o1.observation_concept_id) as num_observations
  	  from  synpuf_100.observation o1
  	 group by  o1.person_id
	 ) t0
), overallstats  as (select  cast(avg(1.0 * count_value)  as float64)  as avg_value, cast(STDDEV(count_value)  as float64)  as stdev_value, min(count_value)  as min_value, max(count_value)  as max_value, COUNT(*)  as total  from  rawdata
), statsview  as ( select  count_value as count_value, COUNT(*)  as total, row_number() over (order by count_value)  as rn   from  rawdata
   group by  1 ), priorstats  as ( select  s.count_value as count_value, s.total as total, sum(p.total)  as accumulated   from  statsview s
  join statsview p on p.rn <= s.rn
   group by  s.count_value, s.total, s.rn
 )
 select  803 as analysis_id, o.total as count_value, o.min_value, o.max_value, o.avg_value, o.stdev_value, min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value, min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value, min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value, min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value, min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
  FROM  priorstats p
cross join overallstats o
 group by  o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
 ;

insert into synpuf_100.achilles_results_dist (analysis_id, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value)
select  analysis_id, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
 from  temp.tempresults
;

truncate table temp.tempresults;

drop table temp.tempresults;


--



--
-- 804	Number of persons with at least one observation occurrence, by observation_concept_id by calendar year by gender by age decile
insert into synpuf_100.achilles_results (analysis_id, stratum_1, stratum_2, stratum_3, stratum_4, count_value)
 select  804 as analysis_id, CAST(o1.observation_concept_id  AS STRING) as stratum_1, CAST(EXTRACT(YEAR from observation_date)  AS STRING) as stratum_2, CAST(p1.gender_concept_id  AS STRING) as stratum_3, CAST(floor((EXTRACT(YEAR from observation_date) - p1.year_of_birth)/10)  AS STRING) as stratum_4, COUNT(distinct p1.person_id) as count_value
  from  synpuf_100.person p1
inner join
synpuf_100.observation o1
on p1.person_id = o1.person_id
 group by  o1.observation_concept_id, 3, p1.gender_concept_id, 5 ;
--

--
-- 805	Number of observation occurrence records, by observation_concept_id by observation_type_concept_id
insert into synpuf_100.achilles_results (analysis_id, stratum_1, stratum_2, count_value)
 select  805 as analysis_id, CAST(o1.observation_concept_id  AS STRING) as stratum_1, CAST(o1.observation_type_concept_id  AS STRING) as stratum_2, COUNT(o1.person_id) as count_value
  from  synpuf_100.observation o1
 group by  o1.observation_concept_id, o1.observation_type_concept_id
 ;
--



--
-- 806	Distribution of age by observation_concept_id
INTO temp.rawdata_806
  SELECT o1.observation_concept_id as subject_id, p1.gender_concept_id, o1.observation_start_year - p1.year_of_birth as count_value
  FROM  synpuf_100.person p1
inner join
(
	 select  person_id, observation_concept_id, min(EXTRACT(YEAR from observation_date)) as observation_start_year
	  from  synpuf_100.observation
	 group by  1, 2 ) o1
on p1.person_id = o1.person_id
;

INTO temp.tempresults
   WITH overallstats  as ( select  subject_id  as stratum1_id, gender_concept_id  as stratum2_id, cast(avg(1.0 * count_value)  as float64)  as avg_value, cast(STDDEV(count_value)  as float64)  as stdev_value, min(count_value)  as min_value, max(count_value)  as max_value, COUNT(*)  as total   from  temp.rawdata_806
	 group by  1, 2 ), statsview  as ( select  subject_id  as stratum1_id, gender_concept_id  as stratum2_id, count_value as count_value, COUNT(*)  as total, row_number() over (partition by subject_id, gender_concept_id order by count_value)  as rn   from  temp.rawdata_806
   group by  1, 2, 3 ), priorstats  as ( select  s.stratum1_id as stratum1_id, s.stratum2_id as stratum2_id, s.count_value as count_value, s.total as total, sum(p.total)  as accumulated   from  statsview s
  join statsview p on s.stratum1_id = p.stratum1_id and s.stratum2_id = p.stratum2_id and p.rn <= s.rn
   group by  s.stratum1_id, s.stratum2_id, s.count_value, s.total, s.rn
 )
 select  806 as analysis_id, CAST(o.stratum1_id  AS STRING) as stratum1_id, CAST(o.stratum2_id  AS STRING) as stratum2_id, o.total as count_value, o.min_value, o.max_value, o.avg_value, o.stdev_value, min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value, min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value, min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value, min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value, min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
  FROM  priorstats p
join overallstats o on p.stratum1_id = o.stratum1_id and p.stratum2_id = o.stratum2_id 
 group by  o.stratum1_id, o.stratum2_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
 ;

insert into synpuf_100.achilles_results_dist (analysis_id, stratum_1, stratum_2, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value)
select  analysis_id, stratum1_id, stratum2_id, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
 from  temp.tempresults
;

truncate table temp.rawdata_806;
drop table temp.rawdata_806;

truncate table temp.tempresults;
drop table temp.tempresults;


--

--
-- 807	Number of observation occurrence records, by observation_concept_id and unit_concept_id
insert into synpuf_100.achilles_results (analysis_id, stratum_1, stratum_2, count_value)
 select  807 as analysis_id, CAST(o1.observation_concept_id  AS STRING) as stratum_1, CAST(o1.unit_concept_id  AS STRING) as stratum_2, COUNT(o1.person_id) as count_value
  from  synpuf_100.observation o1
 group by  o1.observation_concept_id, o1.unit_concept_id
 ;
--





--
-- 809	Number of observation records with invalid person_id
insert into synpuf_100.achilles_results (analysis_id, count_value)
select  809 as analysis_id, COUNT(o1.person_id) as count_value
 from 
	synpuf_100.observation o1
	left join synpuf_100.person p1
	on p1.person_id = o1.person_id
where p1.person_id is null
;
--


--
-- 810	Number of observation records outside valid observation period
insert into synpuf_100.achilles_results (analysis_id, count_value)
select  810 as analysis_id, COUNT(o1.person_id) as count_value
 from 
	synpuf_100.observation o1
	left join synpuf_100.observation_period op1
	on op1.person_id = o1.person_id
	and o1.observation_date >= op1.observation_period_start_date
	and o1.observation_date <= op1.observation_period_end_date
where op1.person_id is null
;
--



--
-- 812	Number of observation records with invalid provider_id
insert into synpuf_100.achilles_results (analysis_id, count_value)
select  812 as analysis_id, COUNT(o1.person_id) as count_value
 from 
	synpuf_100.observation o1
	left join synpuf_100.provider p1
	on p1.provider_id = o1.provider_id
where o1.provider_id is not null
	and p1.provider_id is null
;
--

--
-- 813	Number of observation records with invalid visit_id
insert into synpuf_100.achilles_results (analysis_id, count_value)
select  813 as analysis_id, COUNT(o1.person_id) as count_value
 from 
	synpuf_100.observation o1
	left join synpuf_100.visit_occurrence vo1
	on o1.visit_occurrence_id = vo1.visit_occurrence_id
where o1.visit_occurrence_id is not null
	and vo1.visit_occurrence_id is null
;
--


--
-- 814	Number of observation records with no value (numeric, string, or concept)
insert into synpuf_100.achilles_results (analysis_id, count_value)
select  814 as analysis_id, COUNT(o1.person_id) as count_value
 from 
	synpuf_100.observation o1
where o1.value_as_number is null
	and o1.value_as_string is null
	and o1.value_as_concept_id is null
;
--


--
-- 815  Distribution of numeric values, by observation_concept_id and unit_concept_id
INTO temp.rawdata_815
  SELECT observation_concept_id as subject_id, unit_concept_id, cast(value_as_number  as float64) as count_value
  FROM  synpuf_100.observation o1
where o1.unit_concept_id is not null
	and o1.value_as_number is not null
;

INTO temp.tempresults
   WITH overallstats  as ( select  subject_id  as stratum1_id, unit_concept_id  as stratum2_id, cast(avg(1.0 * count_value)  as float64)  as avg_value, cast(STDDEV(count_value)  as float64)  as stdev_value, min(count_value)  as min_value, max(count_value)  as max_value, COUNT(*)  as total   from  temp.rawdata_815
	 group by  1, 2 ), statsview  as ( select  subject_id  as stratum1_id, unit_concept_id  as stratum2_id, count_value as count_value, COUNT(*)  as total, row_number() over (partition by subject_id, unit_concept_id order by count_value)  as rn   from  temp.rawdata_815
   group by  1, 2, 3 ), priorstats  as ( select  s.stratum1_id as stratum1_id, s.stratum2_id as stratum2_id, s.count_value as count_value, s.total as total, sum(p.total)  as accumulated   from  statsview s
  join statsview p on s.stratum1_id = p.stratum1_id and s.stratum2_id = p.stratum2_id and p.rn <= s.rn
   group by  s.stratum1_id, s.stratum2_id, s.count_value, s.total, s.rn
 )
 select  815 as analysis_id, CAST(o.stratum1_id  AS STRING) as stratum1_id, CAST(o.stratum2_id  AS STRING) as stratum2_id, o.total as count_value, o.min_value, o.max_value, o.avg_value, o.stdev_value, min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value, min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value, min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value, min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value, min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
  FROM  priorstats p
join overallstats o on p.stratum1_id = o.stratum1_id and p.stratum2_id = o.stratum2_id 
 group by  o.stratum1_id, o.stratum2_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
 ;

insert into synpuf_100.achilles_results_dist (analysis_id, stratum_1, stratum_2, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value)
select  analysis_id, stratum1_id, stratum2_id, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
 from  temp.tempresults
;

truncate table temp.rawdata_815;
drop table temp.rawdata_815;

truncate table temp.tempresults;
drop table temp.tempresults;

--


--


--



--



--
-- 820	Number of observation records by condition occurrence start month
insert into synpuf_100.achilles_results (analysis_id, stratum_1, count_value)
 select  820 as analysis_id, CAST(EXTRACT(YEAR from observation_date)*100 + EXTRACT(MONTH from observation_date)  AS STRING) as stratum_1, COUNT(person_id) as count_value
  from  synpuf_100.observation o1
 group by  2 ;
--



--
-- 891	Number of total persons that have at least x observations
insert into synpuf_100.achilles_results (analysis_id, stratum_1, stratum_2, count_value)
 select  891 as analysis_id, observation_concept_id as stratum_1, obs_cnt as stratum_2, sum(count(person_id))	over (partition by observation_concept_id order by obs_cnt desc) as count_value
  from  (
	 select  o.observation_concept_id, count(o.observation_id) as obs_cnt, o.person_id
	  from  synpuf_100.observation o 
	 group by  o.person_id, o.observation_concept_id
 ) cnt_q
 group by  2, 3 ;
--



/********************************************

ACHILLES Analyses on DRUG_ERA table

*********************************************/


--
-- 900	Number of persons with at least one drug occurrence, by drug_concept_id
insert into synpuf_100.achilles_results (analysis_id, stratum_1, count_value)
 select  900 as analysis_id, CAST(de1.drug_concept_id  AS STRING) as stratum_1, COUNT(distinct de1.person_id) as count_value
  from  synpuf_100.drug_era de1
 group by  de1.drug_concept_id
 ;
--


--
-- 901	Number of drug occurrence records, by drug_concept_id
insert into synpuf_100.achilles_results (analysis_id, stratum_1, count_value)
 select  901 as analysis_id, CAST(de1.drug_concept_id  AS STRING) as stratum_1, COUNT(de1.person_id) as count_value
  from  synpuf_100.drug_era de1
 group by  de1.drug_concept_id
 ;
--



--
-- 902	Number of persons by drug occurrence start month, by drug_concept_id
insert into synpuf_100.achilles_results (analysis_id, stratum_1, stratum_2, count_value)
 select  902 as analysis_id, CAST(de1.drug_concept_id  AS STRING) as stratum_1, CAST(EXTRACT(YEAR from drug_era_start_date)*100 + EXTRACT(MONTH from drug_era_start_date)  AS STRING) as stratum_2, COUNT(distinct person_id) as count_value
  from  synpuf_100.drug_era de1
 group by  de1.drug_concept_id, 3 ;
--



--
-- 903	Number of distinct drug era concepts per person
INTO temp.tempresults
   WITH rawdata as ( select  COUNT(distinct de1.drug_concept_id)  as count_value   from  synpuf_100.drug_era de1
	 group by  de1.person_id
 ), overallstats  as (select  cast(avg(1.0 * count_value)  as float64)  as avg_value, cast(STDDEV(count_value)  as float64)  as stdev_value, min(count_value)  as min_value, max(count_value)  as max_value, COUNT(*)  as total  from  rawdata
), statsview  as ( select  count_value as count_value, COUNT(*)  as total, row_number() over (order by count_value)  as rn   from  rawdata
   group by  1 ), priorstats  as ( select  s.count_value as count_value, s.total as total, sum(p.total)  as accumulated   from  statsview s
  join statsview p on p.rn <= s.rn
   group by  s.count_value, s.total, s.rn
 )
 select  903 as analysis_id, o.total as count_value, o.min_value, o.max_value, o.avg_value, o.stdev_value, min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value, min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value, min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value, min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value, min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
  FROM  priorstats p
cross join overallstats o
 group by  o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
 ;

insert into synpuf_100.achilles_results_dist (analysis_id, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value)
select  analysis_id, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
 from  temp.tempresults
;

truncate table temp.tempresults;
drop table temp.tempresults;


--



--
-- 904	Number of persons with at least one drug occurrence, by drug_concept_id by calendar year by gender by age decile
insert into synpuf_100.achilles_results (analysis_id, stratum_1, stratum_2, stratum_3, stratum_4, count_value)
 select  904 as analysis_id, CAST(de1.drug_concept_id  AS STRING) as stratum_1, CAST(EXTRACT(YEAR from drug_era_start_date)  AS STRING) as stratum_2, CAST(p1.gender_concept_id  AS STRING) as stratum_3, CAST(floor((EXTRACT(YEAR from drug_era_start_date) - p1.year_of_birth)/10)  AS STRING) as stratum_4, COUNT(distinct p1.person_id) as count_value
  from  synpuf_100.person p1
inner join
synpuf_100.drug_era de1
on p1.person_id = de1.person_id
 group by  de1.drug_concept_id, 3, p1.gender_concept_id, 5 ;
--




--
-- 906	Distribution of age by drug_concept_id
INTO temp.rawdata_906
  SELECT de.drug_concept_id as subject_id, p1.gender_concept_id, de.drug_start_year - p1.year_of_birth as count_value
  FROM  synpuf_100.person p1
inner join
(
	 select  person_id, drug_concept_id, min(EXTRACT(YEAR from drug_era_start_date)) as drug_start_year
	  from  synpuf_100.drug_era
	 group by  1, 2 ) de on p1.person_id =de.person_id
;

INTO temp.tempresults
   WITH overallstats  as ( select  subject_id  as stratum1_id, gender_concept_id  as stratum2_id, cast(avg(1.0 * count_value)  as float64)  as avg_value, cast(STDDEV(count_value)  as float64)  as stdev_value, min(count_value)  as min_value, max(count_value)  as max_value, COUNT(*)  as total   from  temp.rawdata_906
	 group by  1, 2 ), statsview  as ( select  subject_id  as stratum1_id, gender_concept_id  as stratum2_id, count_value as count_value, COUNT(*)  as total, row_number() over (partition by subject_id, gender_concept_id order by count_value)  as rn   from  temp.rawdata_906
   group by  1, 2, 3 ), priorstats  as ( select  s.stratum1_id as stratum1_id, s.stratum2_id as stratum2_id, s.count_value as count_value, s.total as total, sum(p.total)  as accumulated   from  statsview s
  join statsview p on s.stratum1_id = p.stratum1_id and s.stratum2_id = p.stratum2_id and p.rn <= s.rn
   group by  s.stratum1_id, s.stratum2_id, s.count_value, s.total, s.rn
 )
 select  906 as analysis_id, CAST(o.stratum1_id  AS STRING) as stratum1_id, CAST(o.stratum2_id  AS STRING) as stratum2_id, o.total as count_value, o.min_value, o.max_value, o.avg_value, o.stdev_value, min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value, min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value, min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value, min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value, min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
  FROM  priorstats p
join overallstats o on p.stratum1_id = o.stratum1_id and p.stratum2_id = o.stratum2_id 
 group by  o.stratum1_id, o.stratum2_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
 ;

insert into synpuf_100.achilles_results_dist (analysis_id, stratum_1, stratum_2, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value)
select  analysis_id, stratum1_id, stratum2_id, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
 from  temp.tempresults
;


truncate table temp.rawdata_906;
drop table temp.rawdata_906;

truncate table temp.tempresults;
drop table temp.tempresults;
--


--
-- 907	Distribution of drug era length, by drug_concept_id
INTO temp.tempresults
   WITH rawdata as (select  drug_concept_id as stratum1_id, DATE_DIFF(cast(drug_era_end_date as date), cast(drug_era_start_date as date), DAY)  as count_value  from  synpuf_100.drug_era de1
), overallstats  as ( select  stratum1_id as stratum1_id, cast(avg(1.0 * count_value)  as float64)  as avg_value, cast(STDDEV(count_value)  as float64)  as stdev_value, min(count_value)  as min_value, max(count_value)  as max_value, COUNT(*)  as total   from  rawdata
   group by  1 ), statsview  as ( select  stratum1_id as stratum1_id, count_value as count_value, COUNT(*)  as total, row_number() over (partition by stratum1_id order by count_value)  as rn   from  rawdata
   group by  1, 2 ), priorstats  as ( select  s.stratum1_id as stratum1_id, s.count_value as count_value, s.total as total, sum(p.total)  as accumulated   from  statsview s
  join statsview p on s.stratum1_id = p.stratum1_id and p.rn <= s.rn
   group by  s.stratum1_id, s.count_value, s.total, s.rn
 )
 select  907 as analysis_id, CAST(p.stratum1_id  AS STRING) as stratum_1, o.total as count_value, o.min_value, o.max_value, o.avg_value, o.stdev_value, min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value, min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value, min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value, min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value, min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
  FROM  priorstats p
join overallstats o on p.stratum1_id = o.stratum1_id
 group by  p.stratum1_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
 ;

insert into synpuf_100.achilles_results_dist (analysis_id, stratum_1, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value)
select  analysis_id, stratum_1, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
 from  temp.tempresults
;

truncate table temp.tempresults;
drop table temp.tempresults;

--



--
-- 908	Number of drug eras with invalid person
insert into synpuf_100.achilles_results (analysis_id, count_value)
select  908 as analysis_id, COUNT(de1.person_id) as count_value
 from 
	synpuf_100.drug_era de1
	left join synpuf_100.person p1
	on p1.person_id = de1.person_id
where p1.person_id is null
;
--


--
-- 909	Number of drug eras outside valid observation period
insert into synpuf_100.achilles_results (analysis_id, count_value)
select  909 as analysis_id, COUNT(de1.person_id) as count_value
 from 
	synpuf_100.drug_era de1
	left join synpuf_100.observation_period op1
	on op1.person_id = de1.person_id
	and de1.drug_era_start_date >= op1.observation_period_start_date
	and de1.drug_era_start_date <= op1.observation_period_end_date
where op1.person_id is null
;
--


--
-- 910	Number of drug eras with end date < start date
insert into synpuf_100.achilles_results (analysis_id, count_value)
select  910 as analysis_id, COUNT(de1.person_id) as count_value
 from 
	synpuf_100.drug_era de1
where de1.drug_era_end_date < de1.drug_era_start_date
;
--



--
-- 920	Number of drug era records by drug era start month
insert into synpuf_100.achilles_results (analysis_id, stratum_1, count_value)
 select  920 as analysis_id, CAST(EXTRACT(YEAR from drug_era_start_date)*100 + EXTRACT(MONTH from drug_era_start_date)  AS STRING) as stratum_1, COUNT(person_id) as count_value
  from  synpuf_100.drug_era de1
 group by  2 ;
--





/********************************************

ACHILLES Analyses on CONDITION_ERA table

*********************************************/


--
-- 1000	Number of persons with at least one condition occurrence, by condition_concept_id
insert into synpuf_100.achilles_results (analysis_id, stratum_1, count_value)
 select  1000 as analysis_id, CAST(ce1.condition_concept_id  AS STRING) as stratum_1, COUNT(distinct ce1.person_id) as count_value
  from  synpuf_100.condition_era ce1
 group by  ce1.condition_concept_id
 ;
--


--
-- 1001	Number of condition occurrence records, by condition_concept_id
insert into synpuf_100.achilles_results (analysis_id, stratum_1, count_value)
 select  1001 as analysis_id, CAST(ce1.condition_concept_id  AS STRING) as stratum_1, COUNT(ce1.person_id) as count_value
  from  synpuf_100.condition_era ce1
 group by  ce1.condition_concept_id
 ;
--



--
-- 1002	Number of persons by condition occurrence start month, by condition_concept_id
insert into synpuf_100.achilles_results (analysis_id, stratum_1, stratum_2, count_value)
 select  1002 as analysis_id, CAST(ce1.condition_concept_id  AS STRING) as stratum_1, CAST(EXTRACT(YEAR from condition_era_start_date)*100 + EXTRACT(MONTH from condition_era_start_date)  AS STRING) as stratum_2, COUNT(distinct person_id) as count_value
  from  synpuf_100.condition_era ce1
 group by  ce1.condition_concept_id, 3 ;
--



--
-- 1003	Number of distinct condition era concepts per person
INTO temp.tempresults
   WITH rawdata as ( select  COUNT(distinct ce1.condition_concept_id)  as count_value   from  synpuf_100.condition_era ce1
	 group by  ce1.person_id
 ), overallstats  as (select  cast(avg(1.0 * count_value)  as float64)  as avg_value, cast(STDDEV(count_value)  as float64)  as stdev_value, min(count_value)  as min_value, max(count_value)  as max_value, COUNT(*)  as total  from  rawdata
), statsview  as ( select  count_value as count_value, COUNT(*)  as total, row_number() over (order by count_value)  as rn   from  rawdata
   group by  1 ), priorstats  as ( select  s.count_value as count_value, s.total as total, sum(p.total)  as accumulated   from  statsview s
  join statsview p on p.rn <= s.rn
   group by  s.count_value, s.total, s.rn
 )
 select  1003 as analysis_id, o.total as count_value, o.min_value, o.max_value, o.avg_value, o.stdev_value, min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value, min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value, min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value, min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value, min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
  FROM  priorstats p
cross join overallstats o
 group by  o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
 ;

insert into synpuf_100.achilles_results_dist (analysis_id, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value)
select  analysis_id, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
 from  temp.tempresults
;

truncate table temp.tempresults;
drop table temp.tempresults;

--



--
-- 1004	Number of persons with at least one condition occurrence, by condition_concept_id by calendar year by gender by age decile
insert into synpuf_100.achilles_results (analysis_id, stratum_1, stratum_2, stratum_3, stratum_4, count_value)
 select  1004 as analysis_id, CAST(ce1.condition_concept_id  AS STRING) as stratum_1, CAST(EXTRACT(YEAR from condition_era_start_date)  AS STRING) as stratum_2, CAST(p1.gender_concept_id  AS STRING) as stratum_3, CAST(floor((EXTRACT(YEAR from condition_era_start_date) - p1.year_of_birth)/10)  AS STRING) as stratum_4, COUNT(distinct p1.person_id) as count_value
  from  synpuf_100.person p1
inner join
synpuf_100.condition_era ce1
on p1.person_id = ce1.person_id
 group by  ce1.condition_concept_id, 3, p1.gender_concept_id, 5 ;
--




--
-- 1006	Distribution of age by condition_concept_id
INTO temp.rawdata_1006
  SELECT ce.condition_concept_id as subject_id, p1.gender_concept_id, ce.condition_start_year - p1.year_of_birth as count_value
  FROM  synpuf_100.person p1
inner join
(
   select  person_id, condition_concept_id, min(EXTRACT(YEAR from condition_era_start_date)) as condition_start_year
    from  synpuf_100.condition_era
   group by  1, 2 ) ce on p1.person_id = ce.person_id
;

INTO temp.tempresults
   WITH overallstats  as ( select  subject_id  as stratum1_id, gender_concept_id  as stratum2_id, cast(avg(1.0 * count_value)  as float64)  as avg_value, cast(STDDEV(count_value)  as float64)  as stdev_value, min(count_value)  as min_value, max(count_value)  as max_value, COUNT(*)  as total   from  temp.rawdata_1006
	 group by  1, 2 ), statsview  as ( select  subject_id  as stratum1_id, gender_concept_id  as stratum2_id, count_value as count_value, COUNT(*)  as total, row_number() over (partition by subject_id, gender_concept_id order by count_value)  as rn   from  temp.rawdata_1006
   group by  1, 2, 3 ), priorstats  as ( select  s.stratum1_id as stratum1_id, s.stratum2_id as stratum2_id, s.count_value as count_value, s.total as total, sum(p.total)  as accumulated   from  statsview s
  join statsview p on s.stratum1_id = p.stratum1_id and s.stratum2_id = p.stratum2_id and p.rn <= s.rn
   group by  s.stratum1_id, s.stratum2_id, s.count_value, s.total, s.rn
 )
 select  1006 as analysis_id, CAST(o.stratum1_id  AS STRING) as stratum1_id, CAST(o.stratum2_id  AS STRING) as stratum2_id, o.total as count_value, o.min_value, o.max_value, o.avg_value, o.stdev_value, min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value, min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value, min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value, min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value, min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
  FROM  priorstats p
join overallstats o on p.stratum1_id = o.stratum1_id and p.stratum2_id = o.stratum2_id 
 group by  o.stratum1_id, o.stratum2_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
 ;

insert into synpuf_100.achilles_results_dist (analysis_id, stratum_1, stratum_2, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value)
select  analysis_id, stratum1_id, stratum2_id, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
 from  temp.tempresults
;

truncate table temp.rawdata_1006;
drop table temp.rawdata_1006;

truncate table temp.tempresults;
drop table temp.tempresults;

--



--
-- 1007	Distribution of condition era length, by condition_concept_id
INTO temp.tempresults
   WITH rawdata as (select  condition_concept_id  as stratum1_id, DATE_DIFF(cast(condition_era_end_date as date), cast(condition_era_start_date as date), DAY)  as count_value  from  synpuf_100.condition_era ce1
), overallstats  as ( select  stratum1_id as stratum1_id, cast(avg(1.0 * count_value)  as float64)  as avg_value, cast(STDDEV(count_value)  as float64)  as stdev_value, min(count_value)  as min_value, max(count_value)  as max_value, COUNT(*)  as total   from  rawdata
   group by  1 ), statsview  as ( select  stratum1_id as stratum1_id, count_value as count_value, COUNT(*)  as total, row_number() over (partition by stratum1_id order by count_value)  as rn   from  rawdata
   group by  1, 2 ), priorstats  as ( select  s.stratum1_id as stratum1_id, s.count_value as count_value, s.total as total, sum(p.total)  as accumulated   from  statsview s
  join statsview p on s.stratum1_id = p.stratum1_id and p.rn <= s.rn
   group by  s.stratum1_id, s.count_value, s.total, s.rn
 )
 select  1007 as analysis_id, CAST(p.stratum1_id  AS STRING) as stratum_1, o.total as count_value, o.min_value, o.max_value, o.avg_value, o.stdev_value, min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value, min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value, min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value, min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value, min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
  FROM  priorstats p
join overallstats o on p.stratum1_id = o.stratum1_id
 group by  p.stratum1_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
 ;

insert into synpuf_100.achilles_results_dist (analysis_id, stratum_1, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value)
select  analysis_id, stratum_1, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
 from  temp.tempresults
;

truncate table temp.tempresults;
drop table temp.tempresults;


--



--
-- 1008	Number of condition eras with invalid person
insert into synpuf_100.achilles_results (analysis_id, count_value)
select  1008 as analysis_id, COUNT(ce1.person_id) as count_value
 from 
	synpuf_100.condition_era ce1
	left join synpuf_100.person p1
	on p1.person_id = ce1.person_id
where p1.person_id is null
;
--


--
-- 1009	Number of condition eras outside valid observation period
insert into synpuf_100.achilles_results (analysis_id, count_value)
select  1009 as analysis_id, COUNT(ce1.person_id) as count_value
 from 
	synpuf_100.condition_era ce1
	left join synpuf_100.observation_period op1
	on op1.person_id = ce1.person_id
	and ce1.condition_era_start_date >= op1.observation_period_start_date
	and ce1.condition_era_start_date <= op1.observation_period_end_date
where op1.person_id is null
;
--


--
-- 1010	Number of condition eras with end date < start date
insert into synpuf_100.achilles_results (analysis_id, count_value)
select  1010 as analysis_id, COUNT(ce1.person_id) as count_value
 from 
	synpuf_100.condition_era ce1
where ce1.condition_era_end_date < ce1.condition_era_start_date
;
--


--
-- 1020	Number of drug era records by drug era start month
insert into synpuf_100.achilles_results (analysis_id, stratum_1, count_value)
 select  1020 as analysis_id, CAST(EXTRACT(YEAR from condition_era_start_date)*100 + EXTRACT(MONTH from condition_era_start_date)  AS STRING) as stratum_1, COUNT(person_id) as count_value
  from  synpuf_100.condition_era ce1
 group by  2 ;
--




/********************************************

ACHILLES Analyses on LOCATION table

*********************************************/

--
-- 1100	Number of persons by location 3-digit zip
insert into synpuf_100.achilles_results (analysis_id, stratum_1, count_value)
 select  1100 as analysis_id, CAST(SUBSTR(l1.zip,0,3)  AS STRING) as stratum_1, COUNT(distinct person_id) as count_value
  from  synpuf_100.person p1
	inner join synpuf_100.location l1
	on p1.location_id = l1.location_id
where p1.location_id is not null
	and l1.zip is not null
 group by  2 ;
--


--
-- 1101	Number of persons by location state
insert into synpuf_100.achilles_results (analysis_id, stratum_1, count_value)
 select  1101 as analysis_id, CAST(l1.state  AS STRING) as stratum_1, COUNT(distinct person_id) as count_value
  from  synpuf_100.person p1
	inner join synpuf_100.location l1
	on p1.location_id = l1.location_id
where p1.location_id is not null
	and l1.state is not null
 group by  l1.state ;
--


--
-- 1102	Number of care sites by location 3-digit zip
insert into synpuf_100.achilles_results (analysis_id, stratum_1, count_value)
 select  1102 as analysis_id, CAST(SUBSTR(l1.zip,0,3)  AS STRING) as stratum_1, COUNT(distinct care_site_id) as count_value
  from  synpuf_100.care_site cs1
	inner join synpuf_100.location l1
	on cs1.location_id = l1.location_id
where cs1.location_id is not null
	and l1.zip is not null
 group by  2 ;
--


--
-- 1103	Number of care sites by location state
insert into synpuf_100.achilles_results (analysis_id, stratum_1, count_value)
 select  1103 as analysis_id, CAST(l1.state  AS STRING) as stratum_1, COUNT(distinct care_site_id) as count_value
  from  synpuf_100.care_site cs1
	inner join synpuf_100.location l1
	on cs1.location_id = l1.location_id
where cs1.location_id is not null
	and l1.state is not null
 group by  l1.state ;
--


/********************************************

ACHILLES Analyses on CARE_SITE table

*********************************************/


--
-- 1200	Number of persons by place of service
insert into synpuf_100.achilles_results (analysis_id, stratum_1, count_value)
 select  1200 as analysis_id, CAST(cs1.place_of_service_concept_id  AS STRING) as stratum_1, COUNT(person_id) as count_value
  from  synpuf_100.person p1
	inner join synpuf_100.care_site cs1
	on p1.care_site_id = cs1.care_site_id
where p1.care_site_id is not null
	and cs1.place_of_service_concept_id is not null
 group by  cs1.place_of_service_concept_id ;
--


--
-- 1201	Number of visits by place of service
insert into synpuf_100.achilles_results (analysis_id, stratum_1, count_value)
 select  1201 as analysis_id, CAST(cs1.place_of_service_concept_id  AS STRING) as stratum_1, COUNT(visit_occurrence_id) as count_value
  from  synpuf_100.visit_occurrence vo1
	inner join synpuf_100.care_site cs1
	on vo1.care_site_id = cs1.care_site_id
where vo1.care_site_id is not null
	and cs1.place_of_service_concept_id is not null
 group by  cs1.place_of_service_concept_id ;
--


--
-- 1202	Number of care sites by place of service
insert into synpuf_100.achilles_results (analysis_id, stratum_1, count_value)
 select  1202 as analysis_id, CAST(cs1.place_of_service_concept_id  AS STRING) as stratum_1, COUNT(care_site_id) as count_value
  from  synpuf_100.care_site cs1
where cs1.place_of_service_concept_id is not null
 group by  cs1.place_of_service_concept_id ;
--


/********************************************

ACHILLES Analyses on ORGANIZATION table

*********************************************/

--





/********************************************

ACHILLES Analyses on PAYOR_PLAN_PERIOD table

*********************************************/


--
-- 1406	Length of payer plan (days) of first payer plan period by gender
INTO temp.tempresults
   WITH rawdata as (select  p1.gender_concept_id  as stratum1_id, DATE_DIFF(cast(ppp1.payer_plan_period_end_date as date), cast(ppp1.payer_plan_period_start_date as date), DAY)  as count_value  from  synpuf_100.person p1
	inner join 
	(select  person_id, payer_plan_period_start_date, payer_plan_period_end_date, row_number() over (partition by person_id order by payer_plan_period_start_date asc) as rn1
		  from  synpuf_100.payer_plan_period
	) ppp1
	on p1.person_id = ppp1.person_id
	where ppp1.rn1 = 1
), overallstats  as ( select  stratum1_id as stratum1_id, cast(avg(1.0 * count_value)  as float64)  as avg_value, cast(STDDEV(count_value)  as float64)  as stdev_value, min(count_value)  as min_value, max(count_value)  as max_value, COUNT(*)  as total   from  rawdata
   group by  1 ), statsview  as ( select  stratum1_id as stratum1_id, count_value as count_value, COUNT(*)  as total, row_number() over (partition by stratum1_id order by count_value)  as rn   from  rawdata
   group by  1, 2 ), priorstats  as ( select  s.stratum1_id as stratum1_id, s.count_value as count_value, s.total as total, sum(p.total)  as accumulated   from  statsview s
  join statsview p on s.stratum1_id = p.stratum1_id and p.rn <= s.rn
   group by  s.stratum1_id, s.count_value, s.total, s.rn
 )
 select  1406 as analysis_id, CAST(p.stratum1_id  AS STRING) as stratum_1, o.total as count_value, o.min_value, o.max_value, o.avg_value, o.stdev_value, min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value, min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value, min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value, min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value, min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
  FROM  priorstats p
join overallstats o on p.stratum1_id = o.stratum1_id
 group by  p.stratum1_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
 ;

insert into synpuf_100.achilles_results_dist (analysis_id, stratum_1, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value)
select  analysis_id, stratum_1, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
 from  temp.tempresults
;

truncate table temp.tempresults;
drop table temp.tempresults;


--



--
-- 1407	Length of payer plan (days) of first payer plan period by age decile
INTO temp.tempresults
   WITH rawdata as (select  floor((EXTRACT(YEAR from ppp1.payer_plan_period_start_date) - p1.year_of_birth)/10)  as stratum_id, DATE_DIFF(cast(ppp1.payer_plan_period_end_date as date), cast(ppp1.payer_plan_period_start_date as date), DAY)  as count_value  from  synpuf_100.person p1
	inner join 
	(select  person_id, payer_plan_period_start_date, payer_plan_period_end_date, row_number() over (partition by person_id order by payer_plan_period_start_date asc) as rn1
		  from  synpuf_100.payer_plan_period
	) ppp1
	on p1.person_id = ppp1.person_id
	where ppp1.rn1 = 1
), overallstats  as ( select  stratum_id as stratum_id, cast(avg(1.0 * count_value)  as float64)  as avg_value, cast(STDDEV(count_value)  as float64)  as stdev_value, min(count_value)  as min_value, max(count_value)  as max_value, COUNT(*)  as total   from  rawdata
   group by  1 ), statsview  as ( select  stratum_id as stratum_id, count_value as count_value, COUNT(*)  as total, row_number() over (order by count_value)  as rn   from  rawdata
   group by  1, 2 ), priorstats  as ( select  s.stratum_id as stratum_id, s.count_value as count_value, s.total as total, sum(p.total)  as accumulated   from  statsview s
  join statsview p on s.stratum_id = p.stratum_id and p.rn <= s.rn
   group by  s.stratum_id, s.count_value, s.total, s.rn
 )
 select  1407 as analysis_id, CAST(o.stratum_id  AS STRING) as stratum_id, o.total as count_value, o.min_value, o.max_value, o.avg_value, o.stdev_value, min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value, min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value, min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value, min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value, min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
  FROM  priorstats p
join overallstats o on p.stratum_id = o.stratum_id
 group by  o.stratum_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
 ;

insert into synpuf_100.achilles_results_dist (analysis_id, stratum_1, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value)
select  analysis_id, stratum_id, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
 from  temp.tempresults
;

truncate table temp.tempresults;
drop table temp.tempresults;


--




--
-- 1408	Number of persons by length of payer plan period, in 30d increments
insert into synpuf_100.achilles_results (analysis_id, stratum_1, count_value)
 select  1408 as analysis_id, CAST(floor(DATE_DIFF(cast(ppp1.payer_plan_period_end_date as date), cast(ppp1.payer_plan_period_start_date as date), DAY)/30)  AS STRING) as stratum_1, COUNT(distinct p1.person_id) as count_value
  from  synpuf_100.person p1
	inner join 
	(select  person_id, payer_plan_period_start_date, payer_plan_period_end_date, row_number() over (partition by person_id order by payer_plan_period_start_date asc) as rn1
		  from  synpuf_100.payer_plan_period
	) ppp1
	on p1.person_id = ppp1.person_id
	where ppp1.rn1 = 1
 group by  2 ;
--


--
-- 1409	Number of persons with continuous payer plan in each year
-- Note: using temp table instead of nested query because this gives vastly improved

DROP TABLE IF EXISTS temp.temp_dates;

INTO temp.temp_dates
  SELECT distinct  EXTRACT(YEAR from payer_plan_period_start_date) as obs_year 
  FROM  
  synpuf_100.payer_plan_period
;

insert into synpuf_100.achilles_results (analysis_id, stratum_1, count_value)
 select  1409 as analysis_id, CAST(t1.obs_year  AS STRING) as stratum_1, COUNT(distinct p1.person_id) as count_value
  from  synpuf_100.person p1
	inner join 
    synpuf_100.payer_plan_period ppp1
	on p1.person_id = ppp1.person_id
	,
	temp.temp_dates t1 
where EXTRACT(YEAR from ppp1.payer_plan_period_start_date) <= t1.obs_year
	and EXTRACT(YEAR from ppp1.payer_plan_period_end_date) >= t1.obs_year
 group by  t1.obs_year
 ;

truncate table temp.temp_dates;
drop table temp.temp_dates;
--


--
-- 1410	Number of persons with continuous payer plan in each month
-- Note: using temp table instead of nested query because this gives vastly improved performance in Oracle

DROP TABLE IF EXISTS temp.temp_dates;

INTO temp.temp_dates
  SELECT distinct  EXTRACT(YEAR from payer_plan_period_start_date)*100 + EXTRACT(MONTH from payer_plan_period_start_date) as obs_month, parse_date('%Y%m%d', concat(concat(CAST(EXTRACT(YEAR from payer_plan_period_start_date)  AS STRING), SUBSTR(concat('0', CAST(EXTRACT(MONTH from payer_plan_period_start_date)  AS STRING)),-2)), '01')) as obs_month_start, DATE_ADD(cast(DATE_ADD(cast(parse_date('%Y%m%d', concat(concat(CAST(EXTRACT(YEAR from payer_plan_period_start_date)  AS STRING), SUBSTR(concat('0', CAST(EXTRACT(MONTH from payer_plan_period_start_date)  AS STRING)),-2)), '01')) as date), interval 1 MONTH) as date), interval -1 DAY) as obs_month_end
  FROM  
  synpuf_100.payer_plan_period
;

insert into synpuf_100.achilles_results (analysis_id, stratum_1, count_value)
 select  1410 as analysis_id, CAST(obs_month  AS STRING) as stratum_1, COUNT(distinct p1.person_id) as count_value
  from  synpuf_100.person p1
	inner join 
  synpuf_100.payer_plan_period ppp1
	on p1.person_id = ppp1.person_id
	,
	temp.temp_dates
where ppp1.payer_plan_period_start_date <= obs_month_start
	and ppp1.payer_plan_period_end_date >= obs_month_end
 group by  2 ;

truncate table temp.temp_dates;
drop table temp.temp_dates;
--



--
-- 1411	Number of persons by payer plan period start month
insert into synpuf_100.achilles_results (analysis_id, stratum_1, count_value)
 select  1411 as analysis_id, CAST(concat(CAST(EXTRACT(YEAR from payer_plan_period_start_date)  AS STRING), concat(SUBSTR(concat('0', CAST(EXTRACT(MONTH from payer_plan_period_start_date)  AS STRING)),-2),'01'))  AS STRING) as stratum_1, COUNT(distinct p1.person_id) as count_value
  from  synpuf_100.person p1
	inner join synpuf_100.payer_plan_period ppp1
	on p1.person_id = ppp1.person_id
 group by  2 ;
--



--
-- 1412	Number of persons by payer plan period end month
insert into synpuf_100.achilles_results (analysis_id, stratum_1, count_value)
 select  1412 as analysis_id, CAST(concat(CAST(EXTRACT(YEAR from payer_plan_period_end_date)  AS STRING), concat(SUBSTR(concat('0', CAST(EXTRACT(MONTH from payer_plan_period_end_date)  AS STRING)),-2), '01'))  AS STRING) as stratum_1, COUNT(distinct p1.person_id) as count_value
  from  synpuf_100.person p1
	inner join synpuf_100.payer_plan_period ppp1
	on p1.person_id = ppp1.person_id
 group by  2 ;
--


--
-- 1413	Number of persons by number of payer plan periods
insert into synpuf_100.achilles_results (analysis_id, stratum_1, count_value)
 select  1413 as analysis_id, CAST(ppp1.num_periods  AS STRING) as stratum_1, COUNT(distinct p1.person_id) as count_value
  from  synpuf_100.person p1
	inner join ( select  person_id, COUNT(payer_plan_period_start_date) as num_periods   from  synpuf_100.payer_plan_period  group by  1 ) ppp1
	on p1.person_id = ppp1.person_id
 group by  ppp1.num_periods
 ;
--

--
-- 1414	Number of persons with payer plan period before year-of-birth
insert into synpuf_100.achilles_results (analysis_id, count_value)
select  1414 as analysis_id, COUNT(distinct p1.person_id) as count_value
 from 
	synpuf_100.person p1
	inner join ( select  person_id, min(EXTRACT(YEAR from payer_plan_period_start_date)) as first_obs_year   from  synpuf_100.payer_plan_period  group by  1 ) ppp1
	on p1.person_id = ppp1.person_id
where p1.year_of_birth > ppp1.first_obs_year
;
--

--
-- 1415	Number of persons with payer plan period end < start
insert into synpuf_100.achilles_results (analysis_id, count_value)
select  1415 as analysis_id, COUNT(ppp1.person_id) as count_value
 from 
	synpuf_100.payer_plan_period ppp1
where ppp1.payer_plan_period_end_date < ppp1.payer_plan_period_start_date
;
--



/********************************************

ACHILLES Analyses on COHORT table

*********************************************/


--
-- 1700	Number of records by cohort_concept_id
insert into synpuf_100.achilles_results (analysis_id, stratum_1, count_value)
 select  1700 as analysis_id, CAST(cohort_definition_id  AS STRING) as stratum_1, COUNT(subject_id) as count_value
  from  synpuf_100.cohort c1
 group by  2 ;
--


--
-- 1701	Number of records with cohort end date < cohort start date
insert into synpuf_100.achilles_results (analysis_id, count_value)
select  1701 as analysis_id, COUNT(subject_id) as count_value
 from 
	synpuf_100.cohort c1
where c1.cohort_end_date < c1.cohort_start_date
;
--

/********************************************

ACHILLES Analyses on MEASUREMENT table

*********************************************/



--
-- 1800	Number of persons with at least one measurement occurrence, by measurement_concept_id
insert into synpuf_100.achilles_results (analysis_id, stratum_1, count_value)
 select  1800 as analysis_id, CAST(m.measurement_concept_id  AS STRING) as stratum_1, COUNT(distinct m.person_id) as count_value
  from  synpuf_100.measurement m
 group by  m.measurement_concept_id
 ;
--


--
-- 1801	Number of measurement occurrence records, by measurement_concept_id
insert into synpuf_100.achilles_results (analysis_id, stratum_1, count_value)
 select  1801 as analysis_id, CAST(m.measurement_concept_id  AS STRING) as stratum_1, COUNT(m.person_id) as count_value
  from  synpuf_100.measurement m
 group by  m.measurement_concept_id
 ;
--



--
-- 1802	Number of persons by measurement occurrence start month, by measurement_concept_id
insert into synpuf_100.achilles_results (analysis_id, stratum_1, stratum_2, count_value)
 select  1802 as analysis_id, CAST(m.measurement_concept_id  AS STRING) as stratum_1, CAST(EXTRACT(YEAR from measurement_date)*100 + EXTRACT(MONTH from measurement_date)  AS STRING) as stratum_2, COUNT(distinct person_id) as count_value
  from  synpuf_100.measurement m
 group by  m.measurement_concept_id, 3 ;
--



--
-- 1803	Number of distinct measurement occurrence concepts per person
INTO temp.tempresults
   WITH rawdata as (select  num_measurements  as count_value  from  (
  	 select  m.person_id, COUNT(distinct m.measurement_concept_id) as num_measurements
  	  from  synpuf_100.measurement m
  	 group by  m.person_id
	 ) t0
), overallstats  as (select  cast(avg(1.0 * count_value)  as float64)  as avg_value, cast(STDDEV(count_value)  as float64)  as stdev_value, min(count_value)  as min_value, max(count_value)  as max_value, COUNT(*)  as total  from  rawdata
), statsview  as ( select  count_value as count_value, COUNT(*)  as total, row_number() over (order by count_value)  as rn   from  rawdata
   group by  1 ), priorstats  as ( select  s.count_value as count_value, s.total as total, sum(p.total)  as accumulated   from  statsview s
  join statsview p on p.rn <= s.rn
   group by  s.count_value, s.total, s.rn
 )
 select  1803 as analysis_id, o.total as count_value, o.min_value, o.max_value, o.avg_value, o.stdev_value, min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value, min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value, min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value, min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value, min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
  FROM  priorstats p
cross join overallstats o
 group by  o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
 ;

insert into synpuf_100.achilles_results_dist (analysis_id, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value)
select  analysis_id, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
 from  temp.tempresults
;

truncate table temp.tempresults;

drop table temp.tempresults;


--



--
-- 1804	Number of persons with at least one measurement occurrence, by measurement_concept_id by calendar year by gender by age decile
insert into synpuf_100.achilles_results (analysis_id, stratum_1, stratum_2, stratum_3, stratum_4, count_value)
 select  1804 as analysis_id, CAST(m.measurement_concept_id  AS STRING) as stratum_1, CAST(EXTRACT(YEAR from measurement_date)  AS STRING) as stratum_2, CAST(p1.gender_concept_id  AS STRING) as stratum_3, CAST(floor((EXTRACT(YEAR from measurement_date) - p1.year_of_birth)/10)  AS STRING) as stratum_4, COUNT(distinct p1.person_id) as count_value
  from  synpuf_100.person p1
inner join synpuf_100.measurement m on p1.person_id = m.person_id
 group by  m.measurement_concept_id, 3, p1.gender_concept_id, 5 ;
--

--
-- 1805	Number of measurement records, by measurement_concept_id by measurement_type_concept_id
insert into synpuf_100.achilles_results (analysis_id, stratum_1, stratum_2, count_value)
 select  1805 as analysis_id, CAST(m.measurement_concept_id  AS STRING) as stratum_1, CAST(m.measurement_type_concept_id  AS STRING) as stratum_2, COUNT(m.person_id) as count_value
  from  synpuf_100.measurement m
 group by  m.measurement_concept_id, m.measurement_type_concept_id
 ;
--



--
-- 1806	Distribution of age by measurement_concept_id
INTO temp.rawdata_1806
  SELECT o1.measurement_concept_id as subject_id, p1.gender_concept_id, o1.measurement_start_year - p1.year_of_birth as count_value
  FROM  synpuf_100.person p1
inner join
(
	 select  person_id, measurement_concept_id, min(EXTRACT(YEAR from measurement_date)) as measurement_start_year
	  from  synpuf_100.measurement
	 group by  1, 2 ) o1
on p1.person_id = o1.person_id
;

INTO temp.tempresults
   WITH overallstats  as ( select  subject_id  as stratum1_id, gender_concept_id  as stratum2_id, cast(avg(1.0 * count_value)  as float64)  as avg_value, cast(STDDEV(count_value)  as float64)  as stdev_value, min(count_value)  as min_value, max(count_value)  as max_value, COUNT(*)  as total   from  temp.rawdata_1806
	 group by  1, 2 ), statsview  as ( select  subject_id  as stratum1_id, gender_concept_id  as stratum2_id, count_value as count_value, COUNT(*)  as total, row_number() over (partition by subject_id, gender_concept_id order by count_value)  as rn   from  temp.rawdata_1806
   group by  1, 2, 3 ), priorstats  as ( select  s.stratum1_id as stratum1_id, s.stratum2_id as stratum2_id, s.count_value as count_value, s.total as total, sum(p.total)  as accumulated   from  statsview s
  join statsview p on s.stratum1_id = p.stratum1_id and s.stratum2_id = p.stratum2_id and p.rn <= s.rn
   group by  s.stratum1_id, s.stratum2_id, s.count_value, s.total, s.rn
 )
 select  1806 as analysis_id, CAST(o.stratum1_id  AS STRING) as stratum1_id, CAST(o.stratum2_id  AS STRING) as stratum2_id, o.total as count_value, o.min_value, o.max_value, o.avg_value, o.stdev_value, min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value, min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value, min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value, min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value, min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
  FROM  priorstats p
join overallstats o on p.stratum1_id = o.stratum1_id and p.stratum2_id = o.stratum2_id 
 group by  o.stratum1_id, o.stratum2_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
 ;

insert into synpuf_100.achilles_results_dist (analysis_id, stratum_1, stratum_2, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value)
select  analysis_id, stratum1_id, stratum2_id, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
 from  temp.tempresults
;

truncate table temp.rawdata_1806;
drop table temp.rawdata_1806;

truncate table temp.tempresults;
drop table temp.tempresults;


--

--
-- 1807	Number of measurement occurrence records, by measurement_concept_id and unit_concept_id
insert into synpuf_100.achilles_results (analysis_id, stratum_1, stratum_2, count_value)
 select  1807 as analysis_id, CAST(m.measurement_concept_id  AS STRING) as stratum_1, CAST(m.unit_concept_id  AS STRING) as stratum_2, COUNT(m.person_id) as count_value
  from  synpuf_100.measurement m
 group by  m.measurement_concept_id, m.unit_concept_id
 ;
--



--
-- 1809	Number of measurement records with invalid person_id
insert into synpuf_100.achilles_results (analysis_id, count_value)
select  1809 as analysis_id, COUNT(m.person_id) as count_value
 from  synpuf_100.measurement m
	left join synpuf_100.person p1 on p1.person_id = m.person_id
where p1.person_id is null
;
--


--
-- 1810	Number of measurement records outside valid observation period
insert into synpuf_100.achilles_results (analysis_id, count_value)
select  1810 as analysis_id, COUNT(m.person_id) as count_value
 from  synpuf_100.measurement m
	left join synpuf_100.observation_period op on op.person_id = m.person_id
	and m.measurement_date >= op.observation_period_start_date
	and m.measurement_date <= op.observation_period_end_date
where op.person_id is null
;
--



--
-- 1812	Number of measurement records with invalid provider_id
insert into synpuf_100.achilles_results (analysis_id, count_value)
select  1812 as analysis_id, COUNT(m.person_id) as count_value
 from  synpuf_100.measurement m
	left join synpuf_100.provider p on p.provider_id = m.provider_id
where m.provider_id is not null
	and p.provider_id is null
;
--

--
-- 1813	Number of observation records with invalid visit_id
insert into synpuf_100.achilles_results (analysis_id, count_value)
select  1813 as analysis_id, COUNT(m.person_id) as count_value
 from  synpuf_100.measurement m
	left join synpuf_100.visit_occurrence vo on m.visit_occurrence_id = vo.visit_occurrence_id
where m.visit_occurrence_id is not null
	and vo.visit_occurrence_id is null
;
--


--
-- 1814	Number of measurement records with no value (numeric or concept)
insert into synpuf_100.achilles_results (analysis_id, count_value)
select  1814 as analysis_id, COUNT(m.person_id) as count_value
 from 
	synpuf_100.measurement m
where m.value_as_number is null
	and m.value_as_concept_id is null
;
--


--
-- 1815  Distribution of numeric values, by measurement_concept_id and unit_concept_id
INTO temp.rawdata_1815
  SELECT measurement_concept_id as subject_id, unit_concept_id, cast(value_as_number  as float64) as count_value
  FROM  synpuf_100.measurement m
where m.unit_concept_id is not null
	and m.value_as_number is not null
;

INTO temp.tempresults
   WITH overallstats  as ( select  subject_id  as stratum1_id, unit_concept_id  as stratum2_id, cast(avg(1.0 * count_value)  as float64)  as avg_value, cast(STDDEV(count_value)  as float64)  as stdev_value, min(count_value)  as min_value, max(count_value)  as max_value, COUNT(*)  as total   from  temp.rawdata_1815
	 group by  1, 2 ), statsview  as ( select  subject_id  as stratum1_id, unit_concept_id  as stratum2_id, count_value as count_value, COUNT(*)  as total, row_number() over (partition by subject_id, unit_concept_id order by count_value)  as rn   from  temp.rawdata_1815
   group by  1, 2, 3 ), priorstats  as ( select  s.stratum1_id as stratum1_id, s.stratum2_id as stratum2_id, s.count_value as count_value, s.total as total, sum(p.total)  as accumulated   from  statsview s
  join statsview p on s.stratum1_id = p.stratum1_id and s.stratum2_id = p.stratum2_id and p.rn <= s.rn
   group by  s.stratum1_id, s.stratum2_id, s.count_value, s.total, s.rn
 )
 select  1815 as analysis_id, CAST(o.stratum1_id  AS STRING) as stratum1_id, CAST(o.stratum2_id  AS STRING) as stratum2_id, o.total as count_value, o.min_value, o.max_value, o.avg_value, o.stdev_value, min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value, min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value, min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value, min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value, min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
  FROM  priorstats p
join overallstats o on p.stratum1_id = o.stratum1_id and p.stratum2_id = o.stratum2_id 
 group by  o.stratum1_id, o.stratum2_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
 ;

insert into synpuf_100.achilles_results_dist (analysis_id, stratum_1, stratum_2, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value)
select  analysis_id, stratum1_id, stratum2_id, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
 from  temp.tempresults
;

truncate table temp.rawdata_1815;
drop table temp.rawdata_1815;

truncate table temp.tempresults;
drop table temp.tempresults;

--


--
-- 1816	Distribution of low range, by measurement_concept_id and unit_concept_id
INTO temp.rawdata_1816
  SELECT measurement_concept_id as subject_id, unit_concept_id, cast(range_low  as float64) as count_value
  FROM  synpuf_100.measurement m
where m.unit_concept_id is not null
	and m.value_as_number is not null
	and m.range_low is not null
	and m.range_high is not null
;

INTO temp.tempresults
   WITH overallstats  as ( select  subject_id  as stratum1_id, unit_concept_id  as stratum2_id, cast(avg(1.0 * count_value)  as float64)  as avg_value, cast(STDDEV(count_value)  as float64)  as stdev_value, min(count_value)  as min_value, max(count_value)  as max_value, COUNT(*)  as total   from  temp.rawdata_1816
	 group by  1, 2 ), statsview  as ( select  subject_id  as stratum1_id, unit_concept_id  as stratum2_id, count_value as count_value, COUNT(*)  as total, row_number() over (partition by subject_id, unit_concept_id order by count_value)  as rn   from  temp.rawdata_1816
   group by  1, 2, 3 ), priorstats  as ( select  s.stratum1_id as stratum1_id, s.stratum2_id as stratum2_id, s.count_value as count_value, s.total as total, sum(p.total)  as accumulated   from  statsview s
  join statsview p on s.stratum1_id = p.stratum1_id and s.stratum2_id = p.stratum2_id and p.rn <= s.rn
   group by  s.stratum1_id, s.stratum2_id, s.count_value, s.total, s.rn
 )
 select  1816 as analysis_id, CAST(o.stratum1_id  AS STRING) as stratum1_id, CAST(o.stratum2_id  AS STRING) as stratum2_id, o.total as count_value, o.min_value, o.max_value, o.avg_value, o.stdev_value, min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value, min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value, min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value, min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value, min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
  FROM  priorstats p
join overallstats o on p.stratum1_id = o.stratum1_id and p.stratum2_id = o.stratum2_id 
 group by  o.stratum1_id, o.stratum2_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
 ;

insert into synpuf_100.achilles_results_dist (analysis_id, stratum_1, stratum_2, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value)
select  analysis_id, stratum1_id, stratum2_id, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
 from  temp.tempresults
;

truncate table temp.rawdata_1816;
drop table temp.rawdata_1816;

truncate table temp.tempresults;
drop table temp.tempresults;

--


--
-- 1817	Distribution of high range, by observation_concept_id and unit_concept_id
INTO temp.rawdata_1817
  SELECT measurement_concept_id as subject_id, unit_concept_id, cast(range_high  as float64) as count_value
  FROM  synpuf_100.measurement m
where m.unit_concept_id is not null
	and m.value_as_number is not null
	and m.range_low is not null
	and m.range_high is not null
;

INTO temp.tempresults
   WITH overallstats  as ( select  subject_id  as stratum1_id, unit_concept_id  as stratum2_id, cast(avg(1.0 * count_value)  as float64)  as avg_value, cast(STDDEV(count_value)  as float64)  as stdev_value, min(count_value)  as min_value, max(count_value)  as max_value, COUNT(*)  as total   from  temp.rawdata_1817
	 group by  1, 2 ), statsview  as ( select  subject_id  as stratum1_id, unit_concept_id  as stratum2_id, count_value as count_value, COUNT(*)  as total, row_number() over (partition by subject_id, unit_concept_id order by count_value)  as rn   from  temp.rawdata_1817
   group by  1, 2, 3 ), priorstats  as ( select  s.stratum1_id as stratum1_id, s.stratum2_id as stratum2_id, s.count_value as count_value, s.total as total, sum(p.total)  as accumulated   from  statsview s
  join statsview p on s.stratum1_id = p.stratum1_id and s.stratum2_id = p.stratum2_id and p.rn <= s.rn
   group by  s.stratum1_id, s.stratum2_id, s.count_value, s.total, s.rn
 )
 select  1817 as analysis_id, CAST(o.stratum1_id  AS STRING) as stratum1_id, CAST(o.stratum2_id  AS STRING) as stratum2_id, o.total as count_value, o.min_value, o.max_value, o.avg_value, o.stdev_value, min(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value, min(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value, min(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value, min(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value, min(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
  FROM  priorstats p
join overallstats o on p.stratum1_id = o.stratum1_id and p.stratum2_id = o.stratum2_id 
 group by  o.stratum1_id, o.stratum2_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
 ;

insert into synpuf_100.achilles_results_dist (analysis_id, stratum_1, stratum_2, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value)
select  analysis_id, stratum1_id, stratum2_id, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
 from  temp.tempresults
;

truncate table temp.rawdata_1817;
drop table temp.rawdata_1817;

truncate table temp.tempresults;
drop table temp.tempresults;

--



--
-- 1818	Number of observation records below/within/above normal range, by observation_concept_id and unit_concept_id
insert into synpuf_100.achilles_results (analysis_id, stratum_1, stratum_2, stratum_3, count_value)
 select  1818 as analysis_id, CAST(m.measurement_concept_id  AS STRING) as stratum_1, CAST(m.unit_concept_id  AS STRING) as stratum_2, CAST(case when m.value_as_number < m.range_low then 'Below Range Low'
		when m.value_as_number >= m.range_low and m.value_as_number <= m.range_high then 'Within Range'
		when m.value_as_number > m.range_high then 'Above Range High'
		else 'Other' end  AS STRING) as stratum_3, COUNT(m.person_id) as count_value
  from  synpuf_100.measurement m
where m.value_as_number is not null
	and m.unit_concept_id is not null
	and m.range_low is not null
	and m.range_high is not null
 group by  2, 3, 4 ;
--




--
-- 1820	Number of observation records by condition occurrence start month
insert into synpuf_100.achilles_results (analysis_id, stratum_1, count_value)
 select  1820 as analysis_id, CAST(EXTRACT(YEAR from measurement_date)*100 + EXTRACT(MONTH from measurement_date)  AS STRING) as stratum_1, COUNT(person_id) as count_value
  from  synpuf_100.measurement m
 group by  2 ;
--

--
-- 1821	Number of measurement records with no numeric value
insert into synpuf_100.achilles_results (analysis_id, count_value)
select  1821 as analysis_id, COUNT(m.person_id) as count_value
 from 
	synpuf_100.measurement m
where m.value_as_number is null
;
--


--
-- 1891	Number of total persons that have at least x measurements
insert into synpuf_100.achilles_results (analysis_id, stratum_1, stratum_2, count_value)
 select  1891 as analysis_id, measurement_concept_id as stratum_1, meas_cnt as stratum_2, sum(count(person_id))	over (partition by measurement_concept_id order by meas_cnt desc) as count_value
  from  (
	 select  m.measurement_concept_id, count(m.measurement_id) as meas_cnt, m.person_id
	  from  synpuf_100.measurement m 
	 group by  m.person_id, m.measurement_concept_id
 ) cnt_q
 group by  2, 3 ;
--
--end of measurment analyses

/********************************************

Reports 

*********************************************/


--
-- 1900	concept_0 report

insert into synpuf_100.achilles_results (analysis_id, stratum_1, stratum_2, count_value)
select  1900 as analysis_id, CAST(table_name  AS STRING) as stratum_1, source_value as stratum_2, cnt as count_value
  from  (
 select  'measurement' as table_name, measurement_source_value as source_value, COUNT(*) as cnt   from  synpuf_100.measurement where measurement_concept_id = 0  group by  measurement_source_value 
union distinct select  'procedure_occurrence' as table_name, procedure_source_value as source_value, COUNT(*) as cnt   from  synpuf_100.procedure_occurrence where procedure_concept_id = 0  group by  procedure_source_value 
union distinct select  'drug_exposure' as table_name, drug_source_value as source_value, COUNT(*) as cnt   from  synpuf_100.drug_exposure where drug_concept_id = 0  group by  drug_source_value 
union distinct select  'condition_occurrence' as table_name, condition_source_value as source_value, COUNT(*) as cnt   from  synpuf_100.condition_occurrence where condition_concept_id = 0  group by  2 ) a
where cnt >= 1 --use other threshold if needed (e.g., 10)
--order by a.table_name desc, cnt desc
;
--


/********************************************

ACHILLES Iris Analyses 

*********************************************/
--starting at id 2000

--
-- 2000	patients with at least 1 Dx and 1 Rx
insert into synpuf_100.achilles_results (analysis_id, count_value)
select  2000 as analysis_id, cast(a.cnt  as int64) as count_value
     from  (
                select  COUNT(*) as cnt from  (SELECT t1.person_id   FROM (SELECT DISTINCT person_id   FROM synpuf_100.condition_occurrence
                     UNION ALL SELECT DISTINCT person_id   FROM synpuf_100.drug_exposure
                ) AS t1 GROUP BY person_id   HAVING COUNT(*) >= 2) b
         ) a
         ;
--



--
-- 2001	patients with at least 1 Dx and 1 Proc
insert into synpuf_100.achilles_results (analysis_id, count_value)
select  2001 as analysis_id, cast(a.cnt  as int64) as count_value
     from  (
                select  COUNT(*) as cnt from  (SELECT t1.person_id   FROM (SELECT DISTINCT person_id   FROM synpuf_100.condition_occurrence
                     UNION ALL SELECT DISTINCT person_id   FROM synpuf_100.procedure_occurrence
                ) AS t1 GROUP BY person_id   HAVING COUNT(*) >= 2) b
         ) a
         ;
--



--
-- 2002	patients with at least 1 Mes and 1 Dx and 1 Rx
insert into synpuf_100.achilles_results (analysis_id, count_value)
select  2002 as analysis_id, cast(a.cnt  as int64) as count_value
     from  (
                select  COUNT(*) as cnt from  (SELECT t1.person_id   FROM (SELECT t1.person_id   FROM (SELECT DISTINCT person_id   FROM synpuf_100.measurement
                     UNION ALL SELECT DISTINCT person_id   FROM synpuf_100.condition_occurrence
                     UNION ALL SELECT DISTINCT person_id   FROM synpuf_100.drug_exposure
                ) AS t1 GROUP BY person_id   HAVING COUNT(*) >= 2) AS t1 GROUP BY person_id   HAVING COUNT(*) >= 2) b
         ) a
         ;
--


--
-- 2003	Patients with at least one visit
-- this analysis is in fact redundant, since it is possible to get it via
-- dist analysis 203 and query select count_value from achilles_results_dist where analysis_id = 203;
insert into synpuf_100.achilles_results (analysis_id, count_value)
select  2003 as analysis_id, COUNT(distinct person_id) as count_value
 from  synpuf_100.visit_occurrence;
--


/********************************************

ACHILLES Analyses on DEVICE_EXPOSURE  table

*********************************************/



--
-- 2100	Number of persons with at least one device exposure , by device_concept_id
insert into synpuf_100.achilles_results (analysis_id, stratum_1, count_value)
 select  2100 as analysis_id, CAST(m.device_concept_id  AS STRING) as stratum_1, COUNT(distinct m.person_id) as count_value
  from  synpuf_100.device_exposure m
 group by  m.device_concept_id
 ;
--


--
-- 2101	Number of device exposure  records, by device_concept_id
insert into synpuf_100.achilles_results (analysis_id, stratum_1, count_value)
 select  2101 as analysis_id, CAST(m.device_concept_id  AS STRING) as stratum_1, COUNT(m.person_id) as count_value
  from  synpuf_100.device_exposure m
 group by  m.device_concept_id
 ;
--



--
-- 2102	Number of persons by device by  start month, by device_concept_id
insert into synpuf_100.achilles_results (analysis_id, stratum_1, stratum_2, count_value)
 select  2102 as analysis_id, CAST(m.device_concept_id  AS STRING) as stratum_1, CAST(EXTRACT(YEAR from device_exposure_start_date)*100 + EXTRACT(MONTH from device_exposure_start_date)  AS STRING) as stratum_2, COUNT(distinct person_id) as count_value
  from  synpuf_100.device_exposure m
 group by  m.device_concept_id, 3 ;
--

--2103 is not implemented at this point


--
-- 2104	Number of persons with at least one device occurrence, by device_concept_id by calendar year by gender by age decile
insert into synpuf_100.achilles_results (analysis_id, stratum_1, stratum_2, stratum_3, stratum_4, count_value)
 select  2104 as analysis_id, CAST(m.device_concept_id  AS STRING) as stratum_1, CAST(EXTRACT(YEAR from device_exposure_start_date)  AS STRING) as stratum_2, CAST(p1.gender_concept_id  AS STRING) as stratum_3, CAST(floor((EXTRACT(YEAR from device_exposure_start_date) - p1.year_of_birth)/10)  AS STRING) as stratum_4, COUNT(distinct p1.person_id) as count_value
  from  synpuf_100.person p1
inner join synpuf_100.device_exposure m on p1.person_id = m.person_id
 group by  m.device_concept_id, 3, p1.gender_concept_id, 5 ;
--


--
-- 2105	Number of exposure records by device_concept_id by device_type_concept_id
insert into synpuf_100.achilles_results (analysis_id, stratum_1, stratum_2, count_value)
 select  2105 as analysis_id, CAST(m.device_concept_id  AS STRING) as stratum_1, CAST(m.device_type_concept_id  AS STRING) as stratum_2, COUNT(m.person_id) as count_value
  from  synpuf_100.device_exposure m
 group by  m.device_concept_id, m.device_type_concept_id
 ;
--

--2106 and more analyses are not implemented at this point





/********************************************

ACHILLES Analyses on NOTE table

*********************************************/



--
-- 2200	Number of persons with at least one device exposure , by device_concept_id
insert into synpuf_100.achilles_results (analysis_id, stratum_1, count_value)
 select  2200 as analysis_id, CAST(m.note_type_concept_id  AS STRING) as stratum_1, COUNT(distinct m.person_id) as count_value
  from  synpuf_100.note m
 group by  m.note_type_concept_id
 ;
--


--
-- 2201	Number of device exposure  records, by device_concept_id
insert into synpuf_100.achilles_results (analysis_id, stratum_1, count_value)
 select  2201 as analysis_id, CAST(m.note_type_concept_id  AS STRING) as stratum_1, COUNT(m.person_id) as count_value
  from  synpuf_100.note m
 group by  m.note_type_concept_id
 ;
--





--final processing of results
delete from synpuf_100.achilles_results 
where count_value <= 5;
delete from synpuf_100.achilles_results_dist 
where count_value <= 5;

