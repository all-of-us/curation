--general derived measures
--non-CDM sources may generate derived measures directly
--for CDM and Achilles: the fastest way to compute derived measures is to use
--existing measures
--derived measures have IDs over 100 000 (not any more, instead, they use measure_id as their id)


--event type derived measures analysis xx05 is often analysis by xx_type
--generate counts for meas type, drug type, proc type, obs type
--optional TODO: possibly rewrite this with CASE statement to better make 705 into drug, 605 into proc ...etc
--               in measure_id column (or make that separate sql calls for each category)
insert into synpuf_100.achilles_results_derived (analysis_id, stratum_1, statistic_value,measure_id)
 select  null as analysis_id, stratum_2 as stratum_1, sum(count_value) as statistic_value, concat(concat('ach_', cast(analysis_id  as string) ), ':GlobalCnt' )as measure_id
  from  synpuf_100.achilles_results
where analysis_id in(1805,705,605,805,405)  group by  4, 2 ;




--total number of rows per domain
--this derived measure is used for later measure of % of unmapped rows
--this produces a total count of rows in condition table, procedure table etc.
--used as denominator in later measures
    insert into synpuf_100.achilles_results_derived (statistic_value,measure_id)
     select  sum(count_value) as statistic_value, concat(concat('ach_', cast(analysis_id  as string) ), ':GlobalRowCnt' )as measure_id
      from  synpuf_100.achilles_results
    where analysis_id in (401,601,701,801,1801)  group by  2 

UNION ALL
--concept_0 global row  Counts per domain
--this is numerator for percentage value of unmapped rows (per domain)
-- insert into synpuf_100.achilles_results_derived (statistic_value,measure_id)
    select  count_value as statistic_value, concat(concat('UnmappedData:ach_', cast(analysis_id  as string) ), ':GlobalRowCnt' )as measure_id
     from  synpuf_100.achilles_results
    --TODO:stratum_1 is varchar and this comparison may fail on some db engines
    --indeed, mysql got error, changed to a string comparison
    where analysis_id in (401,601,701,801,1801) and stratum_1 = '0'
--    ;



--iris measures by percentage
--for this part, derived table is trying to adopt DQI terminolgy
--and generalize analysis naming scheme (and generalize the DQ rules)
UNION ALL
-- insert into synpuf_100.achilles_results_derived (statistic_value,measure_id)
select
	CASE
	  WHEN (select  count_value as total_pts  from  synpuf_100.achilles_results r where analysis_id =1) = 0
		  THEN NULL
	  ELSE
	    100.0*count_value/(select  count_value as total_pts  from  synpuf_100.achilles_results r where analysis_id =1)
	END as statistic_value,
	concat(concat('ach_', cast(analysis_id  as string) ), ':Percentage' ) as measure_id
   from  synpuf_100.achilles_results

  where analysis_id in (2000,2001,2002,2003)
UNION ALL
-- insert into synpuf_100.achilles_results_derived (statistic_value,measure_id)
    select  sum(count_value) as statistic_value, 'Visit:InstanceCnt' as measure_id
  from  synpuf_100.achilles_results where analysis_id = 201;

 --in dist analysis/measure 203 - a number similar to that is computed above but it is on person level


--age at first observation by decile
insert into synpuf_100.achilles_results_derived (stratum_1,statistic_value,measure_id)
 select  cast(floor(cast(stratum_1  as int64)/10)  as string) as stratum_1, sum(count_value) as statistic_value, 'AgeAtFirstObsByDecile:PersonCnt' as measure_id
    from  synpuf_100.achilles_results where analysis_id = 101
 group by  1 ;

--count whether all deciles from 0 to 8 are there  (has later a rule: if less the threshold, issue notification)
insert into synpuf_100.achilles_results_derived (statistic_value,measure_id)
select  count(*) as statistic_value, 'AgeAtFirstObsByDecile:DecileCnt' as measure_id
 from  synpuf_100.achilles_results_derived
where measure_id = 'AgeAtFirstObsByDecile:PersonCnt'
and cast(stratum_1  as int64) <=8


--data density measures

UNION ALL
select  count(*) as statistic_value, 'DrugExposure:ConceptCnt' as measure_id
 from  synpuf_100.achilles_results where analysis_id = 701

UNION ALL
select  count(*) as statistic_value, 'DrugEra:ConceptCnt' as measure_id
 from  synpuf_100.achilles_results where analysis_id = 901

UNION ALL
select  count(*) as statistic_value, 'Condition:ConceptCnt' as measure_id
 from  synpuf_100.achilles_results where analysis_id = 401

UNION ALL
select  count(*) as statistic_value, 'Procedure:ConceptCnt' as measure_id
 from  synpuf_100.achilles_results where analysis_id = 601

UNION ALL
select  count(*) as statistic_value, 'Observation:ConceptCnt' as measure_id
 from  synpuf_100.achilles_results where analysis_id = 801

UNION ALL
select  count(*) as statistic_value, 'Measurement:ConceptCnt' as measure_id
 from  synpuf_100.achilles_results where analysis_id = 1801

UNION ALL
select  count(*) as statistic_value, 'Visit:ConceptCnt' as measure_id
 from  synpuf_100.achilles_results where analysis_id = 201

UNION ALL
select  count(*) as statistic_value, 'Death:DeathType:ConceptCnt' as measure_id
 from  synpuf_100.achilles_results where analysis_id = 505

UNION ALL
select  count(*) as statistic_value, 'Death:DeathCause:ConceptCnt' as measure_id
 from  synpuf_100.achilles_results where analysis_id = 501

UNION ALL
select  count(*) as statistic_value, 'Person:Race:ConceptCnt' as measure_id
 from  synpuf_100.achilles_results where analysis_id = 4

UNION ALL
select  count(*) as statistic_value, 'Person:Ethnicity:ConceptCnt' as measure_id
 from  synpuf_100.achilles_results where analysis_id = 5


UNION ALL
select  count(*) as statistic_value, 'Device:ConceptCnt' as measure_id
 from  synpuf_100.achilles_results where analysis_id = 2101

UNION ALL
select  count(*) as statistic_value, 'Note:ConceptCnt' as measure_id
 from  synpuf_100.achilles_results where analysis_id = 2201;

--unmapped data (concept_0) derived measures (focusing on source values)

insert into synpuf_100.achilles_results_derived (stratum_1,statistic_value,measure_id)
 select  stratum_1, count(*) as statistic_value, 'UnmappedDataByDomain:SourceValueCnt' as measure_id
  from  synpuf_100.achilles_results where analysis_id = 1900  group by  1 ;


--count of specialties in the provider table
--(subsequent rule can check if this count is > trehshold) (general population dataset only))
insert into synpuf_100.achilles_results_derived (statistic_value,measure_id)
select  count(*) as statistic_value, 'Provider:SpeciatlyCnt' as measure_id
 from  synpuf_100.achilles_results where analysis_id = 301;



--derived data that are safe to share (greater aggregation and small patient count discarded at query level)
-- in derived result table; not at the end of the script


insert into synpuf_100.achilles_results_derived (stratum_1,statistic_value,measure_id)
select  decade as stratum_1, temp_cnt as statistic_value, 'Death:byDecade:SafePatientCnt' as measure_id
 from
   ( select  SUBSTR(stratum_1,0,3) as decade, sum(count_value) as temp_cnt   from  synpuf_100.achilles_results where analysis_id = 504   group by  1 )a
where temp_cnt >= 11


UNION ALL
-- insert into synpuf_100.achilles_results_derived (stratum_1,statistic_value,measure_id)
select  stratum_1, temp_cnt as statistic_value, 'Death:byYear:SafePatientCnt' as measure_id
 from
   ( select  stratum_1, sum(count_value) as temp_cnt   from  synpuf_100.achilles_results where analysis_id = 504   group by  1 )a
where temp_cnt >= 11;



--more aggregated view of visit type by decile (derived from analysis_id 204)
--denominator calculation will be replaced with new measure 212 in next version

insert into synpuf_100.achilles_results_derived (stratum_1,stratum_2,statistic_value,measure_id)
select  a.stratum_1, a.stratum_4 as stratum_2, 1.0*a.person_cnt/b.population_size as statistic_value, 'Visit:Type:PersonWithAtLeastOne:byDecile:Percentage' as measure_id
 from
( select  stratum_1, stratum_4, sum(count_value) as person_cnt    from  synpuf_100.achilles_results where analysis_id = 204  group by  1, 2 ) a
inner join
( select  stratum_4, sum(count_value) as population_size    from  synpuf_100.achilles_results where analysis_id = 204  group by  1 ) b
on  a.stratum_4=b.stratum_4
where a.person_cnt >= 11;


--size of Achilles Metadata
insert into synpuf_100.achilles_results_derived (stratum_1,statistic_value,measure_id)
 select  cast(analysis_id as string) as stratum_1, COUNT(*) as statistic_value, 'Achilles:byAnalysis:RowCnt' as measure_id
  from  synpuf_100.achilles_results  group by  1 
--;

UNION ALL
--General Population Only: ratio of born to deceased (indicates missing birth or death events) stratified by year
-- insert into synpuf_100.achilles_results_derived (stratum_1,statistic_value,measure_id)
select  a.stratum_1, 1.0*a.born_cnt/b.died_cnt as statistic_value, 'Death:BornDeceasedRatio' as measure_id
 from  (select  stratum_1, count_value as born_cnt  from  synpuf_100.achilles_results where analysis_id = 3) a
inner join
( select  stratum_1, count(count_value) as died_cnt   from  synpuf_100.achilles_results where analysis_id = 504  group by  1 ) b
on a.stratum_1 = b.stratum_1
where b.died_cnt > 0
;



--end of derived general measures ********************************************************************







--actual Heel rules start from here *****************************************







--Some rules check conformance to the CDM model, other rules look at data quality


--ruleid 1 check for non-zero counts from checks of improper data (invalid ids, out-of-bound data, inconsistent dates)
insert into synpuf_100.achilles_heel_results (
	analysis_id,
	achilles_heel_warning,
	rule_id,
	record_count
	)
select distinct  or1.analysis_id, concat(concat(concat(concat(concat(concat('ERROR: ' , cast(or1.analysis_id  as string) ), '-' ), oa1.analysis_name ), '; count (n=' ), cast(or1.count_value  as string) ), ') should not be > 0' )as achilles_heel_warning, 1 as rule_id, or1.count_value
 from  synpuf_100.achilles_results or1
inner join synpuf_100.achilles_analysis oa1
	on or1.analysis_id = oa1.analysis_id
where or1.analysis_id in (
		207,
		209,
		409,
		411,
		413,
		509,
		609,
		613,
		709,
		711,
		713,
		809,
		813,
		814
		) --all explicit counts of data anamolies
	and or1.count_value > 0
UNION ALL
--ruleid 2 distributions where min should not be negative
--insert into synpuf_100.achilles_heel_results (
--	analysis_id,
--	achilles_heel_warning,
--	rule_id,
--	record_count
--	)
 select distinct  ord1.analysis_id, concat(concat(concat(concat(concat(concat('ERROR: ' , cast(ord1.analysis_id  as string) ), ' - ' ), oa1.analysis_name ), ' (count = ' ), cast(COUNT(ord1.min_value)  as string) ), '); min value should not be negative' )as achilles_heel_warning, 2 as rule_id, COUNT(ord1.min_value) as record_count
  from  synpuf_100.achilles_results_dist ord1
inner join synpuf_100.achilles_analysis oa1
	on ord1.analysis_id = oa1.analysis_id
where ord1.analysis_id in (
		206,
		406,
		506,
		606,
		706,
		715,
		716,
		717,
		806,
		906,
		1006
		)
	and ord1.min_value < 0
	 group by  ord1.analysis_id, oa1.analysis_name
UNION ALL
--ruleid 3 death distributions where max should not be positive
--insert into synpuf_100.achilles_heel_results (
--	analysis_id,
--	achilles_heel_warning,
--	rule_id,
--	record_count
--)
 select distinct  ord1.analysis_id, concat(concat(concat(concat(concat(concat('WARNING: ' , cast(ord1.analysis_id  as string) ), '-' ), oa1.analysis_name ), ' (count = ' ), cast(COUNT(ord1.max_value)  as string) ), '); max value should not be positive, otherwise its a zombie with data >1mo after death ' )as achilles_heel_warning, 3 as rule_id, COUNT(ord1.max_value) as record_count
  from  synpuf_100.achilles_results_dist ord1
inner join synpuf_100.achilles_analysis oa1
	on ord1.analysis_id = oa1.analysis_id
where ord1.analysis_id in (
		511,
		512,
		513,
		514,
		515
		)
	and ord1.max_value > 30
 group by  ord1.analysis_id, oa1.analysis_name 
    UNION ALL
-- 
-- --ruleid 4 CDM-conformance rule: invalid concept_id
-- insert into synpuf_100.achilles_heel_results (
-- 	analysis_id,
-- 	achilles_heel_warning,
-- 	rule_id,
-- 	record_count
-- )
 select  or1.analysis_id, concat(concat(concat(concat(concat(concat('ERROR: ' , cast(or1.analysis_id  as string) ), '-' ), oa1.analysis_name ), '; ' ), cast(COUNT(distinct stratum_1)  as string) ), ' concepts in data are not in vocabulary' )as achilles_heel_warning, 4 as rule_id, COUNT(distinct stratum_1) as record_count
  from  synpuf_100.achilles_results or1
inner join synpuf_100.achilles_analysis oa1
	on or1.analysis_id = oa1.analysis_id
left join concept c1
	on or1.stratum_1 = cast(c1.concept_id  as string)
where or1.analysis_id in (
		2,
		4,
		5,
		200,
-- 		301, --Number of providers by specialty concept_id
		400,
		500,
		505,
		600,
		700,
		800
-- 		,
-- 		900, Move to NOTIFICATION Drug Era
-- 		1000,Move to NOTIFICATION Drug Era
-- 		1609,Move to NOTIFICATION Cost
-- 		1610
		)
	and or1.stratum_1 is not null
	and c1.concept_id is null
 group by  or1.analysis_id, oa1.analysis_name 
UNION ALL
----ruleid 5 CDM-conformance rule:invalid type concept_id
----this rule is only checking that the concept is valid (joins to concept table at all)
----it does not check the vocabulary_id to further restrict the scope of the valid concepts
----to only include,for example, death types
--insert into synpuf_100.achilles_heel_results (
--	analysis_id,
--	achilles_heel_warning,
--	rule_id,
--	record_count
--	)
 select  or1.analysis_id, concat(concat(concat(concat(concat(concat('ERROR: ' , cast(or1.analysis_id  as string) ), '-' ), oa1.analysis_name ), '; ' ), cast(COUNT(distinct stratum_2)  as string) ), ' concepts in data are not in vocabulary' )as achilles_heel_warning, 5 as rule_id, COUNT(distinct stratum_2) as record_count
  from  synpuf_100.achilles_results or1
inner join synpuf_100.achilles_analysis oa1
	on or1.analysis_id = oa1.analysis_id
left join concept c1
	on or1.stratum_2 = cast(c1.concept_id  as string)
where or1.analysis_id in (
		405,
		605,
		705,
		805,
		1805
		)
	and or1.stratum_2 is not null
	and c1.concept_id is null
 group by  or1.analysis_id, oa1.analysis_name 
UNION ALL
----ruleid 6 CDM-conformance rule:invalid concept_id
--insert into synpuf_100.achilles_heel_results (
--	analysis_id,
--	achilles_heel_warning,
--	rule_id,
--	record_count
--	)
 select  or1.analysis_id, concat(concat(concat(concat('WARNING: ' , cast(or1.analysis_id  as string) ), '-' ), oa1.analysis_name ), '; data with unmapped concepts' )as achilles_heel_warning, 6 as rule_id, null as record_count
  from  synpuf_100.achilles_results or1
inner join synpuf_100.achilles_analysis oa1
	on or1.analysis_id = oa1.analysis_id
where or1.analysis_id in (
		2,
		4,
		5,
		200,
		301,
		400,
		500,
		505,
		600,
		700,
		800,
		900,
		1000,
		1609,
		1610
		)
	and or1.stratum_1 = '0'
 group by  or1.analysis_id, oa1.analysis_name 
UNION ALL
----concept from the wrong vocabulary
----ruleid 7 CDM-conformance rule:gender  - 12 HL7
--insert into synpuf_100.achilles_heel_results (
--	analysis_id,
--	achilles_heel_warning,
--	rule_id,
--	record_count
--	)
 select  or1.analysis_id, concat(concat(concat(concat(concat(concat('ERROR: ' , cast(or1.analysis_id  as string) ), '-' ), oa1.analysis_name ), '; ' ), cast(COUNT(distinct stratum_1)  as string) ), ' concepts in data are not in correct vocabulary' )as achilles_heel_warning, 7 as rule_id, COUNT(distinct stratum_1) as record_count
  from  synpuf_100.achilles_results or1
inner join synpuf_100.achilles_analysis oa1
	on or1.analysis_id = oa1.analysis_id
inner join concept c1
	on or1.stratum_1 = cast(c1.concept_id  as string)
where or1.analysis_id in (2)
	and or1.stratum_1 is not null
	and c1.concept_id <> 0
  and lower(c1.domain_id) not in ('gender')
 group by  or1.analysis_id, oa1.analysis_name 
UNION ALL
----ruleid 8 race  - 13 CDC Race
--insert into synpuf_100.achilles_heel_results (
--	analysis_id,
--	achilles_heel_warning,
--	rule_id,
--	record_count
--	)
 select  or1.analysis_id, concat(concat(concat(concat(concat(concat('ERROR: ' , cast(or1.analysis_id  as string) ), '-' ), oa1.analysis_name ), '; ' ), cast(COUNT(distinct stratum_1)  as string) ), ' concepts in data are not in correct vocabulary' )as achilles_heel_warning, 8 as rule_id, COUNT(distinct stratum_1) as record_count
  from  synpuf_100.achilles_results or1
inner join synpuf_100.achilles_analysis oa1
	on or1.analysis_id = oa1.analysis_id
inner join concept c1
	on or1.stratum_1 = cast(c1.concept_id  as string)
where or1.analysis_id in (4)
	and or1.stratum_1 is not null
	and c1.concept_id <> 0
  and lower(c1.domain_id) not in ('race')
 group by  or1.analysis_id, oa1.analysis_name 
UNION ALL
----ruleid 9 ethnicity - 44 ethnicity
--insert into synpuf_100.achilles_heel_results (
--	analysis_id,
--	achilles_heel_warning,
--	rule_id,
--	record_count
--	)
 select  or1.analysis_id, concat(concat(concat(concat(concat(concat('ERROR: ' , cast(or1.analysis_id  as string) ), '-' ), oa1.analysis_name ), '; ' ), cast(COUNT(distinct stratum_1)  as string) ), ' concepts in data are not in correct vocabulary (CMS Ethnicity)' )as achilles_heel_warning, 9 as rule_id, COUNT(distinct stratum_1) as record_count
  from  synpuf_100.achilles_results or1
inner join synpuf_100.achilles_analysis oa1
	on or1.analysis_id = oa1.analysis_id
inner join concept c1
	on or1.stratum_1 = cast(c1.concept_id  as string)
where or1.analysis_id in (5)
	and or1.stratum_1 is not null
	and c1.concept_id <> 0
  and lower(c1.domain_id) not in ('ethnicity')
 group by  or1.analysis_id, oa1.analysis_name 
UNION ALL
--ruleid 10 place of service - 14 CMS place of service, 24 OMOP visit
--insert into synpuf_100.achilles_heel_results (
--	analysis_id,
--	achilles_heel_warning,
--	rule_id,
--	record_count
--	)
 select  or1.analysis_id, concat(concat(concat(concat(concat(concat('ERROR: ' , cast(or1.analysis_id  as string) ), '-' ), oa1.analysis_name ), '; ' ), cast(COUNT(distinct stratum_1)  as string) ), ' concepts in data are not in correct vocabulary' )as achilles_heel_warning, 10 as rule_id, COUNT(distinct stratum_1) as record_count
  from  synpuf_100.achilles_results or1
inner join synpuf_100.achilles_analysis oa1
	on or1.analysis_id = oa1.analysis_id
inner join concept c1
	on or1.stratum_1 = cast(c1.concept_id  as string)
where or1.analysis_id in (202)
	and or1.stratum_1 is not null
	and c1.concept_id <> 0
  and lower(c1.domain_id) not in ('visit')
 group by  or1.analysis_id, oa1.analysis_name 
UNION ALL
--ruleid 11 CDM-conformance rule:specialty - 48 specialty
--insert into synpuf_100.achilles_heel_results (
--	analysis_id,
--	achilles_heel_warning,
--	rule_id,
--	record_count
--	)
 select  or1.analysis_id, concat(concat(concat(concat(concat(concat('ERROR: ' , cast(or1.analysis_id  as string) ), '-' ), oa1.analysis_name ), '; ' ), cast(COUNT(distinct stratum_1)  as string) ), ' concepts in data are not in correct vocabulary (Specialty)' )as achilles_heel_warning, 11 as rule_id, COUNT(distinct stratum_1) as record_count
  from  synpuf_100.achilles_results or1
inner join synpuf_100.achilles_analysis oa1
	on or1.analysis_id = oa1.analysis_id
inner join concept c1
	on or1.stratum_1 = cast(c1.concept_id  as string)
where or1.analysis_id in (301)
	and or1.stratum_1 is not null
	and c1.concept_id <> 0
  and lower(c1.domain_id) not in ('provider specialty')
 group by  or1.analysis_id, oa1.analysis_name 
UNION ALL
--ruleid 12 condition occurrence, era - 1 SNOMED
--insert into synpuf_100.achilles_heel_results (
--	analysis_id,
--	achilles_heel_warning,
--	rule_id,
--	record_count
--	)
 select  or1.analysis_id, concat(concat(concat(concat(concat(concat('ERROR: ' , cast(or1.analysis_id  as string) ), '-' ), oa1.analysis_name ), '; ' ), cast(COUNT(distinct stratum_1)  as string) ), ' concepts in data are not in correct vocabulary' )as achilles_heel_warning, 12 as rule_id, COUNT(distinct stratum_1) as record_count
  from  synpuf_100.achilles_results or1
inner join synpuf_100.achilles_analysis oa1
	on or1.analysis_id = oa1.analysis_id
inner join concept c1
	on or1.stratum_1 = cast(c1.concept_id  as string)
where or1.analysis_id in (
		400
-- 		1000
		)
	and or1.stratum_1 is not null
	and c1.concept_id <> 0
  and lower(c1.domain_id) not in ('condition','condition/drug', 'condition/meas', 'condition/obs', 'condition/procedure')
 group by  or1.analysis_id, oa1.analysis_name 
UNION ALL
--ruleid 13 drug exposure - 8 RxNorm
--insert into synpuf_100.achilles_heel_results (
--	analysis_id,
--	achilles_heel_warning,
--	rule_id,
--	record_count
--	)
 select  or1.analysis_id, concat(concat(concat(concat(concat(concat('ERROR: ' , cast(or1.analysis_id  as string) ), '-' ), oa1.analysis_name ), '; ' ), cast(COUNT(distinct stratum_1)  as string) ), ' concepts in data are not in correct vocabulary' )as achilles_heel_warning, 13 as rule_id, COUNT(distinct stratum_1) as record_count
  from  synpuf_100.achilles_results or1
inner join synpuf_100.achilles_analysis oa1
	on or1.analysis_id = oa1.analysis_id
inner join concept c1
	on or1.stratum_1 = cast(c1.concept_id  as string)
where or1.analysis_id in (
		700,
		900
		)
	and or1.stratum_1 is not null
	and c1.concept_id <> 0
  and lower(c1.domain_id) not in ('drug','condition/drug', 'device/drug', 'drug/measurement', 'drug/obs', 'drug/procedure')
 group by  or1.analysis_id, oa1.analysis_name 
UNION ALL
--ruleid 14 procedure - 4 CPT4/5 HCPCS/3 ICD9P
--insert into synpuf_100.achilles_heel_results (
--	analysis_id,
--	achilles_heel_warning,
--	rule_id,
--	record_count
--	)
 select  or1.analysis_id, concat(concat(concat(concat(concat(concat('ERROR: ' , cast(or1.analysis_id  as string) ), '-' ), oa1.analysis_name ), '; ' ), cast(COUNT(distinct stratum_1)  as string) ), ' concepts in data are not in correct vocabulary' )as achilles_heel_warning, 14 as rule_id, COUNT(distinct stratum_1) as record_count
  from  synpuf_100.achilles_results or1
inner join synpuf_100.achilles_analysis oa1
	on or1.analysis_id = oa1.analysis_id
inner join concept c1
	on or1.stratum_1 = cast(c1.concept_id  as string)
where or1.analysis_id in (600)
	and or1.stratum_1 is not null
	and c1.concept_id <> 0
  and lower(c1.domain_id) not in ('procedure','condition/procedure', 'device/procedure', 'drug/procedure', 'obs/procedure')
 group by  or1.analysis_id, oa1.analysis_name 
UNION ALL
--15 observation  - 6 LOINC

--NOT APPLICABLE IN CDMv5


--16 disease class - 40 DRG

--NOT APPLICABLE IN CDMV5

--ruleid 17 revenue code - 43 revenue code
--insert into synpuf_100.achilles_heel_results (
--	analysis_id,
--	achilles_heel_warning,
--	rule_id,
--	record_count
--	)
 select  or1.analysis_id, concat(concat(concat(concat(concat(concat('ERROR: ' , cast(or1.analysis_id  as string) ), '-' ), oa1.analysis_name ), '; ' ), cast(COUNT(distinct stratum_1)  as string) ), ' concepts in data are not in correct vocabulary (revenue code)' )as achilles_heel_warning, 17 as rule_id, COUNT(distinct stratum_1) as record_count
  from  synpuf_100.achilles_results or1
inner join synpuf_100.achilles_analysis oa1
	on or1.analysis_id = oa1.analysis_id
inner join concept c1
	on or1.stratum_1 = cast(c1.concept_id  as string)
where or1.analysis_id in (1610)
	and or1.stratum_1 is not null
	and c1.concept_id <> 0
  and lower(c1.domain_id) not in ('revenue code')
 group by  or1.analysis_id, oa1.analysis_name 
UNION ALL
--ruleid 18 ERROR:  year of birth in the future
--insert into synpuf_100.achilles_heel_results (
--	analysis_id,
--	achilles_heel_warning,
--	rule_id,
--	record_count
--	)
 select distinct  or1.analysis_id, concat(concat(concat(concat(concat(concat('ERROR: ' , cast(or1.analysis_id  as string) ), '-' ), oa1.analysis_name ), '; should not have year of birth in the future, (n=' ), cast(sum(or1.count_value)  as string) ), ')' )as achilles_heel_warning, 18 as rule_id, sum(or1.count_value) as record_count
  from  synpuf_100.achilles_results or1
inner join synpuf_100.achilles_analysis oa1
	on or1.analysis_id = oa1.analysis_id
where or1.analysis_id in (3)
	and cast(or1.stratum_1  as int64) > EXTRACT(YEAR from CURRENT_DATE())
	and or1.count_value > 0
 group by  or1.analysis_id, oa1.analysis_name 
UNION ALL
--ruleid 19 WARNING:  year of birth < 1800
--insert into synpuf_100.achilles_heel_results (
--	analysis_id,
--	achilles_heel_warning,
--	rule_id,
--	record_count
--	)
 select  or1.analysis_id, concat(concat(concat(concat(concat(concat('ERROR: ' , cast(or1.analysis_id  as string) ), '-' ), oa1.analysis_name ), '; should not have year of birth < 1800, (n=' ), cast(sum(or1.count_value)  as string) ), ')' )as achilles_heel_warning, 19 as rule_id, sum(or1.count_value) as record_count
  from  synpuf_100.achilles_results or1
inner join synpuf_100.achilles_analysis oa1
	on or1.analysis_id = oa1.analysis_id
where or1.analysis_id in (3)
	and cast(or1.stratum_1  as int64) < 1800
	and or1.count_value > 0
 group by  or1.analysis_id, oa1.analysis_name
UNION ALL

--ruleid 20 ERROR:  age < 0
--insert into synpuf_100.achilles_heel_results (
--	analysis_id,
--	achilles_heel_warning,
--	rule_id,
--	record_count
--	)
 select  or1.analysis_id, concat(concat(concat(concat(concat(concat('ERROR: ' , cast(or1.analysis_id  as string) ), '-' ), oa1.analysis_name ), '; should not have age < 0, (n=' ), cast(sum(or1.count_value)  as string) ), ')' )as achilles_heel_warning, 20 as rule_id, sum(or1.count_value) as record_count
  from  synpuf_100.achilles_results or1
inner join synpuf_100.achilles_analysis oa1
	on or1.analysis_id = oa1.analysis_id
where or1.analysis_id in (101)
	and cast(or1.stratum_1  as int64) < 0
	and or1.count_value > 0
 group by  or1.analysis_id, oa1.analysis_name
UNION ALL

--ruleid 21 ERROR: age > 150  (TODO lower number seems more appropriate)
-- insert into synpuf_100.achilles_heel_results (
-- 	analysis_id,
-- 	achilles_heel_warning,
-- 	rule_id,
-- 	record_count
-- 	)
 select  or1.analysis_id, concat(concat(concat(concat(concat(concat('ERROR: ' , cast(or1.analysis_id  as string) ), '-' ), oa1.analysis_name ), '; should not have age > 150, (n=' ), cast(sum(or1.count_value)  as string) ), ')' )as achilles_heel_warning, 21 as rule_id, sum(or1.count_value) as record_count
  from  synpuf_100.achilles_results or1
inner join synpuf_100.achilles_analysis oa1
	on or1.analysis_id = oa1.analysis_id
where or1.analysis_id in (101)
	and cast(or1.stratum_1  as int64) > 150
	and or1.count_value > 0
 group by  or1.analysis_id, oa1.analysis_name;

--ruleid 22 WARNING:  monthly change > 100%
insert into synpuf_100.achilles_heel_results (
	analysis_id,
	achilles_heel_warning,
	rule_id

	)
select distinct  ar1.analysis_id, concat(concat(concat(concat('WARNING: ' , cast(ar1.analysis_id  as string) ), '-' ), aa1.analysis_name ), '; theres a 100% change in monthly count of events' )as achilles_heel_warning, 22 as rule_id

 from  synpuf_100.achilles_analysis aa1
inner join synpuf_100.achilles_results ar1
	on aa1.analysis_id = ar1.analysis_id
inner join synpuf_100.achilles_results ar2
	on ar1.analysis_id = ar2.analysis_id
		and ar1.analysis_id in (
			420,
			620,
			720,
			820,
			920,
			1020
			)
where (
		cast(ar1.stratum_1  as int64) + 1 = cast(ar2.stratum_1  as int64)
		or cast(ar1.stratum_1  as int64) + 89 = cast(ar2.stratum_1  as int64)
		)
	and 1.0 * abs(ar2.count_value - ar1.count_value) / ar1.count_value > 1
	and ar1.count_value > 10;

--ruleid 23 WARNING:  monthly change > 100% at concept level
insert into synpuf_100.achilles_heel_results (
	analysis_id,
	achilles_heel_warning,
	rule_id,
	record_count
	)
 select  ar1.analysis_id, concat(concat(concat(concat(concat(concat('WARNING: ' , cast(ar1.analysis_id  as string) ), '-' ), aa1.analysis_name ), '; ' ), cast(COUNT(distinct ar1.stratum_1)  as string) ), ' concepts have a 100% change in monthly count of events' )as achilles_heel_warning, 23 as rule_id, COUNT(distinct ar1.stratum_1) as record_count
  from  synpuf_100.achilles_analysis aa1
inner join synpuf_100.achilles_results ar1
	on aa1.analysis_id = ar1.analysis_id
inner join synpuf_100.achilles_results ar2
	on ar1.analysis_id = ar2.analysis_id
		and ar1.stratum_1 = ar2.stratum_1
		and ar1.analysis_id in (
			402,
			602,
			702,
			802,
			902,
			1002
			)
where (
		round(cast(ar1.stratum_2 as float64),0) + 1 = round(cast(ar2.stratum_2 as float64),0)
		or round(cast(ar1.stratum_2 as float64),0) + 89 = round(cast(ar2.stratum_2 as float64),0)
		)
	and 1.0 * abs(ar2.count_value - ar1.count_value) / ar1.count_value > 1
	and ar1.count_value > 10
 group by  ar1.analysis_id, aa1.analysis_name 
UNION ALL
--ruleid 24 WARNING: days_supply > 180
-- insert into synpuf_100.achilles_heel_results (
-- 	analysis_id,
-- 	achilles_heel_warning,
-- 	rule_id,
-- 	record_count
-- 	)
 select distinct  ord1.analysis_id, concat(concat(concat(concat(concat(concat('WARNING: ' , cast(ord1.analysis_id  as string) ), '-' ), oa1.analysis_name ), ' (count = ' ), cast(COUNT(ord1.max_value)  as string) ), '); max value should not be > 180' )as achilles_heel_warning, 24 as rule_id, COUNT(ord1.max_value) as record_count
  from  synpuf_100.achilles_results_dist ord1
inner join synpuf_100.achilles_analysis oa1
	on ord1.analysis_id = oa1.analysis_id
where ord1.analysis_id in (715)
	and ord1.max_value > 180
 group by  ord1.analysis_id, oa1.analysis_name 
UNION ALL
--ruleid 25 WARNING:  refills > 10
-- insert into synpuf_100.achilles_heel_results (
-- 	analysis_id,
-- 	achilles_heel_warning,
-- 	rule_id,
-- 	record_count
-- 	)
 select distinct  ord1.analysis_id, concat(concat(concat(concat(concat(concat('WARNING: ' , cast(ord1.analysis_id  as string) ), '-' ), oa1.analysis_name ), ' (count = ' ), cast(COUNT(ord1.max_value)  as string) ), '); max value should not be > 10' )as achilles_heel_warning, 25 as rule_id, COUNT(ord1.max_value) as record_count
  from  synpuf_100.achilles_results_dist ord1
inner join synpuf_100.achilles_analysis oa1
	on ord1.analysis_id = oa1.analysis_id
where ord1.analysis_id in (716)
	and ord1.max_value > 10
 group by  ord1.analysis_id, oa1.analysis_name 
UNION ALL
--ruleid 26 DQ rule: WARNING: quantity > 600
-- insert into synpuf_100.achilles_heel_results (
-- 	analysis_id,
-- 	achilles_heel_warning,
-- 	rule_id,
-- 	record_count
-- 	)
 select distinct  ord1.analysis_id, concat(concat(concat(concat(concat(concat('WARNING: ' , cast(ord1.analysis_id  as string) ), '-' ), oa1.analysis_name ), ' (count = ' ), cast(count(ord1.max_value)  as string) ), '); max value should not be > 600' )as achilles_heel_warning, 26 as rule_id, count(ord1.max_value) as record_count
  from  synpuf_100.achilles_results_dist ord1
inner join synpuf_100.achilles_analysis oa1
	on ord1.analysis_id = oa1.analysis_id
where ord1.analysis_id in (717)
	and ord1.max_value > 600
 group by  ord1.analysis_id, oa1.analysis_name

 UNION ALL

 select distinct  or1.analysis_id, concat(concat(concat(concat(concat(concat('WARNING: ' , cast(or1.analysis_id  as string) ), '-' ), oa1.analysis_name ), '; count (n=' ), cast(or1.count_value  as string) ), ') should not be > 0' )as achilles_heel_warning, 1 as rule_id, or1.count_value
 from  synpuf_100.achilles_results or1
inner join synpuf_100.achilles_analysis oa1
	on or1.analysis_id = oa1.analysis_id
where or1.analysis_id in (
		7,
		8,
		9
		) --all explicit counts of potential data anamolies
	and or1.count_value > 0

UNION ALL

select distinct  or1.analysis_id, concat(concat(concat(concat(concat(concat('WARNING: ' , cast(or1.analysis_id  as string) ), '-' ), oa1.analysis_name ), '; count (n=' ), cast(or1.count_value  as string) ), ') should not be > 0' )as achilles_heel_warning, 1 as rule_id, or1.count_value
 from  synpuf_100.achilles_results or1
inner join synpuf_100.achilles_analysis oa1
	on or1.analysis_id = oa1.analysis_id
where or1.analysis_id in (
		210, --Number of visit records with invalid care_site_id
		302, --Number of providers with invalid care site id
		412, --Number of condition occurrence records with invalid provider_id
		612, --Number of procedure occurrence records with invalid provider_id
		712, --Number of drug exposure records with invalid provider_id
		812 --Number of observation records with invalid provider_id
		) --all explicit counts of data anamolies
	and or1.count_value > 0
;


--rules may require first a derived measure and the subsequent data quality
--check is simpler to implement
--also results are accessible even if the rule did not generate a warning

--rule27
--due to most likely missint sql cast errors it was removed from this release
--will be included after more testing
--being fixed in this update

--compute derived measure first
insert into synpuf_100.achilles_results_derived (statistic_value,stratum_1,measure_id)
select  100.0*(select  statistic_value  from  synpuf_100.achilles_results_derived where measure_id like 'UnmappedData:ach_401:GlobalRowCnt')/statistic_value as statistic_value, 'Condition' as stratum_1, 'UnmappedData:byDomain:Percentage' as measure_id
 from  synpuf_100.achilles_results_derived where measure_id ='ach_401:GlobalRowCnt'
UNION ALL

--insert into synpuf_100.achilles_results_derived (statistic_value,stratum_1,measure_id)
select  100.0*(select  statistic_value  from  synpuf_100.achilles_results_derived where measure_id = 'UnmappedData:ach_601:GlobalRowCnt')/statistic_value as statistic_value, 'Procedure' as stratum_1, 'UnmappedData:byDomain:Percentage' as measure_id
 from  synpuf_100.achilles_results_derived where measure_id ='ach_601:GlobalRowCnt'
UNION ALL
-- insert into synpuf_100.achilles_results_derived (statistic_value,stratum_1,measure_id)
select  100.0*(select  statistic_value  from  synpuf_100.achilles_results_derived where measure_id = 'UnmappedData:ach_701:GlobalRowCnt')/statistic_value as statistic_value, 'DrugExposure' as stratum_1, 'UnmappedData:byDomain:Percentage' as measure_id
 from  synpuf_100.achilles_results_derived where measure_id ='ach_701:GlobalRowCnt'
UNION ALL
-- insert into synpuf_100.achilles_results_derived (statistic_value,stratum_1,measure_id)
select  100.0*(select  statistic_value  from  synpuf_100.achilles_results_derived where measure_id = 'UnmappedData:ach_801:GlobalRowCnt')/statistic_value as statistic_value, 'Observation' as stratum_1, 'UnmappedData:byDomain:Percentage' as measure_id
 from  synpuf_100.achilles_results_derived where measure_id ='ach_801:GlobalRowCnt'
UNION ALL
-- insert into synpuf_100.achilles_results_derived (statistic_value,stratum_1,measure_id)
select  100.0*(select  statistic_value  from  synpuf_100.achilles_results_derived where measure_id = 'UnmappedData:ach_1801:GlobalRowCnt')/statistic_value as statistic_value, 'Measurement' as stratum_1, 'UnmappedData:byDomain:Percentage' as measure_id
 from  synpuf_100.achilles_results_derived where measure_id ='ach_1801:GlobalRowCnt';


--actual rule27

  insert into synpuf_100.achilles_heel_results (achilles_heel_warning,rule_id)
  select  concat('NOTIFICATION:Unmapped data over percentage threshold in:' , cast(d.stratum_1  as string) )as achilles_heel_warning, 27 as rule_id
   from  synpuf_100.achilles_results_derived d
  where d.measure_id = 'UnmappedData:byDomain:Percentage'
  and d.statistic_value > 0.1  --thresholds will be decided in the ongoing DQ-Study2
  ;

--end of rule27

--rule28 DQ rule
--are all values (or more than threshold) in measurement table non numerical?
--(count of Measurment records with no numerical value is in analysis_id 1821)



INTO temp.tempresults
  WITH t1  as (select  sum(count_value)  as all_count  from  synpuf_100.achilles_results where analysis_id = 1820)
select  (select  count_value  from  synpuf_100.achilles_results where analysis_id = 1821)*100.0/all_count as statistic_value, CAST('Meas:NoNumValue:Percentage'  AS STRING) as measure_id
  FROM  t1;


insert into synpuf_100.achilles_results_derived (statistic_value, measure_id)
  select  statistic_value, measure_id  from  temp.tempresults;



insert into synpuf_100.achilles_heel_results (achilles_heel_warning,rule_id,record_count)
select  'NOTIFICATION: percentage of non-numerical measurement records exceeds general population threshold ' as achilles_heel_warning, 28 as rule_id, cast(statistic_value  as int64) as record_count
 from  temp.tempresults t
--WHERE t.analysis_id IN (100730,100430) --umbrella version
where measure_id='Meas:NoNumValue:Percentage' --t.analysis_id IN (100000)
--the intended threshold is 1 percent, this value is there to get pilot data from early adopters
	and t.statistic_value >= 80
;


--clean up temp tables for rule 28
truncate table temp.tempresults;
drop table temp.tempresults;

--end of rule 28

--rule29 DQ rule
--unusual diagnosis present, this rule is terminology dependend

INTO temp.tempresults
--set threshold here, currently it is zero
  WITH tempcnt as(
	select  sum(count_value) as pt_cnt  from  synpuf_100.achilles_results
	where analysis_id = 404 --dx by decile
	and stratum_1 = '195075' --meconium
	--and stratum_3 = '8507' --possible limit to males only
	and cast(stratum_4  as int64) >= 5 --fifth decile or more
)
select  pt_cnt as record_count
  FROM  tempcnt where pt_cnt > 0;


--using temp table because with clause that occurs prior insert into is causing problems
--and with clause makes the code more readable
insert into synpuf_100.achilles_heel_results (achilles_heel_warning,rule_id,record_count)
select  'WARNING:[PLAUSIBILITY] infant-age diagnosis (195075) at age 50+' as achilles_heel_warning, 29 as rule_id, record_count
 from  temp.tempresults t;

truncate table temp.tempresults;
drop table temp.tempresults;
--end of rule29


--rule30 CDM-conformance rule: is CDM metadata table created at all?
  --create a derived measure for rule30
  --done strangly to possibly avoid from dual error on Oracle
  --done as not null just in case sqlRender has NOT NULL  hard coded
  --check if table exist and if yes - derive 1 for a derived measure

  --does not work on redshift :-( --commenting it out
--IF OBJECT_ID('synpuf_100.CDM_SOURCE', 'U') IS NOT NULL
--insert into synpuf_100.ACHILLES_results_derived (statistic_value,measure_id)
--  select distinct analysis_id as statistic_value,
--  'MetaData:TblExists' as measure_id
--  from synpuf_100.ACHILLES_results
--  where analysis_id = 1;

  --actual rule30

--end of rule30


--rule31 DQ rule
--ratio of providers to total patients

--compute a derived reatio
--TODO if provider count is zero it will generate division by zero (not sure how dirrerent db engins will react)
--insert into synpuf_100.achilles_results_derived (statistic_value,measure_id)
--    select  1.0*(select  count_value as total_pts  from  synpuf_100.achilles_results r where analysis_id =1)/(count_value+1) as statistic_value, 'Provider:PatientProviderRatio' as measure_id
--     from  synpuf_100.achilles_results where analysis_id = 300
--;
--
insert into synpuf_100.achilles_results_derived (statistic_value,measure_id)
   select 
    CASE
        WHEN (  SELECT count_value FROM synpuf_100.achilles_results  WHERE analysis_id = 300) > 0 
            THEN (  SELECT 1.0*(  SELECT count_value AS total_pts  FROM synpuf_100.achilles_results r 
                    WHERE analysis_id =1)/count_value
            )
        ELSE 0.0 
    END AS statistic_value,'Provider:PatientProviderRatio' AS measure_id 
    FROM 
    synpuf_100.achilles_results  
    WHERE 
    analysis_id = 300 ;

--actual rule
insert into synpuf_100.achilles_heel_results (achilles_heel_warning,rule_id)
select  'NOTIFICATION:[PLAUSIBILITY] database has too few providers defined (given the total patient number)' as achilles_heel_warning, 31 as rule_id
 from  synpuf_100.achilles_results_derived d
where d.measure_id = 'Provider:PatientProviderRatio'
and d.statistic_value > 10000  --thresholds will be decided in the ongoing DQ-Study2
-- ;
UNION ALL

--rule32 DQ rule
--uses iris: patients with at least one visit visit
--does 100-THE IRIS MEASURE to check for percentage of patients with no visits

-- insert into synpuf_100.achilles_heel_results (achilles_heel_warning,rule_id)
select  'NOTIFICATION: Percentage of patients with no visits exceeds threshold' as achilles_heel_warning, 32 as rule_id
 from  synpuf_100.achilles_results_derived d
where d.measure_id = 'ach_2003:Percentage'
and 100-d.statistic_value > 27  --threshold identified in the DataQuality study
-- ;
UNION ALL
--rule33 DQ rule (for general population only)
--NOTIFICATION: database does not have all age 0-80 represented


-- insert into synpuf_100.achilles_heel_results (achilles_heel_warning,rule_id)
select  'NOTIFICATION: [GeneralPopulationOnly] Not all deciles represented at first observation' as achilles_heel_warning, 33 as rule_id
 from  synpuf_100.achilles_results_derived d
where d.measure_id = 'AgeAtFirstObsByDecile:DecileCnt'
and d.statistic_value <9  --we expect deciles 0,1,2,3,4,5,6,7,8
;


--rule34 DQ rule
--NOTIFICATION: number of unmapped source values exceeds threshold
--related to rule 27 that looks at percentage of unmapped rows (rows as focus)
--this rule is looking at source values (as focus)


insert into synpuf_100.achilles_heel_results (achilles_heel_warning,rule_id,record_count)
select  concat('NOTIFICATION: Count of unmapped source values exceeds threshold in: ' , cast(stratum_1  as string) )as achilles_heel_warning, 34 as rule_id, cast(statistic_value  as int64) as record_count
 from  synpuf_100.achilles_results_derived d
where measure_id = 'UnmappedDataByDomain:SourceValueCnt'
and statistic_value > 1000; --threshold will be decided in DQ study 2



--rule35 DQ rule, NOTIFICATION
--this rule analyzes Units recorded for measurement

insert into synpuf_100.achilles_heel_results (achilles_heel_warning,rule_id,record_count)
 select  'NOTIFICATION: Count of measurement_ids with more than 5 distinct units  exceeds threshold' as achilles_heel_warning, 35 as rule_id, cast(meas_concept_id_cnt  as int64) as record_count
   from  (
        select  meas_concept_id_cnt  from  (select  sum(freq) as meas_concept_id_cnt  from
                        ( select  u_cnt, count(*) as freq   from  ( select  stratum_1, count(*) as u_cnt
                                      from  synpuf_100.achilles_results where analysis_id = 1807  group by  1 ) a
                                     group by  1 ) b
                where u_cnt >= 5 --threshold one for the rule
            ) c
           where meas_concept_id_cnt >= 10 --threshold two for the rule
       ) d
;



--ruleid 36 WARNING: age > 125   (related to an error grade rule 21 that has higher threshold)
insert into synpuf_100.achilles_heel_results (
	analysis_id,
	achilles_heel_warning,
	rule_id,
	record_count
	)
 select  or1.analysis_id, concat(concat(concat(concat(concat(concat('WARNING: ' , cast(or1.analysis_id  as string) ), '-' ), oa1.analysis_name ), '; should not have age > 125, (n=' ), cast(sum(or1.count_value)  as string) ), ')' )as achilles_heel_warning, 36 as rule_id, sum(or1.count_value) as record_count
  from  synpuf_100.achilles_results or1
inner join synpuf_100.achilles_analysis oa1
	on or1.analysis_id = oa1.analysis_id
where or1.analysis_id in (101)
	and cast(or1.stratum_1  as int64) > 125
	and or1.count_value > 0
 group by  or1.analysis_id, oa1.analysis_name ;

--ruleid 37 DQ rule

--derived measure for this rule - ratio of notes over the number of visits
insert into synpuf_100.achilles_results_derived (statistic_value,measure_id)
select 1.0*(select  sum(count_value) as all_notes  from  synpuf_100.achilles_results r where analysis_id =2201 )/1.0*(select  sum(count_value) as all_visits  from  synpuf_100.achilles_results r where  analysis_id =201 ) as statistic_value,
  'Note:NoteVisitRatio' as measure_id;

--one co-author of the DataQuality study suggested measuring data density on visit level (in addition to
-- patient and dataset level)
--Assumption is that at least one data event (e.g., diagnisis, note) is generated for each visit
--this rule is testing that at least some notes exist (considering the number of visits)
--for datasets with zero notes the derived measure is null and rule does not fire at all
--possible elaboration of this rule include number of inpatient notes given number of inpatient visits
--current rule is on overall data density (for notes only) per visit level

insert into synpuf_100.achilles_heel_results (achilles_heel_warning,rule_id,record_count)
select  'NOTIFICATION: Notes data density is below threshold'  as achilles_heel_warning, 37 as rule_id, cast(statistic_value  as int64) as record_count
 from  synpuf_100.achilles_results_derived d
where measure_id = 'Note:NoteVisitRatio'
and statistic_value < 0.01; --threshold will be decided in DataQuality study




--ruleid 38 DQ rule; in a general dataset, it is expected that more than providers with a wide range of specialties
--(at least more than just one specialty) is present
--notification  may indicate that provider table is missing data on specialty
--typical dataset has at least 28 specialties present in provider table

insert into synpuf_100.achilles_heel_results (achilles_heel_warning,rule_id,record_count)
select  'NOTIFICATION: [GeneralPopulationOnly] Count of distinct specialties of providers in the PROVIDER table is below threshold'  as achilles_heel_warning, 38 as rule_id, cast(statistic_value  as int64) as record_count
 from  synpuf_100.achilles_results_derived d
where measure_id = 'Provider:SpeciatlyCnt'
and statistic_value <2; --DataQuality data indicate median of 55 specialties (percentile25 is 28; percentile10 is 2)


--ruleid 39 DQ rule; Given lifetime record DQ assumption if more than 30k patients is born for every deceased patient
--the dataset may not be recording complete records for all senior patients in that year
--derived ratio measure Death:BornDeceasedRatio only exists for years where death data exist
--to avoid alerting on too early years such as 1925 where births exist but no deaths

insert into synpuf_100.achilles_heel_results (achilles_heel_warning,rule_id,record_count)
select  'NOTIFICATION: [GeneralPopulationOnly] In some years, number of deaths is too low considering the number of birhts (lifetime record DQ assumption)'
 as achilles_heel_warning, 39 as rule_id, year_cnt as record_count
  from
 (select  count(*) as year_cnt  from  synpuf_100.achilles_results_derived
 where measure_id =  'Death:BornDeceasedRatio' and statistic_value > 30000) a
where a.year_cnt> 0;


--ruleid 40  this rule was under umbrella rule 1 and was made into a separate rule


-- insert into synpuf_100.achilles_heel_results (
-- 	analysis_id,
-- 	achilles_heel_warning,
-- 	rule_id,
-- 	record_count
-- 	)
-- select distinct  or1.analysis_id, concat(concat(concat(concat(concat(concat('ERROR: Death event outside observation period, ' , cast(or1.analysis_id  as string) ), '-' ), oa1.analysis_name ), '; count (n=' ), cast(or1.count_value  as string) ), ') should not be > 0' )as achilles_heel_warning, 40 as rule_id, or1.count_value
--  from  synpuf_100.achilles_results or1
-- inner join synpuf_100.achilles_analysis oa1
-- 	on or1.analysis_id = oa1.analysis_id
-- where or1.analysis_id in (510)
-- 	and or1.count_value > 0;


--ruleid 41 DQ rule, data density
--porting a Sentinel rule that checks for certain vital signs data (weight, in this case)
--multiple concepts_ids may be added to broaden the rule, however standardizing on a single
--concept would be more optimal

insert into synpuf_100.achilles_heel_results (achilles_heel_warning,rule_id)
select  'NOTIFICATION:No body weight data in MEASUREMENT table (under concept_id 3025315 (LOINC code 29463-7))'
 as achilles_heel_warning, 41 as rule_id
 from
(select  count(*) as row_present
  from  synpuf_100.achilles_results
 where analysis_id = 1800 and stratum_1 = '3025315'
) a
where a.row_present = 0


--ruleid 42 DQ rule
--Percentage of outpatient visits (concept_id 9202) is too low (for general population).
--This may indicate a dataset with mostly inpatient data (that may be biased and missing some EHR events)
--Threshold was decided as 10th percentile in empiric comparison of 12 real world datasets in the DQ-Study2

UNION ALL

-- insert into synpuf_100.achilles_heel_results (achilles_heel_warning,rule_id)
select  'NOTIFICATION: [GeneralPopulationOnly] Percentage of outpatient visits is below threshold'
 as achilles_heel_warning, 42 as rule_id
 from
 (
  select  1.0*count_value/(select  sum(count_value)  from  synpuf_100.achilles_results where analysis_id = 201)  as outp_perc
   from  synpuf_100.achilles_results where analysis_id = 201 and stratum_1='9202'
  ) d
where d.outp_perc < 0.43
UNION ALL

--ruleid 43 DQ rule
--looks at observation period data, if all patients have exactly one the rule alerts the user
--This rule is based on majority of real life datasets.
--For some datasets (e.g., UK national data with single payor, one observation period is perfectly valid)


-- insert into synpuf_100.achilles_heel_results (achilles_heel_warning,rule_id)
select  'NOTIFICATION: 99+ percent of persons have exactly one observation period'
 as achilles_heel_warning, 43 as rule_id
 from
 (select
	CASE
	 WHEN (select  count_value as total_pts  from  synpuf_100.achilles_results r where analysis_id =1) = 0
		 THEN NULL
	 ELSE
		100.0*count_value/(select  count_value as total_pts  from  synpuf_100.achilles_results r where analysis_id =1)
	END as one_obs_per_perc
  from  synpuf_100.achilles_results where analysis_id = 113 and stratum_1 = '1'
  ) d
where d.one_obs_per_perc >= 99.0



--ruleid 44 DQ rule
--uses iris measure: patients with at least 1 Meas, 1 Dx and 1 Rx

UNION ALL
-- insert into synpuf_100.achilles_heel_results (achilles_heel_warning,rule_id)
select  'NOTIFICATION: Percentage of patients with at least 1 Measurement, 1 Dx and 1 Rx is below threshold' as achilles_heel_warning, 44 as rule_id
 from  synpuf_100.achilles_results_derived d
where d.measure_id = 'ach_2002:Percentage'
and d.statistic_value < 20.5  --threshold identified in the DataQuality study
;

insert into synpuf_100.achilles_heel_results (
	analysis_id,
	achilles_heel_warning,
	rule_id,
	record_count
	)
select distinct  or1.analysis_id, concat(concat(concat(concat(concat(concat('NOTIFICATION: ' , cast(or1.analysis_id  as string) ), '-' ), oa1.analysis_name ), '; count (n=' ), cast(or1.count_value  as string) ), ') should not be > 0' )as achilles_heel_warning, 1 as rule_id, or1.count_value
 from  synpuf_100.achilles_results or1
inner join synpuf_100.achilles_analysis oa1
	on or1.analysis_id = oa1.analysis_id
where or1.analysis_id in (
		114, --Number of persons with observation period before year-of-birth
		115, --Number of persons with observation period end < observation period start
		118, --Number of observation periods with invalid person_id
		208, --Number of visit records outside valid observation period
		410, --Number of condition occurrence records outside valid observation period
		610, --Number of procedure occurrence records outside valid observation period
		710, --Number of drug exposure records outside valid observation period
		810, --Number of observation records outside valid observation period
		908, --Number of drug eras without valid person
		909, --Number of drug eras outside valid observation period
		910, --Number of drug eras with end date < start date
		1008, --Number of condition eras without valid person
		1009, --Number of condition eras outside valid observation period
		1010 --Number of condition eras with end date < start date
		) --all explicit counts of data anamolies
	and or1.count_value > 0

UNION ALL
--ruleid 2 distributions where min should not be negative
--insert into synpuf_100.achilles_heel_results (
--	analysis_id,
--	achilles_heel_warning,
--	rule_id,
--	record_count
--	)
select distinct  ord1.analysis_id, concat(concat(concat(concat(concat(concat('NOTIFICATION: ' , cast(ord1.analysis_id  as string) ), ' - ' ), oa1.analysis_name ), ' (count = ' ), cast(COUNT(ord1.min_value)  as string) ), '); min value should not be negative' )as achilles_heel_warning, 2 as rule_id, COUNT(ord1.min_value) as record_count
  from  synpuf_100.achilles_results_dist ord1
inner join synpuf_100.achilles_analysis oa1
	on ord1.analysis_id = oa1.analysis_id
where ord1.analysis_id in (
		103, --Distribution of age at first observation period
		105, --Length of observation (days) of first observation period
		907 --Distribution of drug era length, by drug_concept_id
		)
	and ord1.min_value < 0
	 group by  ord1.analysis_id, oa1.analysis_name

UNION ALL

select  or1.analysis_id, concat(concat(concat(concat(concat(concat('NOTIFICATION: ' , cast(or1.analysis_id  as string) ), '-' ), oa1.analysis_name ), '; ' ), cast(COUNT(distinct stratum_1)  as string) ), ' concepts in data are not in vocabulary' )as achilles_heel_warning, 4 as rule_id, COUNT(distinct stratum_1) as record_count
  from  synpuf_100.achilles_results or1
inner join synpuf_100.achilles_analysis oa1
	on or1.analysis_id = oa1.analysis_id
left join concept c1
	on or1.stratum_1 = cast(c1.concept_id  as string)
where or1.analysis_id in (
		301, --Number of providers by specialty concept_id
		900, --Number of persons with at least one drug era, by drug_concept_id
		1000 --Number of persons with at least one condition era, by condition_concept_id
		)
	and or1.stratum_1 is not null
	and c1.concept_id is null
 group by  or1.analysis_id, oa1.analysis_name
;
