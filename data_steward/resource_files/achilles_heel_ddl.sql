CREATE TABLE achilles_heel_results (
  analysis_id int NOT NULLABLE,
	achilles_heel_warning varchar(255) NULLABLE,
	rule_id int NULLABLE,
	record_count bigint NULLABLE
);

CREATE TABLE achilles_results_derived
(
	analysis_id int NOT NULLABLE,
	stratum_1 varchar(255) NULLABLE,
	stratum_2 varchar(255) NULLABLE,
	statistic_value float NULLABLE,
	measure_id varchar(255) NULLABLE
);
