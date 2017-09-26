CREATE TABLE achilles_analysis
(
	analysis_id int NOT NULLABLE,
	analysis_name varchar(255) NULLABLE,
	stratum_1_name varchar(255) NULLABLE,
	stratum_2_name varchar(255) NULLABLE,
	stratum_3_name varchar(255) NULLABLE,
	stratum_4_name varchar(255) NULLABLE,
	stratum_5_name varchar(255) NULLABLE
);

CREATE TABLE achilles_results
(
	analysis_id int NOT NULLABLE,
	stratum_1 varchar(255) NULLABLE,
	stratum_2 varchar(255) NULLABLE,
	stratum_3 varchar(255) NULLABLE,
	stratum_4 varchar(255) NULLABLE,
	stratum_5 varchar(255) NULLABLE,
	count_value bigint NULLABLE
);

CREATE TABLE achilles_results_dist
(
	analysis_id int NOT NULLABLE,
	stratum_1 varchar(255) NULLABLE,
	stratum_2 varchar(255) NULLABLE,
	stratum_3 varchar(255) NULLABLE,
	stratum_4 varchar(255) NULLABLE,
	stratum_5 varchar(255) NULLABLE,
	count_value bigint NULLABLE,
	min_value float NULLABLE,
	max_value float NULLABLE,
	avg_value float NULLABLE,
	stdev_value float NULLABLE,
	median_value float NULLABLE,
	p10_value float NULLABLE,
	p25_value float NULLABLE,
	p75_value float NULLABLE,
	p90_value float NULLABLE
);
