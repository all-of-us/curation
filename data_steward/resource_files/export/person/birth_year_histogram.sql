select min(cast(ar1.stratum_1 as int64)) as min,
  max(cast(ar1.stratum_1 as int64)) as max,
	1 as interval_size,
  max(cast(ar1.stratum_1 as int64)) - min(cast(ar1.stratum_1 as int64)) as intervals
from `@results_database_schema.achilles_results` ar1
where ar1.analysis_id = 3
