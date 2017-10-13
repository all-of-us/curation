select analysis_id as AttributeName, achilles_heel_warning as AttributeValue
from @results_database_schema.achilles_heel_results
order by case when SUBSTR(achilles_heel_warning, 0, 5) = 'Error' then 1 else 2 end, analysis_id