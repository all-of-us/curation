IF NOT EXISTS (
SELECT  schema_name
FROM    information_schema.schemata
WHERE   schema_name = 'hpo_schema')

BEGIN
EXEC sp_executesql N'CREATE SCHEMA hpo_schema'
END