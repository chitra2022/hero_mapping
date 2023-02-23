--Inbound SQL
SELECT 
    original_vic_name AS name,
    mds_code AS code,
    market_code, 
    vic_name 
FROM `apmena-onedata-dna-apac-${GCP_ENV}.mds_stg_inbound.consumer_vic_mapping` 
ORDER BY mds_code;

-- Outbound SQL
SELECT 
    [Name] AS original_vic_name
    ,[Code] AS mds_code
    ,[market_code]
    ,[vic_name]
FROM {DATABASE}.[mdm].[consumer_vic_mapping];
