CREATE TABLE `apmena-onedata-dna-apac-qa.rdm_compass_pdt_stg.rdm_task_audit_control`
(
  run_id                   NUMERIC NOT NULL, 
  task_code                STRING NOT NULL,
  task_name                STRING NOT NULL,
  task_type                STRING NOT NULL,  
  task_start_timestamp     TIMESTAMP,
  task_end_timestamp       TIMESTAMP,
  task_status              STRING,
  processed_partition_name STRING,
  row_count                NUMERIC,
  error_message            STRING,
  notification_sent_flag   STRING,
  insert_timestamp         TIMESTAMP NOT NULL,
  update_timestamp         TIMESTAMP NOT NULL
)
OPTIONS(
  description="Audit table for GCP/MDS data syncs"
);
