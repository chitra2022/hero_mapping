import sys
import pandas as pd
import numpy as np
from google.cloud import bigquery
from google.cloud import storage
client = bigquery.Client()
import logging
import json
import os
from numpy import random
from datetime import datetime
pwd = os.getcwd()
with open(F"{pwd}/config/config.json") as file:
    config=json.load(file)
bq_project = config['audit_params']['bq_project']
bq_audit_dataset =  config['audit_params']['bq_audit_dataset']
bq_audit_table =  config['audit_params']['bq_audit_table']
dataset_ref = client.dataset(bq_audit_dataset, project=bq_project)
table_ref = dataset_ref.table(bq_audit_table)
table = client.get_table(table_ref)
dest_table = bq_project+'.'+bq_audit_dataset+'.'+bq_audit_table

def insert_audit_gcp(run_id,task_code,task_name,task_type,task_start_timestamp,task_status,error_message):
    insert_timestamp=datetime.now()
    update_timestamp=datetime.now()
    notification_sent_flag=''
    row_count=0
    processed_partition_name=''
    task_end_timestamp=datetime.now()
    lst=[run_id,task_code,task_name,task_type,task_start_timestamp,task_end_timestamp,task_status,processed_partition_name,row_count,error_message,notification_sent_flag,insert_timestamp,update_timestamp]
    cols=['run_id','task_code','task_name','task_type','task_start_timestamp','task_end_timestamp','task_status','processed_partition_name','row_count','error_message','notification_sent_flag','insert_timestamp','update_timestamp']
    df1=pd.DataFrame(lst).T
    df = pd.DataFrame(df1.values , columns = cols)
    job_config = bigquery.LoadJobConfig(schema=table.schema)
    job = client.load_table_from_dataframe(df, dest_table, job_config=job_config)
    job.result()

def update_audit_gcp(run_id,task_end_timestamp,task_status,processed_partition_name,row_count,error_message):
    update_timestamp=datetime.now()
    update_query = ("Update " + dest_table + " set  update_timestamp =" +"'"+str(update_timestamp)+"',task_end_timestamp = "+"'"+str(task_end_timestamp)+"' ,task_status = "+ "'"+task_status+"',processed_partition_name = "+"'"+str(processed_partition_name)+"',row_count = "+ str(row_count) + " ,error_message = "+ "'"+str(error_message)+ "' where run_id = " + str(run_id))
    logging.info("update_query --> "+ str(update_query))  
    query_job = client.query(update_query)
    query_job.result()

def get_runid(task_timestamp):
    sql ='SELECT FARM_FINGERPRINT(' + "'"+str(task_timestamp)+"'" + ') as run_id'
    df = client.query(sql).to_dataframe()
    run_id_obj = df['run_id']
    return int(run_id_obj[0])
