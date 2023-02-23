import pyodbc
import sys
import pandas as pd
import numpy as np
from google.cloud import bigquery
from google.cloud import storage
client = bigquery.Client()
import sqlalchemy as sa
import urllib
import logging
import decimal
from numpy import random
import json
from datetime import datetime
import os
import logging as log
import google.cloud.logging as logging
from data_sync_mds_gcp import *
from utils.generic_utls import *
pwd = os.getcwd()
with open(F"{pwd}/config/config.json") as file:
    config=json.load(file)


#From MDS to GCP
def outbound(task_code,source_data_schema,target_data_schema,engine,task_name,run_id,sql_mapping_enabled_flag,sql_text): # from mds to gcp
    task_start_timestamp=datetime.now()
    task_end_timestamp=datetime.now()
    processed_time_sql=''
    processed_partition_name=''
    row_count=0    

    #BQ Table Properties:
    logging.info("outbound--> !!!")    
    project = config['gcp']['project']
    bq_outbound_dataset =  config['audit_params']['bq_outbound_dataset']    
    dataset_id = target_data_schema[0]
    table_id = target_data_schema[1]
    dataset_ref = client.dataset(dataset_id, project=project)
    table_ref = dataset_ref.table(table_id)
    table = client.get_table(table_ref)
    dest_table = project+'.'+dataset_id+'.'+table_id
    
    if str(sql_mapping_enabled_flag).lower()== 'y':
        sql = str(sql_text)
    else :
        sql_mds = "select COLUMN_NAME as column_name from "+mds_database+".INFORMATION_SCHEMA.COLUMNS where TABLE_NAME = " + "'" + source_data_schema[1] + "'"
        logging.info("outbound - sql_mds --> "+ str(sql_mds))

        info_schema_source_df = pd.read_sql(sql_mds, con=engine, index_col=None, coerce_float=True, params=None, parse_dates=None, columns=None, chunksize=None)
        info_schema_source_df['column_name2'] = info_schema_source_df['column_name'].str.lower()

        sql_gcp = "select column_name as column_name From "+"`"+project+"."+ bq_outbound_dataset+".INFORMATION_SCHEMA.COLUMNS` where table_name = " + "'" + target_data_schema[1] + "'" + " and column_name not like  ('%PARTITIONTIME%')"
        logging.info("outbound - sql_gcp --> "+ str(sql_gcp))      

        info_schema_target_df = client.query(sql_gcp).to_dataframe()
        info_schema_target_df['column_name2'] = info_schema_target_df['column_name'].str.lower()

        common_cols  = info_schema_source_df.set_index('column_name2').join(info_schema_target_df.set_index('column_name2'),lsuffix='_caller', rsuffix='_other', how='inner')
        col_lst = str(list(common_cols['column_name_caller']))
        logging.info("outbound - common_cols --> "+ str(common_cols))   
         
        for char in ['[',']','\'']:
            col_lst = col_lst.replace(char,"") # removing unwanted chars from col string.
   
        sql = "select " + col_lst + " from "+mds_database+"." + source_data_schema[0] + "." + source_data_schema[1]
        logging.info("outbound - sql --> "+ str(sql))        

    try:
        df = pd.read_sql(sql, con=engine, index_col=None, coerce_float=True, params=None, parse_dates=None, columns=None, chunksize=None)
        df.columns = map(str.lower, df.columns)
        logging.info("df--> df")             
        #logging.info(df)
         
        df['mds_table_name'] = str(source_data_schema[1])
        df['insert_timestamp'] =pd.Timestamp.now(tz='UTC')
        error_message=''
        row_count=df.shape[0]

        log.info("Table truncate Started "+ str(dest_table))
        #print(dest_table)
        client.query("truncate table "+dest_table)
        log.info("Table truncated "+ str(dest_table))

        job_config = bigquery.LoadJobConfig(schema=table.schema)
        job = client.load_table_from_dataframe(df, dest_table, job_config=job_config) # to gcp
        job.result()
                                
        #processed_time_sql = "SELECT  date(_PARTITIONTIME) as partition_name FROM " +project+'.'+ target_data_schema[0] + '.' + target_data_schema[1] + " WHERE  DATE(_PARTITIONTIME) = current_date() limit 1 "
        #processed_time_sql = "SELECT CASE WHEN COUNT(1) = 0 THEN '' ELSE FORMAT_DATETIME('%Y-%m-%d', CURRENT_DATE()) END AS partition_name FROM " +project+'.'+ target_data_schema[0] + '.' + target_data_schema[1] + " WHERE DATE(_PARTITIONTIME) = CURRENT_DATE() LIMIT 1 ";
        #df2= client.query(processed_time_sql).to_dataframe()     
        #processed_partition_name_obj = df2['partition_name']
        #processed_partition_name = processed_partition_name_obj.all()
        processed_partition_name=''
        task_status = 'Completed'
        task_end_timestamp=datetime.now()            
        update_audit_gcp(run_id,task_end_timestamp,task_status,processed_partition_name,row_count,error_message)
    except Exception as e:
        print("Error during Connecting to MDS Server")
        task_status = 'Failed'
        print(e)
        row_count=0
        #error_message=str(e)
        error_message='Task may be incorrectly configured !!, Please refer clound run log for more details !!'
        task_end_timestamp=datetime.now()
        update_audit_gcp(run_id,task_end_timestamp,task_status,processed_partition_name,row_count,error_message)
        raise Exception (str(e))
