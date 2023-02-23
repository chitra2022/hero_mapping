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

# MDS properties
mds_server = config['mds']['mds_server']
mds_database = config['mds']['mds_database']
mds_username = config['mds']['mds_username']
mds_password = config['mds']['mds_password']

# from GCP to MDS
def inbound(task_code,source_data_schema,target_data_schema,engine,task_name,run_id,sql_mapping_enabled_flag,sql_text): 
    #GCP Table Properties
    task_start_timestamp=datetime.now()
    processed_partition_name = ''
    row_count=0
    logging.info("inbound--> !!!")        
    project = config['gcp']['project']
    bq_inbound_dataset =  config['audit_params']['bq_inbound_dataset']
    dataset_id = source_data_schema[0]
    table_id = source_data_schema[1]
    dataset_ref = client.dataset(dataset_id, project=project)
    table_ref = dataset_ref.table(table_id)
    table = client.get_table(table_ref)
    source_table = project+'.'+dataset_id+'.'+table_id
    sql = ''

    if str(sql_mapping_enabled_flag).lower()== 'y':
        sql = str(sql_text)
    else :
        sql_mds = "select COLUMN_NAME as column_name from "+mds_database+".INFORMATION_SCHEMA.COLUMNS where TABLE_NAME = " + "'" + target_data_schema[1] + "'" 
        logging.info("inbound - sql_mds --> "+ str(sql_mds))                              
        
        info_schema_source_df = pd.read_sql(sql_mds, con=engine, index_col=None, coerce_float=True, params=None, parse_dates=None, columns=None, chunksize=None)
        info_schema_source_df['column_name2'] = info_schema_source_df['column_name'].str.lower()
        bq_audit_dataset =  config['audit_params']['bq_audit_dataset']
        sql_gcp = "select column_name as column_name From "+"`"+project+"."+ bq_inbound_dataset+".INFORMATION_SCHEMA.COLUMNS` where table_name = " + "'" + source_data_schema[1] + "'" + " and column_name not like  ('%PARTITIONTIME%')"
        logging.info("inbound - sql_gcp --> "+ str(sql_gcp))          
       
        info_schema_target_df = client.query(sql_gcp).to_dataframe()
        info_schema_target_df['column_name2'] = info_schema_target_df['column_name'].str.lower()

        common_cols  = info_schema_source_df.set_index('column_name2').join(info_schema_target_df.set_index('column_name2'),lsuffix='_caller', rsuffix='_other', how='inner')
        col_lst = str(list(common_cols['column_name_other']))
        logging.info("intbound - common_cols --> "+ str(common_cols))    
        
        for char in ['[',']','\'']:
            col_lst = col_lst.replace(char,"") # removing unwanted chars from col string.
        
        sql = "select " +  col_lst + " from "+ project+"." + source_data_schema[0] + "." + source_data_schema[1]
        logging.info("intbound - sql --> "+ str(sql))   

    try:
        df = client.query(sql).to_dataframe()
        df.insert(loc =0, column = 'ImportType',value=0)
        df.insert(loc =1, column = 'BatchTag',value=random.randint(100))
        row_count = df.shape[0]
        # print('Connection Started')
        conn_params1 = urllib.parse.quote_plus('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+mds_server+';DATABASE='+mds_database+';UID='+mds_username+';PWD='+ mds_password)
        engine1 = sa.create_engine('mssql+pyodbc:///?odbc_connect={}'.format(conn_params1), fast_executemany=False)
        df.to_sql(target_data_schema[1], schema=target_data_schema[0], con = engine1, if_exists='append', index=False, chunksize=2000)

        log.info("Table truncate Started "+ str(source_table))
        #print(source_table)
        client.query("truncate table "+source_table)
        log.info("Table truncated "+ str(source_table))

        error_message = ''
        task_end_timestamp=datetime.now()
        task_status = 'Completed'
        processed_partition_name = ''
        update_audit_gcp(run_id,task_end_timestamp,task_status,processed_partition_name,row_count,error_message)
    except Exception as e:
        print("Error during Connecting to MDS Server")
        print(e)
        #error_message = str(e)
        error_message='Task may be incorrectly configured !!, Please refer clound run log for more details !!'        
        task_status = 'Failed'
        row_count=0
        task_end_timestamp = datetime.now()
        update_audit_gcp(run_id,task_end_timestamp,task_status,processed_partition_name,row_count,error_message)
        raise Exception (str(e))
