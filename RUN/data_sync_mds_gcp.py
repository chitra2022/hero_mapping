import pyodbc
import traceback
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
from utils.inbound_utls import *
from utils.outbound_utls import *
from utils.generic_utls import *
#Getting Connection Configs
pwd = os.getcwd()
with open(F"{pwd}/config/config.json") as file:
    config=json.load(file)

# # MDS properties
mds_server = config['mds']['mds_server']
mds_database = config['mds']['mds_database']
mds_username = config['mds']['mds_username']
mds_password = config['mds']['mds_password']

def config_mds(task_flow,task_name):
    sql = "Select ID,Name,Code,source_data_schema,target_data_schema,sql_mapping_enabled_flag,sql_text,active_flag from [mdm].[MDS_Integration_"+task_flow.capitalize()+"]"
    task_where= " WHERE upper(active_flag)='Y' "

    if task_name is not None:
        task_where= task_where+"and name="+ "'"+task_name+"';"
    else:
        task_where=task_where+";" 
        task_name = ''        

    sql=sql+task_where     

    print("Setting default values")
    error_message = None
    row_count=None
    task_start_timestamp=datetime.now()
    task_end_timestamp=datetime.now()      
    task_status='In Progress'
    processed_partition_name = None
    task_code = ''
    task_type=task_flow.lower()
    run_id = get_runid(datetime.now())
    print("Run ID generated --> ", run_id)    
    
    try:
        task_start_timestamp = datetime.now()
        print("MDS Connection statrted")        
        conn_params = urllib.parse.quote_plus('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+mds_server+';DATABASE='+mds_database+';UID='+mds_username+';PWD='+ mds_password)
        engine = sa.create_engine('mssql+pyodbc:///?odbc_connect={}'.format(conn_params), fast_executemany=True)
        config_df_full = pd.read_sql(sql, con=engine, index_col=None, coerce_float=True, params=None, parse_dates=None, columns=None, chunksize=None)
        print("MDS Connection Successful")

        if config_df_full.empty:
            insert_audit_gcp(run_id,task_code,task_name,task_type,task_start_timestamp,'Failed',"No Matching schema in BQ - MDS task may be inactive or incorrectly configured !! task_flow -->" + str(task_flow) +", task_name "+ str(task_name))
            logging.info("outbound - Failed !! --> " + str(task_flow) + str(task_name))   
            raise Exception("Task may be inactive or incorrectly configured !!, Please refer clound run log for more details !!") 

        #getting config data
        for row in range(config_df_full.shape[0]):
            config_df = config_df_full.iloc[[row]]
            source_data_schema_obj = config_df['source_data_schema'].str.split('.')
            target_data_schema_obj = config_df['target_data_schema'].str.split('.')
            task_code_obj =  config_df['Code']
            task_name_obj =  config_df['Name']
            active_flag_obj = config_df['active_flag']
            sql_mapping_enabled_flag_obj = config_df['sql_mapping_enabled_flag']
            sql_text_obj = config_df['sql_text']
            
            task_code = task_code_obj.all()
            task_name = task_name_obj.all()
            source_data_schema = source_data_schema_obj.all()
            target_data_schema = target_data_schema_obj.all()
            active_flag = active_flag_obj.all()
            sql_mapping_enabled_flag = sql_mapping_enabled_flag_obj.all()
            sql_text = sql_text_obj.all()

            run_id = get_runid(datetime.now())
            task_start_timestamp = datetime.now()            
            insert_audit_gcp(run_id,task_code,task_name,task_type,task_start_timestamp,task_status,error_message)

            print("Audit entry created")  
            if task_flow.lower() == 'inbound':
                inbound(task_code,source_data_schema,target_data_schema,engine,task_name,run_id,sql_mapping_enabled_flag,sql_text)
            elif task_flow.lower() == 'outbound':
                outbound(task_code,source_data_schema,target_data_schema,engine,task_name,run_id,sql_mapping_enabled_flag,sql_text)
            print("Integration task completed")  
    except:
        print("Error during Connecting to MDS Server")
        error_message=traceback.format_exc()
        task_end_timestamp=datetime.now()
        task_status = 'Failed'
        update_audit_gcp(run_id,task_end_timestamp,task_status,processed_partition_name,row_count,error_message)

    return 'integration finished successfully'
